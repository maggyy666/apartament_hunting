# geo_processing.py  (poprawiona wersja)
import asyncio
import aiohttp
import pandas as pd
import json
import re
import time
import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List

INPUT_FILE   = "../data/otodom_results.csv"
OUTPUT_FILE  = "../data/oferty_geo.csv"
CACHE_FILE   = "../data/geocode_cache.json"

# Publiczny Nominatim – nie przekraczamy 1 rps (podnieś tylko dla prywatnego/komercyjnego)
MAX_RPS      = 1.0
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
CITY = "Kraków"
COUNTRY = "Polska"

# viewbox: left, top, right, bottom (lng, lat, lng, lat)
VIEWBOX = (19.77, 50.12, 20.11, 49.97)

# Negative cache TTL (2 dni)
NEG_TTL_SECONDS = 2 * 24 * 3600

KNOWN_DISTRICTS = {
    "Stare Miasto", "Grzegórzki", "Prądnik Czerwony", "Prądnik Biały", "Krowodrza",
    "Bronowice", "Zwierzyniec", "Dębniki", "Łagiewniki-Borek Fałęcki", "Swoszowice",
    "Podgórze Duchackie", "Bieżanów-Prokocim", "Podgórze", "Czyżyny", "Mistrzejowice",
    "Bieńczyce", "Wzgórza Krzesławickie", "Nowa Huta", "Kazimierz", "Zabłocie",
    "Płaszów", "Ruczaj", "Łobzów", "Kleparz", "Piasek", "Nowy Świat", "Salwator",
    "Olsza", "Azory", "Żabiniec", "Tonie", "Wola Duchacka", "Łęg", "Bielany",
    "Stradom", "Ludwinów", "Dąbie", "Borek Fałęcki", "Prokocim", "Kozłówek",
    "Przewóz", "Bieżanów", "Krowodrza Górka", "Dębniki"
}

# proste korekty literówek/odmian
CORRECTIONS = {
    "rondo grunwaldzie": "rondo Grunwaldzkie",
    "bulwary wiślane": "Bulwary Wiślane",
    "al.  29": "al. 29 Listopada",
}

def apply_corrections(s: str) -> str:
    k = re.sub(r"\s+", " ", s.strip().lower())
    return CORRECTIONS.get(k, s)

# ---------------- utils ----------------
def load_cache(path: Path) -> Dict[str, dict]:
    """Ładuje cache - obsługuje zarówno starą jak i nową strukturę."""
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:  # sprawdź czy plik nie jest pusty
                    cache = json.loads(content)
                    # Konwersja ze starej struktury (tuple) do nowej (dict)
                    converted = {}
                    for k, v in cache.items():
                        if isinstance(v, (list, tuple)) and len(v) >= 3:
                            # Stara struktura: (lat, lon, dz)
                            converted[k] = {
                                "lat": v[0], "lon": v[1], "dz": v[2],
                                "precision": "unknown", "ts": int(time.time())
                            }
                        elif isinstance(v, dict):
                            # Nowa struktura
                            converted[k] = v
                    return converted
        except (json.JSONDecodeError, ValueError) as e:
            print(f"⚠️ Błąd w pliku cache {path}: {e}")
            print("🔄 Tworzę nowy cache...")
    return {}

def save_cache(path: Path, cache: Dict[str, dict]):
    """Zapisuje cache w nowej strukturze."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def norm_key(address: str) -> str:
    """Stabilny klucz cache z normalizacją skrótów i odmian."""
    if not address:
        return ""
    s = address.strip()
    # usuń "ul." na początku
    s = re.sub(r"^ul\.\s+", "", s, flags=re.I)
    # zamień wielokrotne spacje
    s = re.sub(r"\s+", " ", s)
    # normalizacja prefiksów
    s = re.sub(r"\baleja\b|\balei\b", "al.", s, flags=re.I)
    s = re.sub(r"\bplac\b|\bplacu\b", "pl.", s, flags=re.I)
    s = re.sub(r"\bosiedle\b|\bosiedlu\b", "os.", s, flags=re.I)
    s = re.sub(r"\brynek\b|\brynku\b", "rynek", s, flags=re.I)
    # lowercase i spacje
    s = s.lower()
    s = re.sub(r"\bsw\.\b", "św.", s)
    return s

def has_housenumber(street_part: str) -> bool:
    return bool(re.search(r"\d", street_part or ""))

def infer_precision(item: dict) -> str:
    """Wyznacza precyzję na podstawie odpowiedzi Nominatim."""
    addrtype = (item.get("addresstype") or "").lower()
    cls = (item.get("class") or "").lower()
    typ = (item.get("type") or "").lower()
    if addrtype in {"house", "building"}:
        return "house"
    if addrtype in {"residential", "tertiary", "secondary", "primary", "service"} or cls == "highway":
        return "street"
    if cls == "place" and typ in {"neighbourhood","quarter","suburb","borough"}:
        return "area"
    if cls in {"amenity","tourism"}:
        return "poi"
    return "unknown"

def inside_viewbox(lat: float, lon: float) -> bool:
    """Sprawdza czy punkt leży w bounding box Krakowa."""
    left, top, right, bottom = VIEWBOX
    return (left <= lon <= right) and (bottom <= lat <= top)

def is_fresh_neg(entry: dict) -> bool:
    """Sprawdza czy negatywny cache jest świeży."""
    if not entry or entry.get("lat") is not None:
        return False
    ts = entry.get("ts")
    return ts and (time.time() - ts) < NEG_TTL_SECONDS

def make_cache_entry(item: dict, dz: Optional[str], method: str, bounded: Optional[bool], 
                    viewbox: bool, query: str) -> dict:
    """Tworzy wpis cache z metadanymi."""
    return {
        "lat": float(item["lat"]),
        "lon": float(item["lon"]),
        "dz": dz,
        "precision": infer_precision(item),
        "addresstype": item.get("addresstype"),
        "class": item.get("class"),
        "type": item.get("type"),
        "query": query,
        "method": method,
        "bounded": bool(bounded) if bounded is not None else None,
        "viewbox": bool(viewbox),
        "ts": int(time.time()),
    }

def split_street(address: str) -> str:
    if not address:
        return ""
    s = address.split(",")[0].strip()
    # lekka normalizacja prefiksów
    s = re.sub(r"\bos\.\b", "osiedle", s, flags=re.I)
    s = re.sub(r"\bal\.\b", "al.", s, flags=re.I)  # zostaw skrót al. (Nominatim ogarnia)
    s = re.sub(r"\bul\.\b", "", s, flags=re.I).strip()
    s = re.sub(r"\bpl\.\b", "pl.", s, flags=re.I)
    s = re.sub(r"\brynek\b", "Rynek", s, flags=re.I)
    return s

def try_deinflect_pl_word(w: str) -> List[str]:
    """Bardzo prosta próba zdjęcia odmiany tylko dla końcówek psujących wyniki.
       Zwraca listę alternatyw (oryginał + ewentualne warianty)."""
    out = [w]
    # Bagrowej -> Bagrowa
    if re.search(r"[a-ząęółśźćń]ej$", w, flags=re.I):
        out.append(re.sub(r"ej$", "a", w, flags=re.I))
    # ...iej -> ...ia  (Karmelickiej -> Karmelicka – dla przymiotników sprawdzi się "iej"->"ia" i "iej"->"a")
    if re.search(r"iej$", w, flags=re.I):
        out.append(re.sub(r"iej$", "ia", w, flags=re.I))
        out.append(re.sub(r"iej$", "a", w, flags=re.I))
    # ...iego -> ...i  (np. (Wielkiego -> Wielki) raczej niepotrzebne, ale nie szkodzi)
    if re.search(r"iego$", w, flags=re.I):
        out.append(re.sub(r"iego$", "i", w, flags=re.I))
    return list(dict.fromkeys(out))

def gen_street_variants(street: str) -> List[str]:
    """Dla 'ul. Bagrowej 6' zwróci m.in. 'Bagrowej 6' i 'Bagrowa 6'."""
    street = street.strip()
    parts = street.split()
    if not parts:
        return [street]
    # zdejmij 'ul.', 'al.', 'pl.' z przodu do analizy deklinacji ostatniego tokenu nazwy
    leading = []
    rest = parts[:]
    while rest and re.match(r"^(ul\.|al\.|pl\.|osiedle|rondo|rynek)$", rest[0], flags=re.I):
        leading.append(rest.pop(0))
    if not rest:
        return [street]

    # wyodrębnij segment „nazwa ulicy” vs numer
    # szukamy pierwszego tokenu z cyfrą – to początek numeru
    num_idx = None
    for i, t in enumerate(rest):
        if re.search(r"\d", t):
            num_idx = i
            break
    name_tokens = rest if num_idx is None else rest[:num_idx]
    suffix_tokens = [] if num_idx is None else rest[num_idx:]

    if not name_tokens:
        return [street]

    # odmiana tylko ostatniego tokenu nazwy
    last = name_tokens[-1]
    variants_last = try_deinflect_pl_word(last)
    variants = []
    for v in variants_last:
        new_name = name_tokens[:-1] + [v]
        candidate = " ".join(leading + new_name + suffix_tokens).strip()
        variants.append(candidate)

    # zawsze dodaj oryginał na początku
    variants = [street] + [v for v in variants if v != street]
    # usuń duplikaty z zachowaniem kolejności
    return list(dict.fromkeys(variants))

def pick_district_from_address(addr: dict) -> Optional[str]:
    for key in ("city_district", "suburb", "borough", "quarter", "neighbourhood"):
        val = addr.get(key)
        if not val:
            continue
        val_std = " ".join(w[:1].upper() + w[1:] for w in str(val).split())
        if val_std in KNOWN_DISTRICTS:
            return val_std
    return None

class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(0.0001, rps)
        self._last = 0.0
        self._lock = asyncio.Lock()
    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self.min_interval:
                await asyncio.sleep(self.min_interval - delta)
            self._last = time.monotonic()

# ---------------- geocoding core ----------------

async def fetch_json(session: aiohttp.ClientSession, params: dict, limiter: RateLimiter) -> list:
    await limiter.wait()
    headers = {"User-Agent": "OtodomScraper/1.0 (kontakt@example.com)", "Accept-Language": "pl"}
    async with session.get(NOMINATIM_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as resp:
        if resp.status == 429:
            # prosty backoff
            await asyncio.sleep(2.0)
            return await fetch_json(session, params, limiter)
        resp.raise_for_status()
        return await resp.json()

def _params_base(include_viewbox: bool, bounded: Optional[bool]):
    p = {
        "format":"json",
        "addressdetails":1,
        "limit":1,
        "countrycodes":"pl",
    }
    if include_viewbox:
        p["viewbox"] = f"{VIEWBOX[0]},{VIEWBOX[1]},{VIEWBOX[2]},{VIEWBOX[3]}"
    if bounded is not None:
        p["bounded"] = 1 if bounded else 0
    return p

async def try_structured(session, limiter, street: str,
                         include_viewbox: bool, bounded: Optional[bool]):
    p = _params_base(include_viewbox, bounded)
    p.update({"street": street, "city": CITY})
    data = await fetch_json(session, p, limiter)
    if data:
        it = data[0]
        dz = pick_district_from_address(it.get("address", {}))
        return it, dz  # zwracamy cały item + dzielnicę
    return None

async def try_q(session, limiter, q: str,
                include_viewbox: bool, bounded: Optional[bool]):
    p = _params_base(include_viewbox, bounded)
    p["q"] = q
    data = await fetch_json(session, p, limiter)
    if data:
        it = data[0]
        dz = pick_district_from_address(it.get("address", {}))
        return it, dz  # zwracamy cały item + dzielnicę
    return None

async def geocode_one(session: aiohttp.ClientSession, limiter: RateLimiter, raw_address: str, cache: dict
                     ) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    street = split_street(apply_corrections(raw_address))
    variants = gen_street_variants(street)
    with_number = has_housenumber(street)

    if with_number:
        # a) structured + viewbox + bounded=1 (najcelniej)
        for v in variants:
            result = await try_structured(session, limiter, v, True, True)
            if result:
                it, dz = result
                if inside_viewbox(float(it["lat"]), float(it["lon"])):
                    entry = make_cache_entry(it, dz, "structured", True, True, f"{v}, {CITY}")
                    cache[norm_key(raw_address)] = entry
                    return float(it["lat"]), float(it["lon"]), dz
        # b) q + viewbox + bounded=1
        for v in variants:
            result = await try_q(session, limiter, f"{v}, {CITY}", True, True)
            if result:
                it, dz = result
                if inside_viewbox(float(it["lat"]), float(it["lon"])):
                    entry = make_cache_entry(it, dz, "q", True, True, f"{v}, {CITY}")
                    cache[norm_key(raw_address)] = entry
                    return float(it["lat"]), float(it["lon"]), dz
        # c) structured + viewbox (bounded=0)
        for v in variants:
            result = await try_structured(session, limiter, v, True, False)
            if result:
                it, dz = result
                if inside_viewbox(float(it["lat"]), float(it["lon"])):
                    entry = make_cache_entry(it, dz, "structured", False, True, f"{v}, {CITY}")
                    cache[norm_key(raw_address)] = entry
                    return float(it["lat"]), float(it["lon"]), dz
        # d) q bez viewboxu (globalne)
        for v in variants:
            result = await try_q(session, limiter, f"{v}, {CITY}", False, None)
            if result:
                it, dz = result
                if inside_viewbox(float(it["lat"]), float(it["lon"])):
                    entry = make_cache_entry(it, dz, "q", None, False, f"{v}, {CITY}")
                    cache[norm_key(raw_address)] = entry
                    return float(it["lat"]), float(it["lon"]), dz
        # Negatywny cache
        cache[norm_key(raw_address)] = {"lat": None, "lon": None, "dz": None, "ts": int(time.time())}
        return None, None, None

    # bez numeru:
    # a) q + viewbox (bounded=0) – bias na Kraków
    for v in variants:
        result = await try_q(session, limiter, f"{v}, {CITY}", True, False)
        if result:
            it, dz = result
            if inside_viewbox(float(it["lat"]), float(it["lon"])):
                entry = make_cache_entry(it, dz, "q", False, True, f"{v}, {CITY}")
                cache[norm_key(raw_address)] = entry
                return float(it["lat"]), float(it["lon"]), dz
    # b) q bez viewboxu – globalnie
    for v in variants:
        result = await try_q(session, limiter, f"{v}, {CITY}", False, None)
        if result:
            it, dz = result
            if inside_viewbox(float(it["lat"]), float(it["lon"])):
                entry = make_cache_entry(it, dz, "q", None, False, f"{v}, {CITY}")
                cache[norm_key(raw_address)] = entry
                return float(it["lat"]), float(it["lon"]), dz
    # c) structured + viewbox (bounded=0)
    for v in variants:
        result = await try_structured(session, limiter, v, True, False)
        if result:
            it, dz = result
            if inside_viewbox(float(it["lat"]), float(it["lon"])):
                entry = make_cache_entry(it, dz, "structured", False, True, f"{v}, {CITY}")
                cache[norm_key(raw_address)] = entry
                return float(it["lat"]), float(it["lon"]), dz

    # Negatywny cache
    cache[norm_key(raw_address)] = {"lat": None, "lon": None, "dz": None, "ts": int(time.time())}
    return None, None, None

# ---------------- main ----------------
async def run():
    print(f"🚀 Geo Processing - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Input: {INPUT_FILE}")
    print(f"📁 Output: {OUTPUT_FILE}")
    print(f"📁 Cache: {CACHE_FILE}")
    
    in_path = Path(INPUT_FILE)
    out_path = Path(OUTPUT_FILE)
    cache_path = Path(CACHE_FILE)

    print(f"\n📖 Wczytuję dane z {in_path}...")
    df = pd.read_csv(in_path)
    print(f"✅ Wczytano {len(df)} rekordów")
    for col in ("lat", "lon", "dzielnica", "metraz_m2"):
        if col not in df.columns:
            df[col] = None

    # przygotuj listę unikalnych adresów, dla których brakuje geo
    todo = []
    for _, row in df.iterrows():
        if pd.notna(row.get("lat")) and pd.notna(row.get("lon")) and pd.notna(row.get("dzielnica")):
            continue
        addr = str(row["ulica"]).strip()
        if addr:
            todo.append(addr)
    unique = list(dict.fromkeys(todo))  # zachowaj kolejność
    print(f"🔎 Do geokodowania unikalnych adresów: {len(unique)}")

    cache = load_cache(cache_path)
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[str]]] = {}
    need_fetch = []

    def _is_valid_cached(entry: dict, require_house: bool) -> bool:
        """Sprawdza czy wpis cache jest ważny z uwzględnieniem precyzji."""
        if not entry:
            return False
        lat, lon = entry.get("lat"), entry.get("lon")
        if lat is None or lon is None:
            return False
        prec = entry.get("precision") or "unknown"
        if require_house:
            # akceptuj tylko 'house' (lub ewentualnie 'street', jeśli chcesz łagodniej)
            return prec in {"house", "street"}
        # bez numeru – street/area ok
        return prec in {"house", "street", "area"}

    for a in unique:
        k = norm_key(a)
        e = cache.get(k)
        with_number = has_housenumber(a)
        
        # Sprawdź negatywny cache
        if is_fresh_neg(e):
            print(f"⏭️ Pomijam {a} (negatywny cache)")
            continue
            
        if _is_valid_cached(e, require_house=with_number):
            results[a] = (e["lat"], e["lon"], e.get("dz"))
        else:
            need_fetch.append(a)
    print(f"⚡ Z cache: {len(results)} | Do pobrania: {len(need_fetch)}")

    if need_fetch:
        print(f"\n🚀 Rozpoczynam geokodowanie {len(need_fetch)} adresów...")
        start_time = time.time()
        
        limiter = RateLimiter(MAX_RPS)
        timeout = aiohttp.ClientTimeout(total=30)
        conn = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        
        # Licznik postępu
        processed = 0
        total_to_process = len(need_fetch)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
            sem = asyncio.Semaphore(10)
            
            async def worker(addr: str):
                nonlocal processed
                async with sem:
                    lat, lon, dz = await geocode_one(session, limiter, addr, cache)
                    results[addr] = (lat, lon, dz)
                    
                    processed += 1
                    
                    # Print co 10 rekordów
                    if processed % 10 == 0:
                        elapsed = time.time() - start_time
                        avg_time_per_record = elapsed / processed
                        remaining = total_to_process - processed
                        eta_seconds = remaining * avg_time_per_record
                        eta = datetime.datetime.now() + datetime.timedelta(seconds=eta_seconds)
                        
                        print(f"[{processed}/{total_to_process}] ⏱️ {elapsed:.1f}s | "
                              f"Średnio: {avg_time_per_record:.1f}s/rekord | "
                              f"ETA: {eta.strftime('%H:%M:%S')} | "
                              f"→ {addr[:30]}{'...' if len(addr) > 30 else ''} → {lat},{lon} | {dz}")
                    
                    # Zapisz cache co 20 rekordów
                    if len(cache) % 20 == 0:
                        save_cache(cache_path, cache)
                        print(f"💾 Cache zapisany ({len(cache)} rekordów)")
            
            await asyncio.gather(*[worker(a) for a in need_fetch])
        
        total_time = time.time() - start_time
        print(f"\n✅ Geokodowanie zakończone w {total_time:.1f}s")
        print(f"📊 Średni czas na rekord: {total_time/len(need_fetch):.1f}s")
        save_cache(cache_path, cache)

    # uzupełnij DF tylko tam, gdzie braki
    print(f"\n🔄 Uzupełniam dane w DataFrame...")
    start_fill = time.time()
    
    def fill_row(row):
        if pd.isna(row.get("lat")) or pd.isna(row.get("lon")) or pd.isna(row.get("dzielnica")):
            addr = str(row["ulica"]).strip()
            if addr in results:
                lat, lon, dz = results[addr]
                if pd.isna(row.get("lat")): row["lat"] = lat
                if pd.isna(row.get("lon")): row["lon"] = lon
                if pd.isna(row.get("dzielnica")): row["dzielnica"] = dz
        return row

    df = df.apply(fill_row, axis=1)
    
    fill_time = time.time() - start_fill
    print(f"✅ Uzupełnianie zakończone w {fill_time:.1f}s")
    
    print(f"\n💾 Zapisuję do {out_path}...")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"✅ Zapisano do {out_path}")
    
    # Statystyki
    total_rows = len(df)
    with_coords = len(df[df['lat'].notna() & df['lon'].notna()])
    with_district = len(df[df['dzielnica'].notna()])
    with_rent = len(df[df['najem_pln'].notna()])
    with_admin = len(df[df['czynsz_adm_pln'].notna()])
    with_area = len(df[df['metraz_m2'].notna()])
    
    print(f"\n📊 STATYSTYKI:")
    print(f"   • Wszystkich ogłoszeń: {total_rows}")
    print(f"   • Z współrzędnymi (lat/lon): {with_coords} ({with_coords/total_rows*100:.1f}%)")
    print(f"   • Z dzielnicą: {with_district} ({with_district/total_rows*100:.1f}%)")
    print(f"   • Z ceną najmu: {with_rent} ({with_rent/total_rows*100:.1f}%)")
    print(f"   • Z czynszem administracyjnym: {with_admin} ({with_admin/total_rows*100:.1f}%)")
    print(f"   • Z metrażem: {with_area} ({with_area/total_rows*100:.1f}%)")
    
    # Statystyki metrażu
    if with_area > 0:
        areas = df[df['metraz_m2'].notna()]['metraz_m2']
        avg_area = areas.mean()
        min_area = areas.min()
        max_area = areas.max()
        print(f"\n📏 STATYSTYKI METRAŻU:")
        print(f"   • Średni metraż: {avg_area:.1f} m²")
        print(f"   • Najmniejszy: {min_area:.1f} m²")
        print(f"   • Największy: {max_area:.1f} m²")
    
    # Top dzielnice
    if with_district > 0:
        district_counts = df['dzielnica'].value_counts().head(10)
        print(f"\n🏘️  TOP 10 DZIELNIC:")
        for district, count in district_counts.items():
            print(f"   • {district}: {count} ogłoszeń")

if __name__ == "__main__":
    asyncio.run(run())
