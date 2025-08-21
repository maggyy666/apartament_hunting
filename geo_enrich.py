#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Geo Enricher - Parsuje ulice z lokalizacja_raw i dodaje dzielnice + współrzędne
Usprawnienia:
- Structured search do Nominatim (street/city/countrycodes).
- Retry + backoff na 429/5xx/timeout.
- Cache (geo_cache.json) by nie duplikować zapytań.
"""

import csv
import json
import os
import re
import time
from typing import Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry


CACHE_PATH = "geo_cache.json"

# Kraków viewbox (left, top, right, bottom) - lekki margines
KRAKOW_VIEWBOX = (19.73, 50.12, 20.17, 49.98)

# Mapowanie skrótów na pełne nazwy
ABBR_REPLACEMENTS = [
    (r"\bgen\.?\b", "Generała"),
    (r"\bdr\.?\b", "Doktora"),
    (r"\bśw\.?\b", "Świętego"),
    (r"\bbl\.?\b", "Błogosławionego"),
    (r"\bks\.?\b", "Księdza"),
]

def strip_street_prefix(street: str) -> str:
    """Usuwa prefiksy ulic (ul., al., pl., os., itp.)"""
    s = street.strip()
    # usuń ul./al./pl./os./rondo/rynek (z kropką lub bez)
    s = re.sub(r"^\s*(ul\.?|ulica|al\.?|aleja|pl\.?|plac|os\.?|osiedle|rondo|rynek)\s+", "", s, flags=re.I)
    return s.strip()

def expand_abbreviations(s: str) -> str:
    """Rozwija skróty w nazwach ulic i poprawia case."""
    out = s
    for pat, repl in ABBR_REPLACEMENTS:
        out = re.sub(pat, repl, out, flags=re.I)
    # poprawiaj case: 'Świętego Jana' itp.
    out = " ".join(w[0].upper()+w[1:] if w else w for w in out.split())
    out = re.sub(r"\bIi\b", "II", out)
    out = re.sub(r"\bIii\b", "III", out)
    out = re.sub(r"\bIv\b", "IV", out)
    return out.strip()

# Mapowanie osiedle → dzielnica (oficjalny podział UMK)
SUBURB_TO_DISTRICT = {
    # Dębniki
    "Skotniki": "Dębniki",
    "Ruczaj": "Dębniki", 
    "Kobierzyn": "Dębniki",
    "Bodzów": "Dębniki",
    "Kostrze": "Dębniki",
    "Tyniec": "Dębniki",
    "Pychowice": "Dębniki",
    "Sidzina": "Dębniki",
    
    # Prądnik Biały
    "Azory": "Prądnik Biały",
    "Tonie": "Prądnik Biały",
    "Żabiniec": "Prądnik Biały",
    "Górka Narodowa": "Prądnik Biały",
    "Witkowice": "Prądnik Biały",
    "Prądnik Biały": "Prądnik Biały",
    
    # Podgórze
    "Stare Podgórze": "Podgórze",
    "Podgórze": "Podgórze",
    "Rybitwy": "Podgórze",
    "Płaszów": "Podgórze",
    "Zabłocie": "Podgórze",
    
    # Podgórze Duchackie
    "Wola Duchacka": "Podgórze Duchackie",
    "Łagiewniki": "Podgórze Duchackie",
    "Borek Fałęcki": "Podgórze Duchackie",
    "Łagiewniki-Borek Fałęcki": "Podgórze Duchackie",
    
    # Zwierzyniec
    "Salwator": "Zwierzyniec",
    "Zwierzyniec": "Zwierzyniec",
    "Bielany": "Zwierzyniec",
    "Chełm": "Zwierzyniec",
    "Olszanica": "Zwierzyniec",
    "Półwsie Zwierzynieckie": "Zwierzyniec",
    
    # Stare Miasto
    "Kazimierz": "Stare Miasto",
    "Stare Miasto": "Stare Miasto",
    "Kleparz": "Stare Miasto",
    "Nowy Świat": "Stare Miasto",
    "Piasek": "Stare Miasto",
    "Wesoła": "Stare Miasto",
    "Wesoła Wschód": "Stare Miasto",
    "Wesoła Zachód": "Stare Miasto",
    "Przedmieście Warszawskie": "Stare Miasto",
    "Przedmieście Rakowickie": "Stare Miasto",
    
    # Grzegórzki
    "Grzegórzki": "Grzegórzki",
    "Dąbie": "Grzegórzki",
    "Olsza": "Grzegórzki",
    "Olsza II": "Grzegórzki",
    "Wzgórza Krzesławickie": "Grzegórzki",
    
    # Krowodrza
    "Krowodrza": "Krowodrza",
    "Nowa Wieś": "Krowodrza",
    "Łobzów": "Krowodrza",
    "Bronowice": "Krowodrza",
    "Bronowice Małe": "Krowodrza",
    "Bronowice Wielkie": "Krowodrza",
    "Mydlniki": "Krowodrza",
    "Mydlniki Wsi": "Krowodrza",
    "Batowice": "Krowodrza",
    "Górka Narodowa": "Krowodrza",
    
    # Mistrzejowice
    "Mistrzejowice": "Mistrzejowice",
    "Piastów": "Mistrzejowice",
    "Oś. Piastów": "Mistrzejowice",
    "os. Piastów": "Mistrzejowice",
    "Złoty Róg": "Mistrzejowice",
    "Oś. Złoty Róg": "Mistrzejowice",
    "os. Złoty Róg": "Mistrzejowice",
    
    # Bieńczyce
    "Bieńczyce": "Bieńczyce",
    "Oś. Albertyńskie": "Bieńczyce",
    "os. Albertyńskie": "Bieńczyce",
    "Oś. Jagiellońskie": "Bieńczyce", 
    "os. Jagiellońskie": "Bieńczyce",
    "Oś. Kalinowe": "Bieńczyce",
    "os. Kalinowe": "Bieńczyce",
    "Oś. Kazimierzowskie": "Bieńczyce",
    "os. Kazimierzowskie": "Bieńczyce",
    "Oś. Niepodległości": "Bieńczyce",
    "os. Niepodległości": "Bieńczyce",
    "Oś. Przy Arce": "Bieńczyce",
    "os. Przy Arce": "Bieńczyce",
    "Oś. Strusia": "Bieńczyce",
    "os. Strusia": "Bieńczyce",
    "Oś. Wysokie": "Bieńczyce",
    "os. Wysokie": "Bieńczyce",
    
    # Czyżyny
    "Czyżyny": "Czyżyny",
    "Oś. 2 Pułku Lotniczego": "Czyżyny",
    "os. 2 Pułku Lotniczego": "Czyżyny",
    "Oś. Akademickie": "Czyżyny",
    "os. Akademickie": "Czyżyny",
    "Oś. Dywizjonu 303": "Czyżyny",
    "os. Dywizjonu 303": "Czyżyny",
    "Oś. Jagiellońskie": "Czyżyny",
    "os. Jagiellońskie": "Czyżyny",
    "Oś. Kolorowe": "Czyżyny",
    "os. Kolorowe": "Czyżyny",
    "Oś. Młodości": "Czyżyny",
    "os. Młodości": "Czyżyny",
    "Oś. Na Skarpie": "Czyżyny",
    "os. Na Skarpie": "Czyżyny",
    "Oś. Orła Białego": "Czyżyny",
    "os. Orła Białego": "Czyżyny",
    "Oś. Spółdzielców": "Czyżyny",
    "os. Spółdzielców": "Czyżyny",
    "Oś. Urocze": "Czyżyny",
    "os. Urocze": "Czyżyny",
    "Oś. Zgody": "Czyżyny",
    "os. Zgody": "Czyżyny",
    
    # Nowa Huta
    "Nowa Huta": "Nowa Huta",
    "Oś. Centrum A": "Nowa Huta",
    "os. Centrum A": "Nowa Huta",
    "Oś. Centrum B": "Nowa Huta", 
    "os. Centrum B": "Nowa Huta",
    "Oś. Centrum C": "Nowa Huta",
    "os. Centrum C": "Nowa Huta",
    "Oś. Centrum D": "Nowa Huta",
    "os. Centrum D": "Nowa Huta",
    "Oś. Centrum E": "Nowa Huta",
    "os. Centrum E": "Nowa Huta",
    "Oś. Górali": "Nowa Huta",
    "os. Górali": "Nowa Huta",
    "Oś. Handlowe": "Nowa Huta",
    "os. Handlowe": "Nowa Huta",
    "Oś. Hutnicze": "Nowa Huta",
    "os. Hutnicze": "Nowa Huta",
    "Oś. Kolorowe": "Nowa Huta",
    "os. Kolorowe": "Nowa Huta",
    "Oś. Krakowiaków": "Nowa Huta",
    "os. Krakowiaków": "Nowa Huta",
    "Oś. Na Lotnisku": "Nowa Huta",
    "os. Na Lotnisku": "Nowa Huta",
    "Oś. Na Skarpie": "Nowa Huta",
    "os. Na Skarpie": "Nowa Huta",
    "Oś. Ogrodowe": "Nowa Huta",
    "os. Ogrodowe": "Nowa Huta",
    "Oś. Słoneczne": "Nowa Huta",
    "os. Słoneczne": "Nowa Huta",
    "Oś. Sportowe": "Nowa Huta",
    "os. Sportowe": "Nowa Huta",
    "Oś. Stalowe": "Nowa Huta",
    "os. Stalowe": "Nowa Huta",
    "Oś. Szkolne": "Nowa Huta",
    "os. Szkolne": "Nowa Huta",
    "Oś. Teatralne": "Nowa Huta",
    "os. Teatralne": "Nowa Huta",
    "Oś. Urocze": "Nowa Huta",
    "os. Urocze": "Nowa Huta",
    "Oś. Wandy": "Nowa Huta",
    "os. Wandy": "Nowa Huta",
    "Oś. Willowe": "Nowa Huta",
    "os. Willowe": "Nowa Huta",
    "Oś. Zgody": "Nowa Huta",
    "os. Zgody": "Nowa Huta",
    "Oś. Zielone": "Nowa Huta",
    "os. Zielone": "Nowa Huta",
}


class GeoEnricher:
    def __init__(self, delay: float = 1.0, email: str = "twoj-email@domena.pl", dry_run: bool = False):
        """
        Args:
            delay: Opóźnienie (sekundy) między zapytaniami do API
            email: e-mail w UA (wymóg uprzejmości Nominatim)
        """
        self.delay = delay
        self.session = requests.Session()
        # Retry + backoff dla błędów sieci/serwera
        retries = Retry(
            total=2,               # mniej retry
            backoff_factor=0.5,    # krótszy backoff
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({
            "User-Agent": f"otodom-scraper/1.0 ({email})",
            "Accept-Language": "pl"
        })
        self.cache: Dict[str, Dict[str, Optional[str]]] = self._load_cache()
        self.dry_run = dry_run

    # ---------- CACHE ----------

    def _load_cache(self) -> Dict[str, Dict[str, Optional[str]]]:
        if os.path.exists(CACHE_PATH):
            try:
                with open(CACHE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self) -> None:
        try:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ---------- PARSING ----------

    def _normalize_prefix(self, prefix: str) -> str:
        p = prefix.lower().strip().rstrip(".")
        mapping = {
            "ul": "ul.",
            "ulica": "ul.",
            "al": "al.",
            "aleja": "al.",
            "pl": "pl.",
            "plac": "pl.",
            "os": "os.",
            "osiedle": "os.",
            "rondo": "rondo",
            "rynek": "rynek",
        }
        return mapping.get(p, prefix)

    def _titlecase(self, s: str) -> str:
        # proste titlecase z polskimi literami + poprawki
        t = s.strip()
        t = " ".join(w[0].upper() + w[1:] if w else w for w in t.split())
        # zamień „Ii/Iii/Iv" na rzymskie wielkie
        t = re.sub(r"\bIi\b", "II", t)
        t = re.sub(r"\bIii\b", "III", t)
        t = re.sub(r"\bIv\b", "IV", t)
        # św. itp.
        t = re.sub(r"\bSw\.?\b", "Św.", t, flags=re.I)
        t = re.sub(r"\bJana Pawla\b", "Jana Pawła", t, flags=re.I)
        return t

    def parse_street_from_location(self, location_raw: str) -> Tuple[Optional[str], str]:
        """
        Parsuje ulicę z surowego tekstu lokalizacji.
        Zwraca (ulica, parse_quality) gdzie parse_quality in {"strict","fallback","none"}.
        """
        if not location_raw:
            return None, "none"

        # wyczyść ogon „, Kraków, małopolskie"
        location_clean = re.sub(
            r",\s*Kraków\s*,\s*małopolskie\s*$",
            "",
            location_raw,
            flags=re.I
        ).strip()

        # 1) Prefiksy: ul./al./pl./os./rondo/rynek
        patterns = [
            r"\b(ul\.?|ulica)\s+([^,]+)",
            r"\b(al\.?|aleja)\s+([^,]+)",
            r"\b(pl\.?|plac)\s+([^,]+)",
            r"\b(os\.?|osiedle)\s+([^,]+)",
            r"\b(rondo)\s+([^,]+)",
            r"\b(rynek)\s+([^,]+)",
        ]
        for pat in patterns:
            m = re.search(pat, location_clean, flags=re.I)
            if m:
                prefix = self._normalize_prefix(m.group(1))
                name = self._titlecase(m.group(2))
                street = f"{prefix} {name}".strip()
                return street, "strict"

        # 2) Fallback: pierwszy token, który nie wygląda jak „dzielnica/obszar"
        parts = [p.strip() for p in location_clean.split(",") if p.strip()]
        bad_tokens = {
            "stare miasto", "kazimierz", "zabłocie", "płaszów", "ruczaj", "dębniki",
            "krowodrza", "grzegórzki", "podgórze", "zwierzyniec", "prądnik biały",
            "prądnik czerwony", "mistrzejowice", "nowa huta", "bieńczyce", "czyżyny",
            "łagiewniki", "salwator", "azory", "żabiniec", "kleparz", "bonarka",
            "oficerskie", "stare podgórze", "tonie"
        }
        for token in parts:
            t = token.lower()
            if t not in bad_tokens:
                # Jeśli to wygląda na „Nazwa 123" (ulica bez prefiksu) – też bierzemy
                if re.search(r"\d", t) or len(t) >= 3:
                    return self._titlecase(token), "fallback"

        return None, "none"

    # ---------- GEOCODING ----------

    def geocode(self, street_or_area: str) -> Dict[str, Optional[str]]:
        """
        Geokoduje nazwę w Krakowie, próbując kilku wariantów i strategii.
        Multi-strategy: structured search → free-text search → fallback raw.
        """
        if not street_or_area:
            return {"dzielnica": None, "lat": None, "lon": None, "status": "empty_address"}

        key = street_or_area.strip().lower()
        if key in self.cache:
            return self.cache[key]

        url = "https://nominatim.openstreetmap.org/search"

        # Zbuduj warianty nazwy ulicy
        variants = []
        base = street_or_area.strip()
        no_prefix = strip_street_prefix(base)
        expanded = expand_abbreviations(no_prefix)
        
        # Dodaj wariant z prefixem + rozwinięciem
        if no_prefix != base:
            prefix = base.split()[0] if base.split() else ""
            expanded_with_prefix = f"{prefix} {expanded}" if expanded != no_prefix else base
        else:
            expanded_with_prefix = expanded

        # Wytnij nr domu (niepotrzebny do geokodowania ulicy)
        no_num = re.sub(r"\s+\d+[A-Za-z]?$", "", no_prefix).strip()
        tokens = no_num.split()

        extra_variants = []
        if tokens:
            # 1) tylko ostatni token (np. "Radzikowskiego")
            extra_variants.append(tokens[-1])
            # 2) ostatnie dwa tokeny (np. "Walerego Radzikowskiego")
            if len(tokens) >= 2:
                extra_variants.append(" ".join(tokens[-2:]))

            # 3) "Nazwisko Imiona" (zamiana kolejności – czasem tak jest w OSM)
            if len(tokens) >= 2:
                last = tokens[-1]
                rest = " ".join(tokens[:-1])
                extra_variants.append(f"{last} {rest}")

        # Sklej unikalne warianty w finalnej kolejności prób
        variants = [
            base,                    # "ul. Bolesława Komorowskiego"
            no_prefix,               # "Bolesława Komorowskiego" 
            expanded,                # "Generała Bolesława Komorowskiego"
            expanded_with_prefix,    # "ul. Generała Bolesława Komorowskiego"
            *extra_variants
        ]
        # deduplikacja z zachowaniem kolejności
        seen = set()
        unique_variants = []
        for v in variants:
            if v and v.lower() not in seen:
                seen.add(v.lower())
                unique_variants.append(v)

        left, top, right, bottom = KRAKOW_VIEWBOX

        # STRATEGIA 1: Structured search dla wszystkich wariantów
        print(f"    🔍 Próbuję structured search: {unique_variants}")
        for variant in unique_variants:
            try:
                params = {
                    "street": variant,
                    "city": "Kraków",
                    "countrycodes": "pl",
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                    "viewbox": f"{left},{top},{right},{bottom}",
                }
                resp = self.session.get(url, params=params, timeout=8)
                resp.raise_for_status()
                data = resp.json()
                if data:
                    hit = data[0]
                    addr = hit.get("address", {}) or {}
                    dzielnica = (
                        addr.get("city_district") or
                        SUBURB_TO_DISTRICT.get(addr.get("suburb")) or
                        addr.get("quarter") or
                        addr.get("neighbourhood") or
                        addr.get("suburb")
                    )
                    result = {
                        "dzielnica": dzielnica,
                        "lat": hit.get("lat"),
                        "lon": hit.get("lon"),
                        "status": "success",
                        "variant_used": f"structured_{variant}"
                    }
                    self.cache[key] = result
                    return result
            except requests.exceptions.RequestException:
                pass
            except Exception:
                pass

        # STRATEGIA 2: Free-text search dla wszystkich wariantów (bez bounded=1)
        print(f"    🔍 Próbuję free-text search: {unique_variants}")
        for variant in unique_variants:
            try:
                q = f"{variant}, Kraków, Polska"
                params = {
                    "q": q,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                    "countrycodes": "pl",
                    "viewbox": f"{left},{top},{right},{bottom}",
                }
                resp = self.session.get(url, params=params, timeout=8)
                resp.raise_for_status()
                data = resp.json()
                if data:
                    hit = data[0]
                    addr = hit.get("address", {}) or {}
                    dzielnica = (
                        addr.get("city_district") or
                        SUBURB_TO_DISTRICT.get(addr.get("suburb")) or
                        addr.get("quarter") or
                        addr.get("neighbourhood") or
                        addr.get("suburb")
                    )
                    result = {
                        "dzielnica": dzielnica,
                        "lat": hit.get("lat"),
                        "lon": hit.get("lon"),
                        "status": "success",
                        "variant_used": f"freetext_{variant}"
                    }
                    self.cache[key] = result
                    return result
            except requests.exceptions.RequestException:
                pass
            except Exception:
                pass

        # STRATEGIA 3: Fallback - geokoduj surową lokalizację (dla obszarów)
        print(f"    🔍 Próbuję fallback raw: {street_or_area}")
        try:
            q = f"{street_or_area}, Kraków, Polska"
            params = {
                "q": q,
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
                "countrycodes": "pl",
                "viewbox": f"{left},{top},{right},{bottom}",
            }
            resp = self.session.get(url, params=params, timeout=8)
            resp.raise_for_status()
            data = resp.json()
            if data:
                hit = data[0]
                addr = hit.get("address", {}) or {}
                dzielnica = (
                    addr.get("city_district") or
                    SUBURB_TO_DISTRICT.get(addr.get("suburb")) or
                    addr.get("quarter") or
                    addr.get("neighbourhood") or
                    addr.get("suburb")
                )
                result = {
                    "dzielnica": dzielnica,
                    "lat": hit.get("lat"),
                    "lon": hit.get("lon"),
                    "status": "success",
                    "variant_used": "fallback_raw"
                }
                self.cache[key] = result
                return result
        except requests.exceptions.RequestException as e:
            result = {"dzielnica": None, "lat": None, "lon": None, "status": f"api_error: {e.__class__.__name__}", "variant_used": "none"}
            self.cache[key] = result
            return result
        except Exception as e:
            result = {"dzielnica": None, "lat": None, "lon": None, "status": f"error: {e.__class__.__name__}", "variant_used": "none"}
            self.cache[key] = result
            return result

        # Brak trafienia
        result = {"dzielnica": None, "lat": None, "lon": None, "status": "not_found", "variant_used": "none"}
        self.cache[key] = result
        return result

    # ---------- CSV ENRICH ----------

    def enrich_csv(self, input_file: str = "otodom_raw.csv", output_file: str = "otodom_geo.csv"):
        print(f"📖 Czytam dane z {input_file}")
        try:
            with open(input_file, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except FileNotFoundError:
            print(f"❌ Nie znaleziono pliku {input_file}")
            return
        except Exception as e:
            print(f"❌ Błąd odczytu pliku {input_file}: {e}")
            return

        if not rows:
            print("❌ Plik jest pusty")
            return

        print(f"📍 Parsuję i geokoduję {len(rows)} adresów...")
        for i, row in enumerate(rows, 1):
            raw = (row.get("lokalizacja_raw") or "").strip()
            street, quality = self.parse_street_from_location(raw)

            label = street or (raw if raw else "(brak)")

            if self.dry_run:
                # Dry run - tylko parsowanie, bez geocodingu
                geo = {"dzielnica": None, "lat": None, "lon": None, "status": "dry_run"}
            elif street:
                geo = self.geocode(street)
            else:
                # Jak nie wyciągnęliśmy ulicy – spróbuj geokodować surowe (obszary)
                geo = self.geocode(raw) if raw else {"dzielnica": None, "lat": None, "lon": None, "status": "parse_failed"}
            
            print(f"[{i}/{len(rows)}] {label} -> {geo.get('status')}", flush=True)

            row.update({
                "ulica": street,
                "dzielnica": geo.get("dzielnica"),
                "lat": geo.get("lat"),
                "lon": geo.get("lon"),
                "geo_status": geo.get("status"),
                "parse_quality": quality,
                "geo_variant_used": geo.get("variant_used", "none")
            })

            # grzecznościowe opóźnienie – i tak mamy retry/backoff
            if i < len(rows) and not self.dry_run:
                time.sleep(self.delay)

        # zapisz cache
        self._save_cache()

        print(f"💾 Zapisuję wyniki do {output_file}")
        fieldnames = list(rows[0].keys())
        try:
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(rows)
        except Exception as e:
            print(f"❌ Błąd zapisu do {output_file}: {e}")
            return

        ok_parse = sum(1 for r in rows if r.get("ulica"))
        ok_geo = sum(1 for r in rows if r.get("geo_status") == "success")

        print(f"✅ Zapisano {len(rows)} rekordów")
        print(f"📊 Ulice sparsowane: {ok_parse}/{len(rows)} ({ok_parse/len(rows)*100:.1f}%)")
        print(f"📊 Geokodowanie OK: {ok_geo}/{len(rows)} ({ok_geo/len(rows)*100:.1f}%)")


def main():
    import sys
    
    # Sprawdź czy to dry run
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("🧪 DRY RUN - tylko parsowanie, bez geocodingu")
    
    enricher = GeoEnricher(delay=0.3, email="kuba.kuba2903@gmail.com", dry_run=dry_run)
    enricher.enrich_csv()


if __name__ == "__main__":
    main()
