# scraping/otodom_locations.py
import asyncio
import csv
import random
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple, Iterable, Dict, Set

from playwright.async_api import async_playwright, Page

LISTING_BASE = ("https://www.otodom.pl/pl/wyniki/wynajem/mieszkanie/"
                "malopolskie/krakow/krakow/krakow?limit=72&by=DEFAULT&direction=DESC")

# --- Konfiguracja scrapowania ---
TARGET_OFFERS = 60  # Ile ofert chcemy zebrać (może być więcej niż na jednej stronie)
OFFERS_PER_PAGE =72  # Ile ofert jest na jednej stronie
SAVE_EVERY = 10  # zapisuj co X rekordów
TITLES_FILE = "../data/seen_titles.txt"
TITLES_LOCK = asyncio.Lock()

# --- słowniki pomocnicze ---
DISTRICTS = {
    # 18 dzielnic + dzielnice-zespoły
    "Stare Miasto", "Grzegórzki", "Prądnik Czerwony", "Prądnik Biały", "Krowodrza",
    "Bronowice", "Zwierzyniec", "Dębniki", "Łagiewniki-Borek Fałęcki",
    "Swoszowice", "Podgórze Duchackie", "Bieżanów-Prokocim", "Podgórze",
    "Czyżyny", "Mistrzejowice", "Bieńczyce", "Wzgórza Krzesławickie", "Nowa Huta",
    # często używane zespoły/osiedla
    "Kazimierz", "Zabłocie", "Płaszów", "Ruczaj", "Skotniki", "Kobierzyn",
    "Łobzów", "Kleparz", "Piasek", "Nowy Świat", "Salwator", "Olsza",
    "Azory", "Żabiniec", "Tonie", "Wola Duchacka", "Łęg", "Bielany",
    "Stradom", "Ludwinów", "Dąbie", "Borek Fałęcki", "Prokocim", "Kozłówek",
    "Przewóz", "Bieżanów", "Krowodrza Górka"
}
CITY_TOKENS = {"kraków", "małopolskie"}

# --- Detekcja blokady CloudFront ---
class CloudfrontBlocked(Exception):
    pass

async def _detect_cloudfront_block(page: Page):
    try:
        html = await page.content()
        if ("Request blocked" in html and "CloudFront" in html) or "The request could not be satisfied" in html:
            raise CloudfrontBlocked("CloudFront 403 block detected")
    except CloudfrontBlocked:
        raise
    except Exception:
        # wątpliwe, ale nie przerywaj
        return

# --- Minimalny, ale szczelny ekstraktor adresu (geo-friendly) ---

from typing import Tuple

# Prefiksy ulicopodobne
_PREFIX = (
    r"(?:\bul\.?|\bulica\b|"          # ulica
    r"\bal\.?|\baleja\b|\balei\b|"    # aleja
    r"\bpl\.|\bplac\b|\bplacu\b|"     # plac
    r"\bos\.?|\bosiedle\b|\bosiedlu\b|" # osiedle
    r"\brondo\b|\brondzie\b|"         # rondo
    r"\brynek\b|\brynek\b|"           # rynek (z zapasową literówką)
    r"\bbulwary\b|\bbulwar\b)"        # bulwar(y)
)

# Uwaga: nazwę wymuszamy od DUŻEJ litery lub LICZBY (np. '29 Listopada')
_UC = "A-ZĄĆĘŁŃÓŚŻŹ"
_NAME_WORD = rf"(?:(?-i:[{_UC}][\w\-ąćęłńóśźż]+)|\d+)"   # dodano wariant z liczbą
_NUM        = r"(?:\d+[A-Za-z]?)"                       # numer domu
_NAME_SEQ_STRICT = rf"{_NAME_WORD}(?:\s+{_NAME_WORD}){{0,4}}(?:\s+{_NUM})?"  # np. '29 Listopada 98'

# 1) Prefiksowane: 'ul./al./pl./os./rondo/rynek/bulwary ...'
PREF_RE = re.compile(rf"{_PREFIX}\s+(?P<name>{_NAME_SEQ_STRICT})", re.U | re.I)

# 2) „Gołe" nazwy z numerem — TYLKO w bardzo bezpiecznych kontekstach.
#    a) linia zaczyna się od: Adres/ Ulica / ul.
UNP_LINE_RE = re.compile(
    rf"^\s*(?:adres|ulica|ul\.?)\s*[:\-]?\s*(?P<name>{_NAME_WORD}(?:\s+{_NAME_WORD}){{0,4}})\s+(?P<num>{_NUM})\b",
    re.U | re.M | re.I
)
#    b) fraza 'przy (ul.|ulicy|alei|placu|os.) XYZ 12'
UNP_PRZY_RE = re.compile(
    rf"\bprzy\s+(?:(?:ul\.?|ulicy|alei|placu|os\.?)\s+)?(?P<name>{_NAME_WORD}(?:\s+{_NAME_WORD}){{0,4}})\s+(?P<num>{_NUM})\b",
    re.U | re.I
)

# śmieci na ogonie nazwy (odetnij wszystko od tych znaczników)
_TAIL_NOISE_RE = re.compile(
    r"\b(eng|english|below|pl|ua|ru|studio|pets?|friendly|co-?work(?:ing)?|bez\s*prowizji|bezpośrednio)\b.*$",
    re.I | re.U
)

# --- metraż (m2) ---

_AREA_UNIT_RE = (
    r"(?:m2|m\^2|m\s*2|m²|mkw|m\.\s*kw|m\s*kw|"
    r"metr(?:ów|y)?\s*kw(?:\.|adratow(?:e|ych)?)?)"
)
_AREA_LABEL_RE = r"(?:powierzch\w*|metra(?:ż|z)|pow\.)"

def _norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\u00A0"," ")).strip()

# --- Helpery do obsługi pliku tytułów ---
def _norm_title(s: Optional[str]) -> str:
    if not s:
        return ""
    t = s.lower().replace("\u00a0"," ")
    t = re.sub(r"\s+", " ", t).strip()
    # wytnij metraże i boilerplate
    t = re.sub(r"\b\d{1,3}(?:[.,]\d{1,2})?\s*(?:m2|m²|m\s*2|m\s*kw|m\.\s*kw|mkw)\b", "", t)
    for w in ("do wynajęcia","wynajem","bez prowizji","bezpośrednio","english below","eng","oferta","ogłoszenie","kraków","krakowie"):
        t = t.replace(w, " ")
    t = re.sub(r"[^\wąćęłńóśżź\s-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def load_seen_titles(path: str) -> Set[str]:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        p.touch()
        return set()
    with p.open("r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

async def append_seen_title(path: str, title_norm: str, lock: asyncio.Lock, seen: Set[str]):
    if not title_norm:
        return
    async with lock:
        if title_norm in seen:
            return
        with open(path, "a", encoding="utf-8") as f:
            f.write(title_norm + "\n")
        seen.add(title_norm)

def _to_float_area(num_str: str) -> Optional[float]:
    if not num_str: return None
    try:
        v = float(num_str.replace(",", ".").replace(" ", ""))
        # sanity: mieszkania w widełkach ~10–300 m2
        return v if 10 <= v <= 300 else None
    except:
        return None

def _area_from_text(text: str) -> Optional[float]:
    """Ogólny parser 'NN[,.]N unit' z sensownymi jednostkami."""
    t = _norm_spaces(text).lower()
    m = re.search(rf"(\d{{1,3}}(?:[.,]\d{{1,2}})?)\s*{_AREA_UNIT_RE}\b", t, flags=re.I)
    return _to_float_area(m.group(1)) if m else None

def _area_from_labeled(text: str) -> Optional[float]:
    """Parser z etykietą 'Powierzchnia:' – jednostka może być pominięta."""
    t = _norm_spaces(text).lower()
    # np. 'Powierzchnia: ok. 41,60 m²' lub 'Pow.: 42'
    m = re.search(
        rf"{_AREA_LABEL_RE}\s*[:=\-]?\s*(?:ok\.?\s*)?"
        rf"(\d{{1,3}}(?:[.,]\d{{1,2}})?)\s*(?:{_AREA_UNIT_RE})?",
        t, flags=re.I
    )
    return _to_float_area(m.group(1)) if m else None

async def extract_area_m2(page: Page, title: Optional[str], desc: str) -> Tuple[Optional[float], str]:
    """Zwraca (metraż_m2, źródło: 'tabela'|'tytuł'|'opis'|'')"""
    # 1) TABELA „Szczegóły" → znajdź wiersz, gdzie lewa kolumna zawiera 'Powierzch'
    try:
        rows = await page.query_selector_all('[data-sentry-element="ItemGridContainer"]')
        for r in rows:
            cells = await r.query_selector_all('div')
            if len(cells) >= 2:
                lab = _norm_spaces(await cells[0].inner_text()).lower()
                if "powierzch" in lab:  # „Powierzchnia:"
                    val = _norm_spaces(await cells[1].inner_text())
                    area = _area_from_text(val) or _area_from_labeled(val)
                    if area: 
                        return area, "tabela"
    except:
        pass

    # 2) Tytuł (często „41,60 m²")
    if title:
        area = _area_from_labeled(title) or _area_from_text(title)
        if area: 
            return area, "tytuł"

    # 3) Opis – najpierw wersja z etykietą, potem ogólna 'NN m²'
    if desc:
        area = _area_from_labeled(desc) or _area_from_text(desc)
        if area: 
            return area, "opis"

    return None, ""

def _preclean_for_match(text: str) -> str:
    """Rozwiń skróty (płk., pil., gen., mjr., kpt., dr., prof., ks., św.) zanim dopasujemy regex."""
    s = text or ""
    repl = [
        (r"\bppłk\.\s*", "Podpułkownika "),
        (r"\bpłk\.\s*",  "Pułkownika "),
        (r"\bpil\.\s*",  "Pilota "),
        (r"\bgen\.\s*",  "Generała "),
        (r"\bmjr\.\s*",  "Majora "),
        (r"\bmaj\.\s*",  "Majora "),
        (r"\bkpt\.\s*",  "Kapitana "),
        (r"\bdr\.\s*",   "Doktora "),
        (r"\bprof\.\s*", "Profesora "),
        (r"\bks\.\s*",   "Księdza "),
        (r"\bśw\.\s*",   "Św. "),
    ]
    for pat, rep in repl:
        s = re.sub(pat, rep, s, flags=re.I)
    return s

def _fix_titles(s: str) -> str:
    s = re.sub(r"\bSw\.?\b", "Św.", s, flags=re.I)
    s = re.sub(r"\bgen\.?\b", "Generała", s, flags=re.I)
    s = re.sub(r"\bpłk\.?\b", "Pułkownika", s, flags=re.I)
    s = re.sub(r"\bprof\.?\b", "Profesora", s, flags=re.I)
    s = re.sub(r"\bdr\.?\b", "Doktora", s, flags=re.I)
    s = " ".join(w[:1].upper()+w[1:] for w in s.split())
    # rzymskie
    s = re.sub(r"\bIi\b","II", s); s = re.sub(r"\bIii\b","III", s); s = re.sub(r"\bIv\b","IV", s)
    # znormalizuj przypadki z 'Generała.' → 'Generała '
    s = re.sub(r"\b(Generała|Pułkownika|Doktora|Profesora|Księdza)\.\s+", r"\1 ", s)
    return s

def _canon_prefix(p: str) -> str:
    p = p.lower()
    if p.startswith("ul"): return "ul."
    if p.startswith("al"): return "al."
    if p.startswith("pl"): return "pl."
    if p.startswith("os"): return "os."
    if p.startswith("rondo"): return "rondo"
    if p.startswith("rynek"): return "rynek"
    if p.startswith("bulwar"): return "bulwary"
    return p

def _drop_redundant_noun(prefix: str, name: str) -> Tuple[str, str]:
    """Usuwa 'Ulica/Aleja/Plac/Osiedle/Rondo/Rynek' z początku nazwy jeśli powtórzone
       i ewentualnie koryguje prefiks, gdy z UNPREFIXED zrobiliśmy omyłkowo 'ul.'."""
    if not name: return prefix, name
    first = name.split()[0].lower()
    # mapa: 'słowo w nazwie' → (docelowy prefiks, czy usuwać pierwszy token z nazwy)
    leading = {
        "ulica": ("ul.", True), "ulicy": ("ul.", True),
        "aleja": ("al.", True), "alei": ("al.", True),
        "plac": ("pl.", True), "placu": ("pl.", True),
        "osiedle": ("os.", True), "osiedlu": ("os.", True), "os.": ("os.", True),
        "rondo": ("rondo", True), "rondzie": ("rondo", True),
        "rynek": ("rynek", True), "rynku": ("rynek", True),
        "bulwar": ("bulwary", True), "bulwary": ("bulwary", True),
    }
    if first in leading:
        new_pref, drop = leading[first]
        if prefix != new_pref: prefix = new_pref
        if drop: name = " ".join(name.split()[1:])
    # Specjalny przypadek: 'al. Aleja …' / 'ul. Ulica …'
    if prefix == "al." and first in {"aleja","alei"}:
        name = " ".join(name.split()[1:])
    if prefix == "ul." and first in {"ulica","ulicy"}:
        name = " ".join(name.split()[1:])
    return prefix, name

def _strip_tail_noise(name: str) -> str:
    return _TAIL_NOISE_RE.sub("", name).strip(" ,.;:-–—")

def _extract_prefixed_first(text: str, *, forbid_rynek: bool = False) -> Optional[str]:
    if not text: 
        return None
    t = re.sub(r"\s+", " ", text)
    t = _preclean_for_match(t)  # ⬅️ NOWE
    m = PREF_RE.search(t)
    if not m: 
        return None
    raw = m.group(0)
    prefix = _canon_prefix(raw.split()[0])
    if forbid_rynek and prefix in ("rynek", "rondo"):
        return None
    name = _fix_titles(m.group("name"))
    name = _strip_tail_noise(name)
    prefix, name = _drop_redundant_noun(prefix, name)
    return f"{prefix} {name}".strip() if name else None

def _extract_unprefixed_strict(line_text: str) -> Optional[str]:
    """Pracuje NA LINII (początek 'Adres/Ulica/ul.') lub fraza 'przy …'."""
    if not line_text: return None
    t = re.sub(r"\s+", " ", line_text)
    for rx in (UNP_LINE_RE, UNP_PRZY_RE):
        m = rx.search(t)
        if m:
            name = _fix_titles(m.group("name"))
            num  = m.group("num")
            prefix = "ul."
            # skoryguj prefiks po wiodącym rzeczowniku (Aleja/Alei/Plac/Os./Rondo/Rynek…)
            prefix, name = _drop_redundant_noun(prefix, name)
            name = _strip_tail_noise(name)
            adr = f"{prefix} {name} {num}".strip()
            return adr
    return None

def _lines(desc: str) -> List[str]:
    raw = (desc or "").replace("•", "\n")
    parts = [re.sub(r"\s+", " ", p).strip(" -") for p in raw.splitlines()]
    return [p for p in parts if p]

def _remove_prefix_for_csv(address: str) -> str:
    """Dla CSV zdejmujemy TYLKO 'ul.'; zostawiamy al./pl./os./rondo/rynek/bulwary."""
    if not address:
        return address

    # zdejmij wyłącznie 'ul.' z początku
    address = re.sub(r'^\s*ul\.\s+', '', address, flags=re.I)

    # usuń ', Kraków' na końcu
    address = re.sub(r',\s*Kraków\s*$', '', address, flags=re.I)

    # edge-case: 'al.  29' → 'al. 29 Listopada' (jeśli nie ma już 'Listopada')
    address = re.sub(r'^(al\.)\s*29\b(?!\s*Listopada)', r'\1 29 Listopada', address, flags=re.I)

    # porządkuj wielokrotne spacje
    address = re.sub(r'\s+', ' ', address).strip()
    return address

def extract_address_for_geocode(header_loc: Optional[str], title: Optional[str], desc: str) -> Optional[str]:
    # 1) header/mapa – NAJPIERW
    s = _extract_prefixed_first(header_loc or "")
    if s: return s + ", Kraków"
    # 2) tytuł
    s = _extract_prefixed_first(title or "")
    if s: return s + ", Kraków"
    s = _extract_unprefixed_strict(title or "")
    if s: return s + ", Kraków"
    # 3) opis (ale bez 'rynek' i 'rondo')
    for ln in _lines(desc):
        s = _extract_unprefixed_strict(ln) or _extract_prefixed_first(ln, forbid_rynek=True)
        if s: return s + ", Kraków"
    return None


def extract_district_from_breadcrumbs(breadcrumbs: List[str]) -> Optional[str]:
    # spróbuj wprost z breadcrumbs
    for b in breadcrumbs:
        bt = b.strip()
        if bt and bt.lower() not in CITY_TOKENS and not bt.lower().startswith(("ul", "al", "pl", "os", "rondo", "rynek")):
            # wybierz pierwszy token, który wygląda na dzielnicę/osiedle
            if bt in DISTRICTS:
                return bt
    # fallback: weź ostatni “nie-miasto/nie-woj” okruszek
    for b in reversed(breadcrumbs):
        bt = b.strip()
        if bt and bt.lower() not in CITY_TOKENS:
            return bt
    return None


def extract_id(url: str) -> str:
    m = re.search(r"-ID(\w+)$", url)
    return f"ID{m.group(1)}" if m else url.rsplit("/", 1)[-1]


def extract_district_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"\s+", " ", text)

    # 1) 'w dzielnicy X' / 'w dzielnicy Y-Z'
    m = re.search(r"w\s+dzielnic[yi]\s+([A-ZĄĆĘŁŃÓŚŻŹ][\w\-ąćęłńóśźż]+(?:\s+[A-ZĄĆĘŁŃÓŚŻŹ][\w\-ąćęłńóśźż]+)?)", t, flags=re.I)
    if m:
        cand = m.group(1).strip()
        if cand in DISTRICTS:
            return cand

    # 2) 'na/w <rejon>' (Kazimierzu, Ruczaju, Bronowicach…) – sprawdzamy przeciwko słownikowi
    m2 = re.search(r"(?:na|w)\s+([A-ZĄĆĘŁŃÓŚŻŹ][\w\-ąćęłńóśźż]+(?:\s+[A-ZĄĆĘŁŃÓŚŻŹ][\w\-ąćęłńóśźż]+)?)", t, flags=re.I)
    if m2:
        cand = m2.group(1).strip().rstrip(".,;:–—")
        # sprostać odmianom: 'Kazimierzu'→'Kazimierz', 'Bronowicach'→'Bronowice'
        norm = re.sub(r"(u|ach|y|ie)$", "", cand)  # bardzo prosta normalizacja
        for item in DISTRICTS:
            if norm.lower() in {item.lower(), item.lower().rstrip("e"), item.lower().rstrip("y")}:
                return item

    return None


async def accept_cookies(page: Page):
    # różne warianty przycisku
    selectors = [
        'button[data-testid="accept-cookies-button"]',
        'button:has-text("Akceptuj")',
        'button:has-text("Zgadzam")'
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await asyncio.sleep(0.8)
                break
        except:
            pass


async def collect_listing_entries_fast(page: Page) -> List[dict]:
    """
    Zwraca listę: [{"url": "...", "title": "..."}] z listingu.
    Używa selektorów:
      a[data-cy="listing-item-link"]  zawiera wewnątrz:
      p[data-cy="listing-item-title"] z tekstem tytułu.
    """
    # Upewnij się, że listing się narysował
    await page.wait_for_selector('a[data-cy="listing-item-link"]', timeout=20000)

    # Szybkie wyciąganie w kontekście przeglądarki – 1 przebieg po DOM
    entries = await page.eval_on_selector_all(
        'a[data-cy="listing-item-link"]',
        """els => els.map(a => {
            const href = a.href; // absolutny URL
            const titleEl = a.querySelector('[data-cy="listing-item-title"]');
            const titleAttr = a.getAttribute('title') || a.getAttribute('aria-label') || '';
            const title = (titleEl?.innerText || titleAttr || '').trim();
            return { url: href, title };
        })"""
    )

    # Usuń duplikaty po URL, zachowując kolejność
    const_seen = set()
    uniq = []
    for it in entries:
        if it["url"] in const_seen:
            continue
        uniq.append(it)
        const_seen.add(it["url"])
    return uniq


async def get_header_location(page: Page) -> Tuple[Optional[str], List[str]]:
    """Zwraca (tekst lokalizacji z headera/mapy lub breadcrumbs, breadcrumbs_list)."""
    loc = None

    # kilka wariantów linka/adresu w nagłówku
    candidates = [
        'a[data-cy="adPageLinkToMap"]',
        'a[href*="map"]',
        '[data-testid="adPageLocation"]',
        '[data-cy="adPageBreadcrumbs"] a[href*="map"]'
    ]
    for sel in candidates:
        el = await page.query_selector(sel)
        if el:
            try:
                t = (await el.inner_text()) or ""
                t = t.strip()
                if t:
                    loc = t
                    break
            except:
                pass

    breadcrumbs = []
    try:
        items = await page.query_selector_all('[data-cy="adPageBreadcrumbs"] li')
        for it in items:
            txt = (await it.inner_text()) or ""
            txt = txt.strip()
            if txt:
                breadcrumbs.append(txt)
        if not loc and breadcrumbs:
            loc = ", ".join(breadcrumbs)
    except:
        pass

    return loc, breadcrumbs


async def get_description_text(page: Page) -> str:
    # różne warianty sekcji opisu
    selectors = [
        '[data-cy="adPageSectionDescription"]',
        '[data-cy="adPageAdDescription"]',
        'section:has(h2:has-text("Opis")), section:has(h2:has-text("OPIS"))'
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                t = await el.inner_text()
                if t:
                    return t
        except:
            pass
    return ""


# --- ceny: najem + czynsz adm. ---

_AMOUNT_RE = r"(\d[\d\s\u00A0.,]*)"  # cyfry z odstępami/nbsp/kropkami/przecinkami
_CURRENCY_RE = r"(?:\s*(?:zł|pln))?"

def _to_int_pln(s: str) -> Optional[int]:
    """Z '2 300', '915 PLN', '790,00 zł' → 2300/915/790 (int).
       Jeśli brak cyfr – None."""
    if not s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else None

def _first_amount(text: str) -> Optional[int]:
    """Znajdź pierwszą kwotę z walutą (zł/PLN) – zwraca int."""
    if not text:
        return None
    m = re.search(rf"{_AMOUNT_RE}\s*{_CURRENCY_RE}", text, flags=re.I)
    return _to_int_pln(m.group(1)) if m else None

def _parse_amount_after(label_re: str, text: str) -> Optional[int]:
    """Znajdź kwotę po danym słowie-kluczu (np. 'Czynsz administracyjny')."""
    if not text:
        return None
    m = re.search(rf"{label_re}\s*[:=\-]?\s*{_AMOUNT_RE}\s*{_CURRENCY_RE}", text, flags=re.I | re.U)
    return _to_int_pln(m.group(1)) if m else None

def _extract_rent_from_header_text(text: str) -> Optional[int]:
    """Wyciąga główną cenę najmu z headera (fallback na wypadek zmian selektora)."""
    return _parse_amount_after(r"(?:cena|price)?", text)  # praktycznie: weź pierwszą kwotę

def _extract_admin_from_text(text: str, rent_hint: Optional[int]=None) -> Optional[int]:
    """Czynsz adm. z tekstu: preferuj wyrażenia 'czynsz administracyjny/opłaty adm.'.
       'czynsz' samotny bierzemy tylko jeśli nie wygląda na 'czynsz najmu' i jest < rent_hint."""
    if not text:
        return None
    # 1) najmocniejsze sygnały
    for lab in (r"czynsz\s*administracyjny", r"opłaty?\s*administracyjne", r"adm\."):
        val = _parse_amount_after(lab, text)
        if val: return val

    # 2) „+ Czynsz 790 zł", „Czynsz: 850 zł" – z wykluczeniem frazy „najmu"
    out = None
    for m in re.finditer(rf"\bczynsz\b(?!\s*najmu)\s*[:=\-]?\s*{_AMOUNT_RE}\s*{_CURRENCY_RE}", text, flags=re.I):
        val = _to_int_pln(m.group(1))
        if not val:
            continue
        # Odfiltruj ew. „czynsz najmu" (gdyby negatyw nie zadziałał przez dziwne białe znaki)
        left = text[max(0, m.start()-30):m.start()].lower()
        right = text[m.end():m.end()+20].lower()
        if "najmu" in left or "najmu" in right:
            continue
        # Heurystyka: jeśli mamy podpowiedź ceny najmu, a „czynsz" >= najem*0.8 — pewnie to nie adm.
        if rent_hint and val >= int(0.8 * rent_hint):
            continue
        out = val
        break
    return out

async def extract_prices(page: Page, desc_text: str) -> Tuple[Optional[int], Optional[int], str]:
    """Zwraca (najem_pln, czynsz_adm_pln, źródło_czynszu)"""
    rent = None
    admin = None
    admin_src = ""

    # --- 1) najem z headera ---
    try:
        price_el = await page.query_selector('[data-cy="adPageHeaderPrice"]')
        if price_el:
            rent_text = (await price_el.inner_text()) or ""
            rent = _first_amount(rent_text)
        if not rent:
            # fallback: cały blok cenowy i spróbuj wyłuskać pierwszą kwotę
            wrap = await page.query_selector('[data-sentry-element="PriceSection"]')
            if wrap:
                rent = _extract_rent_from_header_text(await wrap.inner_text())
    except:
        pass

    # --- 2) czynsz adm. z headera (np. „+ Czynsz 790 zł") ---
    try:
        addl = await page.query_selector('[data-sentry-element="AdditionalPriceWrapper"]')
        if addl:
            t = (await addl.inner_text()) or ""
            admin = _extract_admin_from_text(t, rent)
            if admin:
                admin_src = "header"
        if not admin:
            # fallback: cały blok cenowy
            wrap = await page.query_selector('[data-sentry-element="PriceSection"]')
            if wrap:
                t = (await wrap.inner_text()) or ""
                admin = _extract_admin_from_text(t, rent)
                if admin:
                    admin_src = "header"
    except:
        pass

    # --- 3) czynsz adm. z opisu (jeśli brak w headerze) ---
    if not admin and desc_text:
        admin = _extract_admin_from_text(desc_text, rent)
        if admin:
            admin_src = "opis"

    return rent, admin, admin_src


async def scrape_offer(page: Page, url: str, seen_titles: Set[str]):
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    # czekaj na konkretny selektor zamiast 'networkidle' - szybsze i cichsze
    try:
        await page.wait_for_selector('[data-cy="adPageHeaderPrice"]', timeout=8000)
    except:
        # fallback jeśli nie ma ceny w headerze
        await page.wait_for_selector('h1[data-cy="adPageAdTitle"]', timeout=8000)
    await _detect_cloudfront_block(page)

    # TYTUŁ (żeby móc z niego łapać)
    title = None
    try:
        el = await page.query_selector('h1[data-cy="adPageAdTitle"]')
        if el: title = (await el.inner_text()).strip()
    except: pass

    # ⬇⬇⬇ SKIP jeśli tytuł już był (po normalizacji)
    if title and _norm_title(title) in seen_titles:
        oid = extract_id(url)
        print(f"\n[{oid}] SKIP: tytuł już przetworzony → '{title}'")
        return None

    # LOKALIZACJE ŹRÓDŁOWE
    header_loc, breadcrumbs = await get_header_location(page)
    desc = await get_description_text(page)

    # CENY: najem + czynsz adm.
    rent_pln = None
    admin_pln = None
    try:
        rent_pln, admin_pln, admin_src = await extract_prices(page, desc)
    except Exception as _e:
        admin_src = ""

    # METRAŻ m2
    metraz_m2 = None
    metraz_src = ""
    try:
        metraz_m2, metraz_src = await extract_area_m2(page, title, desc)
    except Exception:
        pass

    # 👉 zamiast extract_street_from_sources(...)
    adres = extract_address_for_geocode(header_loc, title, desc)

    oid = extract_id(url)
    print(f"\n[{oid}]:")
    print(f"- Lokalizacja na header: {header_loc or '—'}")
    print(f"- Lokalizacja w opis: {(desc[:180] + ' ...') if desc else '—'}")
    print(f"- Najem (header): {rent_pln if rent_pln is not None else '—'} PLN")
    print(f"- Czynsz adm.: {admin_pln if admin_pln is not None else '—'} PLN"
          + (f" (źródło: {admin_src})" if admin_pln else ""))
    print(f"- Metraż: {metraz_m2 if metraz_m2 is not None else '—'} m²"
          + (f" (źródło: {metraz_src})" if metraz_src else ""))

    if not adres:
        print(f"❌ NIE UDAŁO ZNALEŹĆ ULICY DLA: {oid} | {url}")
        return None
    else:
        print(f"- ADRES do geokodera: {adres}")
        # Wyczyść adres dla CSV (usuń prefiksy i ", Kraków")
        clean_address = _remove_prefix_for_csv(adres)
        print(f"- ADRES do CSV: {clean_address}")
        return {
            "id": oid,
            "title": title,
            "ulica": clean_address,
            "metraz_m2": metraz_m2,   # ⬅️ NOWE
            "url": url,
            "najem_pln": rent_pln,
            "czynsz_adm_pln": admin_pln
        }


from pathlib import Path

def save_to_csv(rows: List[dict], filename: str = "../data/otodom_results.csv"):
    """
    Dopisuje wiersze do CSV. Nagłówek zapisywany tylko gdy plik nie istnieje lub jest pusty.
    """
    if not rows:
        print("❌ Brak danych do zapisania (batch pusty)")
        return

    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)

    write_header = not path.exists() or path.stat().st_size == 0
    fieldnames = ["id", "title", "ulica", "metraz_m2", "najem_pln", "czynsz_adm_pln", "url"]

    with path.open('a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    print(f"💾 Dopisano {len(rows)} ogłoszeń do {filename}")

def save_partial(rows: List[dict], filename: str = "../data/otodom_results.csv"):
    """
    Alias na save_to_csv – tu przekazujemy już tylko batch (nowe rekordy),
    więc nie grozi duplikat.
    """
    save_to_csv(rows, filename)


# --- Równoległe scrapowanie ---
CONCURRENCY = 2  # ile ogłoszeń naraz - zmniejszone dla stabilności



async def scrape_all(context, links, seen_titles: Set[str], progress: Dict[str, object], reserved_titles: Set[str]):
    """
    Scrapuje wszystkie ogłoszenia równolegle z limitem współbieżności.
    progress: {'done': int, 'target': int, 'lock': asyncio.Lock, 'blocked': bool}
    Zwraca (results_list, blocked_bool, collected_count)
    """
    sem = asyncio.Semaphore(CONCURRENCY)
    total_links = len(links)
    results = []
    blocked = False

    async def sem_worker(link, index):
        nonlocal blocked
        async with sem:  # ⬅⬅⬅ TERAZ DZIAŁA OGRANICZENIE RÓWNOLEGŁOŚCI
            offer_page = await context.new_page()
            try:
                res = await scrape_offer(offer_page, link, seen_titles)
                # ⬇⬇⬇ dopisz tytuł do listy, tylko gdy oferta wejdzie do CSV (res != None)
                if res and res.get("title"):
                    title_norm = _norm_title(res["title"])
                    await append_seen_title(TITLES_FILE, title_norm, TITLES_LOCK, seen_titles)
                    reserved_titles.add(title_norm)  # dodaj do rezerwacji w tym runie
                return res
            except CloudfrontBlocked:
                blocked = True
                return None
            except Exception as e:
                print(f"[WARN] Błąd przy {link}: {e}")
                return None
            finally:
                await offer_page.close()
                # progress po zamknięciu strony (żeby zawsze się liczył)
                async with progress['lock']:  # type: ignore
                    progress['done'] = int(progress.get('done', 0)) + 1  # type: ignore
                    done = progress['done']  # type: ignore
                    target = progress['target']  # type: ignore
                offer_id = extract_id(link)
                left = max(0, target - done)
                print(f"[{done}/{target}] [{offer_id}] (zostało: {left})")

    print(f"🚀 Rozpoczynam równoległe scrapowanie ({CONCURRENCY} ogłoszeń naraz)...")
    print(f"📋 Łącznie do przetworzenia na tej stronie: {total_links}")

    tasks = [asyncio.create_task(sem_worker(link, i)) for i, link in enumerate(links)]
    batch = await asyncio.gather(*tasks)
    results = [r for r in batch if r]
    return results, blocked, len(results)  # dodatkowo licznik faktycznie zebranych


async def main():
    all_results: List[dict] = []
    collected = 0   # ⬅️ licznik faktycznie zebranych (res != None)
    current_page = 1
    pages_visited = 0
    total_processed = 0
    last_saved_at = 0

    # globalny progress względem TARGET_OFFERS
    progress = {'done': 0, 'target': TARGET_OFFERS, 'lock': asyncio.Lock(), 'blocked': False}

    async with async_playwright() as p:
        # --- anti-bot: UA + cookies + małe losowe pauzy ---
        print("[anti-bot] ustawiam UA/viewport i wchodzę na homepage…")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/127.0.0.0 Safari/537.36"),
            viewport={"width": 1440, "height": 900},
            locale="pl-PL",
            extra_http_headers={"Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7"}
        )
        
        # Blokada ciężkich zasobów - mniej requestów = mniej szans na bana
        async def _route_filter(route, request):
            rtype = request.resource_type
            if rtype in {"image", "media", "font", "stylesheet"}:
                await route.abort()
            else:
                await route.continue_()
        await context.route("**/*", _route_filter)
        page = await context.new_page()
        await page.goto("https://www.otodom.pl/", wait_until="domcontentloaded")
        await accept_cookies(page)
        await asyncio.sleep(random.uniform(0.5, 1.2))
        print("połączenie ze stroną: check")

        # --- lista znanych tytułów (persist) ---
        seen_titles = load_seen_titles(TITLES_FILE)
        print(f"🧠 Załadowano {len(seen_titles)} znanych tytułów z {TITLES_FILE}")

        try:
            # --- przechodź przez strony aż zbierzesz X ofert ---
            while collected < TARGET_OFFERS:
                listing_url = f"{LISTING_BASE}&page={current_page}"
                print(f"\n📄 Przechodzę na stronę {current_page}...")

                await page.goto(listing_url, wait_until="networkidle", timeout=30000)
                await _detect_cloudfront_block(page)  # ⬅️ detekcja blokady listingu
                await page.wait_for_selector('[data-cy="search.listing.organic"]', timeout=30000)

                # 1) Zbierz wpisy (URL + tytuł) prosto z listingu
                entries = await collect_listing_entries_fast(page)
                
                # ⬇⬇⬇ DODAJ:
                if not entries:
                    print("❌ Listing zwrócił 0 ogłoszeń – koniec wyników.")
                    break
                
                # po udanym wczytaniu listingu i zebraniu entries:
                pages_visited += 1
                
                # 2) Użyj już załadowanych tytułów + RAM-owa „rezerwacja" na ten przebieg
                reserved_titles: Set[str] = set()

                # 3) Odetnij duplikaty po TYTULE (dynamiczne ID nas nie interesuje)
                new_entries = []
                for it in entries:
                    t_norm = _norm_title(it["title"])  # użyj Twojej funkcji normalizującej
                    if not t_norm:
                        # Polityka: kompletnie puste tytuły omijamy, żeby nie marnować requestów.
                        # (jeśli chcesz je jednak łapać, usuń ten 'continue')
                        continue
                    if t_norm in seen_titles or t_norm in reserved_titles:
                        continue
                    new_entries.append(it)
                    reserved_titles.add(t_norm)  # rezerwacja w tym runie

                print(f"📋 Na stronie {current_page}: {len(entries)} ogłoszeń (NOWE po tytule: {len(new_entries)})")

                if not new_entries:
                    # Same duplikaty – od razu przejdź dalej, bez klikania w ogłoszenia
                    current_page += 1
                    await asyncio.sleep(random.uniform(0.8, 1.5))
                    continue

                # 4) Dalej pracujesz już na czystej liście linków:
                links = [it["url"] for it in new_entries]

                # przytnij, jeśli zbliżamy się do TARGET_OFFERS
                if collected + len(links) > TARGET_OFFERS:
                    links = links[:TARGET_OFFERS - collected]

                page_results, blocked, got = await scrape_all(context, links, seen_titles, progress, reserved_titles)
                all_results.extend(page_results)
                collected += got
                total_processed += len(links)

                print(f"✅ Strona {current_page}: {len(page_results)}/{len(links)} ogłoszeń z adresami")
                print(f"📊 Łącznie zebrano: {collected}/{TARGET_OFFERS} ofert")

                # zapis co 50 rekordów
                if len(all_results) // SAVE_EVERY > last_saved_at // SAVE_EVERY:
                    batch = all_results[last_saved_at:]
                    print(f"📝 Zapis batchu: {len(batch)} rekordów (od {last_saved_at} do {len(all_results)-1})")
                    save_partial(batch)
                    last_saved_at = len(all_results)

                if blocked:
                    print("🛑 Wykryto blokadę CloudFront — zapisuję stan i kończę.")
                    batch = all_results[last_saved_at:]
                    save_partial(batch)
                    last_saved_at = len(all_results)
                    break

                if collected >= TARGET_OFFERS:
                    print(f"🎯 Osiągnięto cel: {collected}/{TARGET_OFFERS} ofert")
                    break

                current_page += 1
                await asyncio.sleep(random.uniform(1.0, 2.0))

        except CloudfrontBlocked:
            print("🛑 Wykryto blokadę CloudFront (poza pętlą) — zapisuję stan i kończę.")
            batch = all_results[last_saved_at:]
            save_partial(batch)
            last_saved_at = len(all_results)
        except Exception as e:
            print(f"⚠️ Nieoczekiwany błąd: {e} — zapisuję częściowe wyniki.")
            batch = all_results[last_saved_at:]
            save_partial(batch)
            last_saved_at = len(all_results)

        print(f"\n🎉 Zakończono scrapowanie!")
        print(f"📊 Przetworzono {total_processed} ogłoszeń z {pages_visited} stron")
        print(f"✅ Znaleziono adresy dla {collected} ogłoszeń")

        # finalny zapis (na wszelki wypadek)
        if all_results and last_saved_at < len(all_results):
            batch = all_results[last_saved_at:]
            save_partial(batch)
            last_saved_at = len(all_results)

        await browser.close()


if __name__ == "__main__":
    # Self-test dla ekstraktora adresów
    tests = [
        ("ul. Macieja Miechowity, Olsza, Kraków", None, ""),
        ("Aleja 29 Listopada 100, Kraków", None, ""),
        ("ul. Na Kozłówce 15, Bieżanów-Prokocim", None, ""),
        ("rondo Hipokratesa, Mistrzejowice", None, ""),
        ("pl. Wolnica, Kazimierz", None, ""),
        ("al. Space ma przyjemność zaprezentować…", None, ""),   # powinno dać None
        ("os. Europejskim, Nowa Huta", None, ""),  # powinno dać os. Europejskie
        ("Dwa pokoje lub pokój do wynajęcia", "Nadwiślańska 11", "Adres: Nadwiślańska 11"),  # test z numerem
        ("rynek Dębnicki, Dębniki, Kraków", None, ""),
        ("al.  29, Kraków", None, "Nowa kawalerka ... Al. 29 Listopada 98."),
        ("al. 29 Listopada 98, Kraków", None, ""),
        ("ul. płk. pil. Stefana Łaszkiewicza, Rakowice, Prądnik Czerwony, Kraków, małopolskie", None, ""),
        (None, None, "5 minut pieszo na Rynek Główny, świetna lokalizacja przy Karmelickiej 7."),
    ]
    
    # Testy dla metrażu
    print("\n🧪 Self-test ekstraktora metrażu:")
    area_tests = [
        ("Mieszkanie, 41,60 m², Kraków", "41.60"),
        ("Kawalerka 24m2 - Stare Dębniki", "24.0"),
        ("2 pokoje 65 m2", "65.0"),
        ("Powierzchnia: ok.42", "42.0"),
        ("Pow.: 42,5 mkw", "42.5"),
        ("42 m 2", "42.0"),
        ("Mieszkanie 100m² z ogrodem", "100.0"),
        ("Nie ma metrażu", None),
        ("Cena 2000 zł za m2", None),  # nie powinno złapać ceny
    ]
    
    for test_input, expected in area_tests:
        result = _area_from_text(test_input) or _area_from_labeled(test_input)
        status = "✅" if result == expected else "❌"
        print(f"  {status} '{test_input}' → {result} (oczekiwane: {expected})")
    print("🧪 Self-test ekstraktora adresów:")
    for header, title, desc in tests:
        result = extract_address_for_geocode(header, title, desc)
        print(f"  header:'{header}' title:'{title}' desc:'{desc[:30]}...' → '{result}'")
    
    print("\n🚀 Uruchamiam scraper...")
    asyncio.run(main())
