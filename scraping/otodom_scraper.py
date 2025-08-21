#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Otodom Scraper - Prosta wersja
Wyciąga: tytuł, cena, lokalizacja z mapy
"""

import asyncio
import csv
import re
import time
from typing import Dict, List, Optional

from playwright.async_api import async_playwright, Page


class OtodomScraper:
    def __init__(self):
        pass
    
    def extract_id(self, url: str) -> str:
        """Wyciąga czyste ID z URL-a (np. ID4xA1c z całego slug-a)."""
        match = re.search(r'-ID(\w+)$', url)
        if match:
            return f"ID{match.group(1)}"
        # Fallback dla przypadków bez -ID
        return url.split('/')[-1]
    

    


    async def scrape_ogloszenie_detail(self, page: Page, url: str) -> Optional[Dict]:
        """Scrapuje szczegóły jednego ogłoszenia."""
        try:
            await page.goto(url, wait_until="domcontentloaded")
            # poczekaj aż pojawi się tytuł
            await page.wait_for_selector('h1[data-cy="adPageAdTitle"]', timeout=8000)

            title_el = await page.query_selector('h1[data-cy="adPageAdTitle"]')
            title = (await title_el.inner_text()).strip() if title_el else None

            price_el = await page.query_selector('strong[data-cy="adPageHeaderPrice"]')
            price_text = (await price_el.inner_text()) if price_el else None
            price_clean = re.sub(r'[^\d]', '', price_text) if price_text else None
            price = int(price_clean) if price_clean else None

            # lokalizacja – link do mapy
            location_text = None
            loc_el = await page.query_selector('a[data-cy="adPageLinkToMap"]')
            if loc_el:
                location_text = (await loc_el.inner_text()).strip()

            # fallback jak było
            if not location_text:
                breadcrumbs_el = await page.query_selector('div[data-cy="adPageBreadcrumbs"]')
                if breadcrumbs_el:
                    location_text = (await breadcrumbs_el.inner_text()).strip()
                else:
                    # Spróbuj inne selektory
                    alt_loc_el = await page.query_selector('a[href*="map"]')
                    if alt_loc_el:
                        location_text = (await alt_loc_el.inner_text()).strip()

            ogloszenie_id = self.extract_id(url)

            return {
                "id": ogloszenie_id,
                "url": url,
                "title": title,
                "cena_mies": price,
                "lokalizacja_raw": location_text
            }

        except Exception as e:
            print(f"    ❌ Błąd podczas scrapowania {url}: {e}")
            return None
    
    async def get_ogloszenia_urls(self, page: Page, max_count: int = 20) -> List[str]:
        """Pobiera listę URL-i ogłoszeń z strony wyników."""
        urls = []
        seen_urls = set()

        try:
            # Sprawdź co się załadowało
            print("⏳ Sprawdzam co się załadowało...")
            await asyncio.sleep(3)
            
            page_title = await page.title()
            print(f"📄 Tytuł strony: {page_title}")
            
            # Sprawdź wszystkie linki na stronie
            all_links = await page.query_selector_all('a')
            print(f"🔗 Wszystkich linków na stronie: {len(all_links)}")
            
            # Sprawdź kilka pierwszych linków
            for i, link in enumerate(all_links[:10]):
                href = await link.get_attribute('href')
                text = await link.inner_text()
                print(f"  Link {i+1}: {href} | {text[:50]}...")
            
            # Spróbuj różnych selektorów
            selectors = [
                'a[data-cy="listing-item-link"]',
                'a[href*="/oferta/"]',
                'a[href*="/pl/oferta/"]',
                'article a',
                'a[data-testid*="listing"]'
            ]
            
            for selector in selectors:
                try:
                    found_links = await page.query_selector_all(selector)
                    if found_links:
                        print(f"✅ Selektor '{selector}' znalazł {len(found_links)} linków")
                        links = found_links
                        break
                    else:
                        print(f"❌ Selektor '{selector}' nie znalazł linków")
                except Exception as e:
                    print(f"❌ Błąd z selektorem '{selector}': {e}")
                    continue
            else:
                print("❌ Żaden selektor nie znalazł linków")
                return []
            
            for link in links:
                if len(urls) >= max_count:
                    break
                    
                href = await link.get_attribute('href')
                if href and '/oferta/' in href:
                    if not href.startswith('http'):
                        href = 'https://www.otodom.pl' + href
                    
                    if href not in seen_urls:
                        seen_urls.add(href)
                        urls.append(href)
                        ogloszenie_id = self.extract_id(href)
                        print(f"  [{len(urls)}] [ID{ogloszenie_id}]")
            
            print(f"📋 Znaleziono {len(urls)} unikalnych ogłoszeń")
            return urls[:max_count]
            
        except Exception as e:
            print(f"❌ Błąd podczas pobierania URL-i: {e}")
            return []

    async def scrape_ogloszenia(self, max_ogloszenia: int = 20) -> List[Dict]:
        """Główna funkcja scrapowania."""
        search_url = "https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/krakow?distanceRadius=0&page=1&limit=72&by=DEFAULT&direction=DESC&viewType=listing"
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={"Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7"}
            )
            page = await context.new_page()
            
            # Najpierw strona główna
            print("🏠 Otwieram stronę główną Otodom...")
            await page.goto("https://www.otodom.pl/", wait_until="domcontentloaded")
            
            # Zaakceptuj cookies na stronie głównej
            try:
                cookie_button = await page.query_selector('button[data-testid="accept-cookies-button"]')
                if cookie_button:
                    await cookie_button.click()
                    print("🍪 Zaakceptowano cookies")
                    await asyncio.sleep(2)
            except:
                pass
        
            # Dopiero teraz przejdź do wyników
            print(f"🔍 Przechodzę do wyników: {search_url}")
            await page.goto(search_url, wait_until="networkidle")
            
            urls = await self.get_ogloszenia_urls(page, max_ogloszenia)
            
            if not urls:
                print("❌ Nie znaleziono żadnych ogłoszeń")
                await browser.close()
                return []
            
            # Scrapuj używając głównej strony
            results = await self.scrape_sequential(page, urls)
            
            await browser.close()
            return results

    async def scrape_parallel(self, browser, urls: List[str], max_workers: int = 4) -> List[Dict]:
        """Scrapuje ogłoszenia równolegle używając workerów."""
        queue = asyncio.Queue()
        for url in urls:
            await queue.put(url)
        
        results = []
        workers = []
        
        async def worker(worker_id: int):
            worker_results = []
            while True:
                try:
                    url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                # Każdy worker ma swoją własną stronę
                page = await browser.new_page()
                
                # Wyciągnij ID dla logów
                ogloszenie_id = self.extract_id(url)
                print(f"[Worker {worker_id}] [ID{ogloszenie_id}] Scrapuję...")
                
                result = await self.scrape_ogloszenie_detail(page, url)
                if result:
                    worker_results.append(result)
                    print(f"  ✅ [ID{ogloszenie_id}] {result['title'][:40]}... | {result['cena_mies']} zł")
                else:
                    print(f"  ❌ [ID{ogloszenie_id}] Nie udało się scrapować")
                
                await page.close()
                await asyncio.sleep(0.2)  # Krótka pauza
                queue.task_done()
            
            return worker_results
        
        # Uruchom workerów
        for i in range(max_workers):
            workers.append(asyncio.create_task(worker(i + 1)))
        
        # Czekaj na zakończenie wszystkich workerów
        worker_results = await asyncio.gather(*workers)
        
        # Złącz wyniki
        for worker_result in worker_results:
            results.extend(worker_result)
        
        print(f"🎯 Zebrano {len(results)} ogłoszeń z {max_workers} workerów")
        return results

    async def scrape_sequential(self, page: Page, urls: List[str]) -> List[Dict]:
        """Scrapuje ogłoszenia sekwencyjnie używając jednej strony."""
        results = []
        
        for i, url in enumerate(urls, 1):
            ogloszenie_id = self.extract_id(url)
            print(f"[{i}/{len(urls)}] [ID{ogloszenie_id}] Scrapuję...")
            
            result = await self.scrape_ogloszenie_detail(page, url)
            if result:
                results.append(result)
                print(f"  ✅ [ID{ogloszenie_id}] {result['title'][:40]}... | {result['cena_mies']} zł")
            else:
                print(f"  ❌ [ID{ogloszenie_id}] Nie udało się scrapować")
            
            await asyncio.sleep(0.3)  # Pauza między ogłoszeniami
        
        return results

    def save_to_csv(self, data: List[Dict], filename: str = "otodom_raw.csv"):
        """Zapisuje dane do CSV."""
        if not data:
            print("❌ Brak danych do zapisania")
            return
        
        fieldnames = ["id", "url", "title", "cena_mies", "lokalizacja_raw"]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f"💾 Zapisano {len(data)} ogłoszeń do {filename}")


async def main():
    scraper = OtodomScraper()
    print("🚀 Rozpoczynam scrapowanie Otodom...")
    
    start_time = time.time()
    results = await scraper.scrape_ogloszenia(max_ogloszenia=20)
    end_time = time.time()
    
    if results:
        scraper.save_to_csv(results)
        print(f"✅ Zakończono! Pobrano {len(results)} ogłoszeń w {end_time - start_time:.1f}s")
    else:
        print("❌ Nie udało się pobrać żadnych ogłoszeń")


if __name__ == "__main__":
    asyncio.run(main())