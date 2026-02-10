import asyncio
import json
import os
import re

from playwright.async_api import async_playwright

URLS = {
    "PG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Perfect+Grade+Kits&MacroType2=Perfect-Grade+Kits",
    "MG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Master+Grade+Kits&MacroType2=Master-Grade+Kits",
    "RG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Real+Grade+Kits&MacroType2=Real-Grade+Kits",
    "HG": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam",
    "HGUC": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=High+Grade+Universal+Century",
    "SDBB": "https://www.hlj.com/search/?Word=sd+gundam&MacroType2=SD+%26+BB+Grade+Kits&Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits",
    "EG": "https://www.hlj.com/search/?Page=1&Word=gundam+entry+grade&MacroType2=Other+Gundam+Kits",
    # by series
    "HG-SEED": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed",
    "HG-SEED-DY": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Destiny",
    "HG-SEED-FM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Freedom",
    "HG-WING": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Wing",
    "HG-UNI": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+UC+%28Unicorn%29",
    "HG-ZETA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Zeta+Gundam",
    "HG-IBO": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam%3A+Iron-Blooded+Orphans",
    "HG-CCA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Char%27s+Counterattack",
    "HG-WFM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+The+Witch+From+Mercury",
    "HG-GQX": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+GQuuuuuuX",
}

REGX_YEN_STR = r"¥|JPY|\s+|,"

HLJ_CURRENCY_COOKIE = (
    "%7B%22currencyCode%22%3A%22JPY%22%2C%22currencyName%22%3A%22Japanese%2BYen%22%2C%22"
    "currencyPrecision%22%3A0%2C%22currencyPattern%22%3A%22%C2%A5%25s%22%2C%22currencySymbol"
    "%22%3A%22%C2%A5%22%2C%22tdelta%22%3A%2220260210200101%22%2C%22fallbackCurrencyRate"
    "%22%3A%221%22%2C%22selectedManually%22%3A1%7D"
)


async def wait_for_prices(page):
    await page.wait_for_selector("div.price span.bold.stock-left", timeout=20_000)
    await page.wait_for_function(
        """
        () => {
          const prices = [...document.querySelectorAll("div.price span.bold.stock-left")];
          return prices.length > 0 && prices.every(p => (p.textContent || "").trim().length > 0);
        }
        """,
        timeout=20_000,
    )


async def last_page_from_pager(page) -> int:
    links = page.locator("ul.pages li a")
    n = await links.count()
    max_page = 1
    for i in range(n):
        txt = ((await links.nth(i).text_content()) or "").strip()
        if txt.isdigit():
            max_page = max(max_page, int(txt))
    return max_page


async def scrape_hlj_page(name: str, url: str, fd):
    print(f"Scraping '{url}'")
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies(
            [
                {
                    "name": "hljCurrencyData",
                    "value": HLJ_CURRENCY_COOKIE,
                    "domain": "www.hlj.com",
                    "path": "/",
                    "httpOnly": False,
                    "secure": False,
                    "sameSite": "Lax",
                }
            ]
        )
        page = await context.new_page()
        await page.goto(url, wait_until="networkidle")

        max_pages = await last_page_from_pager(page)

        page_number = 1
        while True:
            await page.wait_for_selector("div.search-widget-block", timeout=20_000)
            await wait_for_prices(page)

            blocks = page.locator("div.search-widget-block")
            block_count = await blocks.count()
            saved_count = 0

            for i in range(block_count):
                block = blocks.nth(i)
                title_link = block.locator("p.product-item-name a").first
                price_span = block.locator("div.price span.bold.stock-left").first

                title = ((await title_link.text_content()) or "").strip()
                item_url = ((await title_link.get_attribute("href")) or "").strip()
                msrp_jpy = ((await price_span.text_content()) or "").strip()

                item_url = f"https://www.hlj.com{item_url}"
                msrp_jpy = int(re.sub(REGX_YEN_STR, "", msrp_jpy))

                if not title or not item_url or not msrp_jpy:
                    print(
                        f"ERROR: Missing fields page {page_number} - title={not not title}, item_url={not not item_url}, msrp_jpy={not not msrp_jpy}"
                    )
                    continue

                if item_url in seen:
                    continue
                seen.add(item_url)

                row = {
                    "title": title,
                    "url": item_url,
                    "msrpJPY": msrp_jpy,
                }
                fd.write(json.dumps(row, ensure_ascii=False) + "\n")
                saved_count += 1

            fd.flush()
            print(f"[{name}] page {page_number}: wrote {saved_count} items")

            next_link = page.locator("ul.pages li a", has_text=">").first
            if await next_link.count() == 0 or not await next_link.is_visible():
                break

            if page_number >= max_pages:
                break

            await next_link.click()
            await asyncio.sleep(3)

            page_number += 1

        await context.close()
        await browser.close()


async def main(name: str, url: str):
    fd = None
    try:
        path = f"./data/{name}.jsonl"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd = open(path, "w", encoding="utf-8")
        await scrape_hlj_page(name, url, fd)
    except Exception as e:
        print(f"Error scraping '{url}': {e}")
    finally:
        if fd is not None:
            fd.close()


if __name__ == "__main__":
    g_type = "SDBB"
    url = URLS.get(g_type)
    assert url is not None
    asyncio.run(main(g_type, url))
