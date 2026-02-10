import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Iterable

from playwright.async_api import BrowserContext, Page, async_playwright

URLS = {
    "PG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Perfect+Grade+Kits&MacroType2=Perfect-Grade+Kits&Sort=releaseDate+desc",
    "MG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Master+Grade+Kits&MacroType2=Master-Grade+Kits&Sort=releaseDate+desc",
    "RG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Real+Grade+Kits&MacroType2=Real-Grade+Kits&Sort=releaseDate+desc",
    "HG": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&Sort=releaseDate+desc",
    "HGUC": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=High+Grade+Universal+Century&Sort=releaseDate+desc",
    "SDBB": "https://www.hlj.com/search/?Word=sd+gundam&MacroType2=SD+%26+BB+Grade+Kits&Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&Sort=releaseDate+desc",
    "EG": "https://www.hlj.com/search/?Page=1&Word=gundam+entry+grade&MacroType2=Other+Gundam+Kits&Sort=releaseDate+desc",
    # by series
    "HG-SEED": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed&Sort=releaseDate+desc",
    "HG-SEED-DY": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Destiny&Sort=releaseDate+desc",
    "HG-SEED-FM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Freedom&Sort=releaseDate+desc",
    "HG-WING": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Wing&Sort=releaseDate+desc",
    "HG-UNI": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+UC+%28Unicorn%29&Sort=releaseDate+desc",
    "HG-ZETA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Zeta+Gundam&Sort=releaseDate+desc",
    "HG-IBO": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam%3A+Iron-Blooded+Orphans&Sort=releaseDate+desc",
    "HG-CCA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Char%27s+Counterattack&Sort=releaseDate+desc",
    "HG-WFM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+The+Witch+From+Mercury&Sort=releaseDate+desc",
    "HG-GQX": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+GQuuuuuuX&Sort=releaseDate+desc",
}

REGX_YEN_STR = re.compile(r"¥|JPY|\s+|,")

SEL_SEARCH_BLOCK = "div.search-widget-block"
SEL_PRICE_SPAN = "div.price span.bold.stock-left"
SEL_PAGER_LINKS = "ul.pages li a"
SEL_NEXT_TEXT = ">"

TIMEOUT_MS = 20_000
PAGE_SLEEP_SEC = 3.0

HLJ_CURRENCY_COOKIE = (
    "%7B%22currencyCode%22%3A%22JPY%22%2C%22currencyName%22%3A%22Japanese%2BYen%22%2C%22"
    "currencyPrecision%22%3A0%2C%22currencyPattern%22%3A%22%C2%A5%25s%22%2C%22currencySymbol"
    "%22%3A%22%C2%A5%22%2C%22tdelta%22%3A%2220260210200101%22%2C%22fallbackCurrencyRate"
    "%22%3A%221%22%2C%22selectedManually%22%3A1%7D"
)


async def wait_for_prices(page: Page, timeout_ms: int) -> None:
    await page.wait_for_selector(SEL_PRICE_SPAN, timeout=timeout_ms)
    await page.wait_for_function(
        """
        () => {
          const prices = [...document.querySelectorAll("div.price span.bold.stock-left")];
          return prices.length > 0 && prices.every(p => (p.textContent || "").trim().length > 0);
        }
        """,
        timeout=timeout_ms,
    )


def parse_price(text: str) -> float | None:
    try:
        return float(REGX_YEN_STR.sub("", text or ""))
    except ValueError:
        return None


async def last_page_from_pager(page: Page) -> int:
    links = page.locator(SEL_PAGER_LINKS)
    n = await links.count()
    max_page = 1
    for i in range(n):
        txt = ((await links.nth(i).text_content()) or "").strip()
        if txt.isdigit():
            max_page = max(max_page, int(txt))
    return max_page


async def extract_hlj_search_results(page: Page) -> list[dict]:
    return await page.eval_on_selector_all(
        SEL_SEARCH_BLOCK,
        """(blocks) => blocks.map(block => {
            const titleLink = block.querySelector("p.product-item-name a");
            const priceSpan = block.querySelector("div.price span.bold.stock-left");
            return {
              title: titleLink?.textContent?.trim() || "",
              href: titleLink?.getAttribute("href") || "",
              priceText: priceSpan?.textContent?.trim() || "",
            };
        })""",
    )


def write_rows(fd, rows: Iterable[dict]) -> int:
    count = 0
    for row in rows:
        fd.write(json.dumps(row, ensure_ascii=False) + "\n")
        count += 1
    fd.flush()
    return count


async def scrape_hlj_search_page(
    context: BrowserContext,
    name: str,
    url: str,
    fd,
    timeout_ms: int = TIMEOUT_MS,
    page_sleep_sec: float = PAGE_SLEEP_SEC,
) -> None:
    print(f"Scraping {name} from '{url}'")
    seen = set()

    page = await context.new_page()
    await page.goto(url, wait_until="networkidle")

    max_pages = await last_page_from_pager(page)

    page_number = 1
    while page_number <= max_pages:
        await page.wait_for_selector(SEL_SEARCH_BLOCK, timeout=timeout_ms)
        await wait_for_prices(page, timeout_ms=timeout_ms)

        rows = []
        raw_items = await extract_hlj_search_results(page)
        for item in raw_items:
            title = item.get("title", "").strip()
            href = item.get("href", "").strip()
            price_text = item.get("priceText", "").strip()
            item_url = f"https://www.hlj.com{href}"

            if item_url in seen:
                continue
            seen.add(item_url)

            msrp_jpy = parse_price(price_text)
            if not all([title, href, price_text]):
                print(
                    f"ERROR: Missing fields page {page_number} - "
                    f"title={bool(title)}, href={bool(href)}, price={bool(price_text)}"
                )
                continue

            rows.append(
                {
                    "title": title,
                    "url": item_url,
                    "msrpJPY": msrp_jpy,
                }
            )

        saved_count = write_rows(fd, rows)
        print(f"[{name}] page {page_number}: wrote {saved_count} items")

        next_link = page.locator(SEL_PAGER_LINKS, has_text=SEL_NEXT_TEXT).first
        if await next_link.count() == 0 or not await next_link.is_visible():
            break

        await next_link.click()
        await asyncio.sleep(page_sleep_sec)  # required because of how HLJ works
        page_number += 1


async def route_handler(route):
    if route.request.resource_type in ("document", "script", "xhr"):
        await route.continue_()
    else:
        await route.abort()


async def scrape_worker(name: str, url: str, fd, **kwargs) -> None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.route("**/*", route_handler)
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
        try:
            await scrape_hlj_search_page(context, name, url, fd, **kwargs)
        finally:
            await context.close()
            await browser.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape HLJ Gunpla listings.")
    parser.add_argument("--grade", default="EG", help="Grade key (e.g. PG, MG, HG).")
    args = parser.parse_args()

    g_type = args.grade.upper()
    url = URLS.get(g_type)
    if url is None:
        raise SystemExit(f"Unknown grade: {g_type}")

    path = Path(f"./data/raw/{g_type}.jsonl").absolute()
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as fd:
        await scrape_worker(
            g_type,
            url,
            fd,
        )

    print(f"[{g_type}] file saved at '{path}'")


if __name__ == "__main__":
    asyncio.run(main())
