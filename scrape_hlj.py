import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Iterable

from playwright.async_api import BrowserContext, Page, async_playwright

URLS = {
    "PG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Perfect+Grade+Kits&MacroType2=Perfect-Grade+Kits&Sort=rss+desc",
    "MG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Master+Grade+Kits&MacroType2=Master-Grade+Kits&Sort=rss+desc",
    "RG": "https://www.hlj.com/search/?Page=1&GenreCode2=Gundam&MacroType2=Real+Grade+Kits&MacroType2=Real-Grade+Kits&Sort=rss+desc",
    "HG": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&Sort=rss+desc",
    "HGUC": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=High+Grade+Universal+Century&Sort=rss+desc",
    "SDBB": "https://www.hlj.com/search/?Word=sd+gundam&MacroType2=SD+%26+BB+Grade+Kits&Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&Sort=rss+desc",
    "EG": "https://www.hlj.com/search/?Page=1&Word=gundam+entry+grade&MacroType2=Other+Gundam+Kits&Sort=rss+desc",
    # by series
    "HG-SEED": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed&Sort=rss+desc",
    "HG-SEED-DY": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Destiny&Sort=rss+desc",
    "HG-SEED-FM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Seed+Freedom&Sort=rss+desc",
    "HG-WING": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+Wing&Sort=rss+desc",
    "HG-UNI": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Gundam+UC+%28Unicorn%29&Sort=rss+desc",
    "HG-ZETA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Zeta+Gundam&Sort=rss+desc",
    "HG-IBO": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam%3A+Iron-Blooded+Orphans&Sort=rss+desc",
    "HG-CCA": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Char%27s+Counterattack&Sort=rss+desc",
    "HG-WFM": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+The+Witch+From+Mercury&Sort=rss+desc",
    "HG-GQX": "https://www.hlj.com/search/?Page=1&MacroType2=High+Grade+Kits&MacroType2=High-Grade+Kits&GenreCode2=Gundam&SeriesID2=Mobile+Suit+Gundam+GQuuuuuuX&Sort=rss+desc",
}

REGX_YEN_STR = re.compile(r"¥|JPY|\s+|,")

SEL_SEARCH_BLOCK = "div.search-widget-block"
SEL_PRICE_SPAN = "div.price span.bold.stock-left"
SEL_PAGER_LINKS = "ul.pages li a"
SEL_NEXT_TEXT = ">"

NAV_TIMEOUT_MS = 30_000
DEFAULT_TIMEOUT_MS = 20_000
PAGE_SLEEP_SEC = 3.0
SEARCH_CHECKPOINTS_DIR = Path("./data/state")

HLJ_CURRENCY_COOKIE = (
    "%7B%22currencyCode%22%3A%22JPY%22%2C%22currencyName%22%3A%22Japanese%2BYen%22%2C%22"
    "currencyPrecision%22%3A0%2C%22currencyPattern%22%3A%22%C2%A5%25s%22%2C%22currencySymbol"
    "%22%3A%22%C2%A5%22%2C%22tdelta%22%3A%2220260210200101%22%2C%22fallbackCurrencyRate"
    "%22%3A%221%22%2C%22selectedManually%22%3A1%7D"
)


async def wait_for_prices(page: Page) -> None:
    await page.wait_for_selector(SEL_PRICE_SPAN)
    await page.wait_for_function(
        """
        () => {
          const prices = [...document.querySelectorAll("div.price span.bold.stock-left")];
          return prices.length > 0 && prices.every(p => (p.textContent || "").trim().length > 0);
        }
        """
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


def read_urls_jsonl(path: Path) -> set[str]:
    urls = set()
    if not path.exists():
        return urls

    with path.open("r", encoding="utf-8") as fd:
        for line in fd:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            url = row.get("url")
            if isinstance(url, str) and url.strip():
                urls.add(url.strip())

    return urls


def checkpoint_path_for_group(group_key: str) -> Path:
    return SEARCH_CHECKPOINTS_DIR / f"{group_key}.txt"


def load_group_checkpoint(path: Path) -> str | None:
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def save_group_checkpoint(path: Path, checkpoint_url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{checkpoint_url}\n", encoding="utf-8")


async def scrape_hlj_search_page(
    context: BrowserContext,
    name: str,
    url: str,
    fd,
    existing_urls: set[str],
    checkpoint_url: str | None = None,
    page_sleep_sec: float = PAGE_SLEEP_SEC,
) -> str | None:
    print(f"Scraping {name} from '{url}'")
    seen = set(existing_urls)
    latest_item_url = None

    page = await context.new_page()
    await page.goto(url, wait_until="networkidle")

    max_pages = await last_page_from_pager(page)

    page_number = 1
    while page_number <= max_pages:
        await page.wait_for_selector(SEL_SEARCH_BLOCK)
        await wait_for_prices(page)

        rows = []
        checkpoint_hit = False
        raw_items = await extract_hlj_search_results(page)
        for item in raw_items:
            title = item.get("title", "").strip()
            href = item.get("href", "").strip()
            price_text = item.get("priceText", "").strip()
            item_url = f"https://www.hlj.com{href}" if href else ""

            if page_number == 1 and not latest_item_url and item_url:
                latest_item_url = item_url

            if checkpoint_url and item_url == checkpoint_url:
                checkpoint_hit = True
                break

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

        if checkpoint_hit:
            print(f"[{name}] reached checkpoint on page {page_number}, stopping early")
            break

        next_link = page.locator(SEL_PAGER_LINKS, has_text=SEL_NEXT_TEXT).first
        if await next_link.count() == 0 or not await next_link.is_visible():
            break

        await next_link.click()
        await asyncio.sleep(page_sleep_sec)  # required because of how HLJ works
        page_number += 1

    return latest_item_url


async def route_handler(route):
    if route.request.resource_type in ("document", "script", "xhr"):
        await route.continue_()
    else:
        await route.abort()


async def scrape_worker(
    name: str,
    url: str,
    fd,
    existing_urls: set[str],
    checkpoint_url: str | None = None,
    **kwargs,
) -> str | None:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        context.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
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
            return await scrape_hlj_search_page(
                context,
                name,
                url,
                fd,
                existing_urls=existing_urls,
                checkpoint_url=checkpoint_url,
                **kwargs,
            )
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
    checkpoint_path = checkpoint_path_for_group(g_type).absolute()

    existing_urls = read_urls_jsonl(path)
    checkpoint_url = load_group_checkpoint(checkpoint_path)

    print(f"[{g_type}] loaded {len(existing_urls)} existing URLs")
    if checkpoint_url:
        print(f"[{g_type}] checkpoint URL: {checkpoint_url}")

    latest_item_url = None
    with path.open("a", encoding="utf-8") as fd:
        latest_item_url = await scrape_worker(
            g_type,
            url,
            fd,
            existing_urls=existing_urls,
            checkpoint_url=checkpoint_url,
        )

    if latest_item_url:
        save_group_checkpoint(checkpoint_path, latest_item_url)
        print(f"[{g_type}] checkpoint updated: {latest_item_url}")

    print(f"[{g_type}] file saved at '{path}'")


if __name__ == "__main__":
    asyncio.run(main())
