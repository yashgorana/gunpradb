import argparse
import asyncio
import json
import re
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from playwright.async_api import BrowserContext, Page, async_playwright

URLS = {
    "BH-HG": "https://bandai-hobby.net/brand/hg/",
    "BH-RG": "https://bandai-hobby.net/brand/rg/",
    "BH-MG": "https://bandai-hobby.net/brand/mg/",
    "BH-MGSD": "https://bandai-hobby.net/brand/mgsd/",
    "BH-MGKA": "https://bandai-hobby.net/brand/mgka/",
    "BH-MGEX": "https://bandai-hobby.net/brand/mgex/",
    "BH-EG": "https://bandai-hobby.net/brand/entry_grade_g/",
    "BH-OPS": "https://bandai-hobby.net/brand/optionpartsset/",
    "BH-FM": "https://bandai-hobby.net/brand/fullmechanics/",
    # p-bandai
    "BHPB-ALL": "https://bandai-hobby.net/brand/pb_gunpla/",
    "BHPB-RG": "https://bandai-hobby.net/brand/pb_rg/",
    "BHPB-HG": "https://bandai-hobby.net/brand/pb_hg/",
    "BHPB-PG": "https://bandai-hobby.net/brand/pb_pg/",
    "BHPB-MG": "https://bandai-hobby.net/brand/pb_mg/",
}

REGX_PRICE_VALUE = re.compile(
    r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*(?=円|yen)", re.IGNORECASE
)
REGX_ANY_NUMBER = re.compile(r"[0-9][0-9,]*(?:\.[0-9]+)?")
REGX_RELEASE_DATE_JP = re.compile(
    r"(\d{4})\s*年\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?"
)
REGX_JAPANESE = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]")
FULLWIDTH_TO_ASCII = str.maketrans("０１２３４５６７８９，．", "0123456789,.")

# product card selectors
SEL_PRODUCT_LINK = "a.c-card.p-card"
SEL_TITLE = ".p-card__tit"
SEL_PRICE = ".p-card__price"
SEL_RELEASE_DATE = ".p-card__date, .p-card_date"

# pagination selectors
SEL_PAGER_ITEMS = ".p-pagination__list"
SEL_LAST_PAGE_LINK = (
    ".p-pagination__list:last-of-type a.c-archives__pagination-list-item-link"
)
SEL_NEXT_LIST_LINKS = ".p-pagination__nextList a"

NAV_TIMEOUT_MS = 30_000
DEFAULT_TIMEOUT_MS = 20_000
PAGE_SLEEP_SEC = 1
SEARCH_CHECKPOINTS_DIR = Path("./data/state")
TAX_MULTIPLIER = Decimal("1.10")


async def wait_for_products(page: Page) -> None:
    await page.wait_for_selector(SEL_PRODUCT_LINK)
    await page.wait_for_selector(f"{SEL_PRODUCT_LINK} {SEL_TITLE}")
    await page.wait_for_selector(f"{SEL_PRODUCT_LINK} {SEL_PRICE}")


def parse_price(text: str) -> float | None:
    normalized_text = (text or "").translate(FULLWIDTH_TO_ASCII)
    matches = REGX_PRICE_VALUE.findall(normalized_text)
    if matches:
        try:
            # Prefer the largest currency-tagged number to avoid capturing tax rates like "10%".
            return max(float(value.replace(",", "")) for value in matches)
        except ValueError:
            return None

    fallback_matches = REGX_ANY_NUMBER.findall(normalized_text)
    if fallback_matches:
        try:
            return max(float(value.replace(",", "")) for value in fallback_matches)
        except ValueError:
            return None

    return None


def to_pre_tax_yen(price_including_tax: float | None) -> int | None:
    if price_including_tax is None:
        return None
    # Bandai listing prices are tax-inclusive (e.g., 税10%込), store pre-tax MSRP.
    pretax = (Decimal(str(price_including_tax)) / TAX_MULTIPLIER).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return int(pretax)


def parse_release_date(text: str) -> str | None:
    normalized_text = (text or "").translate(FULLWIDTH_TO_ASCII).strip()
    if not normalized_text:
        return None

    match = REGX_RELEASE_DATE_JP.search(normalized_text)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3) or "1")
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z"

    return normalized_text


def has_japanese(text: str) -> bool:
    return bool(REGX_JAPANESE.search(text or ""))


def normalize_title(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


async def last_page_from_pager(page: Page) -> int:
    last_page_link = page.locator(SEL_LAST_PAGE_LINK).first
    if await last_page_link.count() > 0:
        txt = ((await last_page_link.text_content()) or "").strip()
        match = re.search(r"\d+", txt)
        if match:
            return int(match.group(0))

    # Fallback in case markup differs on some pages.
    items = page.locator(SEL_PAGER_ITEMS)
    n = await items.count()
    max_page = 1
    for i in range(n):
        txt = ((await items.nth(i).text_content()) or "").strip()
        match = re.search(r"\d+", txt)
        if match:
            max_page = max(max_page, int(match.group(0)))
    return max_page


async def extract_bandai_search_results(page: Page) -> list[dict]:
    return await page.eval_on_selector_all(
        SEL_PRODUCT_LINK,
        """(cards) => cards.map(card => {
            const collapse = (value) => (value || "").replace(/\\s+/g, " ").trim();
            const titleNode = card.querySelector(".p-card__tit");
            const priceNode = card.querySelector(".p-card__price");
            const releaseNode = card.querySelector(".p-card__date, .p-card_date");
            return {
                title: collapse(titleNode?.textContent),
                href: collapse(card.getAttribute("href")),
                priceText: collapse(priceNode?.textContent),
                releaseDateText: collapse(releaseNode?.textContent),
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


async def find_next_link(page: Page):
    next_link = page.locator(SEL_NEXT_LIST_LINKS).first
    if await next_link.count() == 0:
        return None
    return next_link


async def scrape_bandai_search_page(
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
        await wait_for_products(page)

        rows = []
        checkpoint_hit = False
        raw_items = await extract_bandai_search_results(page)
        for item in raw_items:
            title = item.get("title", "").strip()
            href = item.get("href", "").strip()
            price_text = item.get("priceText", "").strip()
            release_date_text = item.get("releaseDateText", "").strip()
            item_url = urljoin("https://bandai-hobby.net", href) if href else ""

            if page_number == 1 and not latest_item_url and item_url:
                latest_item_url = item_url

            if checkpoint_url and item_url == checkpoint_url:
                checkpoint_hit = True
                break

            if item_url in seen:
                continue
            seen.add(item_url)

            listed_price_jpy = parse_price(price_text)
            msrp_jpy = to_pre_tax_yen(listed_price_jpy)
            release_date = parse_release_date(release_date_text)
            if not all([title, href]):
                print(
                    f"ERROR: Missing fields page {page_number} - "
                    f"title={bool(title)}, href={bool(href)}, price={bool(price_text)}"
                )
                continue

            rows.append(
                {
                    "titleJP": title,
                    "url": item_url,
                    "msrpJPY": msrp_jpy,
                    "releaseDate": release_date,
                }
            )

        saved_count = write_rows(fd, rows)
        print(f"[{name}] page {page_number}/{max_pages}: wrote {saved_count} items")

        if checkpoint_hit:
            print(f"[{name}] reached checkpoint on page {page_number}, stopping early")
            break
        if page_number >= max_pages:
            break

        next_link = await find_next_link(page)
        if not next_link:
            break

        next_href_raw = ((await next_link.get_attribute("href")) or "").strip()
        next_href_abs = ((await next_link.evaluate("el => el.href")) or "").strip()
        next_url = next_href_abs or urljoin(page.url, next_href_raw)

        if not next_url:
            print(f"[{name}] no next URL found; stopping pagination")
            break
        if next_url == page.url:
            print(f"[{name}] next URL equals current page; stopping pagination")
            break

        await page.goto(next_url, wait_until="networkidle")
        await asyncio.sleep(page_sleep_sec)
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
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        context.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
        await context.route("**/*", route_handler)
        try:
            return await scrape_bandai_search_page(
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
    parser = argparse.ArgumentParser(description="Scrape Bandai Hobby Gunpla listings.")
    parser.add_argument(
        "--grade", default="BHPB-PG", help="Grade key (e.g. BH-HG, BH-MG)."
    )
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
