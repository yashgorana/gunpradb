import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path

from playwright.async_api import BrowserContext, Page, async_playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

HLJ_CURRENCY_COOKIE = (
    "%7B%22currencyCode%22%3A%22JPY%22%2C%22currencyName%22%3A%22Japanese%2BYen%22%2C%22"
    "currencyPrecision%22%3A0%2C%22currencyPattern%22%3A%22%C2%A5%25s%22%2C%22currencySymbol"
    "%22%3A%22%C2%A5%22%2C%22tdelta%22%3A%2220260210200101%22%2C%22fallbackCurrencyRate"
    "%22%3A%221%22%2C%22selectedManually%22%3A1%7D"
)

REGX_YEN_STR = r"¥|JPY|\s+|,"
REGX_RELEASE_DATE = re.compile(r"^Release Date:\s*(\d{4}/\d{2}/\d{2})$", re.IGNORECASE)
REGX_SERIES = re.compile(r"^Series:\s*(.+)$", re.IGNORECASE)
REGX_ITEM_TYPE = re.compile(r"^Item Type:\s*(.+)$", re.IGNORECASE)
REGX_SIZE_WEIGHT = re.compile(
    r"^Item Size/Weight:\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*x\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*x\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*(kg|g)",
    re.IGNORECASE,
)

CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "30000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
DEFAULT_TIMEOUT_MS = 20_000


def collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_rfc3339(date_text):
    if not date_text:
        return None

    try:
        date_value = datetime.strptime(date_text, "%Y/%m/%d")
        return date_value.strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return None


async def extract_hlj_details_section(page: Page) -> dict:
    release_date = series = item_type = None
    l_val = w_val = h_val = weight = None

    await page.wait_for_selector("div.product-details")
    items_locator = page.locator("div.product-details ul li")
    if await items_locator.count() == 0:
        return {}
    items = await items_locator.all_inner_texts()

    for item_text in items:
        if item_text.startswith("Release Date"):
            release_match = REGX_RELEASE_DATE.search(item_text)
            release_date = parse_rfc3339(release_match.group(1))
        elif item_text.startswith("Series"):
            series_match = REGX_SERIES.search(item_text)
            series = series_match.group(1)
        elif item_text.startswith("Item Type"):
            type_match = REGX_ITEM_TYPE.search(item_text)
            item_type = type_match.group(1)
        elif item_text.startswith("Item Size/Weight"):
            size_weight_match = REGX_SIZE_WEIGHT.search(item_text)
            l_val, w_val, h_val, weight_val, weight_unit = size_weight_match.groups()
            l_val = float(l_val)
            w_val = float(w_val)
            h_val = float(h_val)
            weight = float(weight_val)
            if weight_unit.lower() == "kg":
                weight *= 1000

    return {
        "releaseDate": release_date,
        "series": series,
        "type": item_type,
        "dimL": l_val,
        "dimW": w_val,
        "dimH": h_val,
        "weight": weight,
    }


async def extract_hlj_details_price(page: Page) -> dict:
    price_text = await page.locator("p.price.product-margin").evaluate(
        "node => node.childNodes[0].textContent"
    )

    price = float(re.sub(REGX_YEN_STR, "", price_text))

    assert price < 100_000, "whoa there"
    return {
        "msrpJPY": price,
    }


async def extract_hlj_details_title(page: Page) -> dict:
    title = await page.locator("h2.page-title").evaluate("node => node.textContent")
    return {
        "name": title.strip(),
    }


async def extract_hlj_details_page(page: Page) -> dict:
    title = await extract_hlj_details_title(page)
    price_data = await extract_hlj_details_price(page)
    details = await extract_hlj_details_section(page)
    return {
        "url": page.url,
        **title,
        **price_data,
        **details,
    }


async def scrape_one_url(
    context: BrowserContext,
    semaphore: asyncio.Semaphore,
    url: str,
) -> dict:
    async with semaphore:
        page = await context.new_page()
        try:
            for attempt in range(MAX_RETRIES + 1):
                try:
                    response = await page.goto(url, wait_until="domcontentloaded")

                    if response and response.status != 200:
                        return {"error": response.status, "url": url}

                    result = await extract_hlj_details_page(page)
                    if result:
                        return result
                    break
                except (PlaywrightTimeoutError, PlaywrightError):
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(2**attempt)
                        continue
                    break
            return {"error": "parse_failed", "url": url}
        except Exception as exc:
            return {"error": "scrape_exception", "url": url, "message": str(exc)}
        finally:
            await page.close()


async def scrape_worker(urls: set, fd):
    if len(urls) == 0:
        print("No URLs found.")
        return

    print(f"Scraping metadata for {len(urls)} URLs with {CONCURRENCY} workers")

    async def route_handler(route):
        if route.request.resource_type == "document":
            await route.continue_()
        else:
            await route.abort()

    async with async_playwright() as p:
        done = 0
        ok = 0
        err = 0
        semaphore = asyncio.Semaphore(CONCURRENCY)

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
            tasks = [
                asyncio.create_task(scrape_one_url(context, semaphore, url))
                for url in urls
            ]

            for future in asyncio.as_completed(tasks):
                result = await future
                done += 1
                if "error" in result:
                    err += 1
                    print(f"ERROR: {result}")
                    continue

                ok += 1
                fd.write(json.dumps(result, sort_keys=True, ensure_ascii=False) + "\n")

                if done % 10 == 0 or done == len(urls):
                    fd.flush()
                    print(f"Progress: {done}/{len(urls)} (ok={ok}, err={err})")
        except Exception as e:
            print(f"ERROR: {e}")
        finally:
            for task in tasks:
                task.cancel()

            fd.flush()
            await context.close()
            await browser.close()


def read_data_jsonl(path: Path) -> set:
    urls = set()
    if not path.exists():
        return urls

    with path.open("r", encoding="utf-8") as fd:
        for line in fd:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            url = record.get("url")
            if isinstance(url, str) and url.strip():
                urls.add(url.strip())
    return urls


async def main():
    fd = None
    urls = set()
    input_data_dir = Path("./data/raw/")
    output_metadata_file = Path("./data/data.jsonl")
    output_metadata_file.parent.mkdir(parents=True, exist_ok=True)

    # read all urls
    for path in sorted(input_data_dir.glob("*.jsonl")):
        urls.update(read_data_jsonl(path))
    scraped_urls = read_data_jsonl(output_metadata_file)
    pending_urls = urls - scraped_urls

    print(f"Discovered {len(urls)} URLs in raw inputs")
    print(f"Already scraped {len(scraped_urls)} URLs")
    print(f"Pending scrape count: {len(pending_urls)}")

    if len(pending_urls) == 0:
        print("No new URLs found.")
        return
    try:
        fd = output_metadata_file.open("a", encoding="utf-8")
        await scrape_worker(pending_urls, fd)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exiting cleanly.")
    except Exception as e:
        print(f"Error scraping metadata: {e}")
    finally:
        fd and fd.close()


if __name__ == "__main__":
    asyncio.run(main())
