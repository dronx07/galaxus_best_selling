import asyncio
import json
import logging
import os
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

load_dotenv()

BASE_URL = "https://www.galaxus.fr/graphql/o/0d495a96a89b5e2464b59c9d84edd325/productListSectorRetailPreloadProductsQuery"
DETAIL_URL = "https://www.galaxus.fr/graphql/o/04212ca368d942fcfac81f49a5bc7e87/productDetailPageQuery"

SITE = "https://www.galaxus.fr"

CAT = [
    "it-multimedia",
    "home-kitchen",
    "interior",
    "diy-garden",
    "sports",
    "toys",
    "health-beauty",
    "office-stationary"
]

TARGET = 10000
CONCURRENCY = 25
PROXY = os.getenv("PROXY")
RETRIES = 3

HEADERS = {
    "Accept": "application/graphql-response+json; charset=utf-8, application/json; charset=utf-8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "Origin": "https://www.galaxus.fr",
    "Pragma": "no-cache",
    "Priority": "u=1, i",
    "Referer": "https://www.galaxus.fr/",
    "Sec-Ch-Ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "Sec-Ch-Ua-Arch": "x86",
    "Sec-Ch-Ua-Bitness": "64",
    "Sec-Ch-Ua-Full-Version": "146.0.7680.80",
    "Sec-Ch-Ua-Full-Version-List": '"Chromium";v="146.0.7680.80", "Not-A.Brand";v="24.0.0.0", "Google Chrome";v="146.0.7680.80"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Model": '""',
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Ch-Ua-Platform-Version": '"19.0.0"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "X-Dg-Graphql-Client-Name": "isomorph",
    "X-Dg-Language": "en-US",
    "X-Dg-Portal": "32",
    "X-Dg-Routename": "/sector/[titleAndSectorId]",
    "X-Dg-Routeowner": "stellapolaris",
    "X-Dg-Team": "endeavour",
}

TEMPLATE = {
    "it-multimedia": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjE=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmkx",
        "asPath": "/en/s1/sector/it-multimedia-1"
    },
    "home-kitchen": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjI=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmky",
        "asPath": "/en/s2/sector/home-kitchen-2"
    },
    "interior": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjE0",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmkxNA==",
        "asPath": "/en/s14/sector/interior-14"
    },
    "diy-garden": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjQ=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmk0",
        "asPath": "/en/s4/sector/diy-garden-4"
    },
    "sports": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjM=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmkz",
        "asPath": "/en/s3/sector/sports-3"
    },
    "toys": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjU=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmk1",
        "asPath": "/en/s5/sector/toys-5"
    },
    "health-beauty": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjY=",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmk2",
        "asPath": "/en/s6/sector/health-beauty-6"
    },
    "office-stationary": {
        "id": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEy",
        "sortOrder": "LOWEST_PRICE",
        "first": 60,
        "sectorId": "U2VjdG9yCmkxMg==",
        "asPath": "/en/s12/sector/office-stationery-12"
    }
}

def get_iso_timestamp(months_offset=0):
    dt = datetime.now(timezone.utc)
    if months_offset:
        dt = dt - relativedelta(months=months_offset)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def load_state():
    try:
        with open("state.json", "r") as f:
            return json.loads(f.read()).get("state", 0)
    except Exception as e:
        logging.warning(e)
        return 0

def save_state(v):
    with open("state.json", "w") as f:
        json.dump({"state": v}, f)

def build_payload(cat, cursor):
    base = TEMPLATE[cat]
    return {
        "variables": {
            "id": base["id"],
            "sortOrder": base["sortOrder"],
            "first": base["first"],
            "after": cursor,
            "sectorId": base["sectorId"],
            "asPath": base["asPath"]
        }
    }

def extract_products(data):
    try:
        edges = data["data"]["navigationItemById"]["products"]["edges"]
    except Exception as e:
        logging.warning(e)
        return [], None, False

    results = []

    for e in edges:
        try:
            n = e["node"]
            results.append({
                "product_name": n["name"],
                "supplier_price": n["price"]["amountInclusive"],
                "product_link": SITE + n["relativeUrl"],
                "slug": n["relativeUrl"],
                "product_gtin": None
            })
        except Exception as e:
            logging.warning(e)
            continue

    try:
        pi = data["data"]["navigationItemById"]["products"]["pageInfo"]
        return results, pi.get("endCursor"), pi.get("hasNextPage", False)
    except Exception as e:
        logging.warning(e)
        return results, None, False

async def post_with_retry(session, url, payload, headers):
    for i in range(RETRIES):
        try:
            r = await session.post(url, json=payload, headers=headers, proxy=PROXY)
            if r is None:
                raise Exception("Response is None")
            if getattr(r, "status_code", 200) != 200:
                logging.warning(f"[HTTP_{r.status_code}] retry={i}")
            return r
        except Exception as e:
            logging.warning(f"[RETRY] attempt={i} err={e}")
            if i == RETRIES - 1:
                logging.error(f"[RETRY_FAIL] {url}")
                return None
            await asyncio.sleep(1)
    return None

async def fetch_page(session, payload, cat, cursor):
    try:
        logging.info(f"[LIST] cat={cat} cursor={cursor}")

        r = await post_with_retry(session, BASE_URL, payload, HEADERS)

        if r is None:
            logging.error(f"[LIST_FAIL] cat={cat} -> response is None")
            return None

        text = r.text
        if isinstance(text, bytes):
            text = text.decode("utf-8", "ignore")

        if not text or text.strip() == "":
            logging.error(f"[EMPTY_RESPONSE] cat={cat} status={getattr(r, 'status_code', 'NA')}")
            return None

        logging.debug(f"[RAW_RESPONSE_SNIPPET] {text[:300]}")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as je:
            logging.error(f"[JSON_ERROR] cat={cat} err={je}")
            logging.error(f"[BAD_RESPONSE_SNIPPET] {text[:500]}")
            return None

        if not isinstance(data, dict) or "data" not in data:
            logging.error(f"[LIST_BLOCK] cat={cat} keys={list(data.keys()) if isinstance(data, dict) else 'NOT_DICT'}")
            return None

        logging.info(f"[LIST_OK] cat={cat}")
        return data

    except Exception as e:
        logging.error(f"[LIST_FAIL] cat={cat} err={e}")
        return None

async def collect_products(session, cat):
    results = []
    cursor = None

    logging.info(f"[START_CATEGORY] {cat}")

    while len(results) < TARGET:
        payload = build_payload(cat, cursor)

        data = await fetch_page(session, payload, cat, cursor)
        if not data:
            break

        items, cursor, has_next = extract_products(data)
        results.extend(items)

        logging.info(f"[COLLECT] cat={cat} total={len(results)} cursor={cursor}")

        if not has_next or not cursor:
            logging.info(f"[END_CATEGORY] {cat}")
            break

    return results[:TARGET]

async def fetch_detail(session, product, sem, i, sector_id):
    async with sem:
        try:
            logging.info(f"[DETAIL_START] {i} {product['product_link']}")
            payload = {
                "variables": {
                    "slug": product["slug"],
                    "offer": None,
                    "shopArea": "RETAIL",
                    "sectorId": sector_id,
                    "tagIds": [],
                    "path": product["slug"],
                    "olderThan3MonthTimestamp": get_iso_timestamp(3),
                    "isSSR": False,
                    "hasTrustLevelLowAndIsNotEProcurement": False,
                    "adventCalendarEnabled": False
                }
            }

            r = await post_with_retry(session, DETAIL_URL, payload, HEADERS)

            if r is None:
                logging.error(f"[DETAIL_FAIL] {i} response None")
                return product

            text = r.text
            if isinstance(text, bytes):
                text = text.decode("utf-8", "ignore")

            if not text or text.strip() == "":
                logging.error(f"[DETAIL_EMPTY] {i} {product['product_link']}")
                return product

            try:
                data = json.loads(text)
            except json.JSONDecodeError as je:
                logging.error(f"[DETAIL_JSON_ERROR] {i} err={je}")
                logging.error(f"[DETAIL_BAD_SNIPPET] {text[:500]}")
                return product

            product["product_gtin"] = (
                data.get("data", {})
                    .get("product", {})
                    .get("gtin")
            )

            logging.info(f"[DETAIL_OK] {i}")

        except Exception as e:
            logging.error(f"[DETAIL_FAIL] {i} {product['product_link']} err={e}")

        return product

async def enrich(products, cat):
    sem = asyncio.Semaphore(CONCURRENCY)
    logging.info(f"[ENRICH_START] {len(products)}")

    sector_id = TEMPLATE[cat]["sectorId"]

    params = {"http_version": "v2", "allow_redirects": True, "timeout": 60}

    async with AsyncSession(impersonate="chrome142", proxy=PROXY, **params) as session:
        tasks = [fetch_detail(session, p, sem, i, sector_id) for i, p in enumerate(products)]
        out = await asyncio.gather(*tasks)

    logging.info("[ENRICH_DONE]")
    return out

def save(products):
    with open("products.json", "w", encoding="utf-8") as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    logging.info(f"[SAVE] {len(products)}")

async def main():
    idx = load_state()
    cat = CAT[idx % len(CAT)]
    params = {"http_version": "v2", "allow_redirects": True, "timeout": 60}

    async with AsyncSession(impersonate="chrome142", proxy=PROXY, **params) as session:
        products = await collect_products(session, cat)

    products = await enrich(products, cat)

    save(products)

    save_state((idx + 1) % len(CAT))

    logging.info("[DONE]")


if __name__ == "__main__":
    asyncio.run(main())
