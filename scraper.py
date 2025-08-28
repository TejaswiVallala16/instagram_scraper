# scraper_100.py
import asyncio, random, json, re, time, argparse
from pathlib import Path
from typing import List, Dict, Any, Set
from playwright.async_api import async_playwright

PERSISTENCE_DIR = ".ig_session"
BRAND_USER = "sherrihill"
START_PROFILE = "https://www.instagram.com/sherrihill/"
RAW_OUT = "raw_sherrihill_rows.jsonl"
TARGET_COUNT = 120  # slightly above 100 to survive cleaning

HASHTAGS = [
    "sherrihill", "sherrihillprom", "sherrihillcouture",
    "sherrihill2025", "sherrihilldress", "sherrihillstyle",
    "sherrihillpageant", "sherrihillhomecoming", "sherrihillgown"
]

COLLAB_WORDS = [
    "Collaboration","Paid partnership","Paid Partnership","sponsored",
    "ad","partnered","in partnership with","gifted","paid","spon"
]

def log(msg): print(msg, flush=True)

async def human_delay(a=0.25, b=1.1):
    await asyncio.sleep(random.uniform(a, b))

async def gently_scroll(page, steps=18, px=1700):
    for _ in range(steps):
        await page.mouse.wheel(0, px)
        await human_delay(0.35, 0.9)

async def open_ctx(headless=False):
    p = await async_playwright().start()
    ctx = await p.chromium.launch_persistent_context(
        user_data_dir=PERSISTENCE_DIR,
        headless=headless,
        viewport={"width": 1360, "height": 900},
        args=["--disable-blink-features=AutomationControlled","--no-sandbox"],
    )
    return p, ctx

async def ensure_logged_in(ctx, pause_seconds=600, login_only=False):
    page = await ctx.new_page()
    await page.goto("https://www.instagram.com/accounts/login/", wait_until="load")
    # try cookie accept
    try:
        for text in ["Allow essential cookies", "Accept all", "Accept All"]:
            btn = await page.get_by_role("button", name=text).first
            if await btn.count() > 0:
                await btn.click()
                break
    except: pass
    if login_only:
        log(f"[Login] Please log in. Waiting up to {pause_seconds//60} minutes…")
        await page.wait_for_timeout(pause_seconds * 1000)
        await page.close()
        return
    # brief pause on normal runs
    await page.wait_for_timeout(5000)
    await page.close()

async def extract_influencer_from_header(page) -> str:
    # Prefer non-brand handle from header anchors
    try:
        anchors = await page.query_selector_all("header a[href^='/'][href$='/']")
        users = []
        for a in anchors:
            href = (await a.get_attribute("href")) or ""
            parts = [p for p in href.strip("/").split("/") if p]
            if parts: users.append(parts[0])
        for u in users:
            if u.lower() != BRAND_USER:
                return u
    except: pass

    # JSON-LD / meta / body fallbacks
    try:
        ld = await page.query_selector("script[type='application/ld+json']")
        if ld:
            js = await ld.inner_text()
            data = json.loads(js)
            def pick(d):
                if not isinstance(d, dict): return ""
                cand = d.get("alternateName") or d.get("name") or d.get("identifier")
                return str(cand).lstrip("@") if cand else ""
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        u = pick(item.get("author"))
                        if u and u.lower() != BRAND_USER: return u
            elif isinstance(data, dict):
                u = pick(data.get("author"))
                if u and u.lower() != BRAND_USER: return u
    except: pass

    try:
        meta = await page.query_selector("meta[property='og:description']")
        if meta:
            content = (await meta.get_attribute("content")) or ""
            m = re.search(r"@([A-Za-z0-9._]+)", content)
            if m and m.group(1).lower() != BRAND_USER: return m.group(1)
    except: pass

    try:
        body = await page.inner_text("body")
        m = re.search(r"@([A-Za-z0-9._]+)", body)
        if m and m.group(1).lower() != BRAND_USER: return m.group(1)
    except: pass
    return ""

def clean_caption(text: str) -> str:
    if not text: return ""
    return " ".join(text.split()).strip()

async def extract_metrics(page) -> Dict[str, Any]:
    out = {"likes": 0, "comments": 0, "views": 0, "caption": ""}
    # caption
    try:
        cap_el = await page.query_selector("h1, div[role='dialog'] h1")
        if cap_el:
            out["caption"] = clean_caption(await cap_el.inner_text())
        else:
            cap_any = await page.query_selector("ul li div[dir='auto']")
            if cap_any:
                out["caption"] = clean_caption(await cap_any.inner_text())
    except: pass

    # metrics
    txt = ""
    try:
        els = await page.query_selector_all("section [role='button'], section span, section div")
        parts = []
        for el in els:
            t = (await el.inner_text()).strip()
            if t: parts.append(t)
        txt = " | ".join(parts)
    except: pass

    def grab(pattern):
        m = re.search(pattern, txt, flags=re.I)
        return m.group(1) if m else 0

    likes = grab(r"([\d\.,]+)\s+likes?")
    views = grab(r"([\d\.,]+)\s+views?")
    comments = grab(r"([\d\.,]+)\s+comments?")
    out["likes"], out["views"], out["comments"] = likes or 0, views or 0, comments or 0

    # meta fallback for likes/comments
    try:
        if (not likes or not comments):
            meta = await page.query_selector("meta[property='og:description']")
            if meta:
                content = (await meta.get_attribute("content")) or ""
                ml = re.search(r"([\d\.,]+)\s+Likes?", content, flags=re.I)
                mc = re.search(r"([\d\.,]+)\s+Comments?", content, flags=re.I)
                if ml and not likes: out["likes"] = ml.group(1)
                if mc and not comments: out["comments"] = mc.group(1)
    except: pass
    return out

async def extract_profile(ctx, username: str) -> Dict[str, Any]:
    out = {"followers": 0, "following": 0, "total_posts": 0}
    if not username: return out
    page = await ctx.new_page()
    try:
        await page.goto(f"https://www.instagram.com/{username}/", wait_until="load")
        await asyncio.sleep(2.2)
        body = await page.inner_text("body")
        def find(label):
            m = re.search(rf"([\d\.,]+)\s+{label}", body, flags=re.I)
            return m.group(1) if m else 0
        out["total_posts"] = find("posts")
        out["followers"] = find("followers")
        out["following"] = find("following")
    except: pass
    finally:
        await page.close()
    return out

async def discover_from_profile(ctx, url: str, limit=500) -> List[str]:
    page = await ctx.new_page()
    urls = []
    try:
        await page.goto(url, wait_until="load")
        await gently_scroll(page, steps=18)
        anchors = await page.query_selector_all("a[href*='/p/'], a[href*='/reel/']")
        for a in anchors:
            href = await a.get_attribute("href")
            if href and href.startswith("/"):
                urls.append("https://www.instagram.com" + href)
    except: pass
    finally:
        await page.close()
    # uniq, trim
    return list(dict.fromkeys(urls))[:limit]

async def discover_tagged(ctx, profile="sherrihill", limit=900) -> List[str]:
    return await discover_from_profile(ctx, f"https://www.instagram.com/{profile}/tagged/", limit)

async def discover_hashtag(ctx, tag="sherrihill", limit=900) -> List[str]:
    return await discover_from_profile(ctx, f"https://www.instagram.com/explore/tags/{tag}/", limit)

async def login_only():
    p, ctx = await open_ctx(headless=False)
    try:
        await ensure_logged_in(ctx, pause_seconds=90, login_only=True)
        print("[Login] Session saved in .ig_session/")
    finally:
        await ctx.close(); await p.stop()

async def run_scrape(target=TARGET_COUNT, headless=False):
    p, ctx = await open_ctx(headless=headless)
    try:
        await ensure_logged_in(ctx, pause_seconds=5, login_only=False)

        log("Discovering candidate posts…")
        urls: Set[str] = set()
        # Brand grid (sometimes co-authors)
        urls.update(await discover_from_profile(ctx, START_PROFILE, limit=500))
        # Tagged (core source)
        urls.update(await discover_tagged(ctx, "sherrihill", limit=900))
        # Hashtags (broad source)
        for tag in HASHTAGS:
            urls.update(await discover_hashtag(ctx, tag, limit=900))

        log(f"Discovered {len(urls)} unique URLs. Scanning for influencers…")

        seen_links = set()
        have_influencers: Set[str] = set()
        out_path = Path(RAW_OUT)
        out_path.touch(exist_ok=True)

        # Load existing to resume
        try:
            for line in out_path.read_text(encoding="utf-8").splitlines():
                if not line.strip(): continue
                row = json.loads(line)
                seen_links.add(row.get("post_reel_link",""))
                u = (row.get("influencer_username") or "").lower()
                if u and u != BRAND_USER: have_influencers.add(u)
        except: pass

        for url in list(urls):
            if url in seen_links:
                continue

            page = await ctx.new_page()
            try:
                await page.goto(url, wait_until="load")
                await human_delay(0.7, 1.8)

                # Skip broken/private
                body_txt = await page.inner_text("body")
                if any(b in body_txt for b in ["This page isn't available", "private"]):
                    await page.close(); continue

                username = await extract_influencer_from_header(page)
                metrics = await extract_metrics(page)

                # Accept only non-brand influencers
                if not username or username.lower() == BRAND_USER:
                    await page.close(); continue

                prof = await extract_profile(ctx, username)

                data = {
                    "post_reel_link": url,
                    "influencer_username": username,
                    "likes": metrics["likes"],
                    "comments": metrics["comments"],
                    "views": metrics["views"],
                    "caption": metrics["caption"],
                    "followers": prof["followers"],
                    "following": prof["following"],
                    "total_posts": prof["total_posts"],
                    "is_reel": "/reel/" in url,
                }

                with out_path.open("a", encoding="utf-8") as fw:
                    fw.write(json.dumps(data, ensure_ascii=False) + "\n")

                have_influencers.add(username.lower())
                seen_links.add(url)
                log(f"Saved: {username} | {url} | followers={prof['followers']} following={prof['following']}")

                if len(have_influencers) >= target:
                    log(f"Reached target of {target} unique influencers.")
                    break

            except Exception as e:
                log(f"Scan error: {url} : {e}")
            finally:
                await page.close()

        log(f"Done. Raw written to {out_path.resolve()}")
    finally:
        await ctx.close(); await p.stop()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--login-only", action="store_true")
    ap.add_argument("--target", type=int, default=TARGET_COUNT, help="Minimum unique influencers to collect")
    ap.add_argument("--headless", action="store_true")
    args = ap.parse_args()

    if args.login_only:
        asyncio.run(login_only())
    else:
        asyncio.run(run_scrape(target=args.target, headless=args.headless))
