"""
Command to run :- 
python3 scraper.py --input links.csv --output output.csv --failed failed.csv --email 'samplemail@gmail.com' --username '@username' --password "password"
"""

import asyncio
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time
import os

from playwright.async_api import async_playwright, Browser, Page


@dataclass
class ScrapeResult:
    url: str
    views: Optional[int]
    likes: Optional[int]
    shares: Optional[int]
    comments: Optional[int]
    status: str
    error: Optional[str]
    posted_at: Optional[str]  # ISO datetime if available
    caption: Optional[str]    # tweet text/caption


NUMBER_RE = re.compile(r"([0-9][0-9,\.\s]*)([kKmMbB]?)")


def normalize_url_to_mobile(url: str) -> str:
    url = url.strip()
    if not url:
        return url
    
    # Add https:// if missing protocol
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    
    if url.startswith("http://"):
        url = "https://" + url[len("http://") :]
    
    url_lower = url.lower()
    if "twitter.com/" in url_lower or "x.com/" in url_lower or "mobile.twitter.com/" in url_lower:
        try:
            after_proto = url.split("//", 1)[1]
            parts = after_proto.split("/", 1)
            path_and_query = parts[1] if len(parts) > 1 else ""
            return f"https://mobile.twitter.com/{path_and_query}"
        except Exception:
            return url
    return url


def parse_compact_number(text: str) -> Optional[int]:
    if text is None:
        return None
    text = text.strip()
    if not text:
        return None
    m = NUMBER_RE.search(text)
    if not m:
        return None
    num_str, suffix = m.groups()
    num_str = num_str.replace(",", "").replace(" ", "")
    try:
        num = float(num_str)
    except ValueError:
        return None
    mult = 1
    if suffix:
        s = suffix.lower()
        if s == "k":
            mult = 1_000
        elif s == "m":
            mult = 1_000_000
        elif s == "b":
            mult = 1_000_000_000
    return int(round(num * mult))


async def extract_counts_from_dom(page: Page) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int], Optional[str], Optional[str]]:
    """
    Returns: views, likes, shares, comments, posted_at, caption
    """
    try:
        return await page.eval_on_selector(
            "article",
            """
            (root) => {
              function parseNum(s) {
                if (!s) return null;
                const m = s.match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                if (!m) return null;
                let v = m[1].replace(/[ ,]/g, '');
                let n = parseFloat(v);
                if (isNaN(n)) return null;
                const suf = (m[2]||'').toLowerCase();
                let mult = 1;
                if (suf === 'k') mult = 1e3;
                else if (suf === 'm') mult = 1e6;
                else if (suf === 'b') mult = 1e9;
                return Math.round(n * mult);
              }

              function parseViewsNum(s) {
                if (!s) return null;
                const m = s.match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)\\s*views?/i);
                if (m) return parseNum(m[0]);
                const idx = s.toLowerCase().lastIndexOf('views');
                if (idx >= 0) {
                  const window = s.slice(Math.max(0, idx - 40), idx);
                  const ms = window.match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/g);
                  if (ms && ms.length) return parseNum(ms[ms.length - 1]);
                }
                return null;
              }

              const out = {views: null, likes: null, shares: null, comments: null, posted_at: null, caption: null};

              const btnReply = root.querySelector('button[data-testid="reply"]');
              const btnRetweet = root.querySelector('button[data-testid="retweet"]');
              const btnLike = root.querySelector('button[data-testid="like"]');

              function getFromEl(el, fallbackTextLabels) {
                if (!el) return null;
                const aria = el.getAttribute('aria-label') || '';
                const ariaNum = parseNum(aria);
                if (ariaNum !== null) return ariaNum;
                const text = (el.textContent || '').toLowerCase();
                for (const label of fallbackTextLabels) {
                  if (text.includes(label)) {
                    const m = (el.textContent || '').match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                    if (m) return parseNum(m[0]);
                  }
                }
                return null;
              }

              out.comments = getFromEl(btnReply, ['repl', 'comment']);
              out.shares = getFromEl(btnRetweet, ['repost', 'retweet', 'share']);
              out.likes = getFromEl(btnLike, ['like']);

              // Views via aria-label that includes "views"
              if (out.views === null) {
                const ariaCand = Array.from(root.querySelectorAll('[aria-label]'))
                  .map(el => el.getAttribute('aria-label') || '')
                  .find(lbl => /\\bviews?\\b/i.test(lbl) && /[0-9]/.test(lbl));
                if (ariaCand) {
                  const v = parseViewsNum(ariaCand);
                  if (v !== null) out.views = v;
                }
              }

              // Views via combined text
              if (out.views === null) {
                const combined = Array.from(root.querySelectorAll('a, button, span, div'))
                  .find(el => {
                    const t = (el.textContent || '');
                    return /\\bviews?\\b/i.test(t) && /[0-9]/.test(t);
                  });
                if (combined) {
                  const v = parseViewsNum(combined.textContent || '');
                  if (v !== null) out.views = v;
                }
              }

              // Views via neighbor number to label "Views"
              if (out.views === null) {
                const labels = Array.from(root.querySelectorAll('a, button, span, div'))
                  .filter(el => /\\bviews?\\b/i.test(el.textContent || ''));
                for (const el of labels) {
                  const next = el.nextElementSibling;
                  if (next) {
                    const m = (next.textContent || '').match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                    if (m) { out.views = parseNum(m[0]); break; }
                  }
                  const prev = el.previousElementSibling;
                  if (prev) {
                    const m = (prev.textContent || '').match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                    if (m) { out.views = parseNum(m[0]); break; }
                  }
                  const parent = el.parentElement;
                  if (parent) {
                    const tokens = (parent.textContent || '').split(/\\s+/);
                    let lastNum = null;
                    for (let i = 0; i < tokens.length; i++) {
                      if (/^views?$/i.test(tokens[i])) {
                        const after = tokens[i+1] || '';
                        const before = tokens[i-1] || '';
                        const mAfter = after.match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                        const mBefore = before.match(/([0-9][0-9,\\.\\s]*)([kKmMbB]?)/);
                        if (mAfter) { lastNum = parseNum(mAfter[0]); break; }
                        if (mBefore) { lastNum = parseNum(mBefore[0]); break; }
                      }
                    }
                    if (lastNum !== null) { out.views = lastNum; break; }
                  }
                }
              }

              // posted_at from <time datetime="..."> inside the article
              const timeEl = root.querySelector('time');
              if (timeEl) {
                const dt = timeEl.getAttribute('datetime') || '';
                if (dt && /\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}/.test(dt)) {
                  out.posted_at = dt; // usually ISO-8601
                } else {
                  const title = timeEl.getAttribute('title') || timeEl.textContent || '';
                  out.posted_at = title || null;
                }
              } else {
                // Defensive fallback: look near header for any datetime attribute
                const header = root.querySelector('div[role="group"], header, h1, h2, h3') || root;
                const t2 = header && Array.from(header.querySelectorAll('a, span, time')).find(el => {
                  return el.getAttribute && (el.getAttribute('datetime') || '').length > 0;
                });
                if (t2) {
                  out.posted_at = t2.getAttribute('datetime');
                }
              }

              // NEW: extract caption text from the tweet
              const textEls = Array.from(root.querySelectorAll('div[data-testid="tweetText"]'));
              if (textEls.length > 0) {
                const parts = textEls.map(el => (el.innerText || '').trim()).filter(Boolean);
                if (parts.length > 0) {
                  out.caption = parts.join('\\n');
                }
              }

              return [out.views, out.likes, out.shares, out.comments, out.posted_at, out.caption];
            }
            """,
        )
    except Exception:
        return (None, None, None, None, None, None)


async def login_and_save_state(context, headless: bool, email: str, username: str, password: str, state_path: Path, timeout_ms: int = 45000) -> Tuple[bool, Optional[str]]:
    # email used on first screen; username used only for identifier challenge if prompted
    page = await context.new_page()
    try:
        print("[login] Navigating to login flow...")
        await page.goto("https://x.com/i/flow/login", wait_until="domcontentloaded", timeout=timeout_ms)

        # Try dismissing cookie/consent banners early if present
        try:
            for label in ["Accept all", "Accept", "I agree", "OK", "Got it"]:
                btn = page.get_by_role("button", name=re.compile(label, re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    break
        except Exception:
            pass

        print("[login] Waiting for email field...")
        await page.wait_for_selector('input[name="text"], input[autocomplete="username"], [data-testid="ocfEnterTextTextInput"] input, [data-testid="ocfEnterTextTextInput"]', timeout=30000)
        id_input = page.locator('input[name="text"], input[autocomplete="username"], [data-testid="ocfEnterTextTextInput"] input').first

        # Always try to use email on the first screen (fallback to username only if email missing)
        first_identifier = email if email else username
        try:
            await id_input.click(timeout=5000)
            try:
                await page.keyboard.press("Meta+A")
            except Exception:
                await page.keyboard.press("Control+A")
            await id_input.fill("")
            await id_input.type(first_identifier, delay=50)
        except Exception:
            try:
                await page.eval_on_selector(
                    'input[name="text"], input[autocomplete="username"], [data-testid="ocfEnterTextTextInput"] input',
                    "(el, val) => { el.focus(); el.value = val; el.dispatchEvent(new Event('input', { bubbles: true })); }",
                    first_identifier,
                )
            except Exception:
                return False, "Could not populate email/identifier field"

        print("[login] Clicking Next...")
        try:
            next_btn = page.get_by_role("button", name=re.compile("next|continue", re.I))
            if await next_btn.count() > 0:
                await next_btn.first.click(timeout=5000)
            else:
                await page.click(':text("Next"), :text("Continue")', timeout=5000)
        except Exception:
            try:
                await page.keyboard.press("Enter")
            except Exception:
                return False, "Could not proceed past identifier step"

        # Poll for either the challenge screen or the password field
        print("[login] Checking for challenge or password...")
        import time as _time
        deadline = _time.monotonic() + 40.0
        while _time.monotonic() < deadline:
            try:
                # If password field is visible, break to password entry
                if await page.locator('input[name="password"]').first.is_visible(timeout=500):  # type: ignore
                    print("[login] Password field detected.")
                    break
            except Exception:
                pass

            # Detect challenge by text and presence of input
            challenge_field = page.locator('[data-testid="ocfEnterTextTextInput"] input, input[name="text"]').first
            challenge_text = ""
            try:
                body_text = await page.eval_on_selector('body', 'el => (el.innerText||"")')
                challenge_text = (body_text or "").lower()
            except Exception:
                challenge_text = ""
            challenge_present = ("enter your phone number or username" in challenge_text) or ("unusual login activity" in challenge_text)
            if (challenge_present and await challenge_field.count() > 0):
                print("[login] Challenge detected: entering username...")
                try:
                    await challenge_field.click(timeout=3000)
                    try:
                        await page.keyboard.press("Meta+A")
                    except Exception:
                        await page.keyboard.press("Control+A")
                    await challenge_field.fill("")
                    await challenge_field.type(username if username else email, delay=50)
                except Exception:
                    # Fallback set value
                    try:
                        await page.eval_on_selector(
                            '[data-testid="ocfEnterTextTextInput"] input, input[name="text"]',
                            "(el, val) => { el.focus(); el.value = val; el.dispatchEvent(new Event('input', { bubbles: true })); }",
                            username if username else email,
                        )
                    except Exception:
                        return False, "Could not populate challenge identifier"
                # Click Next/Continue after filling the challenge
                try:
                    btn = page.get_by_role("button", name=re.compile("next|continue|submit", re.I))
                    if await btn.count() > 0:
                        await btn.first.click(timeout=5000)
                    else:
                        await page.keyboard.press("Enter")
                except Exception:
                    await page.keyboard.press("Enter")
                # After submitting challenge, continue polling for password
                await asyncio.sleep(0.8)
                continue

            # Neither challenge nor password yet; small wait and retry
            await asyncio.sleep(0.5)
        else:
            return False, "Timed out waiting for challenge resolution or password"

        print("[login] Waiting for password field...")
        try:
            await page.wait_for_selector('input[name="password"]', timeout=25000)
            pwd_input = page.locator('input[name="password"]').first
            await pwd_input.click(timeout=5000)
            try:
                await page.keyboard.press("Meta+A")
            except Exception:
                await page.keyboard.press("Control+A")
            await pwd_input.fill("")
            await pwd_input.type(password, delay=50)
        except Exception:
            return False, "Password field did not appear in time"

        print("[login] Submitting login...")
        try:
            submit_btn = page.get_by_role("button", name=re.compile("log in|login", re.I))
            if await submit_btn.count() > 0:
                await submit_btn.first.click(timeout=5000)
            else:
                await page.click(':text("Log in"), :text("Login")', timeout=5000)
        except Exception:
            try:
                await page.keyboard.press("Enter")
            except Exception:
                return False, "Could not submit login"

        try:
            await page.wait_for_url(re.compile(r"https://x\.com/home|/notifications|/explore"), timeout=30000)
            await context.storage_state(path=str(state_path))
            return True, None
        except Exception:
            try:
                await page.wait_for_selector('[data-testid="SideNav_AccountSwitcher_Button"], [data-testid="tweetTextarea_0"]', timeout=25000)
                await context.storage_state(path=str(state_path))
                return True, None
            except Exception:
                pass

        # Collect failure reason
        reason = None
        try:
            texts = await page.eval_on_selector_all(
                'div[role="alert"], [data-testid="toast"], [aria-live="polite"], [aria-live="assertive"], div[role="status"], [data-testid="app-text-transition-container"]',
                'els => Array.from(new Set(els.map(e => (e.innerText||"").trim()).filter(Boolean)))',
            )
            if isinstance(texts, list) and texts:
                reason = " | ".join(texts)
        except Exception:
            pass
        if not reason:
            try:
                body_text = await page.eval_on_selector('body', 'el => (el.innerText||"").slice(0, 2000)')
                if isinstance(body_text, str) and body_text.strip():
                    for kw in [
                        "Wrong password",
                        "couldn’t confirm your identity",
                        "Try again later",
                        "locked",
                        "suspicious",
                        "rate limit",
                        "Too many attempts",
                        "Enter your phone number or username",
                        "unusual login activity",
                        "verification",
                    ]:
                        if kw.lower() in body_text.lower():
                            reason = (reason + ' | ' if reason else '') + kw
            except Exception:
                pass
        return False, reason or "Unknown login error"
    except Exception as e:
        return False, f"Exception during login: {str(e)}"
    finally:
        await page.close()


def parse_cookie_header(cookie_header: str) -> List[Dict[str, str]]:
    cookies = []
    parts = [p.strip() for p in cookie_header.split(';') if p.strip()]
    for p in parts:
        if '=' not in p:
            continue
        name, value = p.split('=', 1)
        cookies.append({"name": name.strip(), "value": value.strip()})
    return cookies


async def scrape_one(page: Page, url: str, timeout_ms: int = 10000) -> ScrapeResult:
    mobile_url = normalize_url_to_mobile(url)
    try:
        await page.goto(mobile_url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            for label in ["Accept all", "Accept", "I agree", "OK", "Got it"]:
                btn = page.get_by_role("button", name=re.compile(label, re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    break
        except Exception:
            pass

        # Ensure main content is present; if not, perform up to 4 hard refreshes
        article_ready = False
        for attempt in range(5):  # initial try + 4 hard refreshes
            try:
                await page.wait_for_selector("article", timeout=10000)
                article_ready = True
                break
            except Exception:
                if attempt < 4:
                    try:
                        print(f"[scrape] Article not found, hard refreshing ({attempt+1}/4)...")
                        # Try a hard reload by forcing revalidation; fallback to cache-busting goto
                        try:
                            await page.evaluate("location.reload(true)")
                            await page.wait_for_load_state("domcontentloaded", timeout=7000)
                        except Exception:
                            # Cache-busting navigate
                            cb_url = mobile_url + ("&" if ("?" in mobile_url) else "?") + f"_cb={int(time.time()*1000)}"
                            await page.goto(cb_url, wait_until="domcontentloaded", timeout=7000)
                        await asyncio.sleep(0.6)
                        continue
                    except Exception:
                        # If hard reload/goto fails, break early
                        break
                else:
                    # Exhausted refresh attempts
                    break

        if not article_ready:
            # Proceed but likely no counts
            pass

        views, likes, shares, comments, posted_at, caption = await extract_counts_from_dom(page)
        status = "ok" if any(v is not None for v in [views, likes, shares, comments]) else "no_counts_found"
        try:
            aria_labels = await page.eval_on_selector_all(
                "article [aria-label]",
                "els => els.map(e => e.getAttribute('aria-label'))",
            )
            if isinstance(aria_labels, list):
                raw = next((lbl for lbl in aria_labels if isinstance(lbl, str) and 'views' in lbl.lower()), None)
                if raw:
                    print(f"RAW_TWEET_DATA: {raw}")
                    if views is None:
                        m = re.search(r"([0-9][0-9,\.\s]*)([kKmMbB]?)\s*views?", raw, flags=re.I)
                        if m:
                            parsed = parse_compact_number(m.group(0))
                            if parsed is not None:
                                views = parsed
                                status = "ok"
        except Exception:
            pass
        return ScrapeResult(
            url=url,
            views=views,
            likes=likes,
            shares=shares,
            comments=comments,
            status=status,
            error=None,
            posted_at=posted_at,
            caption=caption,
        )
    except Exception as e:
        return ScrapeResult(
            url=url,
            views=None,
            likes=None,
            shares=None,
            comments=None,
            status="error",
            error=str(e),
            posted_at=None,
            caption=None,
        )


async def run(input_csv: Path, output_csv: Path, failed_csv: Path, headless: bool, retries: int, email: Optional[str], username: Optional[str], password: Optional[str], state_path: Optional[Path], cookie_header: Optional[str], cookie_file: Optional[Path]) -> None: 
    urls: List[str] = []
    with input_csv.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)
        if not rows:
            print("Input CSV is empty", file=sys.stderr)
            return
        if len(rows[0]) == 1 and rows[0][0].strip().lower() not in {"url", "link", "tweet", "status_url"}:
            urls = [r[0].strip() for r in rows if r and r[0].strip()]
        else:
            header = [h.strip().lower() for h in rows[0]]
            header = [h.replace("\ufeff", "") for h in header]
            col_idx = None
            for name in ("url", "link", "tweet", "status_url"):
                if name in header:
                    col_idx = header.index(name)
                    break
            if col_idx is None:
                col_idx = 0
            for r in rows[1:]:
                if not r:
                    continue
                if col_idx < len(r) and r[col_idx].strip():
                    urls.append(r[col_idx].strip())

    output_csv.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=headless, args=["--disable-blink-features=AutomationControlled"])  # type: ignore

        # Prepare storage state path
        state_file: Optional[Path] = state_path
        if state_file is None:
            state_file = Path.cwd() / ".twitter_storage_state.json"

        # Load existing storage state if present
        context_kwargs = {
            "viewport": {"width": 980, "height": 1400},
            "user_agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
        }
        if state_file.exists():
            context_kwargs["storage_state"] = str(state_file)  # type: ignore

        context = await browser.new_context(
            **context_kwargs
        )

        # If cookie header or file is provided, set cookies before scraping
        if cookie_header or cookie_file:
            header_value = cookie_header
            if not header_value and cookie_file and cookie_file.exists():
                try:
                    header_value = cookie_file.read_text(encoding="utf-8").strip()
                except Exception:
                    header_value = None
            if header_value:
                raw = parse_cookie_header(header_value)
                cookie_objs = []
                for c in raw:
                    if not c.get("name"):
                        continue
                    cookie_objs.append({
                        "name": c["name"],
                        "value": c.get("value", ""),
                        "domain": ".x.com",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    })
                    cookie_objs.append({
                        "name": c["name"],
                        "value": c.get("value", ""),
                        "domain": ".twitter.com",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    })
                if cookie_objs:
                    await context.add_cookies(cookie_objs)  # type: ignore
                    # Save cookies to storage state immediately for reuse
                    await context.storage_state(path=str(state_file))

        # If no storage and we have creds, log in and save state
        if not state_file.exists() and (email or username) and password:
            print("Logging into Twitter...")
            ok, fail_reason = await login_and_save_state(context, headless=headless, email=email or "", username=username or "", password=password, state_path=state_file)
            if ok:
                print("Login successful! Session saved for future use.")
                # Re-create context with saved state for clean session
                await context.close()
                context = await browser.new_context(
                    viewport={"width": 980, "height": 1400},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/127.0.0.0 Safari/537.36"
                    ),
                    storage_state=str(state_file),
                )
            else:
                print(f"Login failed! Reason: {fail_reason}")
                print("Continuing without authentication...")

        page: Page = await context.new_page()

        with output_csv.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.writer(f_out)
            # keep existing columns, append caption at the end
            writer.writerow(["url", "posted_at", "views", "likes", "shares", "comments", "status", "error", "caption"])

            last_request_ts: float = 0.0
            # Counters for summary
            total_urls = len(urls)
            successful = 0
            failed = 0
            no_counts = 0
            failed_urls = []  # Track failed URLs for failed.csv
            print(f"Starting to scrape {total_urls} URLs...")

            for idx, url in enumerate(urls, 1):
                print(f"[{idx}/{total_urls}] Processing: {url}")
                attempt = 0
                res: Optional[ScrapeResult] = None
                while attempt <= retries:
                    attempt += 1
                    now = time.monotonic()
                    elapsed = now - last_request_ts
                    if elapsed < 1.0:
                        await asyncio.sleep(1.0 - elapsed)
                    result = await scrape_one(page, url)
                    last_request_ts = time.monotonic()
                    res = result
                    if result.status == "ok" or result.status == "no_counts_found":
                        break
                    if attempt <= retries:
                        await asyncio.sleep(1.0)
                if res is None:
                    res = ScrapeResult(
                        url=url,
                        views=None,
                        likes=None,
                        shares=None,
                        comments=None,
                        status="error",
                        error="unknown",
                        posted_at=None,
                        caption=None,
                    )
                
                # Log result and update counters
                if res.status == "ok":
                    successful += 1
                    print(f"  ✓ SUCCESS: {res.views} views, {res.likes} likes, {res.shares} shares, {res.comments} comments, posted_at={res.posted_at or 'n/a'}")
                elif res.status == "no_counts_found":
                    no_counts += 1
                    failed_urls.append(url)  # Add no_counts URLs to failed list too
                    print(f"  ⚠ NO COUNTS: Could not extract metrics (likely requires login for views)")
                else:
                    failed += 1
                    failed_urls.append(url)  # Add to failed URLs list
                    print(f"  ✗ FAILED: {res.status} - {res.error or 'Unknown error'}")
                
                writer.writerow([
                    res.url,
                    res.posted_at or "",
                    res.views if res.views is not None else "",
                    res.likes if res.likes is not None else "",
                    res.shares if res.shares is not None else "",
                    res.comments if res.comments is not None else "",
                    res.status,
                    res.error or "",
                    res.caption or "",
                ])
                await asyncio.sleep(1.0)

            # Create failed.csv if there are failed URLs (including no_counts_found)
            if failed_urls:
                failed_csv.parent.mkdir(parents=True, exist_ok=True)
                with failed_csv.open("w", newline="", encoding="utf-8") as f_failed:
                    failed_writer = csv.writer(f_failed)
                    failed_writer.writerow(["url"])  # Header
                    for failed_url in failed_urls:
                        failed_writer.writerow([failed_url])
                print(f"Failed URLs saved to: {failed_csv}")

            # Print summary
            print(f"\n{'='*50}")
            print(f"SCRAPING COMPLETED")
            print(f"{'='*50}")
            print(f"Total URLs processed: {total_urls}")
            print(f"Successful retrievals: {successful}")
            print(f"No counts found: {no_counts}")
            print(f"Failed retrievals: {failed}")
            print(f"Success rate: {(successful/total_urls*100):.1f}%")
            print(f"Output saved to: {output_csv}")
            if failed_urls:
                print(f"Failed URLs saved to: {failed_csv}")
            print(f"{'='*50}")

        await context.close()
        await browser.close()


def parse_args(argv: List[str]) -> Tuple[Path, Path, Path, bool, int, Optional[str], Optional[str], Optional[str], Optional[Path], Optional[str], Optional[Path]]:
    import argparse

    parser = argparse.ArgumentParser(description="Scrape X/Twitter post metrics without API")
    parser.add_argument("--input", "-i", type=str, default=str(Path.cwd() / "links.csv"), help="Path to input CSV of tweet links")
    parser.add_argument("--output", "-o", type=str, default=str(Path.cwd() / "output.csv"), help="Path to write output CSV")
    parser.add_argument("--failed", "-f", type=str, default=str(Path.cwd() / "failed.csv"), help="Path to write failed URLs CSV")
    parser.add_argument("--no-headless", action="store_true", help="Run browser non-headless for debugging")
    parser.add_argument("--retries", type=int, default=2, help="Retries per URL on failure")
    # Auth options
    parser.add_argument("--email", type=str, default=None, help="Email to login (used on first screen)")
    parser.add_argument("--username", type=str, default=None, help="Username/handle for identifier challenge")
    parser.add_argument("--password", type=str, default=None, help="Twitter password to login")
    parser.add_argument("--state", type=str, default=None, help="Path to Playwright storage state JSON")
    parser.add_argument("--cookie", type=str, default=None, help="Raw Cookie header string (e.g., 'a=1; b=2')")
    parser.add_argument("--cookie-file", type=str, default=None, help="Path to a file containing Cookie header string")

    args = parser.parse_args(argv)
    input_csv = Path(args.input)
    output_csv = Path(args.output)
    failed_csv = Path(args.failed)
    headless = not args.no_headless   # ✅ fixed
    retries = max(0, int(args.retries))
    # Allow env fallbacks
    email = args.email or os.environ.get("TW_EMAIL")
    username = args.username or os.environ.get("TW_USERNAME") or os.environ.get("TWITTER_USERNAME")
    password = args.password or os.environ.get("TW_PASSWORD") or os.environ.get("TWITTER_PASSWORD")
    state_path = Path(args.state) if args.state else (Path(os.environ.get("TW_STATE", "")) if os.environ.get("TW_STATE") else None)
    cookie_header = args.cookie or os.environ.get("TW_COOKIE")
    cookie_file = Path(args.cookie_file) if args.cookie_file else (Path(os.environ.get("TW_COOKIE_FILE", "")) if os.environ.get("TW_COOKIE_FILE") else None)
    return input_csv, output_csv, failed_csv, headless, retries, email, username, password, state_path, cookie_header, cookie_file


def main() -> None:
    input_csv, output_csv, failed_csv, headless, retries, email, username, password, state_path, cookie_header, cookie_file = parse_args(sys.argv[1:])
    if not input_csv.exists():
        print(f"Input CSV not found: {input_csv}", file=sys.stderr)
        sys.exit(1)
    try:
        asyncio.run(run(input_csv, output_csv, failed_csv, headless, retries, email, username, password, state_path, cookie_header, cookie_file))
    except KeyboardInterrupt:
        print("Aborted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
