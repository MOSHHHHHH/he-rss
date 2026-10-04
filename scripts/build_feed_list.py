#!/usr/bin/env python3
"""
בניית רשימת פידי RSS של אתרי חדשות בעברית (גרסה 2).

שלב 1: חיפוש ב-Google News RSS (לא רשמי) בעברית, במספר רב של נושאים. ברצף (סינכרוני) כדי לא להיחסם.
שלב 2: מיפוי כל האתרים (לפי <source url=...>) שמופיעים בתוצאות.
שלב 3: איתור פיד לכל אתר, לפי הסדר:
        ניסיון 1 - כתובות נפוצות (/feed, /rss ...)
        ניסיון 2 - קוד דף הבית (כולל סריקת דפי "אינדקס RSS" בעומק אחד)
        ניסיון 3 - Serper: 3 תוצאות ראשונות, חיפוש קישור לפיד
        לא נמצא - ויתור.
בשלב 3 אתרים שונים מטופלים במקביל (thread לכל אתר), אבל בתוך אתר אחד הכול ברצף עם השהיה,
ואתרים מאותו דומיין-אב (למשל maariv.co.il ותתי-דומיינים שלו) לא רצים במקביל.

משתני סביבה:
  SERPER_API_KEY     מפתח Serper (סיקרט)
  WORKERS            כמה אתרים במקביל בשלב 3 (ברירת מחדל 8)
  REQUEST_DELAY      השהיה (שניות) בין בקשות לאותו אתר (ברירת מחדל 0.4)
  SKIP_DISCOVERY     "1" = לדלג על שלבים 1-2 ולהשתמש ב-data/sites.json הקיים
  RETRY_NOT_FOUND    "1" = לנסות שוב גם אתרים שלא נמצא להם פיד (גם מאותה גרסת לוגיקה)
  FORCE_RECHECK      "1" = לבדוק מחדש הכול
  MAX_SITES          הגבלת מספר אתרים (לבדיקות)
  WHEN_VARIANTS      מסנני זמן לשלב 1, ברירת מחדל: "when:1d,when:7d,when:30d,"
"""
import csv
import json
import os
import re
import signal
import sys
import threading
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

try:  # מדמה דפדפן אמיתי (TLS) - עוזר מול חסימות בסיסיות של Cloudflare ודומיו
    from curl_cffi import requests as cffi_requests
except Exception:  # pragma: no cover
    cffi_requests = None

ROOT = Path(__file__).resolve().parent.parent
QUERIES_FILE = ROOT / "config" / "queries.txt"
DATA_DIR = ROOT / "data"
SITES_JSON = DATA_DIR / "sites.json"
FEEDS_JSON = DATA_DIR / "feeds.json"
FEEDS_CSV = DATA_DIR / "feeds.csv"
FEEDS_ALL_CSV = DATA_DIR / "feeds_all.csv"
FEEDS_OPML = DATA_DIR / "feeds.opml"

LOGIC_VERSION = 2  # רשומות not_found מגרסה ישנה יותר ייבדקו מחדש אוטומטית

SERPER_KEY = os.environ.get("SERPER_API_KEY", "").strip()
RETRY_NOT_FOUND = os.environ.get("RETRY_NOT_FOUND") == "1"
FORCE_RECHECK = os.environ.get("FORCE_RECHECK") == "1"
SKIP_DISCOVERY = os.environ.get("SKIP_DISCOVERY") == "1"
MAX_SITES = int(os.environ.get("MAX_SITES") or 0)
WORKERS = max(1, int(os.environ.get("WORKERS") or 8))
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY") or 0.4)
WHEN_VARIANTS = os.environ.get("WHEN_VARIANTS", "when:1d,when:7d,when:30d,").split(",")

MAX_FEEDS_PER_SITE = 10      # פיד ראשי + עד 9 נוספים (מדף אינדקס RSS)
MAX_LINKS_PER_PAGE = 15      # כמה קישורים מועמדים בודקים מכל דף
SERPER_CONCURRENCY = 4

GN_PARAMS = "hl=iw&gl=IL&ceid=IL:he"
GN_BASE = "https://news.google.com/rss"
GN_DELAY = 1.0

COMMON_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss/", "/rss.xml", "/feed.xml", "/atom.xml",
    "/index.xml", "/?feed=rss2", "/feed/rss", "/rss/feed", "/feeds/posts/default",
    "/rssfeed", "/RSS", "/rss/news", "/rss/main", "/xml/rss.xml", "/feed/atom",
    "/rss.html", "/rss-feeds", "/feeds", "/misc/rss",
]

SKIP_DOMAINS = ("google.com", "news.google.com", "googleusercontent.com")
MULTI_PART_SLD = {"co", "org", "gov", "ac", "net", "muni", "k12"}
TRUSTED_FEED_HOSTS = ("feedburner.com", "feedblitz.com")

RSSISH = re.compile(r"(rss|/feed|atom|\.xml|feeds?\b)", re.I)
INDEX_HINT = re.compile(r"(rss|feed)", re.I)
BAD_LINK = re.compile(r"(comments?[/.]|/comments|utm_|facebook\.com|twitter\.com|whatsapp|t\.me/)", re.I)
FEED_START = (b"<?xml", b"<rss", b"<feed", b"<rdf")

BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
}

# --------------------------------------------------------------------------
# לוג (בטוח ל-threads; כל שורה מקבלת קידומת של האתר)
# --------------------------------------------------------------------------
_print_lock = threading.Lock()
_tl = threading.local()
STATE_LOCK = threading.Lock()
SERPER_SEM = threading.BoundedSemaphore(SERPER_CONCURRENCY)
_base_locks = defaultdict(threading.Lock)
_base_locks_guard = threading.Lock()


def log(msg):
    prefix = getattr(_tl, "prefix", "")
    with _print_lock:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {prefix}{msg}", flush=True)


def base_lock(base):
    with _base_locks_guard:
        return _base_locks[base]


def norm_domain(host):
    host = host.lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def base_domain(host):
    parts = norm_domain(host).split(".")
    if len(parts) >= 3 and parts[-2] in MULTI_PART_SLD:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def same_site(url, domain):
    host = urlparse(url).netloc
    if base_domain(host) == base_domain(domain):
        return True
    return any(norm_domain(host).endswith(h) for h in TRUSTED_FEED_HOSTS)


# --------------------------------------------------------------------------
# שלב 1 + 2: Google News (ברצף, בכוונה - כדי לא להיחסם)
# --------------------------------------------------------------------------
GN_SESSION = requests.Session()
GN_SESSION.headers.update(BROWSER_HEADERS)


def gn_get(url):
    log(f"   GET {url}")
    for attempt, backoff in enumerate((0, 30, 90, 180)):
        if backoff:
            log(f"      ממתין {backoff} שניות (הגבלת קצב) לפני ניסיון חוזר {attempt}")
            time.sleep(backoff)
        try:
            r = GN_SESSION.get(url, timeout=25)
        except requests.RequestException as e:
            log(f"      -> שגיאה: {type(e).__name__}")
            continue
        log(f"      -> {r.status_code} ({len(r.content)} bytes)")
        if r.status_code in (429, 503):
            continue
        return r
    return None


def load_queries():
    queries = []
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            queries.append(line)
    return queries


def gn_urls(queries):
    urls = [("ראשי", f"{GN_BASE}?{GN_PARAMS}")]
    for q in queries:
        if q.startswith("topic:"):
            tid = q.split(":", 1)[1].strip()
            urls.append((q, f"{GN_BASE}/topics/{tid}?{GN_PARAMS}"))
            continue
        for when in WHEN_VARIANTS:
            full_q = f"{q} {when}".strip()
            urls.append((full_q, f"{GN_BASE}/search?q={quote_plus(full_q)}&{GN_PARAMS}"))
    return urls


def discover_sites():
    sites = {}
    queries = load_queries()
    urls = gn_urls(queries)
    log(f"=== שלב 1: {len(queries)} נושאים, {len(urls)} בקשות ל-Google News ===")
    for i, (label, url) in enumerate(urls, 1):
        log(f"[{i}/{len(urls)}] חיפוש: {label}")
        r = gn_get(url)
        time.sleep(GN_DELAY)
        if r is None or r.status_code != 200:
            continue
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError:
            log("      -> XML לא תקין, מדלג")
            continue
        new_here = 0
        items = root.findall(".//item")
        for item in items:
            src = item.find("source")
            if src is None or not src.get("url"):
                continue
            home = src.get("url")
            domain = norm_domain(urlparse(home).netloc)
            if not domain or any(domain == d or domain.endswith("." + d) for d in SKIP_DOMAINS):
                continue
            site = sites.get(domain)
            if site is None:
                site = sites[domain] = {
                    "domain": domain,
                    "name": (src.text or "").strip(),
                    "homepage": home,
                    "articles_seen": 0,
                    "found_via_queries": [],
                }
                new_here += 1
            site["articles_seen"] += 1
            base_label = label if label == "ראשי" else re.sub(r"\s*when:\S+", "", label)
            if base_label not in site["found_via_queries"]:
                site["found_via_queries"].append(base_label)
        log(f"      -> {len(items)} כתבות, {new_here} אתרים חדשים, סה\"כ {len(sites)} אתרים")
        if MAX_SITES and len(sites) >= MAX_SITES:
            log(f"=== הגענו ל-MAX_SITES={MAX_SITES} אתרים, מפסיקים את שלב 1 ===")
            break
    return sites


# --------------------------------------------------------------------------
# שלב 3: איתור פיד
# --------------------------------------------------------------------------
def make_session():
    if cffi_requests is not None:
        s = cffi_requests.Session(impersonate="chrome")
        s.headers.update({"Accept-Language": "he-IL,he;q=0.9,en;q=0.8"})
        return s
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    return s


class Fetcher:
    """לקוח HTTP לאתר אחד: ברצף, עם השהיה, וזיהוי חסימה / אתר שמחזיר דף בית לכל כתובת."""

    def __init__(self):
        self.session = make_session()
        self._last = 0.0
        self.blocked_streak = 0
        self.blocked = False
        self.ever_blocked = False
        self.html_lengths = Counter()
        self.html_hits = []  # (url, length) של דפי HTML שנראים כדף RSS

    def get(self, url, timeout=20):
        wait = REQUEST_DELAY - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        log(f"   GET {url}")
        try:
            r = self.session.get(url, timeout=timeout, allow_redirects=True)
        except Exception as e:
            self._last = time.time()
            log(f"      -> שגיאה: {type(e).__name__}")
            return None
        self._last = time.time()
        log(f"      -> {r.status_code} ({len(r.content)} bytes)")
        self._note(r.status_code)
        return r

    def _note(self, code):
        if code in (403, 429, 503):
            self.blocked_streak += 1
            if self.blocked_streak >= 3 and not self.blocked:
                self.blocked = self.ever_blocked = True
                log("   האתר חוסם גישה אוטומטית (3 תגובות חסימה ברצף) - מפסיק ניסיונות ישירים")
        else:
            self.blocked_streak = 0

    def reset_block(self):
        self.blocked = False
        self.blocked_streak = 0

    @property
    def catchall(self):
        """אתר שמחזיר אותו דף HTML (באותו גודל) לכל כתובת."""
        return any(n >= 3 for n in self.html_lengths.values())


def is_html(r):
    try:
        ctype = (r.headers.get("content-type") or "").lower()
    except Exception:
        ctype = ""
    return "html" in ctype or b"<html" in r.content[:3000].lower()


def check_feed(r):
    """אם התגובה היא פיד תקין עם פריטים - מחזיר מידע עליו, אחרת None."""
    if r is None or r.status_code != 200:
        return None
    head = r.content[:4000].lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    if not head.startswith(FEED_START):
        return None
    if not any(t in head for t in (b"<rss", b"<feed", b"<rdf:rdf")):
        return None
    parsed = feedparser.parse(r.content)
    if not parsed.entries:
        return None
    return {
        "url": r.url,
        "title": (parsed.feed.get("title") or "").strip(),
        "entries": len(parsed.entries),
        "verified": True,
    }


def feed_links_from_html(html, page_url):
    """קישורים מועמדים לפיד/לדף RSS: קודם <link rel=alternate>, אחר כך קישורים שנראים כמו RSS."""
    soup = BeautifulSoup(html, "html.parser")
    primary, secondary = [], []
    for link in soup.find_all("link", href=True):
        t = (link.get("type") or "").lower()
        rel = " ".join(link.get("rel") or []).lower()
        if "alternate" in rel and ("rss" in t or "atom" in t or t in ("application/xml", "text/xml")):
            primary.append(urljoin(page_url, link["href"].strip()))
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("javascript:", "mailto:", "#")):
            continue
        if RSSISH.search(href) or re.search(r"\brss\b", a.get_text(" ", strip=True), re.I):
            secondary.append(urljoin(page_url, href))
    secondary.sort(key=lambda u: 0 if re.search(r"rss", urlparse(u).path, re.I) else 1)
    out, seen = [], set()
    for u in primary + secondary:
        if u in seen or not u.startswith("http") or BAD_LINK.search(u):
            continue
        seen.add(u)
        out.append(u)
    return out[:MAX_LINKS_PER_PAGE]


def resolve(f, url, domain, seen, depth=1, homelen=None):
    """בודק כתובת. פיד -> [info]. דף HTML שנראה כדף אינדקס RSS -> סורק אותו (עומק אחד) ומחזיר כמה פידים."""
    if url in seen or f.blocked:
        return []
    seen.add(url)
    if not same_site(url, domain):
        log(f"   מדלג (דומיין זר): {url}")
        return []
    r = f.get(url)
    info = check_feed(r)
    if info:
        return [info]
    if r is None or r.status_code != 200 or not is_html(r):
        return []
    f.html_lengths[len(r.content)] += 1
    if not INDEX_HINT.search(urlparse(url).path):
        return []
    if depth <= 0:
        f.html_hits.append((url, len(r.content)))
        return []
    if homelen is not None and len(r.content) == homelen:
        return []  # זה בעצם דף הבית
    links = feed_links_from_html(r.text, r.url)
    log(f"   דף אינדקס RSS: {url} - {len(links)} קישורים מועמדים")
    out = []
    for link in links:
        out += resolve(f, link, domain, seen, depth - 1, homelen)
        if len(out) >= MAX_FEEDS_PER_SITE:
            break
    return out


def toggle_www(url):
    p = urlparse(url)
    host = p.netloc[4:] if p.netloc.startswith("www.") else "www." + p.netloc
    return p._replace(netloc=host).geturl()


def attempt1(site, f, seen):
    log(" ניסיון 1: כתובות נפוצות")
    p = urlparse(site["homepage"])
    origin = f"{p.scheme or 'https'}://{p.netloc}"
    for path in COMMON_FEED_PATHS:
        if f.blocked:
            log("   האתר חוסם - מדלג על שאר הכתובות")
            break
        if f.catchall:
            log("   האתר מחזיר אותו דף לכל כתובת - מדלג על שאר הכתובות")
            break
        res = resolve(f, origin + path, site["domain"], seen, depth=0)
        if res:
            return res
    return []


def attempt2(site, f, seen):
    log(" ניסיון 2: חיפוש בקוד דף הבית")
    if f.blocked:
        log("   האתר חוסם - מדלג")
        return []
    r = f.get(site["homepage"])
    if r is None or r.status_code != 200:
        alt = toggle_www(site["homepage"])
        log(f"   דף הבית לא זמין, מנסה גרסה חלופית: {alt}")
        r = f.get(alt)
    if r is None or r.status_code != 200:
        return []
    direct = check_feed(r)
    if direct:
        return [direct]
    homelen = len(r.content)
    links = feed_links_from_html(r.text, r.url)
    log(f"   נמצאו {len(links)} קישורים מועמדים")
    for link in links:
        res = resolve(f, link, site["domain"], seen, depth=1, homelen=homelen)
        if res:
            return res
    # דפי RSS שנמצאו בניסיון 1 (200 + HTML) ושאינם דף הבית - סורקים אותם עכשיו
    for url, length in list(f.html_hits):
        if length == homelen:
            continue
        seen.discard(url)
        res = resolve(f, url, site["domain"], seen, depth=1, homelen=homelen)
        if res:
            return res
    return []


def serper_search(query):
    if not SERPER_KEY:
        log("   אין SERPER_API_KEY, מדלג")
        return []
    with SERPER_SEM:
        log(f"   Serper: {query}")
        for attempt in range(3):
            try:
                r = requests.post(
                    "https://google.serper.dev/search",
                    headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
                    json={"q": query, "gl": "il", "hl": "iw"},
                    timeout=30,
                )
                log(f"      -> {r.status_code}")
                if r.status_code == 429:
                    time.sleep(5 * (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json().get("organic", [])
            except (requests.RequestException, ValueError) as e:
                log(f"      -> שגיאה: {type(e).__name__}")
                return []
    return []


def attempt3(site, f, seen):
    log(" ניסיון 3: חיפוש ב-Serper")
    was_blocked = f.ever_blocked
    results = serper_search(f"{site['domain']} RSS feed")[:3]
    f.reset_block()
    unverified = None
    for n, res in enumerate(results, 1):
        link = res.get("link")
        if not link:
            continue
        log(f"   תוצאה {n}/3: {link}")
        if f.blocked:
            break
        r = f.get(link)
        if r is None or r.status_code != 200:
            if was_blocked and same_site(link, site["domain"]) and RSSISH.search(link) and not unverified:
                unverified = link
            continue
        direct = check_feed(r)
        if direct and same_site(direct["url"], site["domain"]):
            return [direct]
        for cand in feed_links_from_html(r.text, r.url):
            found = resolve(f, cand, site["domain"], seen, depth=1)
            if found:
                return found
    if unverified:
        log(f"   האתר חוסם גישה אוטומטית; נשמרת כתובת לא מאומתת מ-Serper: {unverified}")
        return [{"url": unverified, "title": "", "entries": 0, "verified": False}]
    return []


def find_feed(site):
    f = Fetcher()
    seen = set()
    for number, fn in ((1, attempt1), (2, attempt2), (3, attempt3)):
        res = fn(site, f, seen)
        if res:
            primary = dict(res[0])
            primary["method"] = f"attempt{number}"
            extras, urls = [], {primary["url"]}
            for x in res[1:]:
                if x["url"] not in urls:
                    urls.add(x["url"])
                    extras.append({"url": x["url"], "title": x.get("title", ""), "entries": x.get("entries", 0)})
            if extras:
                primary["extra_feeds"] = extras[: MAX_FEEDS_PER_SITE - 1]
            return primary
    return None


def process_site(site):
    """רץ ב-thread. מחזיר (domain, info|None, errored)."""
    _tl.prefix = f"[{site['domain']}] "
    with base_lock(base_domain(site["domain"])):
        log(f"התחלה: {site.get('name', '')}")
        try:
            return site["domain"], find_feed(site), False
        except Exception as e:  # לא שומרים not_found על שגיאה לא צפויה, כדי שינסו שוב
            log(f"   שגיאה לא צפויה: {type(e).__name__}: {e}")
            return site["domain"], None, True


# --------------------------------------------------------------------------
# ייצוא
# --------------------------------------------------------------------------
def save_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def export_all(sites, feeds):
    found = [(d, feeds[d]) for d in sorted(feeds) if feeds[d].get("status") == "found"]

    def nm(d, info):
        return info.get("name") or sites.get(d, {}).get("name", "") or d

    def hp(d, info):
        return info.get("homepage") or sites.get(d, {}).get("homepage", "")

    with FEEDS_CSV.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["site_name", "domain", "homepage", "feed_url", "feed_title", "entries",
                    "verified", "extra_feeds", "method", "checked_at"])
        for d, info in found:
            w.writerow([nm(d, info), d, hp(d, info), info["url"], info.get("title", ""),
                        info.get("entries", ""), info.get("verified", True),
                        len(info.get("extra_feeds", [])), info.get("method", ""), info.get("checked_at", "")])

    with FEEDS_ALL_CSV.open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["site_name", "domain", "feed_url", "feed_title", "entries", "is_primary", "verified"])
        for d, info in found:
            w.writerow([nm(d, info), d, info["url"], info.get("title", ""), info.get("entries", ""),
                        True, info.get("verified", True)])
            for x in info.get("extra_feeds", []):
                w.writerow([nm(d, info), d, x["url"], x.get("title", ""), x.get("entries", ""), False, True])

    root = ET.Element("opml", version="2.0")
    head = ET.SubElement(root, "head")
    ET.SubElement(head, "title").text = "Hebrew News Feeds"
    body = ET.SubElement(root, "body")
    for d, info in found:
        name = nm(d, info)
        extras = info.get("extra_feeds", [])
        if extras:
            folder = ET.SubElement(body, "outline", text=name, title=name)
            ET.SubElement(folder, "outline", type="rss", text=info.get("title") or name,
                          title=info.get("title") or name, xmlUrl=info["url"], htmlUrl=hp(d, info))
            for x in extras:
                t = x.get("title") or name
                ET.SubElement(folder, "outline", type="rss", text=t, title=t, xmlUrl=x["url"], htmlUrl=hp(d, info))
        else:
            ET.SubElement(body, "outline", type="rss", text=name, title=name,
                          xmlUrl=info["url"], htmlUrl=hp(d, info))
    ET.indent(root)
    ET.ElementTree(root).write(FEEDS_OPML, encoding="utf-8", xml_declaration=True)
    log(f"יוצאו {len(found)} אתרים עם פיד: {FEEDS_CSV.name}, {FEEDS_ALL_CSV.name}, {FEEDS_OPML.name}, {FEEDS_JSON.name}")


def should_skip(prev):
    if not prev or FORCE_RECHECK:
        return None
    if prev.get("status") == "found":
        return f"כבר קיים פיד: {prev.get('url')}"
    if prev.get("status") == "not_found" and not RETRY_NOT_FOUND and prev.get("v", 1) >= LOGIC_VERSION:
        return "נבדק בעבר ולא נמצא"
    return None


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    previous_sites = json.loads(SITES_JSON.read_text(encoding="utf-8")) if SITES_JSON.exists() else {}
    feeds = json.loads(FEEDS_JSON.read_text(encoding="utf-8")) if FEEDS_JSON.exists() else {}

    if SKIP_DISCOVERY and previous_sites:
        sites = previous_sites
        log(f"=== דילוג על שלבים 1-2: נטענו {len(sites)} אתרים מ-sites.json ===")
    else:
        sites = discover_sites()
        for d, s in previous_sites.items():
            sites.setdefault(d, s)
        save_json(SITES_JSON, sites)
        log(f"=== שלב 2: מופו {len(sites)} אתרים ===")

    ordered = sorted(sites.values(), key=lambda s: -s.get("articles_seen", 0))
    if MAX_SITES:
        ordered = ordered[:MAX_SITES]

    todo = []
    for site in ordered:
        reason = should_skip(feeds.get(site["domain"]))
        if reason:
            log(f"דילוג {site['domain']}: {reason}")
            continue
        todo.append(site)

    # פיזור אתרים מאותו דומיין-אב, כדי שה-threads לא ימתינו אחד לשני
    rank, grouped = {}, defaultdict(int)
    for s in todo:
        b = base_domain(s["domain"])
        rank[s["domain"]] = grouped[b]
        grouped[b] += 1
    todo.sort(key=lambda s: (rank[s["domain"]], -s.get("articles_seen", 0)))

    log(f"=== שלב 3: איתור פידים ל-{len(todo)} אתרים, {WORKERS} במקביל "
        f"({'curl_cffi' if cffi_requests else 'requests'}) ===")

    def on_term(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, on_term)

    executor = ThreadPoolExecutor(max_workers=WORKERS)
    futures = {executor.submit(process_site, s): s for s in todo}
    done = 0
    try:
        for fut in as_completed(futures):
            site = futures[fut]
            domain, info, errored = fut.result()
            done += 1
            with STATE_LOCK:
                if errored:
                    log(f"[{done}/{len(todo)}] {domain}: שגיאה, לא נשמר (ינסו שוב בריצה הבאה)")
                else:
                    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    meta = {"checked_at": now, "name": site.get("name", ""),
                            "homepage": site.get("homepage", ""), "v": LOGIC_VERSION}
                    if info:
                        info.update(meta, status="found")
                        feeds[domain] = info
                        tag = "" if info.get("verified", True) else " (לא מאומת)"
                        extra = f" +{len(info['extra_feeds'])} נוספים" if info.get("extra_feeds") else ""
                        log(f"[{done}/{len(todo)}] ✔ {domain} ({info['method']}){tag}{extra}: {info['url']}")
                    else:
                        feeds[domain] = dict(meta, status="not_found")
                        log(f"[{done}/{len(todo)}] ✘ {domain}: לא נמצא פיד, מוותר")
                save_json(FEEDS_JSON, feeds)
                if done % 20 == 0:
                    export_all(sites, feeds)
    except KeyboardInterrupt:
        log("!!! הריצה הופסקה - שומר את מה שהושלם")
        executor.shutdown(wait=False, cancel_futures=True)
        with STATE_LOCK:
            save_json(FEEDS_JSON, feeds)
            export_all(sites, feeds)
        sys.stdout.flush()
        os._exit(130)

    executor.shutdown(wait=True)
    with STATE_LOCK:
        save_json(FEEDS_JSON, feeds)
        export_all(sites, feeds)


if __name__ == "__main__":
    sys.exit(main())
