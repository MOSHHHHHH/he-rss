
#!/usr/bin/env python3
"""
בניית רשימת פידי RSS של אתרי חדשות בעברית.

שלב 1: חיפוש ב-Google News RSS (לא רשמי) בעברית, במספר רב של נושאים.
שלב 2: מיפוי כל האתרים (לפי <source url=...>) שמופיעים בתוצאות.
שלב 3: איתור פיד לכל אתר, לפי הסדר:
        ניסיון 1 - כתובות נפוצות (/feed, /rss ...)
        ניסיון 2 - חיפוש בקוד של דף הבית
        ניסיון 3 - Serper: 3 תוצאות ראשונות, חיפוש קישור לפיד
        לא נמצא - ויתור.
הכול סינכרוני, ברצף, עם הדפסה לקונסול של כל פעולה.

משתני סביבה:
  SERPER_API_KEY     מפתח Serper (סיקרט)
  RETRY_NOT_FOUND    "1" = לנסות שוב גם אתרים שלא נמצא להם פיד בריצות קודמות
  FORCE_RECHECK      "1" = לבדוק מחדש הכול, כולל אתרים שכבר נמצא להם פיד
  MAX_SITES          הגבלת מספר אתרים (לבדיקות)
  WHEN_VARIANTS      מסנני זמן מופרדים בפסיק, ברירת מחדל: "when:1d,when:7d,when:30d,"
                     (ערך ריק = בלי הגבלת זמן)
"""
import csv
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
QUERIES_FILE = ROOT / "config" / "queries.txt"
DATA_DIR = ROOT / "data"
SITES_JSON = DATA_DIR / "sites.json"
FEEDS_JSON = DATA_DIR / "feeds.json"
FEEDS_CSV = DATA_DIR / "feeds.csv"
FEEDS_OPML = DATA_DIR / "feeds.opml"

SERPER_KEY = os.environ.get("SERPER_API_KEY", "").strip()
RETRY_NOT_FOUND = os.environ.get("RETRY_NOT_FOUND") == "1"
FORCE_RECHECK = os.environ.get("FORCE_RECHECK") == "1"
MAX_SITES = int(os.environ.get("MAX_SITES") or 0)
WHEN_VARIANTS = os.environ.get("WHEN_VARIANTS", "when:1d,when:7d,when:30d,").split(",")

GN_PARAMS = "hl=iw&gl=IL&ceid=IL:he"
GN_BASE = "https://news.google.com/rss"
SLEEP_BETWEEN_REQUESTS = 1.0

COMMON_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss/", "/rss.xml", "/feed.xml", "/atom.xml",
    "/index.xml", "/?feed=rss2", "/feed/rss", "/rss/feed", "/feeds/posts/default",
    "/rssfeed", "/RSS", "/rss/news", "/rss/main", "/xml/rss.xml", "/feed/atom",
]

SKIP_DOMAINS = ("google.com", "news.google.com", "googleusercontent.com")
MULTI_PART_SLD = {"co", "org", "gov", "ac", "net", "muni", "k12"}
TRUSTED_FEED_HOSTS = ("feedburner.com", "feeds.feedburner.com", "feedblitz.com", "feeds.megaphone.fm")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
})


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def norm_domain(host):
    host = host.lower().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def base_domain(host):
    parts = norm_domain(host).split(".")
    if len(parts) >= 3 and parts[-2] in MULTI_PART_SLD:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def same_site(feed_url, domain):
    host = urlparse(feed_url).netloc
    if base_domain(host) == base_domain(domain):
        return True
    return any(norm_domain(host).endswith(h) for h in TRUSTED_FEED_HOSTS)


def http_get(url, timeout=20):
    log(f"   GET {url}")
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        log(f"      -> {r.status_code} ({len(r.content)} bytes)")
        return r
    except requests.RequestException as e:
        log(f"      -> שגיאה: {type(e).__name__}")
        return None


# --------------------------------------------------------------------------
# שלב 1 + 2: Google News
# --------------------------------------------------------------------------
def load_queries():
    queries = []
    for line in QUERIES_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            queries.append(line)
    return queries


def gn_urls(queries):
    """מחזיר רשימת (תיאור, כתובת). שורה שמתחילה ב-topic: היא מזהה נושא של גוגל."""
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
        r = http_get(url)
        time.sleep(SLEEP_BETWEEN_REQUESTS)
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
    return sites


# --------------------------------------------------------------------------
# שלב 3: איתור פיד
# --------------------------------------------------------------------------
def check_feed(resp):
    """אם התגובה היא פיד תקין עם פריטים - מחזיר מידע עליו, אחרת None."""
    if resp is None or resp.status_code != 200:
        return None
    head = resp.content[:4000].lower()
    if not any(tag in head for tag in (b"<rss", b"<feed", b"<rdf:rdf")):
        return None
    parsed = feedparser.parse(resp.content)
    if not parsed.entries:
        return None
    return {
        "url": resp.url,
        "title": (parsed.feed.get("title") or "").strip(),
        "entries": len(parsed.entries),
    }


def try_candidates(candidates, domain, seen):
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        if not same_site(url, domain):
            log(f"   מדלג (דומיין זר): {url}")
            continue
        info = check_feed(http_get(url))
        if info:
            return info
    return None


def feed_links_from_html(html, page_url):
    soup = BeautifulSoup(html, "html.parser")
    found = []
    for link in soup.find_all("link", href=True):
        t = (link.get("type") or "").lower()
        rel = " ".join(link.get("rel") or []).lower()
        if "alternate" in rel and ("rss" in t or "atom" in t or t == "application/xml" or t == "text/xml"):
            found.append(urljoin(page_url, link["href"]))
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"(rss|/feed|atom|\.xml)", href, re.I):
            found.append(urljoin(page_url, href))
    out, seen = [], set()
    for u in found:
        if u not in seen and u.startswith("http"):
            seen.add(u)
            out.append(u)
    return out[:12]


def attempt1(site, seen):
    log(" ניסיון 1: כתובות נפוצות")
    p = urlparse(site["homepage"])
    origin = f"{p.scheme or 'https'}://{p.netloc}"
    return try_candidates([origin + path for path in COMMON_FEED_PATHS], site["domain"], seen)


def attempt2(site, seen):
    log(" ניסיון 2: חיפוש בקוד דף הבית")
    r = http_get(site["homepage"])
    if r is None or r.status_code != 200:
        return None
    direct = check_feed(r)
    if direct:
        return direct
    links = feed_links_from_html(r.text, r.url)
    log(f"   נמצאו {len(links)} קישורים מועמדים")
    return try_candidates(links, site["domain"], seen)


def serper_search(query):
    if not SERPER_KEY:
        log("   אין SERPER_API_KEY, מדלג")
        return []
    log(f"   Serper: {query}")
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
            json={"q": query, "gl": "il", "hl": "iw"},
            timeout=30,
        )
        log(f"      -> {r.status_code}")
        r.raise_for_status()
        return r.json().get("organic", [])
    except (requests.RequestException, ValueError) as e:
        log(f"      -> שגיאה: {type(e).__name__}")
        return []


def attempt3(site, seen):
    log(" ניסיון 3: חיפוש ב-Serper")
    results = serper_search(f"{site['domain']} RSS feed")[:3]
    for n, res in enumerate(results, 1):
        link = res.get("link")
        if not link:
            continue
        log(f"   תוצאה {n}/3: {link}")
        r = http_get(link)
        if r is None or r.status_code != 200:
            continue
        direct = check_feed(r)
        if direct and same_site(direct["url"], site["domain"]):
            return direct
        info = try_candidates(feed_links_from_html(r.text, r.url), site["domain"], seen)
        if info:
            return info
    return None


def find_feed(site):
    seen = set()
    for number, fn in ((1, attempt1), (2, attempt2), (3, attempt3)):
        info = fn(site, seen)
        time.sleep(SLEEP_BETWEEN_REQUESTS / 2)
        if info:
            info["method"] = f"attempt{number}"
            return info
    return None


# --------------------------------------------------------------------------
# ייצוא
# --------------------------------------------------------------------------
def save_json(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def export_all(sites, feeds):
    found = [(d, feeds[d]) for d in sorted(feeds) if feeds[d].get("status") == "found"]
    with FEEDS_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["domain", "site_name", "feed_url", "feed_title", "entries", "method", "checked_at"])
        for d, info in found:
            w.writerow([d, sites.get(d, {}).get("name", ""), info["url"], info.get("title", ""),
                        info.get("entries", ""), info.get("method", ""), info.get("checked_at", "")])
    root = ET.Element("opml", version="2.0")
    head = ET.SubElement(root, "head")
    ET.SubElement(head, "title").text = "Hebrew News Feeds"
    body = ET.SubElement(root, "body")
    for d, info in found:
        name = sites.get(d, {}).get("name") or d
        ET.SubElement(body, "outline", type="rss", text=name, title=name,
                      xmlUrl=info["url"], htmlUrl=sites.get(d, {}).get("homepage", ""))
    ET.indent(root)
    ET.ElementTree(root).write(FEEDS_OPML, encoding="utf-8", xml_declaration=True)
    log(f"יוצאו {len(found)} פידים: {FEEDS_CSV.name}, {FEEDS_OPML.name}, {FEEDS_JSON.name}")


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    previous_sites = json.loads(SITES_JSON.read_text(encoding="utf-8")) if SITES_JSON.exists() else {}
    feeds = json.loads(FEEDS_JSON.read_text(encoding="utf-8")) if FEEDS_JSON.exists() else {}

    sites = discover_sites()
    # איחוד עם אתרים מריצות קודמות
    for d, s in previous_sites.items():
        if d not in sites:
            sites[d] = s
    save_json(SITES_JSON, sites)
    log(f"=== שלב 2: מופו {len(sites)} אתרים ===")

    ordered = sorted(sites.values(), key=lambda s: -s.get("articles_seen", 0))
    if MAX_SITES:
        ordered = ordered[:MAX_SITES]

    log(f"=== שלב 3: איתור פידים ל-{len(ordered)} אתרים ===")
    for i, site in enumerate(ordered, 1):
        d = site["domain"]
        prev = feeds.get(d)
        log(f"[{i}/{len(ordered)}] {d} ({site.get('name', '')})")
        if prev and not FORCE_RECHECK:
            if prev.get("status") == "found":
                log(f"   כבר קיים פיד: {prev['url']} - מדלג")
                continue
            if prev.get("status") == "not_found" and not RETRY_NOT_FOUND:
                log("   נבדק בעבר ולא נמצא - מדלג")
                continue
        info = find_feed(site)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if info:
            info.update(status="found", checked_at=now)
            feeds[d] = info
            log(f"   ✔ נמצא ({info['method']}): {info['url']}")
        else:
            feeds[d] = {"status": "not_found", "checked_at": now}
            log("   ✘ לא נמצא פיד, מוותר")
        save_json(FEEDS_JSON, feeds)  # שמירה הדרגתית

    save_json(FEEDS_JSON, feeds)
    export_all(sites, feeds)


if __name__ == "__main__":
    sys.exit(main())
