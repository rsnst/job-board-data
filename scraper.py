#!/usr/bin/env python3
"""Scrape public job boards (12 ATS platforms) into static JSON shards.

Reads boards.csv (company, ats, slug, status), fetches each board's public
job list, and writes:
  data/shards/<shard>.json   jobs grouped by first letter of company
  data/index.json            totals and last-run time
  data/board_status.json     per-board result of the last run
Runs without any database, so it stays inside free hosting limits.
"""
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
BOARDS_CSV = os.path.join(HERE, "boards.csv")
DATA_DIR = os.path.join(HERE, "data")
SHARD_DIR = os.path.join(DATA_DIR, "shards")
WORKERS = int(os.environ.get("SCRAPE_WORKERS", "16"))
TIMEOUT = 20
RETRIES = 2
SUPPORTED = {"greenhouse", "lever", "ashby", "smartrecruiters", "workable", "bamboohr", "breezy", "rippling", "recruitee", "pinpoint", "gem", "teamtailor"}
UA = "job-board-search/1.0 (+public job list reader)"


def fetch_json(url):
    last_err = None
    for attempt in range(RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as res:
                return json.loads(res.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (404, 410):  # board does not exist; no point retrying
                break
            if e.code == 429:
                time.sleep(5 * (attempt + 1))
        except Exception as e:  # network, timeout, bad JSON
            last_err = e
            time.sleep(1 + attempt)
    raise last_err


def fetch_greenhouse(board):
    data = fetch_json("https://boards-api.greenhouse.io/v1/boards/%s/jobs" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("jobs", []):
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": (j.get("location") or {}).get("name") or "",
            "url": j.get("absolute_url") or "",
            "postedAt": j.get("updated_at") or "",
        })
    return out


def fetch_lever(board):
    data = fetch_json("https://api.lever.co/v0/postings/%s?mode=json" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data if isinstance(data, list) else []:
        created = j.get("createdAt")
        posted = datetime.fromtimestamp(created / 1000, timezone.utc).isoformat() if created else ""
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("text") or "Untitled",
            "location": (j.get("categories") or {}).get("location") or "",
            "url": j.get("hostedUrl") or "",
            "postedAt": posted,
        })
    return out


def fetch_ashby(board):
    data = fetch_json("https://api.ashbyhq.com/posting-api/job-board/%s" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("jobs", []):
        if j.get("isListed") is False:
            continue
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": j.get("location") or ("Remote" if j.get("isRemote") else ""),
            "url": j.get("jobUrl") or "",
            "postedAt": j.get("publishedAt") or "",
        })
    return out


def _loc_join(*parts):
    return ", ".join(p for p in parts if p)


def fetch_smartrecruiters(board):
    out = []
    offset = 0
    while True:
        data = fetch_json("https://api.smartrecruiters.com/v1/companies/%s/postings?limit=100&offset=%d" % (urllib.parse.quote(board["slug"]), offset))
        content = data.get("content", []) or []
        for j in content:
            if (j.get("visibility") or "PUBLIC") != "PUBLIC":
                continue
            loc = j.get("location") or {}
            out.append({
                "id": "%s__%s" % (board["id"], j.get("id")),
                "title": j.get("name") or "Untitled",
                "location": loc.get("fullLocation") or _loc_join(loc.get("city"), loc.get("region"), loc.get("country")),
                "url": "https://jobs.smartrecruiters.com/%s/%s" % (board["slug"], j.get("id")),
                "postedAt": j.get("releasedDate") or "",
            })
        offset += 100
        if not content or offset >= (data.get("totalFound") or 0) or offset >= 5000:
            break
    return out


def fetch_workable(board):
    data = fetch_json("https://apply.workable.com/api/v1/widget/accounts/%s?details=true" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("jobs", []) or []:
        out.append({
            "id": "%s__%s" % (board["id"], j.get("shortcode")),
            "title": j.get("title") or "Untitled",
            "location": _loc_join(j.get("city"), j.get("state"), j.get("country")) or ("Remote" if j.get("telecommuting") else ""),
            "url": j.get("url") or "",
            "postedAt": j.get("published_on") or j.get("created_at") or "",
        })
    return out


def fetch_bamboohr(board):
    data = fetch_json("https://%s.bamboohr.com/careers/list" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("result", []) or []:
        loc = j.get("location") or {}
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": (j.get("jobOpeningName") or "Untitled").strip(),
            "location": _loc_join(loc.get("city"), loc.get("state")) or ("Remote" if j.get("isRemote") else ""),
            "url": "https://%s.bamboohr.com/careers/%s" % (board["slug"], j.get("id")),
            "postedAt": "",
        })
    return out


def fetch_breezy(board):
    data = fetch_json("https://%s.breezy.hr/json" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data if isinstance(data, list) else []:
        loc = j.get("location") or {}
        city = loc.get("city") or ""
        state = (loc.get("state") or {}).get("name") or ""
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("name") or "Untitled",
            "location": _loc_join(city, state) or ("Remote" if loc.get("is_remote") else ""),
            "url": j.get("url") or "",
            "postedAt": j.get("published_date") or "",
        })
    return out


def fetch_rippling(board):
    data = fetch_json("https://api.rippling.com/platform/api/ats/v1/board/%s/jobs" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data if isinstance(data, list) else []:
        out.append({
            "id": "%s__%s" % (board["id"], j.get("uuid")),
            "title": j.get("name") or "Untitled",
            "location": (j.get("workLocation") or {}).get("label") or "",
            "url": j.get("url") or "",
            "postedAt": "",
        })
    return out


def fetch_recruitee(board):
    data = fetch_json("https://%s.recruitee.com/api/offers/" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("offers", []) or []:
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": _loc_join(j.get("city"), j.get("state_name"), j.get("country")) or ("Remote" if j.get("remote") else ""),
            "url": j.get("careers_url") or "",
            "postedAt": j.get("published_at") or "",
        })
    return out


def fetch_pinpoint(board):
    data = fetch_json("https://%s.pinpointhq.com/postings.json" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("data", []) or []:
        loc = j.get("location") or {}
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": (loc.get("name") if isinstance(loc, dict) else "") or j.get("workplace_type_text") or "",
            "url": j.get("url") or "",
            "postedAt": "",
        })
    return out


def fetch_gem(board):
    data = fetch_json("https://api.gem.com/job_board/v0/%s/job_posts/" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data if isinstance(data, list) else []:
        loc = j.get("location") or {}
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": (loc.get("name") if isinstance(loc, dict) else "") or "",
            "url": j.get("absolute_url") or "",
            "postedAt": j.get("first_published_at") or "",
        })
    return out


def fetch_teamtailor(board):
    data = fetch_json("https://%s.teamtailor.com/jobs.json" % urllib.parse.quote(board["slug"]))
    out = []
    for j in data.get("items", []) or []:
        out.append({
            "id": "%s__%s" % (board["id"], j.get("id")),
            "title": j.get("title") or "Untitled",
            "location": "",
            "url": j.get("url") or "",
            "postedAt": j.get("date_published") or "",
        })
    return out


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "smartrecruiters": fetch_smartrecruiters,
    "workable": fetch_workable,
    "bamboohr": fetch_bamboohr,
    "breezy": fetch_breezy,
    "rippling": fetch_rippling,
    "recruitee": fetch_recruitee,
    "pinpoint": fetch_pinpoint,
    "gem": fetch_gem,
    "teamtailor": fetch_teamtailor,
}


def board_id(ats, slug):
    return ats + "__" + re.sub(r"[^a-z0-9_.-]", "", slug.lower())


def load_boards():
    boards = []
    seen = set()
    with open(BOARDS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ats = (row.get("ats") or "").strip().lower()
            slug = (row.get("slug") or "").strip().lower()
            status = (row.get("status") or "").strip().lower()
            if ats not in SUPPORTED or not slug or status == "empty":
                continue
            bid = board_id(ats, slug)
            if bid in seen:
                continue
            seen.add(bid)
            boards.append({"id": bid, "ats": ats, "slug": slug, "company": (row.get("company") or slug).strip()})
    return boards


def shard_key(company):
    first = (company or "?")[:1].lower()
    return first if first.isalnum() and first.isascii() else "_other"


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None  # optional cap for test runs
    boards = load_boards()
    if limit:
        boards = boards[:limit]
    print("boards to scrape:", len(boards), flush=True)

    shards = {}
    status = {}
    done = 0

    def work(b):
        return b, FETCHERS[b["ats"]](b)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(work, b): b for b in boards}
        for fut in as_completed(futures):
            b = futures[fut]
            done += 1
            try:
                _, jobs = fut.result()
                for j in jobs:
                    j.update({"company": b["company"], "ats": b["ats"], "boardId": b["id"]})
                    shards.setdefault(shard_key(b["company"]), []).append(j)
                status[b["id"]] = {"ok": True, "jobs": len(jobs), "checked": datetime.now(timezone.utc).isoformat()}
            except Exception as e:
                status[b["id"]] = {"ok": False, "error": str(e)[:200], "checked": datetime.now(timezone.utc).isoformat()}
            if done % 500 == 0 or done == len(boards):
                print("progress %d/%d" % (done, len(boards)), flush=True)

    os.makedirs(SHARD_DIR, exist_ok=True)
    for key, jobs in shards.items():
        jobs.sort(key=lambda j: (j["company"].lower(), j["title"].lower()))
        with open(os.path.join(SHARD_DIR, key + ".json"), "w", encoding="utf-8") as f:
            json.dump(jobs, f, ensure_ascii=False, separators=(",", ":"))

    total_jobs = sum(len(v) for v in shards.values())
    index = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "boards_scraped": len(boards),
        "boards_ok": sum(1 for s in status.values() if s["ok"]),
        "boards_failed": sum(1 for s in status.values() if not s["ok"]),
        "jobs": total_jobs,
        "shards": sorted(shards.keys()),
    }
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)
    with open(os.path.join(DATA_DIR, "board_status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, separators=(",", ":"))
    print("done: jobs=%d ok=%d failed=%d" % (total_jobs, index["boards_ok"], index["boards_failed"]), flush=True)


if __name__ == "__main__":
    main()
