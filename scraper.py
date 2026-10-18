#!/usr/bin/env python3
"""Scrape public job boards (Greenhouse, Lever, Ashby) into static JSON shards.

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
SUPPORTED = {"greenhouse", "lever", "ashby"}
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


FETCHERS = {"greenhouse": fetch_greenhouse, "lever": fetch_lever, "ashby": fetch_ashby}


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
