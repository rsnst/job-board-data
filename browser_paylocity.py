"""Headless-browser fetcher for Paylocity recruiting pages.

Paylocity renders its job list with JavaScript, so plain HTTP returns no jobs.
Each board page is opened in headless Chromium and the rendered rows are read.
"""
import asyncio
import re
import time

BASE = "https://recruiting.paylocity.com"
UA = "job-board-search/1.0 (+public job list reader)"

EXTRACT_JS = r"""
() => Array.from(document.querySelectorAll('div.job-listing-job-item')).map(function (row) {
  var a = row.querySelector('a[href*="Jobs/Details"]');
  var href = a ? (a.getAttribute('href') || '') : '';
  var m = href.match(/Details\/(\d+)/);
  var date = '';
  Array.from(row.querySelectorAll('.job-title-column span')).forEach(function (s) {
    if (!date && !s.classList.contains('job-item-title') && /\d{1,2}\/\d{1,2}\/\d{4}/.test(s.textContent)) {
      date = s.textContent.trim();
    }
  });
  var loc = row.querySelector('.location-column span');
  return {
    jobId: m ? m[1] : '',
    title: a ? a.textContent.trim() : '',
    href: href,
    postedText: date,
    location: loc ? loc.textContent.trim() : ''
  };
})
"""


async def _scrape_one(context, board, sem, results, timeout_ms):
    async with sem:
        page = await context.new_page()
        try:
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ("image", "font", "stylesheet", "media") else route.continue_())
            url = "%s/recruiting/jobs/All/%s" % (BASE, board["slug"])
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                await page.wait_for_selector("div.job-listing-job-item", timeout=2000)
            except Exception:
                pass  # empty board, or the list did not render in time
            rows = await page.evaluate(EXTRACT_JS)
            jobs = []
            for r in rows:
                if not r.get("jobId"):
                    continue
                href = r.get("href") or ""
                jobs.append({
                    "id": "%s__%s" % (board["id"], r["jobId"]),
                    "title": r.get("title") or "Untitled",
                    "location": r.get("location") or "",
                    "url": (BASE + href) if href.startswith("/") else href,
                    "postedAt": r.get("postedText") or "",
                })
            results[board["id"]] = {"ok": True, "jobs": jobs}
        except Exception as e:
            results[board["id"]] = {"ok": False, "error": str(e)[:200]}
        finally:
            await page.close()


async def run_async(boards, concurrency=8, timeout_ms=30000, log=print):
    from playwright.async_api import async_playwright

    results = {}
    total = len(boards)
    started = time.time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(args=["--no-sandbox"])
        context = await browser.new_context(user_agent=UA)
        sem = asyncio.Semaphore(concurrency)

        async def tracked(b):
            await _scrape_one(context, b, sem, results, timeout_ms)
            done = len(results)
            if done % 250 == 0 or done == total:
                log("paylocity progress %d/%d in %.0fs" % (done, total, time.time() - started))

        await asyncio.gather(*(tracked(b) for b in boards))
        await context.close()
        await browser.close()
    return results


def run(boards, concurrency=8, log=print):
    return asyncio.run(run_async(boards, concurrency=concurrency, log=log))
