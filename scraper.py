"""
Padel Beach BR - Court Occupancy Scraper
Appends rows to data/occupancy.csv on each run.
Designed to run via GitHub Actions every 5 minutes.
"""

import asyncio
import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL        = "https://padelbeach-br.matchpoint.com.es/Booking/Grid.aspx"
TARGET_LOCATION = "Padel (Leopoldina)"
NUM_COURTS      = 3
DAYS_AHEAD      = 7
DATA_DIR        = Path(__file__).parent / "data"
CSV_PATH        = DATA_DIR / "occupancy.csv"
CSV_COLUMNS     = ["captured_at", "court_date", "court_id", "total_slots", "booked_slots", "pct_booked"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── CSV helpers ───────────────────────────────────────────────────────────────
def ensure_csv():
    DATA_DIR.mkdir(exist_ok=True)
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        log.info("Created %s", CSV_PATH)


def append_rows(rows: list[dict]):
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerows(rows)
    log.info("Appended %d rows to %s", len(rows), CSV_PATH)


# ── Scraper ───────────────────────────────────────────────────────────────────
async def scrape_date(page, target_date: datetime) -> list[dict]:
    date_str = target_date.strftime("%d/%m/%Y")
    date_key = target_date.strftime("%Y-%m-%d")
    now_utc  = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    log.info("Scraping %s", date_key)

    await page.goto(BASE_URL, wait_until="networkidle", timeout=30_000)

    # ── Select location ────────────────────────────────────────────────────────
    location_selectors = [
        "#ctl00_ContentPlaceHolder1_ddlLocation",
        "#ddlLocation",
        "select[id*='Location']",
        "select[id*='location']",
        "select[name*='Location']",
    ]
    for sel in location_selectors:
        try:
            if await page.locator(sel).count() > 0:
                await page.select_option(sel, label=TARGET_LOCATION)
                await page.wait_for_load_state("networkidle", timeout=10_000)
                log.info("Location selected via %s", sel)
                break
        except Exception:
            continue

    # ── Select date ────────────────────────────────────────────────────────────
    date_selectors = [
        "#ctl00_ContentPlaceHolder1_txtDate",
        "#txtDate",
        "input[id*='Date']",
        "input[id*='date']",
    ]
    date_set = False
    for sel in date_selectors:
        try:
            if await page.locator(sel).count() > 0:
                await page.fill(sel, date_str)
                await page.press(sel, "Enter")
                await page.wait_for_load_state("networkidle", timeout=10_000)
                date_set = True
                log.info("Date set via %s → %s", sel, date_str)
                break
        except Exception:
            continue

    if not date_set:
        await page.goto(f"{BASE_URL}?date={date_key}", wait_until="networkidle", timeout=30_000)

    # ── Parse grid via JS ──────────────────────────────────────────────────────
    slot_data = await page.evaluate("""
        () => {
            // Booked slots show a time RANGE e.g. "08:00-09:00" with a colored bg.
            // Free slots show a single time e.g. "08:00" with no background.
            const TIME_RANGE  = /^\\d{2}:\\d{2}-\\d{2}:\\d{2}$/;
            const TIME_SINGLE = /^\\d{2}:\\d{2}$/;

            const tables = Array.from(document.querySelectorAll('table'));

            // Find the booking grid: contains time-range cells, not the datepicker
            const grid = tables.find(t =>
                !t.className.includes('ui-datepicker') &&
                Array.from(t.querySelectorAll('td')).some(td =>
                    TIME_RANGE.test(td.textContent.trim())
                )
            );

            if (!grid) return { error: 'booking grid not found', courts: [] };

            const rows = Array.from(grid.querySelectorAll('tr'));
            if (rows.length < 2) return { error: 'too few rows', courts: [] };

            const headerCols = rows[0].querySelectorAll('th,td');
            const numCols    = headerCols.length;
            const tallies    = Array.from({ length: numCols }, () => ({ total: 0, booked: 0 }));

            for (let r = 1; r < rows.length; r++) {
                const cells = rows[r].querySelectorAll('td');
                cells.forEach((cell, c) => {
                    if (c === 0) return; // skip time label column
                    const text = cell.textContent.trim();
                    const bg   = cell.style.backgroundColor;

                    const isBooked = TIME_RANGE.test(text) ||
                                     (bg && bg !== '' && bg !== 'transparent');
                    const isFree   = TIME_SINGLE.test(text) && !isBooked;

                    if (isBooked || isFree) {
                        tallies[c].total++;
                        if (isBooked) tallies[c].booked++;
                    }
                });
            }

            const names = Array.from(headerCols).map(h => h.textContent.trim());
            return {
                courts: tallies.slice(1).map((t, i) => ({
                    courtIndex: i + 1,
                    courtName:  names[i + 1] || `Court ${i + 1}`,
                    total:  t.total,
                    booked: t.booked,
                }))
            };
        }
    """)

    if slot_data.get("error"):
        log.warning("JS parse error for %s: %s", date_key, slot_data["error"])
        screenshot = Path(__file__).parent / f"debug_{date_key}.png"
        await page.screenshot(path=str(screenshot), full_page=True)
        log.info("Debug screenshot: %s", screenshot)
        return []

    results = []
    for c in slot_data.get("courts", [])[:NUM_COURTS]:
        total  = c["total"]
        booked = c["booked"]
        pct    = round(booked / total * 100, 2) if total > 0 else 0.0
        results.append({
            "captured_at":  now_utc,
            "court_date":   date_key,
            "court_id":     c["courtIndex"],
            "total_slots":  total,
            "booked_slots": booked,
            "pct_booked":   pct,
        })
        log.info("  Court %d: %d/%d (%.1f%%)", c["courtIndex"], booked, total, pct)

    return results


async def run():
    ensure_csv()
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    dates = [today + timedelta(days=d) for d in range(DAYS_AHEAD)]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx     = await browser.new_context(
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
        )
        page = await ctx.new_page()

        all_rows = []
        for d in dates:
            try:
                rows = await scrape_date(page, d)
                all_rows.extend(rows)
            except PlaywrightTimeout:
                log.error("Timeout on %s — skipping", d.strftime("%Y-%m-%d"))
            except Exception as e:
                log.exception("Error on %s: %s", d.strftime("%Y-%m-%d"), e)

        await browser.close()

    if all_rows:
        append_rows(all_rows)
    else:
        log.warning("No rows collected")


if __name__ == "__main__":
    asyncio.run(run())
