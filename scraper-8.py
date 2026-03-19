"""
Padel Beach BR - Court Occupancy Scraper
Parses the SVG-based booking grid at Matchpoint.
Appends rows to data/occupancy.csv on each run.
"""

import asyncio
import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL          = "https://padelbeach-br.matchpoint.com.es/Booking/Grid.aspx"
TARGET_LOCATION   = "PADEL (LEOPOLDINA)"
NUM_COURTS        = 3
DAYS_AHEAD        = 7
SLOT_HEIGHT_PX    = 35
TOTAL_SLOTS       = 17
COURT_X_POSITIONS = [50, 150, 250]
DATA_DIR          = Path(__file__).parent / "data"
CSV_PATH          = DATA_DIR / "occupancy.csv"
CSV_COLUMNS       = ["captured_at", "court_date", "court_id",
                     "total_slots", "booked_slots", "pct_booked"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

MONTH_NAMES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}


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


# ── JS snippets ───────────────────────────────────────────────────────────────
JS_SELECT_LOCATION = """
(label) => {
    const sel = document.querySelector('#calendarios');
    if (!sel) return false;
    const opt = Array.from(sel.options).find(o => o.text.trim() === label);
    if (opt) {
        sel.value = opt.value;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
    }
    return false;
}
"""

JS_GET_PICKER_MONTH_YEAR = """
() => {
    const picker = document.querySelector('#ui-datepicker-div');
    if (!picker) return { month: null, year: null };
    return {
        month: picker.querySelector('.ui-datepicker-month')?.textContent?.trim(),
        year:  picker.querySelector('.ui-datepicker-year')?.textContent?.trim(),
    };
}
"""

JS_IS_LOADING = """
() => {
    const spinner = document.querySelector('#imgCarga');
    if (!spinner) return false;
    const style = window.getComputedStyle(spinner);
    return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
}
"""

JS_GET_EVENT_COUNT = """
() => {
    const svg = document.querySelector('svg#tablaReserva');
    if (!svg) return -1;
    return svg.querySelectorAll('g[id^="event_"]').length;
}
"""

JS_PARSE_SVG = """
([courtXs, slotHeightPx]) => {
    const svg = document.querySelector('svg#tablaReserva');
    if (!svg) return { error: 'SVG not found', courts: [] };

    const tally = {};
    courtXs.forEach(x => { tally[x] = 0; });

    const events = Array.from(svg.querySelectorAll('g[id^="event_"]'));
    events.forEach(e => {
        const rect = e.querySelector('rect');
        if (!rect) return;
        const x = parseFloat(rect.getAttribute('x') || -1);
        const h = parseFloat(rect.getAttribute('height') || 0);
        let closest = null, minDist = Infinity;
        courtXs.forEach(cx => {
            const d = Math.abs(x - cx);
            if (d < minDist) { minDist = d; closest = cx; }
        });
        if (closest !== null && minDist < 10) {
            tally[closest] += h / slotHeightPx;
        }
    });

    return {
        courts: courtXs.map((x, i) => ({
            courtIndex: i + 1,
            x: x,
            bookedSlots: Math.round(tally[x] * 10) / 10,
        }))
    };
}
"""


# ── SVG parser ────────────────────────────────────────────────────────────────
async def parse_svg(page, date_key, now_utc):
    slot_data = await page.evaluate(JS_PARSE_SVG, [COURT_X_POSITIONS, SLOT_HEIGHT_PX])

    if slot_data.get("error"):
        log.warning("Parse error for %s: %s", date_key, slot_data["error"])
        return []

    results = []
    for c in slot_data.get("courts", [])[:NUM_COURTS]:
        booked = c["bookedSlots"]
        pct    = round(booked / TOTAL_SLOTS * 100, 2)
        results.append({
            "captured_at":  now_utc,
            "court_date":   date_key,
            "court_id":     c["courtIndex"],
            "total_slots":  TOTAL_SLOTS,
            "booked_slots": booked,
            "pct_booked":   pct,
        })
        log.info("  Court %d: %.1f/%d slots booked (%.1f%%)",
                 c["courtIndex"], booked, TOTAL_SLOTS, pct)
    return results


# ── Wait for SVG to stabilise ─────────────────────────────────────────────────
async def wait_for_svg(page):
    # Wait for spinner to appear then disappear
    for _ in range(4):
        await page.wait_for_timeout(500)
        if await page.evaluate(JS_IS_LOADING):
            log.info("Spinner appeared")
            break

    for _ in range(30):
        await page.wait_for_timeout(500)
        if not await page.evaluate(JS_IS_LOADING):
            break

    # Wait for event count to stabilise
    stable_count = 0
    last_count   = -1
    for _ in range(20):
        await page.wait_for_timeout(500)
        count = await page.evaluate(JS_GET_EVENT_COUNT)
        if count == last_count:
            stable_count += 1
            if stable_count >= 3:
                log.info("SVG stable with %d events", count)
                break
        else:
            stable_count = 0
            last_count   = count


# ── Click a date in the datepicker ────────────────────────────────────────────
async def click_date(page, target: datetime):
    target_month = MONTH_NAMES[target.month]
    target_year  = str(target.year)
    target_day   = str(target.day)

    # Open the datepicker by clicking the input
    await page.click("#fechaTabla")
    await page.wait_for_timeout(500)

    # Navigate to the correct month (max 12 clicks forward)
    for _ in range(12):
        info = await page.evaluate(JS_GET_PICKER_MONTH_YEAR)
        current_month = info.get("month", "")
        current_year  = info.get("year", "")
        log.info("Datepicker showing: %s %s", current_month, current_year)

        if current_month == target_month and current_year == target_year:
            break

        # Click the "next month" arrow
        next_btn = page.locator(".ui-datepicker-next")
        if await next_btn.count() > 0:
            await next_btn.click()
            await page.wait_for_timeout(300)
        else:
            log.warning("No next button found in datepicker")
            break

    # Click the correct day
    day_links = page.locator("#ui-datepicker-div td a")
    count = await day_links.count()
    for i in range(count):
        link = day_links.nth(i)
        text = (await link.text_content()).strip()
        if text == target_day:
            log.info("Clicking day %s", target_day)
            await link.click()
            return

    log.warning("Day %s not found in datepicker", target_day)


# ── Main ──────────────────────────────────────────────────────────────────────
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

        # ── Load page once ─────────────────────────────────────────────────────
        await page.goto(BASE_URL, wait_until="networkidle", timeout=30_000)

        # ── Dismiss cookie banner once ─────────────────────────────────────────
        try:
            await page.wait_for_selector(
                "#ctl00_ButtonPermitirNecesarios", state="visible", timeout=8_000)
            await page.click("#ctl00_ButtonPermitirNecesarios")
            await page.wait_for_load_state("networkidle", timeout=8_000)
            log.info("Cookie banner dismissed")
        except Exception:
            log.info("No cookie banner, continuing")

        # ── Select location once ───────────────────────────────────────────────
        try:
            await page.wait_for_selector("#calendarios", state="attached", timeout=15_000)
            await page.evaluate(JS_SELECT_LOCATION, TARGET_LOCATION)
            await page.wait_for_load_state("networkidle", timeout=10_000)
            await page.wait_for_timeout(1_000)
            log.info("Location selected")
        except Exception as e:
            log.warning("Could not select location: %s", e)

        # ── Loop through all 7 dates ───────────────────────────────────────────
        all_rows = []
        for d in dates:
            date_key = d.strftime("%Y-%m-%d")
            now_utc  = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            try:
                log.info("Clicking date %s", date_key)
                await click_date(page, d)
                await wait_for_svg(page)
                rows = await parse_svg(page, date_key, now_utc)
                all_rows.extend(rows)
            except PlaywrightTimeout:
                log.error("Timeout on %s — skipping", date_key)
            except Exception as e:
                log.exception("Error on %s: %s", date_key, e)

        await browser.close()

    if all_rows:
        append_rows(all_rows)
    else:
        log.warning("No rows collected")


if __name__ == "__main__":
    asyncio.run(run())
