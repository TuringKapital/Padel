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
BASE_URL        = "https://padelbeach-br.matchpoint.com.es/Booking/Grid.aspx"
TARGET_LOCATION = "Padel (Leopoldina)"
NUM_COURTS      = 3
DAYS_AHEAD      = 7
SLOT_HEIGHT_PX  = 35      # 1 time slot = 35px in the SVG
TOTAL_SLOTS     = 17      # 06:00–22:00 = 17 hourly slots per court
DATA_DIR        = Path(__file__).parent / "data"
CSV_PATH        = DATA_DIR / "occupancy.csv"
CSV_COLUMNS     = ["captured_at", "court_date", "court_id",
                   "total_slots", "booked_slots", "pct_booked"]

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

    # ── 1. Navigate ────────────────────────────────────────────────────────────
    await page.goto(BASE_URL, wait_until="networkidle", timeout=30_000)

    # ── 2. Select location ─────────────────────────────────────────────────────
    try:
        await page.select_option("#calendarios", label=TARGET_LOCATION)
        await page.wait_for_load_state("networkidle", timeout=10_000)
        log.info("Location selected")
    except Exception as e:
        log.warning("Could not select location: %s", e)

    # ── 3. Select date ─────────────────────────────────────────────────────────
    try:
        await page.fill("#fechaTabla", date_str)
        await page.press("#fechaTabla", "Enter")
        await page.wait_for_load_state("networkidle", timeout=10_000)
        log.info("Date set to %s", date_str)
    except Exception as e:
        log.warning("Could not set date: %s", e)

    # ── 4. Wait for SVG grid ───────────────────────────────────────────────────
    try:
        await page.wait_for_selector("svg#tablaReserva", timeout=10_000)
    except Exception:
        log.warning("SVG grid not found for %s — site may be offline", date_key)
        return []

    # ── 5. Parse SVG via JS ────────────────────────────────────────────────────
    slot_data = await page.evaluate("""
        (slotHeightPx) => {
            const svg = document.querySelector('svg#tablaReserva');
            if (!svg) return { error: 'SVG not found', courts: [] };

            // Discover court x-positions from ocupacion groups
            const ocupGroups = Array.from(svg.querySelectorAll('g[id^="ocupacion_"]'));
            const courtXSet  = new Set();
            ocupGroups.forEach(g => {
                const rect = g.querySelector('rect');
                if (rect) courtXSet.add(parseFloat(rect.getAttribute('x') || 0));
            });
            const courtXs = Array.from(courtXSet).sort((a, b) => a - b);

            if (courtXs.length === 0) return { error: 'No court columns found', courts: [] };

            // Initialise per-court booked slot tally
            const tally = {};
            courtXs.forEach(x => { tally[x] = 0; });

            // Sum booked slots from event rects (height / slotHeightPx)
            const events = Array.from(svg.querySelectorAll('g[id^="event_"]'));
            events.forEach(e => {
                const rect = e.querySelector('rect');
                if (!rect) return;
                const x = parseFloat(rect.getAttribute('x') || -1);
                const h = parseFloat(rect.getAttribute('height') || 0);
                // Find closest court x
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
                    x,
                    bookedSlots: Math.round(tally[x] * 10) / 10,
                }))
            };
        }
    """, SLOT_HEIGHT_PX)

    if slot_data.get("error"):
        log.warning("Parse error for %s: %s", date_key, slot_data["error"])
        screenshot = Path(__file__).parent / f"debug_{date_key}.png"
        await page.screenshot(path=str(screenshot), full_page=True)
        return []

    results = []
    for c in slot_data.get("courts", [])[:NUM_COURTS]:
        booked = c["bookedSlots"]
        total  = TOTAL_SLOTS
        pct    = round(booked / total * 100, 2)
        results.append({
            "captured_at":  now_utc,
            "court_date":   date_key,
            "court_id":     c["courtIndex"],
            "total_slots":  total,
            "booked_slots": booked,
            "pct_booked":   pct,
        })
        log.info("  Court %d: %.1f/%d slots booked (%.1f%%)",
                 c["courtIndex"], booked, total, pct)

    return results


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
