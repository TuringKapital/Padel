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


# ── SVG parser ────────────────────────────────────────────────────────────────
async def parse_svg(page, date_key: str, now_utc: str) -> list[dict]:
    slot_data = await page.evaluate("""
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
                    x,
                    bookedSlots: Math.round(tally[x] * 10) / 10,
                }))
            };
        }
    """, [COURT_X_POSITIONS, SLOT_HEIGHT_PX])

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
            await page.wait_for_selector("#ctl00_ButtonPermitirNecesarios",
                                         state="visible", timeout=8_000)
            await page.click("#ctl00_ButtonPermitirNecesarios")
            await page.wait_for_load_state("networkidle", timeout=8_000)
            log.info("Cookie banner dismissed")
        except Exception:
            log.info("No cookie banner, continuing")

        # ── Select location once ───────────────────────────────────────────────
        try:
            await page.wait_for_selector("#calendarios", state="attached", timeout=15_000)
            await page.evaluate("""
                (label) => {
                    const sel = document.querySelector('#calendarios');
                    const opt = Array.from(sel.options).find(o => o.text.trim() === label);
                    if (opt) {
                        sel.value = opt.value;
                        sel.dispatchEven
