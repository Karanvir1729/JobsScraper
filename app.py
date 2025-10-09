import os
import io
import time
from datetime import datetime
from pathlib import Path
import re
from urllib.parse import quote

import streamlit as st
import yaml
import json
import csv

import sys
import subprocess
from scrapy.utils.project import get_project_settings
from golden_record_gen import (
    main as build_golden_main,
    GOLDEN_CSV as _GOLDEN_CSV,
    read_csv_rows as _read_rows_g,
    write_csv_rows as _write_rows_g,
    normalize_phone as _norm_phone,
)


# Default to JSON config (can still read YAML if provided)
DEFAULT_CONFIG_PATH = Path("config/sources.json")
EXAMPLE_CONFIG_PATH = Path("config/sources.example.json")
OUTPUT_DIR = Path("output")
GOLDEN_CSV = OUTPUT_DIR / "providers-golden.csv"

# Provinces and default categories
PROVINCES = [
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
]

PROVINCE_NAMES = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "YT": "Yukon",
}

DEFAULT_CATEGORIES = [
    # Core trades
    "Plumbing","HVAC","Electrician","Roofing","Lawn Care","Landscaping","Handyman","Painting",
    "Flooring","Carpet Cleaning","Window Cleaning","Gutter Cleaning","Pest Control","Appliance Repair",
    "Moving","Junk Removal","Fencing","Deck Builder","Garage Door","Locksmith","Security Systems",
    "Solar Installer","Siding","Masonry","Concrete","Excavation","Tree Service","Snow Removal",
    "Waterproofing","Foundation Repair","Bathroom Remodeling","Kitchen Remodeling","General Contractor",
    "Home Inspection","Pool Service","Septic Service","Well Drilling","Water Treatment","Heating Oil",
    "Chimney Sweep","Duct Cleaning","Insulation","Mold Remediation","Fire Damage Restoration",
    "Water Damage Restoration","Upholstery Cleaning","Tile and Grout","Countertops","Cabinet Maker",
    "Interior Design","Architect","Surveyor","Window Installation","Door Installation","Drywall",
    "Framing Contractor","Demolition","Asphalt Paving","Paving Stones","Sprinkler Systems","Irrigation",
    "Fence Repair","Deck Repair","Pergola Builder","Patio Builder","Gazebo Builder","Shed Builder",
    "Basement Waterproofing","Basement Finishing","Attic Insulation","Skylight Installer","Gutter Installer",
    "Eavestrough","Downspout Cleaning","Power Washing","Pressure Washing","Stonework","Tiling",
    "Hardwood Flooring","Laminate Flooring","Vinyl Flooring","Epoxy Flooring","Garage Epoxy",
    "Window Tinting","Blinds and Shades","Curtain Installation","Smart Home Installer","Alarm Installer",
    "Audio Video Installer","Home Theater","Home Automation","Data Cabling","Network Installer",
    "Cable Installer","Satellite Installer","Antenna Installer","TV Mounting","EV Charger Installer",
    "Handrail Installer","Stair Contractor","Plastering","Skim Coating","Stucco","Soffit and Fascia",
    "Sump Pump","Radon Mitigation","Air Duct Sealing","Energy Audit","Backflow Testing",
]


def _slugify_for_hotfrog(q: str) -> str:
    s = re.sub(r"[^a-z0-9\- ]+", "", q.lower())
    s = re.sub(r"\s+", "-", s).strip("-")
    return s or q.lower()


def _path_for_opendi(q: str) -> str:
    # Opendi uses words with '+' and .html
    s = "+".join(re.findall(r"[A-Za-z0-9]+", q))
    return f"{s}.html" if s else f"{q}.html"


def build_dynamic_sources(selected_sources: list[str], categories: list[str], provinces: list[str], visit_web_email: bool = True) -> dict:
    sources: list[dict] = []
    # Common headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9",
    }

    sel_411 = {
        "item_selector": [
            "div.listing",
            "div.result",
            "article.listing",
            "li",
            "article",
            "div[class*='result']",
        ],
        "fields": {
            # Prefer specific title/name selectors first; keep generic fallback last
            "business_name": [
                "h2 a::text",
                "a.business-name::text",
                "h3 a::text",
                "[itemprop='name']::text",
                "a[title]::attr(title)",
                "a::text",
            ],
            "phone": [
                "a[href^='tel:']::attr(href)",
                "span[itemprop='telephone']::text",
                "div.phone::text",
                ".phone::text",
            ],
            "website": ["a[href^='http']::attr(href)", "a.website::attr(href)"],
            "address": ["div.address::text", "address::text"],
        },
        "detail_link_selector": ["h2 a", "a.business-name", "h3 a", "a"],
        "follow_links_selector": [
            "a[href*='/business/profile/']",
            "a[href*='/business/']",
        ],
    }

    sel_hotfrog = {
        "item_selector": ["article", "div.result", "li", "div.card", "div.search-results__result"],
        "fields": {
            "business_name": [
                "h3 a::text",
                "h2 a::text",
                "[itemprop='name']::text",
                "a[title]::attr(title)",
                "a::text",
            ],
            "phone": [
                "a[href^='tel:']::attr(href)",
                "span[itemprop='telephone']::text",
                "div.phone::text",
                ".phone::text",
            ],
            "website": ["a[href^='http']::attr(href)"],
            "address": ["address::text", "div.address::text"],
        },
        "detail_link_selector": ["h3 a", "h2 a", "a"],
        "follow_links_selector": ["a[href*='/company/']", "a[href*='/business/']"],
    }

    sel_opendi = {
        "item_selector": ["article", "div.result", "li"],
        "fields": {
            "business_name": ["h2 a::text", "h3 a::text", "a::text"],
            "phone": ["a[href^='tel:']::attr(href)"],
            "website": ["a[href^='http']::attr(href)"],
            "address": ["address::text", "div.address::text"],
        },
        "detail_link_selector": ["h2 a", "h3 a", "a"],
        "follow_links_selector": ["a[href*='/place/']", "a[href*='/listing/']"],
    }

    sel_yelp = {
        # We primarily follow links to detail pages and use JSON-LD there
        # because Yelp listing card CSS changes frequently.
        "follow_links_selector": ["a[href*='/biz/']"],
    }

    for cat in categories:
        q = cat.strip()
        if not q:
            continue
        if "411.ca" in selected_sources:
            start_urls = [f"https://411.ca/business/search?q={q.replace(' ', '%20')}&st={p.lower()}" for p in provinces]
            sources.append({
                "name": f"411.ca - {q}",
                "category": q,
                "region": ", ".join(provinces),
                "jsonld_fallback": True,
                "visit_website_for_email": visit_web_email,
                "scan_phones_on_page": True,
                "skip_visited_listings": True,
                "skip_visited_details": True,
                "headers": {**headers, "Referer": "https://411.ca/"},
                "start_urls": start_urls,
                "listing": sel_411,
                "detail": {"fields": {
                    "business_name": ["h1::text", "[itemprop='name']::text"],
                    "phone": ["a[href^='tel:']::attr(href)", "[itemprop='telephone']::text"],
                    "email": "a[href^='mailto:']::attr(href)",
                    "website": ["a[href^='http']", "[itemprop='url']::attr(href)"],
                    "address": "address::text"
                }},
                "pagination": {
                    "next_page_selector": ["a[rel='next']", "a.next", "a[aria-label='Next']", "a.pagination__next", ".pagination a[rel='next']"],
                    "param": {"name": "page", "start": 1, "max_pages": 40},
                },
            })
        if "Hotfrog" in selected_sources:
            start_urls = [f"https://www.hotfrog.ca/find/{_slugify_for_hotfrog(q)}"]
            sources.append({
                "name": f"Hotfrog - {q}",
                "category": q,
                "region": "Canada",
                "jsonld_fallback": True,
                "visit_website_for_email": visit_web_email,
                "scan_phones_on_page": True,
                "skip_visited_listings": True,
                "skip_visited_details": True,
                "headers": {**headers, "Referer": "https://www.hotfrog.ca/"},
                "start_urls": start_urls,
                "listing": sel_hotfrog,
                "detail": {"fields": {
                    "business_name": ["h1::text", "[itemprop='name']::text"],
                    "phone": ["a[href^='tel:']::attr(href)", "[itemprop='telephone']::text"],
                    "email": "a[href^='mailto:']::attr(href)",
                    "website": ["a[href^='http']::attr(href)", "[itemprop='url']::attr(href)"],
                    "address": "address::text"
                }},
                "pagination": {
                    "next_page_selector": ["a[rel='next']", "a.next", "a[aria-label='Next']"],
                    "param": {"name": "page", "start": 1, "max_pages": 40},
                },
            })
        if "Opendi" in selected_sources:
            # Use Opendi search with what/where parameters; generate per province.
            start_urls = [
                f"https://www.opendi.ca/search/?what={quote(q)}&where={quote(PROVINCE_NAMES.get(p, p))}"
                for p in provinces
            ]
            sources.append({
                "name": f"Opendi - {q}",
                "category": q,
                "region": ", ".join(provinces),
                "jsonld_fallback": True,
                "visit_website_for_email": visit_web_email,
                "headers": {**headers, "Referer": "https://www.opendi.ca/"},
                "start_urls": start_urls,
                "listing": sel_opendi,
                "detail": {"fields": {"email": "a[href^='mailto:']::attr(href)", "website": "a[href^='http']::attr(href)", "address": "address::text"}},
                "pagination": {
                    "next_page_selector": ["a[rel='next']", "a.next", "a[aria-label='Next']", ".pagination a[rel='next']"],
                    "param": {"name": "page", "start": 1, "max_pages": 40},
                },
            })
        if "Yelp" in selected_sources:
            # Yelp HTML search requires a resolvable location string, not just province codes.
            # Use full province names with ", Canada" and URL-encode them.
            start_urls = [
                f"https://www.yelp.ca/search?find_desc={quote(q)}&find_loc={quote(PROVINCE_NAMES.get(p, p) + ', Canada')}"
                for p in provinces
            ]
            sources.append({
                "name": f"Yelp - {q}",
                "category": q,
                "region": ", ".join(provinces),
                "jsonld_fallback": True,
                "visit_website_for_email": visit_web_email,
                "headers": {**headers, "Referer": "https://www.yelp.ca/"},
                "start_urls": start_urls,
                "listing": sel_yelp,
                # Rely on JSON-LD on detail pages; keep explicit selectors minimal for resilience
                "detail": {"fields": {}},
                "pagination": {
                    "next_page_selector": ["a[rel='next']"],
                },
            })
        if "Yelp API" in selected_sources:
            # Yelp Fusion API source. Requires env var YELP_API_KEY at runtime.
            locations = [f"{PROVINCE_NAMES.get(p, p)}, Canada" for p in provinces]
            sources.append({
                "name": f"Yelp API - {q}",
                "category": q,
                "region": ", ".join(provinces),
                "api": "yelp",
                "jsonld_fallback": True,
                "visit_website_for_email": visit_web_email,
                "locations": locations,
                "limit": 50,
                "max_pages": 10,
                "locale": "en_CA"
            })

    return {"sources": sources}


def ensure_paths():
    OUTPUT_DIR.mkdir(exist_ok=True)
    DEFAULT_CONFIG_PATH.parent.mkdir(exist_ok=True)
    if not DEFAULT_CONFIG_PATH.exists():
        # If a YAML example exists, convert it to JSON; else create a minimal JSON config.
        yml_example = Path("config/sources.example.yml")
        if EXAMPLE_CONFIG_PATH.exists():
            DEFAULT_CONFIG_PATH.write_text(EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        elif yml_example.exists():
            try:
                data = yaml.safe_load(yml_example.read_text(encoding="utf-8")) or {"sources": []}
                DEFAULT_CONFIG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                DEFAULT_CONFIG_PATH.write_text(json.dumps({"sources": []}, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            DEFAULT_CONFIG_PATH.write_text(json.dumps({"sources": []}, ensure_ascii=False, indent=2), encoding="utf-8")


def load_config_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Default text by extension
    if path.suffix.lower() == ".json":
        return json.dumps({"sources": []}, ensure_ascii=False, indent=2)
    return "sources: []\n"


def save_config_text(path: Path, text: str) -> None:
    # Validate and save based on extension
    if path.suffix.lower() == ".json":
        json.loads(text)
        path.write_text(text, encoding="utf-8")
    else:
        yaml.safe_load(text)
        path.write_text(text, encoding="utf-8")


def _preferred_csv_fields() -> list[str]:
    return [
        "source","category","region",
        "business_name","phone","email","website",
        "address","city","province","postal_code",
        "listing_url","detail_url",
    ]


def _read_csv_rows(p: Path) -> list[dict]:
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(r) for r in reader]
    except Exception:
        return []


def _write_csv_rows(p: Path, rows: list[dict]):
    if not rows:
        # Ensure file exists even if empty
        p.write_text("", encoding="utf-8")
        return
    # Union of keys with preferred ordering
    keys = set()
    for r in rows:
        keys.update(r.keys())
    ordered = [k for k in _preferred_csv_fields() if k in keys]
    extras = [k for k in sorted(keys) if k not in ordered]
    fieldnames = ordered + extras
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _merge_dict_priority(a: dict, b: dict) -> dict:
    # Merge two dicts favoring non-empty values from a, then b
    out = dict(a)
    for k, v in b.items():
        av = out.get(k)
        if (av is None or str(av).strip() == "") and v:
            out[k] = v
    return out


def update_golden_and_augment(csv_path: Path) -> None:
    """Ensure this run's CSV starts with Golden baseline, then adds new exact-unique rows.

    Rules:
    - Do not remove any existing Golden rows
    - Only drop exact duplicates (row equality across fields)
    - Include only rows with a phone number
    - Persist updated Golden after augmenting
    """
    try:
        OUTPUT_DIR.mkdir(exist_ok=True)
        if not csv_path.exists():
            return
        # Helper to canonicalize a row for exact-dup detection
        def _canon(r: dict) -> str:
            items = sorted((str(k), "" if r.get(k) is None else str(r.get(k))) for k in r.keys())
            return "\u241F".join([f"{k}\u241E{v}" for k, v in items])

        # Load baseline (Golden) and current
        golden_rows = _read_rows_g(_GOLDEN_CSV) if _GOLDEN_CSV.exists() else []
        golden_rows = [dict(r) for r in golden_rows if (_norm_phone(r.get("phone")) or "")]
        current_rows = [dict(r) for r in _read_csv_rows(csv_path) if (_norm_phone(r.get("phone")) or "")]

        seen = { _canon(r) for r in golden_rows }
        new_rows = []
        for r in current_rows:
            key = _canon(r)
            if key in seen:
                continue
            new_rows.append(r)
            seen.add(key)

        combined = list(golden_rows) + new_rows
        _write_rows_g(csv_path, combined)
        _write_rows_g(_GOLDEN_CSV, combined)
    except Exception:
        # Silent failure: do not block the run on golden handling
        pass


def run_scrape(config_path: Path, time_limit_sec: int, max_items: int | None,
               concurrent_requests: int, download_delay: float, min_per_source: int) -> Path:
    """Run the Scrapy spider in a subprocess to avoid Twisted signal issues."""
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = OUTPUT_DIR / f"providers-{ts}.csv"
    summary_path = OUTPUT_DIR / f"providers-{ts}-summary.json"
    errors_path = OUTPUT_DIR / f"providers-{ts}-errors.json"

    cmd = [
        sys.executable,
        str(Path(__file__).parent / "run_spider.py"),
        "--config", str(config_path),
        "--csv", str(csv_path),
        "--timeout", str(int(time_limit_sec)),
        "--summary", str(summary_path),
        "--errors", str(errors_path),
        "--concurrent", str(int(concurrent_requests)),
        "--delay", str(float(download_delay)),
        "--min-per-source", str(int(min_per_source)),
    ]
    if max_items:
        cmd += ["--max-items", str(int(max_items))]

    # Run and stream output to Streamlit logs if needed
    completed = subprocess.run(cmd, capture_output=True, text=True)
    if completed.returncode != 0:
        st.error(f"Crawler failed. stderr:\n{completed.stderr}")
    else:
        # Show a bit of last lines of stdout for context
        tail = "\n".join(completed.stdout.splitlines()[-10:])
        if tail:
            st.code(tail)

    # Update golden CSV and augment this run's CSV silently
    try:
        if csv_path.exists():
            update_golden_and_augment(csv_path)
    except Exception:
        pass

    # Attach paths to session for display
    st.session_state["last_summary_path"] = str(summary_path)
    st.session_state["last_errors_path"] = str(errors_path)
    st.session_state["last_csv_path"] = str(csv_path)
    return csv_path


def start_scrape_async(config_path: Path, time_limit_sec: int, max_items: int | None,
                       concurrent_requests: int, download_delay: float, min_per_source: int) -> dict:
    """Start the Scrapy spider as a background subprocess and return job info.
    Stores paths and PID in session_state for polling and UI updates.
    """
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    csv_path = OUTPUT_DIR / f"providers-{ts}.csv"
    summary_path = OUTPUT_DIR / f"providers-{ts}-summary.json"
    errors_path = OUTPUT_DIR / f"providers-{ts}-errors.json"
    log_path = OUTPUT_DIR / f"providers-{ts}.log"

    cmd = [
        sys.executable,
        str(Path(__file__).parent / "run_spider.py"),
        "--config", str(config_path),
        "--csv", str(csv_path),
        "--timeout", str(int(time_limit_sec)),
        "--summary", str(summary_path),
        "--errors", str(errors_path),
        "--concurrent", str(int(concurrent_requests)),
        "--delay", str(float(download_delay)),
        "--min-per-source", str(int(min_per_source)),
    ]
    if max_items:
        cmd += ["--max-items", str(int(max_items))]

    lf = open(log_path, "w", encoding="utf-8")
    proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, text=True)

    job = {
        "pid": int(proc.pid),
        "cmd": cmd,
        "csv_path": str(csv_path),
        "summary_path": str(summary_path),
        "errors_path": str(errors_path),
        "log_path": str(log_path),
        "started_at": time.time(),
    }
    st.session_state["job"] = job
    st.session_state["last_summary_path"] = str(summary_path)
    st.session_state["last_errors_path"] = str(errors_path)
    st.session_state["last_csv_path"] = str(csv_path)
    return job


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except Exception:
        return False
    return True


def _rerun():
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()  # for older Streamlit
        except Exception:
            pass


def _build_yelp_api_csvs(yelp_api: dict):
    """Return (overview_csv_bytes, locations_csv_bytes) for Yelp API metrics."""
    # Overview CSV
    ov_io = io.StringIO()
    ov_writer = csv.writer(ov_io)
    ov_writer.writerow(["source", "api_requests", "businesses", "errors", "locations"])
    for src, s in (yelp_api or {}).items():
        ov_writer.writerow([
            src,
            int((s or {}).get("api_requests", 0)),
            int((s or {}).get("businesses", 0)),
            int((s or {}).get("errors", 0)),
            len(((s or {}).get("per_location") or {})),
        ])
    ov_bytes = ov_io.getvalue().encode("utf-8")

    # Per-location CSV
    loc_io = io.StringIO()
    loc_writer = csv.writer(loc_io)
    loc_writer.writerow(["source", "location", "returned", "reported_total"])
    for src, s in (yelp_api or {}).items():
        per = (s or {}).get("per_location") or {}
        rep = (s or {}).get("reported_totals") or {}
        for k in per.keys():
            loc_writer.writerow([src, k, int(per.get(k, 0)), int(rep.get(k, 0))])
    loc_bytes = loc_io.getvalue().encode("utf-8")
    return ov_bytes, loc_bytes


def main():
    st.set_page_config(page_title="Canada Home Services Scraper", layout="wide")
    ensure_paths()
    if "is_running" not in st.session_state:
        st.session_state["is_running"] = False

    st.title("Canada Home Services Scraper")
    st.caption("Config-driven Scrapy crawler with Streamlit UI. Edit sources and run.")

    with st.sidebar:
        st.header("Run Settings")
        time_limit = st.number_input("Max runtime (seconds)", min_value=15, max_value=60*60, value=15, step=15)
        max_items = st.number_input("Max items (optional, reccomended 0)", min_value=0, max_value=100000, value=0, step=50,
                                    help="Stop after N items. Leave 0 to ignore.")
        concurrent_requests = st.slider("Concurrent requests", min_value=2, max_value=32, value=10)
        download_delay = st.slider("Download delay (seconds)", min_value=0.0, max_value=5.0, value=0.6, step=0.1)
        min_per_source = st.number_input("Minimum items per source", min_value=0, max_value=10000, value=100, step=10,
                                         help="Crawler tries to paginate until at least this many items per source or timeout.")
        st.divider()

    st.subheader("Sources Configuration")
    st.caption("Build from categories or edit raw JSON.")

    with st.expander("Build from Categories", expanded=True):
        sel_sources = st.multiselect(
            "Sources",
            options=["411.ca","Hotfrog","Opendi","Yelp","Yelp API"],
            default=["411.ca","Hotfrog"]
        ) 
        sel_provinces = st.multiselect("Provinces", options=PROVINCES, default=["ON","BC","AB","QC"]) 
        visit_web_email = st.checkbox("Visit business websites to find emails", value=True)
        st.caption("Pick service categories (type to search)")
        sel_categories = st.multiselect("Categories", options=DEFAULT_CATEGORIES, default=["Plumbing","HVAC","Roofing","Lawn Care","Electrician","Pest Control","Appliance Repair","Moving","Junk Removal","Handyman","Painting"]) 
        if st.button("Preview Generated Config"):
            generated_dict = build_dynamic_sources(sel_sources, sel_categories, sel_provinces, visit_web_email)
            generated = json.dumps(generated_dict, ensure_ascii=False, indent=2)
            st.code(generated, language="json")

    with st.expander("Raw JSON (advanced)", expanded=False):
        config_text = load_config_text(DEFAULT_CONFIG_PATH)
        current_text = st.session_state.get("config_text", config_text)
        config_editor = st.text_area(
            "Edit and Save to use these sources",
            value=current_text,
            height=260,
            label_visibility="collapsed",
            key="config_text",
        )
    cols = st.columns([1,1,1])
    with cols[0]:
        if st.button("Save Config", type="primary"):
            try:
                save_config_text(DEFAULT_CONFIG_PATH, st.session_state.get("config_text", config_editor))
                st.success("Config saved.")
            except Exception as e:
                st.error(f"Failed to save config: {e}")
    with cols[1]:
        if st.button("Load Example"):
            if EXAMPLE_CONFIG_PATH.exists():
                st.session_state["config_text"] = EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8")
                _rerun()
            else:
                st.info("No example config shipped.")
    with cols[2]:
        if st.button("Revert to Disk"):
            st.session_state["config_text"] = load_config_text(DEFAULT_CONFIG_PATH)
            _rerun()

    st.subheader("Run Scraper")
    is_running = st.session_state.get("is_running", False)
    run_clicked = False
    if not is_running:
        # If the last run just finished, surface success here
        last_elapsed = st.session_state.get("last_finish_elapsed")
        last_csv_name = st.session_state.get("last_finish_csv_name")
        if last_elapsed is not None and last_csv_name:
            st.success(f"Done in {float(last_elapsed):.1f}s. CSV generated: {last_csv_name}")
        run_clicked = st.button("Run", type="primary")
    else:
        st.info("Once scraper is done running you should should see the output below")
        # While running, surface the latest CSV found in the output folder if present
        try:
            _running_latest = sorted(OUTPUT_DIR.glob("providers-*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            if _running_latest:
                st.caption(f"Newest CSV detected: {_running_latest[0].name}")
        except Exception:
            pass
        # If we have a background job, show basic status
        job = st.session_state.get("job") or {}
        if job:
            st.caption(f"PID: {job.get('pid')} | CSV: {Path(job.get('csv_path','')).name}")

    if run_clicked and not st.session_state.get("is_running", False):
        try:
            # Build dynamic config from categories as JSON
            generated_dict = build_dynamic_sources(sel_sources, sel_categories, sel_provinces, visit_web_email)
            generated = json.dumps(generated_dict, ensure_ascii=False, indent=2)
            save_config_text(DEFAULT_CONFIG_PATH, generated)
        except Exception as e:
            st.error(f"Config invalid: {e}")
            return
        # Persist target to session for summary comparisons
        st.session_state["min_per_source_target"] = int(min_per_source)
        st.session_state["is_running"] = True
        job = start_scrape_async(
            DEFAULT_CONFIG_PATH,
            int(time_limit),
            int(max_items) if max_items > 0 else None,
            int(concurrent_requests),
            float(download_delay),
            int(min_per_source),
        )
        st.success(f"Started scraper (PID {job['pid']}). Logs: {job['log_path']}")
        _rerun()

    # If a job is recorded, poll its status and update UI
    job = st.session_state.get("job")
    if job and st.session_state.get("is_running", False):
        pid = int(job.get("pid"))
        if not _pid_is_running(pid):
            st.session_state["is_running"] = False
            # Compute elapsed time and show success with immediate download
            try:
                elapsed = max(0.0, time.time() - float(job.get("started_at") or 0.0))
            except Exception:
                elapsed = 0.0
            csv_path = Path(job.get("csv_path", ""))
            if csv_path.exists():
                # Update golden CSV and augment user CSV silently
                try:
                    update_golden_and_augment(csv_path)
                except Exception:
                    pass
                st.session_state["last_csv_path"] = str(csv_path)
                st.session_state["last_finish_elapsed"] = float(elapsed)
                st.session_state["last_finish_csv_name"] = csv_path.name
                st.success(f"Done in {elapsed:.1f}s. CSV generated: {csv_path.name}")
                try:
                    st.download_button(
                        "Download CSV",
                        data=csv_path.read_bytes(),
                        file_name=csv_path.name,
                        mime="text/csv",
                        key=f"done-dl-{csv_path.name}"
                    )
                except Exception:
                    pass
            else:
                st.warning("No CSV produced. Check logs/selectors and try again.")
            # Keep last paths in session; UI below will render summary/errors if present
            _rerun()

        # Show per-source summary and alerts
        summary_file = Path(st.session_state.get("last_summary_path", ""))
        st.subheader("Run Summary")
        if summary_file.exists():
            try:
                data = json.loads(summary_file.read_text(encoding="utf-8"))
                counts = data.get("counts", {})
                configured = data.get("configured_sources", [])
                target = int(st.session_state.get("min_per_source_target", 0))
                rows = [{"source": s or "(unnamed)", "items_found": int(counts.get(s, 0)), "target": target} for s in configured]
                if rows:
                    st.table(rows)
                zero_rows = [r for r in rows if r["items_found"] == 0]
                below_rows = [r for r in rows if target and r["items_found"] < target]
                if zero_rows:
                    st.warning(f"No items fetched from {len(zero_rows)} source(s). See table above for details.")
                if below_rows:
                    st.warning(f"{len(below_rows)} source(s) are below the target of {target} items. Consider increasing timeout, enabling more regions, or adjusting selectors.")
                elif rows and not zero_rows:
                    st.success("Fetched items from all configured sources at or above target.")
                # Yelp API summary if present
                yelp_api = data.get("yelp_api") or {}
                if isinstance(yelp_api, dict) and yelp_api:
                    st.subheader("Yelp API Summary")
                    yrows = []
                    for src, s in yelp_api.items():
                        yrows.append({
                            "source": src,
                            "api_requests": int(s.get("api_requests", 0)),
                            "businesses": int(s.get("businesses", 0)),
                            "errors": int(s.get("errors", 0)),
                            "locations": len((s.get("per_location") or {})),
                        })
                    if yrows:
                        st.table(yrows)
                        ov_csv, loc_csv = _build_yelp_api_csvs(yelp_api)
                        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                        st.download_button(
                            "Download Yelp API Overview CSV",
                            data=ov_csv,
                            file_name=f"yelp_api_overview_{ts}.csv",
                            mime="text/csv",
                            key=f"yelp-ov-{ts}"
                        )
                        st.download_button(
                            "Download Yelp API Per-Location CSV",
                            data=loc_csv,
                            file_name=f"yelp_api_locations_{ts}.csv",
                            mime="text/csv",
                            key=f"yelp-loc-{ts}"
                        )
                    with st.expander("Yelp API per-location breakdown"):
                        for src, s in yelp_api.items():
                            st.markdown(f"- {src}")
                            per = s.get("per_location") or {}
                            rep = s.get("reported_totals") or {}
                            if per:
                                rows_pl = [
                                    {"location": k, "returned": int(per.get(k, 0)), "reported_total": int(rep.get(k, 0))}
                                    for k in per.keys()
                                ]
                                st.table(rows_pl)
                            else:
                                st.caption("(no per-location data)")
            except Exception as e:
                st.info(f"No summary available: {e}")
        else:
            st.info("No summary file found.")

        # Show errors table
        errors_file = Path(st.session_state.get("last_errors_path", ""))
        st.subheader("Errors")
        if errors_file.exists():
            try:
                edata = json.loads(errors_file.read_text(encoding="utf-8"))
                rows = []
                for src, items in edata.items():
                    for it in items:
                        rows.append({
                            "source": src,
                            "type": it.get("type"),
                            "status": it.get("status"),
                            "url": it.get("url"),
                            "error": (it.get("error") or it.get("note")),
                        })
                if rows:
                    st.table(rows)
                else:
                    st.info("No errors recorded.")
            except Exception as e:
                st.info(f"No errors available: {e}")
        else:
            st.info("No errors file found.")


    # Always show the latest CSV if available, even while running
    def _find_latest_csv() -> Path | None:
        try:
            files = sorted(OUTPUT_DIR.glob("providers-*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
            return files[0] if files else None
        except Exception:
            return None

    st.subheader("Latest Output")
    last_csv_path = st.session_state.get("last_csv_path")
    last_csv = Path(last_csv_path) if last_csv_path else None
    if not last_csv or not last_csv.exists():
        last_csv = _find_latest_csv()
    if last_csv and last_csv.is_file():
        try:
            st.caption(f"File: {last_csv}")
            csv_bytes = last_csv.read_bytes()
            st.download_button("Download Latest CSV", data=csv_bytes, file_name=last_csv.name, mime="text/csv", key=f"dl-{last_csv.name}")
        except Exception as e:
            st.warning(f"CSV at {last_csv} couldn't be opened: {e}")
    else:
        st.info("No CSVs found yet in output/")

    # Auto-refresh while running to surface new CSVs and logs
    if st.session_state.get("is_running", False):
        st.caption("Auto-refreshing every 5s while scraper is running…")
        try:
            time.sleep(5)
            _rerun()
        except Exception:
            pass

    last_summary_path = st.session_state.get("last_summary_path")
    last_summary = Path(last_summary_path) if last_summary_path else None
    if last_summary and last_summary.is_file() and not is_running:
        st.subheader("Last Run Summary")
        try:
            data = json.loads(last_summary.read_text(encoding="utf-8"))
            counts = data.get("counts", {})
            configured = data.get("configured_sources", [])
            target = int(st.session_state.get("min_per_source_target", 0))
            rows = [{"source": s or "(unnamed)", "items_found": int(counts.get(s, 0)), "target": target} for s in configured]
            if rows:
                st.table(rows)
            yelp_api = data.get("yelp_api") or {}
            if isinstance(yelp_api, dict) and yelp_api:
                st.subheader("Yelp API Summary")
                yrows = []
                for src, s in yelp_api.items():
                    yrows.append({
                        "source": src,
                        "api_requests": int(s.get("api_requests", 0)),
                        "businesses": int(s.get("businesses", 0)),
                        "errors": int(s.get("errors", 0)),
                        "locations": len((s.get("per_location") or {})),
                    })
                if yrows:
                    st.table(yrows)
                    ov_csv, loc_csv = _build_yelp_api_csvs(yelp_api)
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    st.download_button(
                        "Download Yelp API Overview CSV",
                        data=ov_csv,
                        file_name=f"yelp_api_overview_{ts}.csv",
                        mime="text/csv",
                        key=f"yelp-last-ov-{ts}"
                    )
                    st.download_button(
                        "Download Yelp API Per-Location CSV",
                        data=loc_csv,
                        file_name=f"yelp_api_locations_{ts}.csv",
                        mime="text/csv",
                        key=f"yelp-last-loc-{ts}"
                    )
                with st.expander("Yelp API per-location breakdown"):
                    for src, s in yelp_api.items():
                        st.markdown(f"- {src}")
                        per = s.get("per_location") or {}
                        rep = s.get("reported_totals") or {}
                        if per:
                            rows_pl = [
                                {"location": k, "returned": int(per.get(k, 0)), "reported_total": int(rep.get(k, 0))}
                                for k in per.keys()
                            ]
                            st.table(rows_pl)
                        else:
                            st.caption("(no per-location data)")
        except Exception as e:
            st.info(f"No summary available: {e}")

    last_errors_path = st.session_state.get("last_errors_path")
    last_errors = Path(last_errors_path) if last_errors_path else None
    if last_errors and last_errors.is_file() and not is_running:
        st.subheader("Last Run Errors")
        try:
            edata = json.loads(last_errors.read_text(encoding="utf-8"))
            rows = []
            for src, items in edata.items():
                for it in items:
                    rows.append({
                        "source": src,
                        "type": it.get("type"),
                        "status": it.get("status"),
                        "url": it.get("url"),
                        "error": (it.get("error") or it.get("note")),
                    })
            if rows:
                st.table(rows)
        except Exception as e:
            st.info(f"No errors available: {e}")

if __name__ == "__main__":
    main()
