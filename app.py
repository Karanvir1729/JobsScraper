import os
import io
import time
from datetime import datetime
from pathlib import Path
import re

import streamlit as st
import yaml

import sys
import subprocess
from scrapy.utils.project import get_project_settings


DEFAULT_CONFIG_PATH = Path("config/sources.yml")
EXAMPLE_CONFIG_PATH = Path("config/sources.example.yml")
OUTPUT_DIR = Path("output")

# Provinces and default categories
PROVINCES = [
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
]

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
        "item_selector": ["div.listing", "div.result", "article.listing", "li", "article", "div[class*='result']"],
        "fields": {
            "business_name": ["a.business-name::text", "h3 a::text", "a::text"],
            "phone": ["a[href^='tel:']::attr(href)", "div.phone::text"],
            "website": ["a[href^='http']::attr(href)", "a.website::attr(href)"],
            "address": ["div.address::text", "address::text"],
        },
        "detail_link_selector": ["a.business-name", "h3 a", "a"],
        "follow_links_selector": ["a[href*='/business/']"],
    }

    sel_hotfrog = {
        "item_selector": ["article", "div.result", "li"],
        "fields": {
            "business_name": ["h3 a::text", "a::text"],
            "phone": ["a[href^='tel:']::attr(href)", "div.phone::text"],
            "website": ["a[href^='http']::attr(href)"],
            "address": ["address::text", "div.address::text"],
        },
        "detail_link_selector": ["h3 a", "a"],
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
                "headers": {**headers, "Referer": "https://411.ca/"},
                "start_urls": start_urls,
                "listing": sel_411,
                "detail": {"fields": {"email": "a[href^='mailto:']::attr(href)", "website": "a[href^='http']", "address": "address::text"}},
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
                "headers": {**headers, "Referer": "https://www.hotfrog.ca/"},
                "start_urls": start_urls,
                "listing": sel_hotfrog,
                "detail": {"fields": {"email": "a[href^='mailto:']::attr(href)", "website": "a[href^='http']::attr(href)", "address": "address::text"}},
                "pagination": {
                    "next_page_selector": ["a[rel='next']", "a.next", "a[aria-label='Next']"],
                    "param": {"name": "page", "start": 1, "max_pages": 40},
                },
            })
        if "Opendi" in selected_sources:
            start_urls = [f"https://www.opendi.ca/search/{_path_for_opendi(q)}"]
            sources.append({
                "name": f"Opendi - {q}",
                "category": q,
                "region": "Canada",
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

    return {"sources": sources}


def ensure_paths():
    OUTPUT_DIR.mkdir(exist_ok=True)
    DEFAULT_CONFIG_PATH.parent.mkdir(exist_ok=True)
    if not DEFAULT_CONFIG_PATH.exists() and EXAMPLE_CONFIG_PATH.exists():
        DEFAULT_CONFIG_PATH.write_text(EXAMPLE_CONFIG_PATH.read_text(), encoding="utf-8")


def load_config_text(path: Path) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return "sources: []\n"


def save_config_text(path: Path, text: str) -> None:
    # Validate YAML before saving to help the user
    yaml.safe_load(text)
    path.write_text(text, encoding="utf-8")


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

    # Attach paths to session for display
    st.session_state["last_summary_path"] = str(summary_path)
    st.session_state["last_errors_path"] = str(errors_path)
    st.session_state["last_csv_path"] = str(csv_path)
    return csv_path


def main():
    st.set_page_config(page_title="Canada Home Services Scraper", layout="wide")
    ensure_paths()
    if "is_running" not in st.session_state:
        st.session_state["is_running"] = False

    st.title("Canada Home Services Scraper")
    st.caption("Config-driven Scrapy crawler with Streamlit UI. Edit sources and run.")

    with st.sidebar:
        st.header("Run Settings")
        time_limit = st.number_input("Max runtime (seconds)", min_value=15, max_value=60*60, value=900, step=15)
        max_items = st.number_input("Max items (optional)", min_value=0, max_value=100000, value=0, step=50,
                                    help="Stop after N items. Leave 0 to ignore.")
        concurrent_requests = st.slider("Concurrent requests", min_value=2, max_value=32, value=10)
        download_delay = st.slider("Download delay (seconds)", min_value=0.0, max_value=5.0, value=0.6, step=0.1)
        min_per_source = st.number_input("Minimum items per source", min_value=0, max_value=10000, value=100, step=10,
                                         help="Crawler tries to paginate until at least this many items per source or timeout.")
        st.divider()
        st.caption("Respect each source's robots.txt and terms.")

    st.subheader("Sources Configuration")
    st.caption("Build from categories or edit raw YAML.")

    with st.expander("Build from Categories", expanded=True):
        sel_sources = st.multiselect("Sources", options=["411.ca","Hotfrog","Opendi"], default=["411.ca","Hotfrog","Opendi"]) 
        sel_provinces = st.multiselect("Provinces", options=PROVINCES, default=["ON","BC","AB","QC"]) 
        visit_web_email = st.checkbox("Visit business websites to find emails", value=True)
        st.caption("Pick service categories (type to search)")
        sel_categories = st.multiselect("Categories", options=DEFAULT_CATEGORIES, default=["Plumbing","HVAC","Roofing","Lawn Care","Electrician","Pest Control","Appliance Repair","Moving","Junk Removal","Handyman","Painting"]) 
        if st.button("Preview Generated Config"):
            generated = yaml.safe_dump(build_dynamic_sources(sel_sources, sel_categories, sel_provinces, visit_web_email))
            st.code(generated, language="yaml")

    with st.expander("Raw YAML (advanced)", expanded=False):
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
                st.experimental_rerun()
            else:
                st.info("No example config shipped.")
    with cols[2]:
        if st.button("Revert to Disk"):
            st.session_state["config_text"] = load_config_text(DEFAULT_CONFIG_PATH)
            st.experimental_rerun()

    st.subheader("Run Scraper")
    is_running = st.session_state.get("is_running", False)
    run_clicked = False
    if not is_running:
        run_clicked = st.button("Run", type="primary")
    else:
        st.info("Running scraper... please wait")

    if run_clicked and not st.session_state.get("is_running", False):
        try:
            # Build dynamic config from categories
            generated = yaml.safe_dump(build_dynamic_sources(sel_sources, sel_categories, sel_provinces, visit_web_email), sort_keys=False)
            save_config_text(DEFAULT_CONFIG_PATH, generated)
        except Exception as e:
            st.error(f"Config invalid: {e}")
            return

        st.session_state["is_running"] = True
        try:
            with st.spinner("Running scraper... This will block until it finishes or times out."):
                start = time.time()
                # Persist target to session for summary comparisons
                st.session_state["min_per_source_target"] = int(min_per_source)
                csv_path = run_scrape(
                    DEFAULT_CONFIG_PATH,
                    int(time_limit),
                    int(max_items) if max_items > 0 else None,
                    int(concurrent_requests),
                    float(download_delay),
                    int(min_per_source),
                )
                elapsed = time.time() - start

            if csv_path.exists():
                st.success(f"Done in {elapsed:.1f}s. CSV generated: {csv_path}")
                st.download_button("Download CSV", data=csv_path.read_bytes(), file_name=csv_path.name, mime="text/csv")
            else:
                st.warning("No CSV produced. Check logs/selectors and try again.")
        finally:
            st.session_state["is_running"] = False
            try:
                st.experimental_rerun()
            except Exception:
                pass

        # Show per-source summary and alerts
        summary_file = Path(st.session_state.get("last_summary_path", ""))
        st.subheader("Run Summary")
        if summary_file.exists():
            try:
                import json
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
            except Exception as e:
                st.info(f"No summary available: {e}")
        else:
            st.info("No summary file found.")

        # Show errors table
        errors_file = Path(st.session_state.get("last_errors_path", ""))
        st.subheader("Errors")
        if errors_file.exists():
            try:
                import json
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


    # Always show the latest results if available (outside run button flow)
    last_csv_path = st.session_state.get("last_csv_path")
    if last_csv_path and not is_running:
        last_csv = Path(last_csv_path)
        if last_csv.is_file():
            st.subheader("Latest Output")
            try:
                csv_bytes = last_csv.read_bytes()
                st.download_button("Download Latest CSV", data=csv_bytes, file_name=last_csv.name, mime="text/csv")
            except Exception as e:
                st.warning(f"CSV at {last_csv} couldn't be opened: {e}")

    last_summary_path = st.session_state.get("last_summary_path")
    last_summary = Path(last_summary_path) if last_summary_path else None
    if last_summary and last_summary.is_file() and not is_running:
        st.subheader("Last Run Summary")
        try:
            import json
            data = json.loads(last_summary.read_text(encoding="utf-8"))
            counts = data.get("counts", {})
            configured = data.get("configured_sources", [])
            target = int(st.session_state.get("min_per_source_target", 0))
            rows = [{"source": s or "(unnamed)", "items_found": int(counts.get(s, 0)), "target": target} for s in configured]
            if rows:
                st.table(rows)
        except Exception as e:
            st.info(f"No summary available: {e}")

    last_errors_path = st.session_state.get("last_errors_path")
    last_errors = Path(last_errors_path) if last_errors_path else None
    if last_errors and last_errors.is_file() and not is_running:
        st.subheader("Last Run Errors")
        try:
            import json
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
