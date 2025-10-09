#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main():
    ap = argparse.ArgumentParser(description="Run config-driven providers spider")
    ap.add_argument("--config", required=True, help="Path to YAML sources file")
    ap.add_argument("--csv", required=True, help="Output CSV path")
    ap.add_argument("--timeout", type=int, default=300, help="CLOSESPIDER_TIMEOUT seconds")
    ap.add_argument("--summary", default=None, help="Path to write JSON summary with per-source counts")
    ap.add_argument("--errors", default=None, help="Path to write JSON errors per source")
    ap.add_argument("--min-per-source", type=int, default=0, help="Target minimum items per source (guides pagination)")
    ap.add_argument("--max-items", type=int, default=0, help="CLOSESPIDER_ITEMCOUNT (0 to disable)")
    ap.add_argument("--concurrent", type=int, default=8, help="CONCURRENT_REQUESTS")
    ap.add_argument("--delay", type=float, default=0.5, help="DOWNLOAD_DELAY seconds")
    args = ap.parse_args()

    settings = get_project_settings()
    # output feed
    settings.set(
        "FEEDS",
        {str(Path(args.csv)): {"format": "csv", "encoding": "utf-8", "overwrite": True}},
    )
    settings.set("CLOSESPIDER_TIMEOUT", int(args.timeout))
    if args.max_items and int(args.max_items) > 0:
        settings.set("CLOSESPIDER_ITEMCOUNT", int(args.max_items))
    settings.set("CONCURRENT_REQUESTS", int(args.concurrent))
    settings.set("DOWNLOAD_DELAY", float(args.delay))
    settings.set("ROBOTSTXT_OBEY", True)

    process = CrawlerProcess(settings)
    from scraper.spiders.config_spider import ConfigSpider

    process.crawl(
        ConfigSpider,
        sources_file=str(Path(args.config)),
        summary_file=str(args.summary) if args.summary else None,
        errors_file=str(args.errors) if args.errors else None,
        min_per_source=int(args.min_per_source),
    )
    process.start()  # blocking

    # After the crawl completes, refresh the golden CSV so new good, unique
    # entries are added. This mirrors the Streamlit app behavior and ensures
    # headless runs also maintain providers-golden.csv.
    try:
        from golden_record_gen import (
            main as build_golden_main,
            GOLDEN_CSV,
            read_csv_rows,
            write_csv_rows,
            normalize_phone,
        )

        # Rebuild golden from all outputs
        build_golden_main()

        # Augment this run's CSV with any golden rows not already present
        try:
            run_csv = Path(args.csv)
            if run_csv.exists() and GOLDEN_CSV.exists():
                cur_rows = read_csv_rows(run_csv)
                cur_phones = set()
                for r in cur_rows:
                    ph = normalize_phone(r.get("phone"))
                    if ph:
                        cur_phones.add(ph)

                golden_rows = read_csv_rows(GOLDEN_CSV)
                add_rows = []
                for r in golden_rows:
                    ph = normalize_phone(r.get("phone"))
                    if not ph or ph in cur_phones:
                        continue
                    rr = dict(r)
                    rr["phone"] = ph
                    add_rows.append(rr)

                if add_rows:
                    write_csv_rows(run_csv, cur_rows + add_rows)
        except Exception:
            pass
    except Exception:
        # Non-fatal: do not fail the run if golden regeneration has issues
        pass


if __name__ == "__main__":
    main()
