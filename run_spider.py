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

    process.crawl(ConfigSpider, sources_file=str(Path(args.config)))
    process.start()  # blocking


if __name__ == "__main__":
    main()

