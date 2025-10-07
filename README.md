Canada Home Services Scraper
=================================

Streamlit UI + Scrapy crawler to extract business contact info (incl. email when available) from config-defined online directories of Canadian home service providers (HVAC, plumbing, lawn maintenance, etc.).

Features
- Streamlit UI to edit YAML config for sources
- Scrapy spider reads config and crawls listings + optional detail pages
- CSV export with standard fields (name, phone, email, website, address, etc.)
- User controls: max runtime (timeout), max items, concurrency, delay

Quick Start
1) Install dependencies:
   - Python 3.10+
   - `pip install -r requirements.txt`

2) Review example sources and copy into active config:
   - `config/sources.example.yml`
   - Active config at `config/sources.yml` (also editable in the UI)

3) Run the UI:
   - `streamlit run app.py`

4) In the app:
   - Edit the YAML (or click “Load Example” to see templates)
   - Set “Max runtime (seconds)” (uses Scrapy CLOSESPIDER_TIMEOUT)
   - Optionally set “Max items” to stop after N results
   - Click Run. A CSV is written to `output/` and can be downloaded.

YAML Config Schema (summary)
```
sources:
  - name: "Source name"
    category: "HVAC|Plumbing|…"            # optional
    region: "Canada|ON|…"                   # optional
    start_urls:
      - "https://…"                         # listing pages
    listing:
      item_selector: "div.card"             # per-provider card selector
      fields:                                # CSS selectors for fields
        business_name: "a.title::text"
        phone: "a[href^='tel:']::attr(href)"
        email: "a[href^='mailto:']::attr(href)"
        website: "a.website"
        address: "div.addr::text"
        city: "span.city::text"
        province: "span.prov::text"
        postal_code: "span.postal::text"
      detail_link_selector: "a.title"       # optional, follow to detail
    detail:
      fields:                                # same keys allowed here
        email: "a[href^='mailto:']::attr(href)"
        website: "a.website::attr(href)"
        address: "address::text"
    pagination:
      next_page_selector: "a[rel='next']"   # optional
```

Notes
- Email discovery also scans for `mailto:` and regex in text as fallback.
- Respect each site’s robots.txt and Terms of Service.
- To reach 1,000+ providers, add multiple categories/regions and raise timeout.

Development
- The Scrapy spider lives in `scraper/spiders/config_spider.py`.
- Settings are in `scraper/settings.py`. The Streamlit app overrides key runtime settings (feeds, timeout, concurrency, delay).

