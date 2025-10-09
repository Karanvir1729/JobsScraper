import json
import os
from typing import Any, Dict, List, Optional
from pathlib import Path

import yaml
import csv
import urllib.parse as urlparse
import scrapy
from itemadapter import ItemAdapter

from scraper.items import ProviderItem
from scraper import utils as U


from collections import defaultdict


class ConfigSpider(scrapy.Spider):
    name = "config_providers"
    handle_httpstatus_all = True

    custom_settings = {
        # Feed export is configured from the caller (Streamlit app)
    }

    def __init__(self, sources_file: str, summary_file: str | None = None, errors_file: str | None = None, min_per_source: int = 0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources_file = sources_file
        self.sources: List[Dict[str, Any]] = []
        self.summary_file = summary_file
        self.errors_file = errors_file
        self._counts = defaultdict(int)
        self._configured_sources: List[str] = []
        self._errors = defaultdict(list)
        self.min_per_source = int(min_per_source) if min_per_source else 0
        # API-specific stats (e.g., Yelp)
        self._yelp_stats: Dict[str, Dict[str, Any]] = {}
        # Golden record caches
        self._golden_phones: set[str] = set()
        self._golden_listing_urls: set[str] = set()
        self._golden_detail_urls: set[str] = set()
        try:
            gpath = Path("output") / "providers-golden.csv"
            if gpath.exists():
                import csv as _csv
                with gpath.open("r", encoding="utf-8", newline="") as f:
                    rdr = _csv.DictReader(f)
                    for r in rdr:
                        ph = (r.get("phone") or "").strip()
                        if ph:
                            self._golden_phones.add(ph)
                        lu = (r.get("listing_url") or "").strip()
                        if lu:
                            self._golden_listing_urls.add(lu)
                        du = (r.get("detail_url") or "").strip()
                        if du:
                            self._golden_detail_urls.add(du)
        except Exception:
            pass
        # Track seen phones per source to avoid duplicate fallback items
        self._seen_phones_by_source: Dict[str, set] = defaultdict(set)

    def start_requests(self):
        # Load sources from YAML or JSON based on file extension
        with open(self.sources_file, "r", encoding="utf-8") as f:
            if str(self.sources_file).lower().endswith((".json",)):
                try:
                    data = json.load(f) or {}
                except Exception:
                    data = {}
            else:
                data = yaml.safe_load(f) or {}
        self.sources = data.get("sources", [])
        self._configured_sources = [s.get("name") for s in self.sources]
        for src in self.sources:
            if src.get("enabled") is False:
                self.logger.info("Skipping disabled source: %s", src.get("name"))
                continue
            meta = {"source": src.get("name"), "cfg": src}
            # Special handling for API-driven sources (e.g., Yelp Fusion API)
            if str(src.get("api")).lower() == "yelp":
                api_key = os.environ.get("YELP_API_KEY") or src.get("api_key")
                if not api_key:
                    self._errors[src.get("name") or "Yelp API"].append({
                        "url": None,
                        "status": None,
                        "note": "Missing YELP_API_KEY. Set env var or put api_key in source (not recommended).",
                    })
                    continue
                term = src.get("term") or src.get("category") or "home services"
                locations = src.get("locations") or []
                limit = int(src.get("limit") or 50)
                limit = max(1, min(50, limit))
                max_pages = int(src.get("max_pages") or 1)
                radius_m = src.get("radius_m")
                locale = src.get("locale") or "en_CA"
                base = "https://api.yelp.com/v3/businesses/search"
                for loc in locations:
                    params = {"term": term, "location": loc, "limit": limit, "offset": 0, "locale": locale}
                    if radius_m:
                        try:
                            params["radius"] = int(radius_m)
                        except Exception:
                            pass
                    url = base + "?" + urlparse.urlencode(params)
                    req = scrapy.Request(
                        url,
                        callback=self.parse_yelp_api,
                        errback=self.on_error,
                        headers={"Authorization": f"Bearer {api_key}"},
                        dont_filter=True,
                    )
                    req.meta.update(meta)
                    req.meta.update({
                        "_yelp": {
                            "term": term,
                            "location": loc,
                            "limit": limit,
                            "offset": 0,
                            "max_pages": max_pages,
                            "page": 1,
                            "api_key": api_key,
                            "base": base,
                            "locale": locale,
                        }
                    })
                    yield req
                continue
            for url in src.get("start_urls", []) or []:
                yield scrapy.Request(
                    url,
                    callback=self.parse_listing,
                    errback=self.on_error,
                    meta=meta,
                    headers=src.get("headers") or {},
                )

    def parse_yelp_api(self, response: scrapy.http.Response):
        cfg = response.meta.get("cfg", {})
        source_name = response.meta.get("source")
        try:
            data = json.loads(response.text)
        except Exception as e:
            self._errors[source_name].append({
                "url": response.url,
                "status": int(response.status) if response.status else None,
                "note": f"Failed to parse Yelp API JSON: {e}",
            })
            st = self._yelp_stats.setdefault(source_name, {"api_requests": 0, "businesses": 0, "per_location": {}, "reported_totals": {}, "errors": 0})
            st["errors"] = int(st.get("errors", 0)) + 1
            return

        if data.get("error"):
            self._errors[source_name].append({
                "url": response.url,
                "status": int(response.status) if response.status else None,
                "note": f"Yelp API error: {data.get('error')}",
            })
            return
        # Update API stats
        y = (response.meta.get("_yelp") or {})
        loc = y.get("location")
        st = self._yelp_stats.setdefault(source_name, {"api_requests": 0, "businesses": 0, "per_location": {}, "reported_totals": {}, "errors": 0})
        st["api_requests"] = int(st.get("api_requests", 0)) + 1
        if isinstance(loc, str) and data.get("total") is not None:
            st.setdefault("reported_totals", {})[loc] = int(data.get("total") or 0)

        businesses = data.get("businesses", [])
        st["businesses"] = int(st.get("businesses", 0)) + len(businesses)
        if isinstance(loc, str):
            per = st.setdefault("per_location", {})
            per[loc] = int(per.get(loc, 0)) + len(businesses)
        produced = 0
        for biz in businesses:
            item = ProviderItem()
            item["source"] = source_name
            item["category"] = cfg.get("category")
            item["region"] = cfg.get("region") or (response.meta.get("_yelp") or {}).get("location")
            item["listing_url"] = response.url
            item["detail_url"] = biz.get("url")
            item["business_name"] = biz.get("name")
            phone = biz.get("display_phone") or biz.get("phone")
            item["phone"] = U.normalize_phone(phone) if phone else None
            loc = biz.get("location") or {}
            if isinstance(loc, dict):
                disp_addr = loc.get("display_address")
                if isinstance(disp_addr, list):
                    item["address"] = ", ".join([a for a in disp_addr if isinstance(a, str)])
                else:
                    item["address"] = loc.get("address1")
                item["city"] = loc.get("city")
                item["province"] = (loc.get("state") or loc.get("country"))
                item["postal_code"] = loc.get("zip_code")

            # Optionally fetch Yelp business HTML to discover external website, then visit it for email
            if cfg.get("visit_website_for_email") and item.get("detail_url"):
                req = scrapy.Request(
                    item["detail_url"],
                    callback=self.parse_yelp_business_page,
                    errback=self.on_error,
                    headers=cfg.get("headers") or {},
                )
                req.meta["item"] = item
                req.meta["cfg"] = cfg
                produced += 1
                yield req
            else:
                # Only emit items with phone
                if item.get("phone"):
                    self._counts[source_name] += 1
                    produced += 1
                    yield item

        # Yelp API pagination via offset
        y = (response.meta.get("_yelp") or {}).copy()
        limit = int(y.get("limit") or 50)
        page = int(y.get("page") or 1)
        max_pages = int(y.get("max_pages") or 1)
        total = int(data.get("total") or 0)
        next_offset = int(y.get("offset") or 0) + limit
        if produced > 0 and page < max_pages and next_offset < total:
            params = {
                "term": y.get("term"),
                "location": y.get("location"),
                "limit": limit,
                "offset": next_offset,
                "locale": y.get("locale") or "en_CA",
            }
            if cfg.get("radius_m"):
                try:
                    params["radius"] = int(cfg.get("radius_m"))
                except Exception:
                    pass
            url = (y.get("base") or "https://api.yelp.com/v3/businesses/search") + "?" + urlparse.urlencode(params)
            req = scrapy.Request(
                url,
                callback=self.parse_yelp_api,
                errback=self.on_error,
                headers={"Authorization": f"Bearer {y.get('api_key')}"},
                dont_filter=True,
            )
            y.update({"offset": next_offset, "page": page + 1})
            req.meta.update({"cfg": cfg, "source": source_name, "_yelp": y})
            yield req

    def parse_yelp_business_page(self, response: scrapy.http.Response):
        item: ProviderItem = response.meta.get("item")
        cfg = response.meta.get("cfg", {})
        source_name = item.get("source") if item else None
        # Try to extract external website URL from Yelp business page via biz_redir param
        hrefs = response.css("a[href*='/biz_redir?']::attr(href)").getall()
        website_url = None
        for href in hrefs:
            try:
                parsed = urlparse.urlparse(href)
                qs = urlparse.parse_qs(parsed.query)
                u = qs.get("url")
                if u and isinstance(u, list) and u[0].startswith("http"):
                    website_url = u[0]
                    break
            except Exception:
                continue
        if website_url:
            item["website"] = website_url
        # If we have a website and want email, use existing website parsing flow
        if (not item.get("email")) and item.get("website") and cfg.get("visit_website_for_email"):
            wreq = scrapy.Request(
                item["website"],
                callback=self.parse_website_email,
                errback=self.on_error,
                headers=cfg.get("website_headers") or cfg.get("headers") or {},
            )
            wreq.meta["item"] = item
            wreq.meta["cfg"] = cfg
            yield wreq
            return
        # Else, yield the item as-is (only if phone present)
        if item.get("phone"):
            if source_name:
                self._counts[source_name] += 1
            yield item

    def parse_listing(self, response: scrapy.http.Response):
        cfg = response.meta.get("cfg", {})
        source_name = response.meta.get("source")
        listing = (cfg.get("listing") or {})
        item_selector = listing.get("item_selector")
        produced = 0

        if response.status and not (200 <= int(response.status) < 300):
            self._errors[source_name].append({
                "url": response.url,
                "status": int(response.status),
                "note": "Non-2xx listing page",
            })

        # Optionally skip listing pages we've already covered in Golden
        if cfg.get("skip_visited_listings") and response.url in self._golden_listing_urls:
            # Try to advance pagination without parsing this page's items
            # pagination via selectors (rel=next etc.)
            next_sel = (cfg.get("pagination") or {}).get("next_page_selector")
            for sel in U.listify(next_sel):
                href = response.css((sel + "::attr(href)") if "::attr(" not in sel else sel).get()
                if href:
                    url = U.absolute_url(response.url, href)
                    yield response.follow(url, callback=self.parse_listing, meta=response.meta, headers=cfg.get("headers") or {})
                    return
            # pagination via query param (?page=2)
            pconf = (cfg.get("pagination") or {}).get("param")
            if isinstance(pconf, dict) and pconf.get("name"):
                name = pconf.get("name")
                start = int(pconf.get("start") or 1)
                cur = U.get_query_int(response.url, name, default=start)
                nxt = cur + 1
                next_url = U.set_query_param(response.url, name, nxt)
                yield response.follow(next_url, callback=self.parse_listing, meta=response.meta, headers=cfg.get("headers") or {})
                return

        if item_selector:
            for sel in U.listify(item_selector):
                for card in response.css(sel):
                    item = ProviderItem()
                    item["source"] = source_name
                    item["category"] = cfg.get("category")
                    item["region"] = cfg.get("region")
                    item["listing_url"] = response.url

                    fields = listing.get("fields", {})
                    item["business_name"] = U.extract_first(card, fields.get("business_name"))
                    item["phone"] = U.normalize_phone(U.extract_first(card, fields.get("phone")))
                    email_val = U.extract_first(card, fields.get("email"))
                    if email_val and email_val.lower().startswith("mailto:"):
                        email_val = email_val.split(":", 1)[1].split("?", 1)[0]
                    item["email"] = email_val
                    item["website"] = U.extract_attr(card, fields.get("website"), "href")
                    if item.get("website") and not str(item["website"]).lower().startswith("http"):
                        item["website"] = U.absolute_url(response.url, str(item["website"]))
                    item["address"] = U.extract_first(card, fields.get("address"))
                    item["city"] = U.extract_first(card, fields.get("city"))
                    item["province"] = U.extract_first(card, fields.get("province"))
                    item["postal_code"] = U.extract_first(card, fields.get("postal_code"))

                    # Fallback discovery within the card
                    if not item.get("email"):
                        item["email"] = U.discover_email_from_selector(card)
                    if not item.get("phone"):
                        item["phone"] = U.discover_phone_from_selector(card)

                    # If business_name is missing or looks like a generic action link, try better fallbacks
                    def _looks_bad_name(name: str | None) -> bool:
                        if not name:
                            return True
                        n = str(name).strip().lower()
                        if len(n) < 2:
                            return True
                        bad = {"call", "website", "directions", "view details", "more info"}
                        return n in bad

                    if _looks_bad_name(item.get("business_name")):
                        # Try to read text or title from the detail link if present
                        dl_sel = listing.get("detail_link_selector")
                        maybe = None
                        if dl_sel:
                            maybe = U.extract_first(card, [s + "::text" for s in U.listify(dl_sel)]) or \
                                    U.extract_attr(card, dl_sel, "title")
                        if not maybe:
                            # Try itemprop/name
                            maybe = U.extract_first(card, "[itemprop='name']::text")
                        if maybe:
                            item["business_name"] = maybe

                    # Record seen phone to avoid duplicate fallback entries
                    if item.get("phone") and source_name:
                        try:
                            self._seen_phones_by_source[source_name].add(item["phone"])
                        except Exception:
                            pass

                    # Optionally follow detail page for richer data
                    detail_link_sel = listing.get("detail_link_selector")
                    detail_url = U.absolute_url(response.url, U.extract_attr(card, detail_link_sel, "href")) if detail_link_sel else None
                    if detail_url:
                        item["detail_url"] = detail_url
                    if detail_url and (cfg.get("detail") or {}).get("fields") and not (cfg.get("skip_visited_details") and detail_url in self._golden_detail_urls):
                        req = scrapy.Request(
                            detail_url,
                            callback=self.parse_detail,
                            errback=self.on_error,
                            headers=cfg.get("headers") or {},
                        )
                        req.meta["item"] = item
                        req.meta["cfg"] = cfg
                        yield req
                    else:
                        # Optionally visit business website to hunt email
                        if (not item.get("email")) and item.get("website") and cfg.get("visit_website_for_email") and not (cfg.get("skip_visited_details") and (item.get("phone") in self._golden_phones)):
                            wreq = scrapy.Request(
                                item["website"],
                                callback=self.parse_website_email,
                                errback=self.on_error,
                                headers=cfg.get("website_headers") or cfg.get("headers") or {},
                            )
                            wreq.meta["item"] = item
                            wreq.meta["cfg"] = cfg
                            yield wreq
                        else:
                            # Only emit items with a phone number
                            if item.get("phone"):
                                produced += 1
                                self._counts[source_name] += 1
                                yield item

        # Follow arbitrary links selector (useful for index pages without clear cards)
        follow_sel = (cfg.get("listing") or {}).get("follow_links_selector")
        for sel in U.listify(follow_sel):
            for href in response.css((sel + "::attr(href)") if "::attr(" not in sel else sel).getall():
                url = U.absolute_url(response.url, href)
                if not url:
                    continue
                req = scrapy.Request(
                    url,
                    callback=self.parse_detail,
                    errback=self.on_error,
                    headers=cfg.get("headers") or {},
                )
                base_item = ProviderItem()
                base_item["source"] = source_name
                base_item["category"] = cfg.get("category")
                base_item["region"] = cfg.get("region")
                base_item["listing_url"] = response.url
                req.meta["item"] = base_item
                req.meta["cfg"] = cfg
                produced += 1
                yield req

        # JSON-LD fallback when no cards yielded
        if produced == 0 and (cfg.get("jsonld_fallback", True)):
            for obj in U.extract_jsonld_objects(response):
                if not isinstance(obj, dict):
                    continue
                types = obj.get("@type")
                type_list = [types] if isinstance(types, str) else (types or [])
                if any(t in ("LocalBusiness", "Organization", "ProfessionalService", "HomeAndConstructionBusiness") for t in type_list):
                    item = ProviderItem()
                    item["source"] = source_name
                    item["category"] = cfg.get("category")
                    item["region"] = cfg.get("region")
                    item["listing_url"] = response.url
                    item["business_name"] = obj.get("name") or obj.get("legalName")
                    item["phone"] = U.normalize_phone(obj.get("telephone")) if obj.get("telephone") else None
                    item["email"] = obj.get("email")
                    url = obj.get("url") or obj.get("sameAs")
                    if isinstance(url, list):
                        url = url[0]
                    item["website"] = url
                    addr = obj.get("address") or {}
                    if isinstance(addr, dict):
                        item["address"] = addr.get("streetAddress")
                        item["city"] = addr.get("addressLocality")
                        item["province"] = addr.get("addressRegion")
                        item["postal_code"] = addr.get("postalCode")
                    if item.get("phone"):
                        produced += 1
                        self._counts[source_name] += 1
                        yield item

        # Broad fallback: scan page for phone anchors and create items if missing (opt-in via config)
        if cfg.get("scan_phones_on_page"):
            try:
                fields = (listing.get("fields") or {})
                for tel in response.css("a[href^='tel:']"):
                    href = tel.attrib.get("href") or tel.css("::attr(href)").get()
                    phone_norm = U.normalize_phone(href)
                    if not phone_norm:
                        continue
                    if source_name and phone_norm in self._seen_phones_by_source.get(source_name, set()):
                        continue
                    # Find nearest container resembling a card
                    container = tel.xpath("ancestor::*[self::div[contains(@class,'listing') or contains(@class,'result')] or self::article or self::li][1]")
                    card = container if container else response
                    item = ProviderItem()
                    item["source"] = source_name
                    item["category"] = cfg.get("category")
                    item["region"] = cfg.get("region")
                    item["listing_url"] = response.url
                    item["phone"] = phone_norm
                    # Attempt to extract a nearby business name using the same field strategy
                    name = U.extract_first(card, (fields.get("business_name") or []))
                    if not name:
                        # Try common alternatives near the phone
                        name = U.extract_first(card, ["h2 a::text", "h3 a::text", "[itemprop='name']::text", "a[title]::attr(title)"])
                    item["business_name"] = name
                    # Try to attach a website if present in container
                    if not item.get("website"):
                        href = U.extract_attr(card, (fields.get("website") or ["a[href^='http']"]), "href")
                        if href:
                            item["website"] = U.absolute_url(response.url, href)
                    # Address best-effort
                    item.setdefault("address", U.extract_first(card, (fields.get("address") or [])))
                    # Emit and track
                    self._counts[source_name] += 1
                    if source_name:
                        self._seen_phones_by_source[source_name].add(phone_norm)
                    yield item
            except Exception as e:
                self.logger.debug("Fallback phone scan failed: %s", e)

        # pagination via selectors (rel=next etc.)
        next_sel = (cfg.get("pagination") or {}).get("next_page_selector")
        for sel in U.listify(next_sel):
            href = response.css((sel + "::attr(href)") if "::attr(" not in sel else sel).get()
            if href:
                url = U.absolute_url(response.url, href)
                yield response.follow(url, callback=self.parse_listing, meta=response.meta, headers=cfg.get("headers") or {})
                break

        # pagination via query param (?page=2)
        pconf = (cfg.get("pagination") or {}).get("param")
        if isinstance(pconf, dict) and pconf.get("name"):
            name = pconf.get("name")
            max_pages = int(pconf.get("max_pages") or 0)
            start = int(pconf.get("start") or 1)
            cur = U.get_query_int(response.url, name, default=start)
            nxt = cur + 1
            # Determine current items collected for this source so far
            current_count = int(self._counts.get(source_name, 0)) + int(produced)
            need_more = self.min_per_source and (current_count < self.min_per_source)
            if (max_pages and nxt <= max_pages) and (need_more or produced > 0):
                # If we're below target or the last page produced items, try the next page
                next_url = U.set_query_param(response.url, name, nxt)
                yield response.follow(next_url, callback=self.parse_listing, meta=response.meta, headers=cfg.get("headers") or {})

    def parse_detail(self, response: scrapy.http.Response):
        if response.status and not (200 <= int(response.status) < 300):
            try:
                src_item = response.meta.get("item")
                src_name = src_item.get("source") if src_item else None
            except Exception:
                src_name = None
            if src_name:
                self._errors[src_name].append({
                    "url": response.url,
                    "status": int(response.status),
                    "note": "Non-2xx detail page",
                })
        item: ProviderItem = response.meta["item"]
        cfg = response.meta["cfg"]
        detail = (cfg.get("detail") or {})
        fields = detail.get("fields", {})

        item["detail_url"] = response.url

        def fill(field: str, sel_any):
            val = U.extract_first(response, sel_any)
            if val:
                item[field] = val

        fill("business_name", fields.get("business_name"))
        fill("phone", fields.get("phone"))
        if not item.get("email"):
            fill("email", fields.get("email"))
            if item.get("email") and str(item["email"]).lower().startswith("mailto:"):
                item["email"] = str(item["email"]).split(":", 1)[1].split("?", 1)[0]
        if not item.get("email"):
            item["email"] = U.discover_email_from_selector(response)
        if not item.get("website"):
            href = U.extract_attr(response, fields.get("website"), "href")
            if href:
                item["website"] = U.absolute_url(response.url, href)

        fill("address", fields.get("address"))
        fill("city", fields.get("city"))
        fill("province", fields.get("province"))
        fill("postal_code", fields.get("postal_code"))

        # JSON-LD enrichment on detail pages
        if cfg.get("jsonld_fallback", True):
            for obj in U.extract_jsonld_objects(response):
                if not isinstance(obj, dict):
                    continue
                types = obj.get("@type")
                type_list = [types] if isinstance(types, str) else (types or [])
                if any(t in ("LocalBusiness", "Organization", "ProfessionalService", "HomeAndConstructionBusiness") for t in type_list):
                    item.setdefault("business_name", obj.get("name") or obj.get("legalName"))
                    if not item.get("phone") and obj.get("telephone"):
                        item["phone"] = U.normalize_phone(obj.get("telephone"))
                    item.setdefault("email", obj.get("email"))
                    if not item.get("website"):
                        url = obj.get("url") or obj.get("sameAs")
                        if isinstance(url, list):
                            url = url[0]
                        item["website"] = url
                    addr = obj.get("address") or {}
                    if isinstance(addr, dict):
                        item.setdefault("address", addr.get("streetAddress"))
                        item.setdefault("city", addr.get("addressLocality"))
                        item.setdefault("province", addr.get("addressRegion"))
                        item.setdefault("postal_code", addr.get("postalCode"))

        # Optionally visit business website to hunt email if still missing
        if (not item.get("email")) and item.get("website") and cfg.get("visit_website_for_email") and not (cfg.get("skip_visited_details") and (item.get("phone") in self._golden_phones)):
            wreq = scrapy.Request(
                item["website"],
                callback=self.parse_website_email,
                errback=self.on_error,
                headers=cfg.get("website_headers") or cfg.get("headers") or {},
            )
            wreq.meta["item"] = item
            wreq.meta["cfg"] = cfg
            yield wreq
        else:
            # Only emit from detail if phone present
            src = item.get("source")
            if item.get("phone"):
                if src:
                    self._counts[src] += 1
                yield item

    def parse_website_email(self, response: scrapy.http.Response):
        item: ProviderItem = response.meta["item"]
        cfg = response.meta.get("cfg", {})
        # Try to find email on this external site
        if not item.get("email"):
            maybe = U.discover_email_from_selector(response)
            if maybe:
                item["email"] = maybe
        # If still no email, try contact page
        tried_contact = response.meta.get("_contact_tried", False)
        if (not item.get("email")) and not tried_contact and cfg.get("visit_contact_page", True):
            href = response.css("a[href*='contact']::attr(href)").get()
            if href:
                url = U.absolute_url(response.url, href)
                creq = scrapy.Request(
                    url,
                    callback=self.parse_website_email,
                    errback=self.on_error,
                    headers=cfg.get("website_headers") or cfg.get("headers") or {},
                )
                creq.meta["item"] = item
                creq.meta["cfg"] = cfg
                creq.meta["_contact_tried"] = True
                yield creq
                return
        # Yield item now only if phone present
        src = item.get("source")
        if item.get("phone"):
            if src:
                self._counts[src] += 1
            yield item

    def closed(self, reason):
        if not self.summary_file:
            return
        try:
            # Prepare Yelp stats in a JSON-serializable form
            yelp_api = None
            if self._yelp_stats:
                yelp_api = {}
                for k, v in self._yelp_stats.items():
                    yelp_api[k] = {
                        "api_requests": int(v.get("api_requests", 0)),
                        "businesses": int(v.get("businesses", 0)),
                        "errors": int(v.get("errors", 0)),
                        "per_location": {lk: int(lc) for lk, lc in (v.get("per_location", {}) or {}).items()},
                        "reported_totals": {lk: int(lc) for lk, lc in (v.get("reported_totals", {}) or {}).items()},
                    }

            data = {
                "reason": reason,
                "counts": dict(self._counts),
                "configured_sources": self._configured_sources,
            }
            if yelp_api is not None:
                data["yelp_api"] = yelp_api
            import json
            with open(self.summary_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            # Also write Yelp API CSV exports alongside the summary file
            if yelp_api is not None and isinstance(yelp_api, dict):
                try:
                    base = str(self.summary_file)
                    suffix = "-summary.json"
                    if base.endswith(suffix):
                        prefix = base[: -len(suffix)]
                    else:
                        prefix = os.path.splitext(base)[0]
                    ov_path = prefix + "-yelp_api_overview.csv"
                    loc_path = prefix + "-yelp_api_locations.csv"
                    # Overview CSV
                    with open(ov_path, "w", encoding="utf-8", newline="") as cf:
                        w = csv.writer(cf)
                        w.writerow(["source", "api_requests", "businesses", "errors", "locations"])
                        for src, s in (yelp_api or {}).items():
                            per = (s.get("per_location") or {}) if isinstance(s, dict) else {}
                            w.writerow([
                                src,
                                int((s or {}).get("api_requests", 0)),
                                int((s or {}).get("businesses", 0)),
                                int((s or {}).get("errors", 0)),
                                len(per),
                            ])
                    # Per-location CSV
                    with open(loc_path, "w", encoding="utf-8", newline="") as cf:
                        w = csv.writer(cf)
                        w.writerow(["source", "location", "returned", "reported_total"])
                        for src, s in (yelp_api or {}).items():
                            per = (s.get("per_location") or {}) if isinstance(s, dict) else {}
                            rep = (s.get("reported_totals") or {}) if isinstance(s, dict) else {}
                            for k in per.keys():
                                w.writerow([src, k, int(per.get(k, 0)), int(rep.get(k, 0))])
                except Exception as e:
                    self.logger.error("Failed writing Yelp API CSV exports: %s", e)
        except Exception as e:
            self.logger.error("Failed writing summary file %s: %s", self.summary_file, e)
        # Write errors file if requested
        if getattr(self, "errors_file", None):
            try:
                import json
                with open(self.errors_file, "w", encoding="utf-8") as f:
                    json.dump({k: v for k, v in self._errors.items()}, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error("Failed writing errors file %s: %s", self.errors_file, e)

    def on_error(self, failure):
        try:
            request = getattr(failure, "request", None) or failure.value.request  # type: ignore
        except Exception:
            request = None
        src_name = None
        if request is not None:
            try:
                src_name = request.meta.get("source") or (request.meta.get("cfg") or {}).get("name")
            except Exception:
                src_name = None
        try:
            msg = failure.getErrorMessage()
        except Exception:
            try:
                msg = str(failure.value)
            except Exception:
                msg = str(failure)
        entry = {
            "url": getattr(request, "url", None) if request is not None else None,
            "error": msg,
            "type": getattr(getattr(failure, "type", None), "__name__", None),
        }
        bucket = src_name or "(unknown)"
        self._errors[bucket].append(entry)
