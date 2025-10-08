import json
import os
from typing import Any, Dict, List, Optional

import yaml
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

    def start_requests(self):
        with open(self.sources_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self.sources = data.get("sources", [])
        self._configured_sources = [s.get("name") for s in self.sources]
        for src in self.sources:
            if src.get("enabled") is False:
                self.logger.info("Skipping disabled source: %s", src.get("name"))
                continue
            meta = {"source": src.get("name"), "cfg": src}
            for url in src.get("start_urls", []) or []:
                yield scrapy.Request(
                    url,
                    callback=self.parse_listing,
                    errback=self.on_error,
                    meta=meta,
                    headers=src.get("headers") or {},
                )

    def parse_listing(self, response: scrapy.http.Response):
        cfg = response.meta.get("cfg", {})
        source_name = response.meta.get("source")
        listing = (cfg.get("listing") or {})
        item_selector = listing.get("item_selector")
        produced = 0

        if response.status and response.status != 200:
            self._errors[source_name].append({
                "url": response.url,
                "status": int(response.status),
                "note": "Non-200 listing page",
            })

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

                    # Optionally follow detail page for richer data
                    detail_link_sel = listing.get("detail_link_selector")
                    detail_url = U.absolute_url(response.url, U.extract_attr(card, detail_link_sel, "href")) if detail_link_sel else None
                    if detail_url:
                        item["detail_url"] = detail_url
                    if detail_url and (cfg.get("detail") or {}).get("fields"):
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
                        else:
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
                    produced += 1
                    self._counts[source_name] += 1
                    yield item

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
        if response.status and response.status != 200:
            try:
                src_item = response.meta.get("item")
                src_name = src_item.get("source") if src_item else None
            except Exception:
                src_name = None
            if src_name:
                self._errors[src_name].append({
                    "url": response.url,
                    "status": int(response.status),
                    "note": "Non-200 detail page",
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
        else:
            src = item.get("source")
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
        # Yield item now
        src = item.get("source")
        if src:
            self._counts[src] += 1
        yield item

    def closed(self, reason):
        if not self.summary_file:
            return
        try:
            data = {
                "reason": reason,
                "counts": dict(self._counts),
                "configured_sources": self._configured_sources,
            }
            import json
            with open(self.summary_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
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
