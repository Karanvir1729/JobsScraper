import json
import os
from typing import Any, Dict, List, Optional

import yaml
import scrapy
from itemadapter import ItemAdapter

from scraper.items import ProviderItem
from scraper import utils as U


class ConfigSpider(scrapy.Spider):
    name = "config_providers"

    custom_settings = {
        # Feed export is configured from the caller (Streamlit app)
    }

    def __init__(self, sources_file: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources_file = sources_file
        self.sources: List[Dict[str, Any]] = []

    def start_requests(self):
        with open(self.sources_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self.sources = data.get("sources", [])
        for src in self.sources:
            if src.get("enabled") is False:
                self.logger.info("Skipping disabled source: %s", src.get("name"))
                continue
            meta = {"source": src.get("name"), "cfg": src}
            for url in src.get("start_urls", []) or []:
                yield scrapy.Request(url, callback=self.parse_listing, meta=meta)

    def parse_listing(self, response: scrapy.http.Response):
        cfg = response.meta.get("cfg", {})
        source_name = response.meta.get("source")
        listing = (cfg.get("listing") or {})
        item_selector = listing.get("item_selector")
        produced = 0

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
                    if detail_url and (cfg.get("detail") or {}).get("fields"):
                        req = scrapy.Request(detail_url, callback=self.parse_detail)
                        req.meta["item"] = item
                        req.meta["cfg"] = cfg
                        yield req
                    else:
                        produced += 1
                        yield item

        # Follow arbitrary links selector (useful for index pages without clear cards)
        follow_sel = (cfg.get("listing") or {}).get("follow_links_selector")
        for sel in U.listify(follow_sel):
            for href in response.css((sel + "::attr(href)") if "::attr(" not in sel else sel).getall():
                url = U.absolute_url(response.url, href)
                if not url:
                    continue
                req = scrapy.Request(url, callback=self.parse_detail)
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
                    yield item

        # pagination
        next_sel = (cfg.get("pagination") or {}).get("next_page_selector")
        for sel in U.listify(next_sel):
            href = response.css((sel + "::attr(href)") if "::attr(" not in sel else sel).get()
            if href:
                url = U.absolute_url(response.url, href)
                yield response.follow(url, callback=self.parse_listing, meta=response.meta)
                break

    def parse_detail(self, response: scrapy.http.Response):
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

        yield item
