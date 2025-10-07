import re
import json
from typing import Iterable, List, Optional, Union
from urllib.parse import urljoin

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(r"(\+?1?[\s\-.]?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4})")


def listify(val: Union[str, Iterable[str], None]) -> List[str]:
    if val is None:
        return []
    if isinstance(val, str):
        return [val]
    return [v for v in val if v]


def extract_first(selector, css: Union[str, Iterable[str], None]) -> Optional[str]:
    for sel in listify(css):
        if not sel:
            continue
        val = selector.css(sel).get()
        if val:
            return clean_text(val)
    return None


def extract_attr(selector, css: Union[str, Iterable[str], None], attr: str) -> Optional[str]:
    for sel in listify(css):
        query = sel
        if sel and "::attr(" not in sel:
            query = sel + f"::attr({attr})"
        val = selector.css(query).get()
        if val:
            return clean_text(val)
    return None


def clean_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", s)
    return s.strip() or None


def absolute_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(base, href)


def discover_email_from_selector(sel) -> str | None:
    # Try explicit mailto first
    mailto = sel.css('a[href^="mailto:"]::attr(href)').get()
    if mailto:
        return clean_text(mailto.replace("mailto:", "").split("?")[0])
    # Next, regex scan visible text (limit to reasonable length)
    text = sel.get()
    if text:
        m = EMAIL_RE.search(text)
        if m:
            return m.group(0)
    return None


def discover_phone_from_selector(sel) -> Optional[str]:
    text = sel.get()
    if not text:
        return None
    m = PHONE_RE.search(text)
    if m:
        return normalize_phone(m.group(0))
    return None


def extract_jsonld_objects(response) -> list:
    objs = []
    try:
        texts = response.css('script[type="application/ld+json"]::text').getall()
    except Exception:
        texts = []
    for t in texts:
        try:
            data = json.loads(t)
        except Exception:
            continue
        if isinstance(data, dict) and "@graph" in data and isinstance(data["@graph"], list):
            objs.extend(data["@graph"])
        elif isinstance(data, list):
            objs.extend(data)
        else:
            objs.append(data)
    return objs



def normalize_phone(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    p = phone.strip()
    if p.lower().startswith("tel:"):
        p = p.split(":", 1)[1]
    p = re.sub(r"\s+", "", p)
    return p or None
