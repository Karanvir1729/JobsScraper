import re
import json
from typing import Iterable, List, Optional, Union
from urllib.parse import urljoin
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import html as _html

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


def html_unescape(s: str | None) -> str | None:
    if s is None:
        return None
    try:
        return _html.unescape(s)
    except Exception:
        return s


def absolute_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    return urljoin(base, href)


def set_query_param(url: str, name: str, value: str | int) -> str:
    parts = list(urlparse(url))
    q = parse_qs(parts[4], keep_blank_values=True)
    q[str(name)] = [str(value)]
    parts[4] = urlencode(q, doseq=True)
    return urlunparse(parts)


def get_query_int(url: str, name: str, default: int = 1) -> int:
    try:
        qs = parse_qs(urlparse(url).query)
        v = qs.get(str(name))
        if v:
            return int(v[0])
    except Exception:
        pass
    return default


def _deobfuscate_email_text(text: str) -> str:
    s = html_unescape(text) or ""
    repl = [
        (r"\s*\[at\]\s*", "@"),
        (r"\s*\(at\)\s*", "@"),
        (r"\s+at\s+", "@"),
        (r"\s*\{at\}\s*", "@"),
        (r"\s*\[dot\]\s*", "."),
        (r"\s*\(dot\)\s*", "."),
        (r"\s+dot\s+", "."),
        (r"\s*\{dot\}\s*", "."),
    ]
    for pattern, val in repl:
        s = re.sub(pattern, val, s, flags=re.IGNORECASE)
    return s


def discover_email_from_selector(sel) -> str | None:
    # Try explicit mailto first
    mailto = sel.css('a[href^="mailto:"]::attr(href)').get()
    if mailto:
        return clean_text(mailto.replace("mailto:", "").split("?")[0])
    # Next, regex scan visible text with deobfuscation
    texts = sel.css('::text').getall()
    text = " ".join(texts[:500]) if texts else (sel.get() or "")
    text = _deobfuscate_email_text(text)
    if text:
        m = EMAIL_RE.search(text)
        if m:
            return m.group(0)
    # Look for common attributes
    data_email = sel.css('[data-email]::attr(data-email)').get()
    if data_email and EMAIL_RE.search(data_email):
        return EMAIL_RE.search(data_email).group(0)
    user = sel.css('[data-user]::attr(data-user)').get()
    domain = sel.css('[data-domain]::attr(data-domain)').get()
    if user and domain:
        cand = f"{user}@{domain}"
        if EMAIL_RE.search(cand):
            return cand
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
