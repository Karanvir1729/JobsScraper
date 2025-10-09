#!/usr/bin/env python3
"""
Golden Record Generator

Scans output/providers-*.csv files, keeps rows that have a phone number,
deduplicates by phone, and writes a unified golden CSV to
output/providers-golden.csv. Newer files take precedence when conflicts arise.

Usage:
  python3 golden_record_gen.py
"""

from __future__ import annotations

import csv
import glob
import os
from pathlib import Path
from typing import Dict, List


OUTPUT_DIR = Path("output")
GOLDEN_CSV = OUTPUT_DIR / "providers-golden.csv"


def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    p = str(phone).strip()
    if p.lower().startswith("tel:"):
        p = p.split(":", 1)[1]
    # remove whitespace and common separators
    for ch in (" ", "\t", "\n", "-", "."):
        p = p.replace(ch, "")
    return p


def preferred_fields() -> List[str]:
    return [
        "source",
        "category",
        "region",
        "business_name",
        "phone",
        "email",
        "website",
        "address",
        "city",
        "province",
        "postal_code",
        "listing_url",
        "detail_url",
    ]


def read_csv_rows(p: Path) -> List[Dict]:
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            return [dict(r) for r in rdr]
    except Exception:
        return []


def write_csv_rows(p: Path, rows: List[Dict]):
    if not rows:
        p.write_text("", encoding="utf-8")
        return
    keys = set()
    for r in rows:
        keys.update(r.keys())
    ordered = [k for k in preferred_fields() if k in keys]
    extras = [k for k in sorted(keys) if k not in ordered]
    fieldnames = ordered + extras
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _canon_row_key(r: Dict) -> str:
    # Canonical representation for exact-duplicate detection
    # Uses all present keys and their string values; order-insensitive
    items = sorted((str(k), "" if r.get(k) is None else str(r.get(k))) for k in r.keys())
    return "\u241F".join([f"{k}\u241E{v}" for k, v in items])


def collect_input_csvs() -> List[Path]:
    OUTPUT_DIR.mkdir(exist_ok=True)
    paths = []
    for pat in ("providers-*.csv",):
        for s in glob.glob(str(OUTPUT_DIR / pat)):
            if s.endswith("providers-golden.csv"):
                continue
            paths.append(Path(s))
    # sort by mtime DESC so newer files are considered first
    paths.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
    return paths


def build_golden(add_to_existing: bool = True) -> List[Dict]:
    csvs = collect_input_csvs()
    # Start with existing golden rows if present
    rows_out: List[Dict] = []
    seen: set[str] = set()
    if add_to_existing and GOLDEN_CSV.exists():
        for r in read_csv_rows(GOLDEN_CSV):
            ph = normalize_phone(r.get("phone"))
            if not ph:
                continue
            rr = dict(r)
            rr["phone"] = ph
            key = _canon_row_key(rr)
            if key not in seen:
                rows_out.append(rr)
                seen.add(key)
    # Walk all run CSVs and add rows with phone if not exact duplicates
    for p in csvs:
        for r in read_csv_rows(p):
            ph = normalize_phone(r.get("phone"))
            if not ph:
                continue
            rr = dict(r)
            rr["phone"] = ph
            key = _canon_row_key(rr)
            if key in seen:
                continue
            rows_out.append(rr)
            seen.add(key)
    return rows_out


def augment_csv_with_golden(csv_path: Path, golden_rows: List[Dict]) -> int:
    """Append golden rows not already present in csv_path (by normalized phone).

    Returns number of rows appended.
    """
    try:
        if not csv_path.exists():
            return 0
        cur_rows = read_csv_rows(csv_path)
        cur_phones = set()
        for r in cur_rows:
            ph = normalize_phone(r.get("phone"))
            if ph:
                cur_phones.add(ph)
        add_rows: List[Dict] = []
        for r in golden_rows:
            ph = normalize_phone(r.get("phone"))
            if not ph or ph in cur_phones:
                continue
            rr = dict(r)
            rr["phone"] = ph
            add_rows.append(rr)
        if add_rows:
            write_csv_rows(csv_path, cur_rows + add_rows)
        return len(add_rows)
    except Exception:
        return 0


def main() -> int:
    OUTPUT_DIR.mkdir(exist_ok=True)
    before = 0
    if GOLDEN_CSV.exists():
        try:
            before = sum(1 for _ in read_csv_rows(GOLDEN_CSV))
        except Exception:
            before = 0
    golden_rows = build_golden(add_to_existing=True)
    write_csv_rows(GOLDEN_CSV, golden_rows)
    after = len(golden_rows)
    added = max(0, after - before)
    print(f"Golden updated: {before} -> {after} rows (added {added}). File: {GOLDEN_CSV}")

    # Augment every providers-*.csv (including boost CSVs) with missing golden rows
    appended_total = 0
    for p in collect_input_csvs():
        if p.name == GOLDEN_CSV.name:
            continue
        appended_total += augment_csv_with_golden(p, golden_rows)
    if appended_total:
        print(f"Appended {appended_total} golden rows across run CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
