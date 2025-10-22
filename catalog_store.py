"""In-memory store for catalog data loaded from Mistral."""
from __future__ import annotations

import re
from threading import RLock
from typing import Dict, Optional, Tuple

try:  # pragma: no cover - optional dependency
    from rapidfuzz import fuzz  # type: ignore

    def _ratio(a: str, b: str) -> int:
        return int(fuzz.token_set_ratio(a, b))

except Exception:  # pragma: no cover - graceful fallback
    from difflib import SequenceMatcher

    def _ratio(a: str, b: str) -> int:
        return int(SequenceMatcher(None, a, b).ratio() * 100)


_LOCK = RLock()
_PROFILE: Optional[str] = None
_MATERIALS: Dict[str, Dict[str, str]] = {}
_BARCODES: Dict[str, str] = {}
_BY_NAME: Dict[str, str] = {}


def clear() -> None:
    with _LOCK:
        global _PROFILE, _MATERIALS, _BARCODES, _BY_NAME
        _PROFILE = None
        _MATERIALS = {}
        _BARCODES = {}
        _BY_NAME = {}


def prepare_name_index(materials: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    prepared: Dict[str, str] = {}
    for code, data in materials.items():
        name = str(data.get("name") or "").strip().lower()
        if not name:
            continue
        prepared[code] = name
    return prepared


def set_catalog(profile: str, data: Dict[str, Dict[str, Dict[str, str]]]) -> None:
    materials = data.get("materials") or {}
    barcodes = data.get("barcodes") or {}
    by_name = data.get("by_name") or {}
    with _LOCK:
        global _PROFILE, _MATERIALS, _BARCODES, _BY_NAME
        _PROFILE = profile
        _MATERIALS = {str(code): dict(info) for code, info in materials.items()}
        _BARCODES = dict(barcodes)
        _BY_NAME = dict(by_name)


def get_profile() -> Optional[str]:
    with _LOCK:
        return _PROFILE


def get_stats() -> Tuple[int, int]:
    with _LOCK:
        return len(_MATERIALS), len(_BARCODES)


def get_material(code: str) -> Optional[Dict[str, str]]:
    code = (code or "").strip()
    if not code:
        return None
    with _LOCK:
        entry = _MATERIALS.get(code)
        return dict(entry) if entry else None


def get_material_by_barcode(barcode: str) -> Optional[Dict[str, str]]:
    barcode = (barcode or "").strip()
    if not barcode:
        return None
    with _LOCK:
        material_code = _BARCODES.get(barcode)
        if not material_code:
            return None
        entry = _MATERIALS.get(material_code)
        if not entry:
            return None
        payload = dict(entry)
        payload.setdefault("barcode", barcode)
        return payload


def find_best_match(text: str, min_score: int = 85) -> Optional[Dict[str, str]]:
    cleaned = " ".join((text or "").strip().split()).lower()
    if not cleaned:
        return None
    best_code: Optional[str] = None
    best_score = 0
    with _LOCK:
        for code, name in _BY_NAME.items():
            score = _ratio(cleaned, name)
            if score > best_score:
                best_score = score
                best_code = code
    if best_code is None or best_score < min_score:
        return None
    material = get_material(best_code)
    if not material:
        return None
    material["score"] = str(best_score)
    return material


def has_data() -> bool:
    with _LOCK:
        return bool(_MATERIALS)


def lookup_token(text: str) -> Optional[Dict[str, str]]:
    if not text:
        return None
    barcode_match = re.search(r"\b\d{8,13}\b", text)
    if barcode_match:
        candidate = get_material_by_barcode(barcode_match.group(0))
        if candidate:
            candidate["barcode"] = barcode_match.group(0)
            return candidate
    code_match = re.search(r"\b\d{1,10}\b", text)
    if code_match:
        candidate = get_material(code_match.group(0))
        if candidate:
            return candidate
    return find_best_match(text)


def is_loaded_for(profile: Optional[str]) -> bool:
    with _LOCK:
        if profile is None:
            return bool(_MATERIALS)
        return bool(_MATERIALS) and _PROFILE == profile


