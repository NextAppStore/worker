"""Übersetzung von Katalog-Einträgen und vorhandenen englischen Texten."""

from __future__ import annotations

import logging
import re
import string
from functools import lru_cache
from typing import Any

from app.i18n.catalog import MESSAGES
from app.i18n.locale import DEFAULT_LOCALE, Locale

logger = logging.getLogger(__name__)

_FORMATTER = string.Formatter()

# Platzhalter, die nur bestimmte Werte annehmen dürfen. Ohne die Einschränkung
# würde ``{action} failed: {error}`` jeden Text der Form ``X failed: Y`` an sich
# reißen.
_CHOICES: dict[str, tuple[str, ...]] = {
    "action": ("pause", "resume"),
    "server_op": ("stop", "start"),
}

# Platzhalter mit festem Muster. ``log_prefix`` ist ``""`` oder ``"[<template-key>] "``
# (Packer-Mehrfach-Template); ein freies ``.*?`` würde auch ``"Packer error: "`` schlucken.
_PATTERNS: dict[str, str] = {
    "log_prefix": r"(?:\[[^\]\s]+\]\s)?",
}

# Werte, die selbst übersetzt werden, bevor sie in den DE-Text kommen. Alles
# andere (IDs, Pfade, Fremdtexte) wird unverändert eingesetzt.
_PARAM_VALUES: dict[str, dict[str, str]] = {
    "action": {"pause": "Pausieren", "resume": "Fortsetzen"},
    "server_op": {"stop": "Stoppen", "start": "Starten"},
}

_MAX_DEPTH = 3


def _localize_param(name: str, value: Any, locale: Locale) -> Any:
    if locale != "de" or not isinstance(value, str):
        return value
    return _PARAM_VALUES.get(name, {}).get(value, value)


def t(key: str, locale: Locale = DEFAULT_LOCALE, **params: Any) -> str:
    """Katalog-Text für ``key``.

    Wirft nie: unbekannter Key → der Key selbst, fehlender Platzhalter → der
    unformatierte Text. Beides wird geloggt.
    """
    entry = MESSAGES.get(key)
    if entry is None:
        logger.warning("i18n: unknown message key %r", key)
        return key
    template = entry.get(locale) or entry[DEFAULT_LOCALE]  # type: ignore[misc]
    params = {name: _localize_param(name, value, locale) for name, value in params.items()}
    try:
        return template.format(**params)
    except (KeyError, IndexError, ValueError):
        logger.warning("i18n: bad parameters for %r: %r", key, sorted(params))
        return template


def placeholders(template: str) -> list[str]:
    """Namen der Platzhalter in Reihenfolge des Auftretens."""
    return [name for _, name, _, _ in _FORMATTER.parse(template) if name]


@lru_cache(maxsize=1)
def _patterns() -> tuple[tuple[str, re.Pattern[str]], ...]:
    """EN-Vorlagen als Regex, spezifischste (längster Festtext) zuerst."""
    compiled: list[tuple[int, str, re.Pattern[str]]] = []
    for key, entry in MESSAGES.items():
        parts: list[str] = []
        literal_len = 0
        for literal, name, _spec, _conv in _FORMATTER.parse(entry["en"]):
            parts.append(re.escape(literal))
            literal_len += len(literal)
            if not name:
                continue
            if name in _CHOICES:
                parts.append(f"(?P<{name}>{'|'.join(_CHOICES[name])})")
            elif name in _PATTERNS:
                parts.append(f"(?P<{name}>{_PATTERNS[name]})")
            else:
                parts.append(f"(?P<{name}>.+?)")
        compiled.append((literal_len, key, re.compile("".join(parts), re.DOTALL)))
    compiled.sort(key=lambda item: -item[0])
    return tuple((key, pattern) for _, key, pattern in compiled)


def find_key(text: str) -> str | None:
    """Katalog-Key, dessen EN-Vorlage ``text`` entspricht (sonst ``None``)."""
    for key, pattern in _patterns():
        if pattern.fullmatch(text):
            return key
    return None


def localize_text(text: str, locale: Locale = DEFAULT_LOCALE, *, _depth: int = 0) -> str:
    """Übersetzt einen englischen Text, wenn er einem Katalogeintrag entspricht.

    Unbekannte Texte (Roh-Ausgabe, Fremdfehler) bleiben unverändert. EN ist der
    Quelltext und wird nie angefasst. Eingebettete Texte (``Git clone failed:
    Failed to clone release …``) werden rekursiv übersetzt.
    """
    if locale == "en" or not isinstance(text, str) or _depth > _MAX_DEPTH:
        return text
    for key, pattern in _patterns():
        match = pattern.fullmatch(text)
        if match:
            params = {
                name: (value if name in _CHOICES else localize_text(value, locale, _depth=_depth + 1))
                for name, value in match.groupdict().items()
            }
            return t(key, locale, **params)
    return text
