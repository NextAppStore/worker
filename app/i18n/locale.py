"""Sprache eines Tasks.

Das Backend schickt die UI-Sprache der Nutzer*in als ``locale``-Feld im
Task-Payload mit (``"de"`` oder ``"en"``). Ältere Backends, die das Feld noch
nicht senden, bekommen den Standard (``DEFAULT_LOCALE``).
"""

from __future__ import annotations

from typing import Any, Literal

Locale = Literal["de", "en"]

SUPPORTED_LOCALES: tuple[Locale, ...] = ("de", "en")

# Gleicher Fallback wie im Backend und im Frontend: Fehlt das Feld oder ist
# die Sprache unbekannt, wird Deutsch ausgegeben.
DEFAULT_LOCALE: Locale = "de"


def normalize_locale(value: Any) -> Locale:
    """``"de"`` / ``"EN"`` / ``"de-DE"`` → ``"de"`` / ``"en"``; alles andere → Standard."""
    if isinstance(value, str):
        primary = value.strip().lower().replace("_", "-").split("-", 1)[0]
        if primary in SUPPORTED_LOCALES:
            return primary  # type: ignore[return-value]
    return DEFAULT_LOCALE
