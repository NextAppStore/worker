"""Zweisprachige (DE/EN) Texte für Log-Zeilen und Fehlermeldungen des Workers.

Siehe ``catalog.py`` für die Konventionen.
"""

from app.i18n.locale import DEFAULT_LOCALE, SUPPORTED_LOCALES, Locale, normalize_locale
from app.i18n.translate import localize_text, t

__all__ = [
    "DEFAULT_LOCALE",
    "SUPPORTED_LOCALES",
    "Locale",
    "localize_text",
    "normalize_locale",
    "t",
]
