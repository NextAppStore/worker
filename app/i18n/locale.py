"""Sprache eines Tasks.

Das Backend schickt die UI-Sprache der Nutzer*in als Celery-Message-Header
``locale`` (``"de"`` oder ``"en"``) mit. Es ist bewusst kein Argument: die
Task-Signaturen bleiben unverändert, und ältere Backends, die den Header noch
nicht senden, funktionieren weiter.
"""

from __future__ import annotations

from typing import Any, Literal

Locale = Literal["de", "en"]

SUPPORTED_LOCALES: tuple[Locale, ...] = ("de", "en")

# Gleicher Fallback wie im Backend und im Frontend: Fehlt der Header oder ist
# die Sprache unbekannt, wird Deutsch ausgegeben.
DEFAULT_LOCALE: Locale = "de"


def normalize_locale(value: Any) -> Locale:
    """``"de"`` / ``"EN"`` / ``"de-DE"`` → ``"de"`` / ``"en"``; alles andere → Standard."""
    if isinstance(value, str):
        primary = value.strip().lower().replace("_", "-").split("-", 1)[0]
        if primary in SUPPORTED_LOCALES:
            return primary  # type: ignore[return-value]
    return DEFAULT_LOCALE


def task_locale(task: Any) -> Locale:
    """Sprache aus dem Header ``locale`` der Celery-Nachricht des laufenden Tasks.

    Für ``bind=True``-Tasks: ``task_locale(self)``. Außerhalb eines echten
    Aufrufs (``task.run(...)`` in Tests) gibt es keinen Header → Standard.
    """
    request = getattr(task, "request", None)
    getter = getattr(request, "get", None)
    if not callable(getter):
        return DEFAULT_LOCALE
    try:
        return normalize_locale(getter("locale"))
    except Exception:  # pragma: no cover - defensiv, ein Log-Detail darf nie einen Task brechen
        return DEFAULT_LOCALE
