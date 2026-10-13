"""Discover Packer templates inside a cloned app repository.

Two layouts are supported:

1. Legacy single-template layout::

       packer/template.pkr.hcl
       packer/variables.pkr.hcl

   Produces a single ``_PackerTemplate(key="default", ...)``. This is
   the layout used by single-image apps: same image name
   (``<app_id>-<tag>``), same lock key, same phase names without any
   ``[key]`` suffix.

2. Multi-template subdirectory layout::

       packer/<key>/template.pkr.hcl
       packer/<key>/variables.pkr.hcl   (optional)
       packer/<other>/template.pkr.hcl
       packer/_common/scripts/...       (ignored — no template.pkr.hcl)

   Produces one ``_PackerTemplate`` per subdirectory, sorted by key.
   Subdirectories without a ``template.pkr.hcl`` are silently ignored,
   which lets templates share helper files under ``packer/_common``
   or ``packer/scripts`` without forcing the discovery to know about
   them.

Hard errors (``PackerTemplateDiscoveryError``):

* Both legacy file AND one or more subdirectory templates present.
  The two layouts are mutually exclusive; mixing them would silently
  bias which template a deploy sees, so we refuse to guess.
* A subdirectory's name does not match ``[a-z][a-z0-9_-]{0,30}``. The
  key becomes part of an image name (``<app_id>-<key>-<tag>``) and an
  OpenTofu variable suffix (``image_name_<key>``), both of which have
  stricter syntax than a generic directory name — surfacing the
  problem as a hard error is friendlier than silently ignoring a
  typo'd subdirectory.

The module is intentionally self-contained — only stdlib imports —
so the worker can reuse the exact same discovery logic without
dragging in any web framework deps.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

_TEMPLATE_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,30}$")


@dataclass
class _PackerTemplate:
    """A single Packer template discovered under ``packer/``.

    Attributes:
        key: Stable identifier for the template. ``"default"`` for the
            legacy single-template layout; the subdirectory name for
            the multi-template layout.
        template_path: Absolute path to the ``template.pkr.hcl`` file.
        variables_path: Absolute path to the (optional) sibling
            ``variables.pkr.hcl``. Callers must check ``os.path.isfile``
            before reading — not every template declares variables.
    """

    key: str
    template_path: str
    variables_path: str


class PackerTemplateDiscoveryError(ValueError):
    """Raised when the ``packer/`` layout can't be interpreted unambiguously."""


def _discover_packer_templates(repo_path: str) -> list[_PackerTemplate]:
    """Walk ``<repo_path>/packer/`` and return the templates it contains.

    See the module docstring for the full layout / error rules. Returns
    an empty list when there is no ``packer/`` directory or the
    directory contains no recognisable templates (subdirectories
    without ``template.pkr.hcl`` are ignored, not an error).
    """
    packer_dir = os.path.join(repo_path, "packer")
    if not os.path.isdir(packer_dir):
        return []

    legacy_template = os.path.join(packer_dir, "template.pkr.hcl")
    has_legacy = os.path.isfile(legacy_template)

    multi_templates: list[_PackerTemplate] = []
    bad_keys: list[str] = []
    for entry in sorted(os.listdir(packer_dir)):
        sub = os.path.join(packer_dir, entry)
        if not os.path.isdir(sub):
            continue
        tmpl = os.path.join(sub, "template.pkr.hcl")
        if not os.path.isfile(tmpl):
            continue
        if not _TEMPLATE_KEY_RE.match(entry):
            bad_keys.append(entry)
            continue
        multi_templates.append(
            _PackerTemplate(
                key=entry,
                template_path=tmpl,
                variables_path=os.path.join(sub, "variables.pkr.hcl"),
            )
        )

    if bad_keys:
        raise PackerTemplateDiscoveryError(
            f"Packer template subdirectories with invalid keys (must match " f"[a-z][a-z0-9_-]{{0,30}}): {bad_keys}"
        )

    if has_legacy and multi_templates:
        raise PackerTemplateDiscoveryError(
            "App repository has BOTH packer/template.pkr.hcl (legacy layout) AND "
            f"packer/<key>/template.pkr.hcl subdirectories ({[t.key for t in multi_templates]}). "
            "Choose one layout — remove the legacy file or the subdirectories."
        )

    if has_legacy:
        return [
            _PackerTemplate(
                key="default",
                template_path=legacy_template,
                variables_path=os.path.join(packer_dir, "variables.pkr.hcl"),
            )
        ]

    return multi_templates
