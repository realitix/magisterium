"""Phase 10 — Corpus Rore Sanctifica (CIRS) + bibliothèque numérique associée.

Source : http://www.rore-sanctifica.org/ (HTTP, certificat TLS cassé).

Organisation cible (livres/rore-sanctifica/) :
  01-cirs-publications/   tomes, notitiae, communiqués, diaporamas, études
  biblio-numerique/        18 sections thématiques (sources tierces collectées)
  translations/             traductions russe / espagnol
  _inventory.json           source de vérité de cette phase

Profondeur d'ingestion :
  - Noyau CIRS (~173 docs, is_core=True) → PDF + sidecar .meta.yaml + .lang.md
    extrait via pdftotext (lecture ligne-à-ligne, doctrinalement utilisable)
  - Biblio numérique (~525 docs, is_core=False) → PDF + sidecar minimal
    (archive brute, indexation locale par filename uniquement)

Idempotent : skip si .meta.yaml existe et que le PDF est déjà présent et non vide.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LIVRES_ROOT = REPO_ROOT / "livres"
RORE_ROOT = LIVRES_ROOT / "rore-sanctifica"
INVENTORY = RORE_ROOT / "_inventory.json"

CONCURRENCY = 4   # parallel downloads (rate-limit douce envers rore-sanctifica.org)
TIMEOUT = 60.0


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def _fetch(client: httpx.AsyncClient, url: str) -> bytes | None:
    """Download a PDF as bytes, retry once on transient error.

    HTML entities in href attributes (&eacute; etc.) are unescaped before
    request — some pages on rore-sanctifica.org leak entity-encoded URLs
    which fail with 404 if sent verbatim.
    """
    import html as _htmllib
    real_url = _htmllib.unescape(url)
    for attempt in (1, 2):
        try:
            r = await client.get(real_url, timeout=TIMEOUT, follow_redirects=True)
            r.raise_for_status()
            return r.content
        except (httpx.HTTPError, httpx.ReadTimeout) as e:
            if attempt == 2:
                print(f"  FAIL {real_url} : {e}")
                return None
            await asyncio.sleep(2.0)
    return None


def _pdftotext(pdf_path: Path, txt_path: Path) -> bool:
    """Convert PDF to text (UTF-8). Returns True if a non-empty .md is produced."""
    try:
        out = subprocess.run(
            ["pdftotext", "-enc", "UTF-8", "-layout", str(pdf_path), str(txt_path)],
            capture_output=True, timeout=120,
        )
        if out.returncode != 0:
            return False
        if not txt_path.exists() or txt_path.stat().st_size < 200:
            return False
        return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _write_sidecar(entry: dict, pdf_bytes: bytes, target_dir: Path) -> Path:
    """Write the .meta.yaml sidecar next to the PDF."""
    slug = entry["slug"]
    lang = entry["lang"]
    sha = _sha256(pdf_bytes)

    # Build doc meta. Title from anchor text when usable, else from filename.
    raw_title = (entry.get("title") or "").strip()
    if not raw_title or raw_title.lower() in {"télécharger en pdf", "telecharger en pdf"}:
        raw_title = entry["filename"].rsplit(".", 1)[0].replace("_", " ").replace("-", " ")
    titre_fr = raw_title[:280]

    incipit = titre_fr  # placeholder — Rore docs have no canonical incipit

    meta = {
        "incipit": incipit,
        "titre_fr": titre_fr,
        "auteur": "CIRS — Comité international Rore Sanctifica" if entry["category"] == "01-cirs-publications" else "Divers (collection CIRS)",
        "periode": "post-vatican-ii",
        "type": _infer_type(entry),
        "date": _infer_date(entry),
        "categorie": "livre",
        "autorite_magisterielle": "etude-privee",
        "langue_originale": lang,
        "denzinger": [],
        "sujets": _infer_sujets(entry),
        "themes_doctrinaux": _infer_themes(entry),
        "references_anterieures": [],
        "references_posterieures": [],
        "sources": [{
            "url": entry["url"],
            "site": "rore-sanctifica.org",
            "langue": lang,
            "fetch_method": "httpx",
        }],
        "traductions": {
            lang: {
                "kind": entry["kind"],
                "sha256": sha,
                "source_url": entry["url"],
                "fetch_method": "httpx",
            }
        },
        "langues_disponibles": [lang],
        "sha256": {lang: sha},
        # Note: pas de bloc `ouvrage` — la collection rore-sanctifica n'est
        # pas un ouvrage en parties séquentielles. La hiérarchie thématique
        # est encodée dans le chemin (rel_dir) et dans `_inventory.json`.
        "collection": {
            "slug": "rore-sanctifica",
            "titre": "Rore Sanctifica (CIRS) — Corpus complet",
            "categorie": entry["category"],
            "sous_categorie": entry["subcategory"],
        },
    }

    target_dir.mkdir(parents=True, exist_ok=True)
    meta_path = target_dir / f"{slug}.meta.yaml"
    meta_path.write_text(yaml.safe_dump(meta, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return meta_path


def _infer_type(entry: dict) -> str:
    sub = entry["subcategory"]
    if sub == "tomes":
        return "etude-tome"
    if sub == "notitiae":
        return "notitia"
    if sub == "communiques":
        return "communique"
    if sub == "diaporamas":
        return "diaporama"
    if sub.startswith("etudes"):
        return "etude"
    if entry["category"] == "biblio-numerique":
        return "document-de-reference"
    return "document"


def _infer_date(entry: dict) -> str | None:
    """Try to extract a YYYY-MM-DD from filename."""
    import re
    fn = entry["filename"]
    m = re.search(r"(20\d\d)[-_](\d{1,2})[-_](\d{1,2})", fn)
    if m:
        try:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        except ValueError:
            pass
    m = re.search(r"(20\d\d)[-_](\d{1,2})", fn)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    m = re.search(r"\b(1[5-9]\d\d|20\d\d)\b", fn)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _infer_sujets(entry: dict) -> list[str]:
    sub = entry["subcategory"]
    base = ["validite-ordinations", "rite-episcopal-1968", "pontificalis-romani"]
    if "anglican" in sub:
        base += ["ordinations-anglicanes"]
    if "consilium" in sub or "reformateurs" in sub:
        base += ["consilium", "reforme-liturgique"]
    if "hippolyte" in sub:
        base += ["tradition-apostolique-hippolyte"]
    if "orient" in sub:
        base += ["rites-orientaux"]
    return base


def _infer_themes(entry: dict) -> list[str]:
    sub = entry["subcategory"]
    if sub == "tomes" or sub == "notitiae" or "invalidite" in sub:
        return ["validite-ordinations", "rite-episcopal-1968", "matiere-forme"]
    if "anglican" in sub:
        return ["ordinations-anglicanes", "matiere-forme-intention"]
    if "hippolyte" in sub:
        return ["tradition-apostolique", "anaphore-eucharistique"]
    if "orient" in sub:
        return ["rites-orientaux", "consecration-episcopale"]
    return []


async def _process(entry: dict, client: httpx.AsyncClient, sem: asyncio.Semaphore,
                    refresh: bool, log_prefix: str) -> tuple[str, str]:
    """Download a single PDF + sidecar. Returns (status, slug)."""
    async with sem:
        target_dir = RORE_ROOT / entry["rel_dir"]
        slug = entry["slug"]
        lang = entry["lang"]
        pdf_path = target_dir / f"{slug}.{lang}.pdf"
        meta_path = target_dir / f"{slug}.meta.yaml"
        md_path = target_dir / f"{slug}.{lang}.md"

        if pdf_path.exists() and pdf_path.stat().st_size > 0 and meta_path.exists() and not refresh:
            return ("skip", slug)

        target_dir.mkdir(parents=True, exist_ok=True)
        data = await _fetch(client, entry["url"])
        if data is None or len(data) < 200:
            return ("fail", slug)
        if not data.startswith(b"%PDF"):
            print(f"  {log_prefix} NOT-PDF {entry['url'][:80]}")
            return ("fail", slug)

        pdf_path.write_bytes(data)
        _write_sidecar(entry, data, target_dir)

        # Extract text only for core docs (sidebar + .md)
        if entry["is_core"]:
            ok = _pdftotext(pdf_path, md_path)
            if not ok:
                # placeholder pointing at PDF
                md_path.write_text(f"[PDF file: {pdf_path.name}]\n\n*Extraction texte non disponible. Consulter le PDF directement.*\n", encoding="utf-8")
        return ("ok", slug)


async def main(refresh: bool = False, only_core: bool = False, only_categories: list[str] | None = None) -> int:
    inv = json.loads(INVENTORY.read_text(encoding="utf-8"))
    if only_core:
        inv = [e for e in inv if e["is_core"]]
    if only_categories:
        inv = [e for e in inv if e["category"] in only_categories or e["subcategory"] in only_categories]
    print(f"Phase 10: {len(inv)} entries to process (refresh={refresh})")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(http2=True, headers={"User-Agent": "Mozilla/5.0 (catholique-archiver)"}) as client:
        tasks = [_process(e, client, sem, refresh, f"[{i+1}/{len(inv)}]")
                 for i, e in enumerate(inv)]
        results = await asyncio.gather(*tasks)

    n_ok = sum(1 for r in results if r[0] == "ok")
    n_skip = sum(1 for r in results if r[0] == "skip")
    n_fail = sum(1 for r in results if r[0] == "fail")
    print(f"\nDone. OK={n_ok}  SKIP={n_skip}  FAIL={n_fail}")
    return 0 if n_fail < len(inv) // 4 else 1


if __name__ == "__main__":
    refresh = "--refresh" in sys.argv
    only_core = "--core" in sys.argv
    cats = []
    for arg in sys.argv:
        if arg.startswith("--cat="):
            cats.append(arg.split("=", 1)[1])
    sys.exit(asyncio.run(main(refresh=refresh, only_core=only_core, only_categories=cats or None)))
