"""Phase 7 — téléchargement local des photos d'évêques (Wikimedia Commons).

Pour chaque évêque dont le brut Wikidata (`clerge/_raw/wikidata.jsonl`) référence
une `image_url` Wikimedia Commons et dont le YAML existe sur disque :

1. Interroge l'API MediaWiki `prop=imageinfo&iiurlwidth=800` pour récupérer le
   thumb URL + les métadonnées de licence (`extmetadata`).
2. Filtre les licences non-libres (skip + log).
3. Télécharge la version 800px, ré-encode en JPEG/PNG/WebP optimisé (Pillow).
4. Met à jour le bloc `photo:` du YAML évêque.
5. Écrit régulièrement un fichier de progression pour reprendre proprement.

Idempotent : skip si la photo locale existe ET que le YAML est déjà à jour.
Rate limit : 1 req/s strict (API + download partagent le même bucket
``commons.wikimedia.org``).

Usage::

    uv run python -m tools.clerge_fetch_photos
    uv run python -m tools.clerge_fetch_photos --limit 200
    uv run python -m tools.clerge_fetch_photos --retry-failed
    uv run python -m tools.clerge_fetch_photos --no-yaml-update
"""

from __future__ import annotations

import argparse
import io
import json
import re
import signal
import sys
import time
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
import yaml
from PIL import Image, UnidentifiedImageError

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "clerge" / "_raw"
META_DIR = ROOT / "clerge" / "_metadata"
EVEQUES_DIR = ROOT / "clerge" / "eveques"
PHOTOS_DIR = ROOT / "clerge" / "photos"

WD_PATH = RAW_DIR / "wikidata.jsonl"
ERROR_LOG = META_DIR / "photo_errors.log"
PROGRESS_PATH = META_DIR / "photo_progress.json"
STATS_PATH = META_DIR / "stats.json"

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "MagisteriumArchiver/1.0 (https://github.com/realitix/catholique)"

# 1 req/s strict, partagé entre API et download.
MIN_INTERVAL = 1.05  # un poil > 1 s pour ne jamais tangenter le quota

# Licences acceptables (libres). On compare insensible à la casse contre
# l'identifiant License ET le LicenseShortName.
FREE_LICENSE_TOKENS: tuple[str, ...] = (
    "cc0",
    "cc-by",
    "cc by",
    "public domain",
    "public-domain",
    "pd-",
    "pdm",
    "no restrictions",
    "fal",  # Free Art License
    "ogl",  # Open Gov Licence
    "gfdl",
)
# Marqueurs de licence non-libre / fair use à exclure explicitement.
NONFREE_TOKENS: tuple[str, ...] = (
    "fair use",
    "fairuse",
    "non-free",
    "non free",
    "all rights reserved",
)

MAX_SIDE = 800
JPEG_QUALITY = 85
SIZE_RECOMPRESS_THRESHOLD = 500 * 1024  # 500 KB
JPEG_RECOMPRESS_QUALITY = 80

PROGRESS_FLUSH_EVERY = 25


# ---------------------------------------------------------------------------
# Rate limiter trivial mono-domaine
# ---------------------------------------------------------------------------


class WikimediaLimiter:
    """1 req/s strict, partagé API + download."""

    def __init__(self, min_interval: float = MIN_INTERVAL) -> None:
        self._min_interval = min_interval
        self._last: float = 0.0

    def wait(self) -> None:
        delta = time.monotonic() - self._last
        if delta < self._min_interval:
            time.sleep(self._min_interval - delta)
        self._last = time.monotonic()


LIMITER = WikimediaLimiter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    out = _HTML_TAG_RE.sub(" ", value)
    out = out.replace("&nbsp;", " ").replace("&amp;", "&")
    out = out.replace("&quot;", '"').replace("&#039;", "'").replace("&apos;", "'")
    out = out.replace("&lt;", "<").replace("&gt;", ">")
    return _WHITESPACE_RE.sub(" ", out).strip()


def extract_filename(image_url: str) -> str | None:
    """Récupère le nom de fichier Commons depuis l'`image_url` Wikidata.

    Format typique :
        http(s)://commons.wikimedia.org/wiki/Special:FilePath/<Filename>
    """
    if not image_url:
        return None
    marker = "/Special:FilePath/"
    idx = image_url.find(marker)
    if idx == -1:
        return None
    filename = image_url[idx + len(marker) :]
    # certains contiennent ?query
    filename = filename.split("?", 1)[0]
    filename = unquote(filename).replace("_", " ").strip()
    return filename or None


def license_is_free(extmeta: dict[str, Any]) -> tuple[bool, str]:
    """Retourne ``(is_free, code)``. ``code`` est destiné au YAML."""
    short = (extmeta.get("LicenseShortName", {}) or {}).get("value", "") or ""
    full = (extmeta.get("License", {}) or {}).get("value", "") or ""
    short_l = short.lower()
    full_l = full.lower()
    blob = f"{short_l} {full_l}"
    if any(t in blob for t in NONFREE_TOKENS):
        return False, short or full or "non-free"
    if any(t in blob for t in FREE_LICENSE_TOKENS):
        code = short.strip() or full.strip()
        # normaliser un peu : "CC BY-SA 4.0" → "CC-BY-SA-4.0"
        code = re.sub(r"\s+", "-", code)
        return True, code or "free"
    return False, short or full or "unknown"


def ext_from_mime(mime: str | None) -> str | None:
    if not mime:
        return None
    mapping = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "image/tiff": "jpg",  # ré-encodé en JPEG
        "image/svg+xml": "svg",  # géré à part
    }
    return mapping.get(mime.lower())


# ---------------------------------------------------------------------------
# Wikimedia API
# ---------------------------------------------------------------------------


@dataclass
class ImageInfo:
    filename: str
    thumb_url: str
    mime: str | None
    width: int | None
    height: int | None
    license_code: str
    is_free: bool
    author: str
    descriptionurl: str


def fetch_image_info(client: httpx.Client, filename: str) -> ImageInfo | None:
    """Interroge l'API MediaWiki et renvoie les infos utiles, ou ``None``."""
    LIMITER.wait()
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "url|size|mime|extmetadata|user",
        "iiurlwidth": str(MAX_SIDE),
        "iiextmetadatafilter": "License|LicenseShortName|Artist|Credit|ImageDescription",
        "format": "json",
        "formatversion": "2",
    }
    resp = client.get(COMMONS_API, params=params, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()
    pages = (data.get("query", {}) or {}).get("pages", []) or []
    if not pages:
        return None
    page = pages[0]
    if page.get("missing"):
        return None
    infos = page.get("imageinfo") or []
    if not infos:
        return None
    info = infos[0]
    extmeta = info.get("extmetadata") or {}
    is_free, code = license_is_free(extmeta)
    author = strip_html((extmeta.get("Artist", {}) or {}).get("value")) or strip_html(
        (extmeta.get("Credit", {}) or {}).get("value")
    )
    return ImageInfo(
        filename=filename,
        thumb_url=info.get("thumburl") or info.get("url") or "",
        mime=info.get("mime"),
        width=info.get("thumbwidth") or info.get("width"),
        height=info.get("thumbheight") or info.get("height"),
        license_code=code,
        is_free=is_free,
        author=author or "unknown",
        descriptionurl=info.get("descriptionurl")
        or f"https://commons.wikimedia.org/wiki/File:{filename}",
    )


def download_bytes(client: httpx.Client, url: str) -> bytes:
    LIMITER.wait()
    resp = client.get(url, timeout=60.0, follow_redirects=True)
    resp.raise_for_status()
    return resp.content


# ---------------------------------------------------------------------------
# Image processing
# ---------------------------------------------------------------------------


def process_image(raw: bytes, mime: str | None) -> tuple[bytes, str, int, int] | None:
    """Redimensionne / ré-encode. Retourne ``(bytes, ext, w, h)``.

    SVG est skip (on n'a pas besoin de portraits vectoriels).
    """
    if mime == "image/svg+xml":
        return None
    try:
        im = Image.open(io.BytesIO(raw))
        im.load()
    except (UnidentifiedImageError, OSError):
        return None

    # GIF, P, RGBA → adapter
    is_animated = getattr(im, "is_animated", False)
    if is_animated:
        im.seek(0)

    # Resize si nécessaire
    w, h = im.size
    longest = max(w, h)
    if longest > MAX_SIDE:
        scale = MAX_SIDE / longest
        new_size = (max(1, int(w * scale)), max(1, int(h * scale)))
        im = im.resize(new_size, Image.LANCZOS)

    # Choix du format de sortie
    fmt = (im.format or "").upper()
    has_alpha = im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info)

    if fmt == "PNG" and has_alpha:
        out_ext = "png"
        if im.mode == "P":
            im = im.convert("RGBA")
        buf = io.BytesIO()
        im.save(buf, format="PNG", optimize=True)
        data = buf.getvalue()
    elif fmt == "WEBP":
        out_ext = "webp"
        buf = io.BytesIO()
        im.save(buf, format="WEBP", quality=JPEG_QUALITY, method=6)
        data = buf.getvalue()
    else:
        # JPEG par défaut
        out_ext = "jpg"
        if im.mode != "RGB":
            im = im.convert("RGB")
        buf = io.BytesIO()
        im.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True, progressive=True)
        data = buf.getvalue()

    # Re-compresse si > 500 KB et c'est un JPEG (les PNG/WebP gardent leur taille)
    if out_ext == "jpg" and len(data) > SIZE_RECOMPRESS_THRESHOLD:
        buf = io.BytesIO()
        im.save(
            buf,
            format="JPEG",
            quality=JPEG_RECOMPRESS_QUALITY,
            optimize=True,
            progressive=True,
        )
        data = buf.getvalue()

    return data, out_ext, im.size[0], im.size[1]


# ---------------------------------------------------------------------------
# YAML update
# ---------------------------------------------------------------------------


def update_eveque_yaml(
    slug: str,
    fichier: str,
    source_url: str,
    licence: str,
    auteur: str,
    largeur: int,
    hauteur: int,
) -> bool:
    """Met à jour le bloc ``photo:`` du YAML. Retourne ``True`` si modifié."""
    path = EVEQUES_DIR / f"{slug}.yaml"
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    new_photo = {
        "fichier": fichier,
        "source": "wikimedia-commons",
        "source_url": source_url,
        "licence": licence,
        "auteur": auteur,
        "largeur": largeur,
        "hauteur": hauteur,
    }
    if doc.get("photo") == new_photo:
        return False
    doc["photo"] = new_photo
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, allow_unicode=True, sort_keys=False)
    return True


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


def slug_from_yaml_for_qid(qid_to_slug: dict[str, str], qid: str) -> str | None:
    return qid_to_slug.get(qid)


def build_qid_to_slug() -> dict[str, str]:
    """Lecture rapide de tous les YAML évêques pour mapper QID → slug."""
    out: dict[str, str] = {}
    for p in EVEQUES_DIR.glob("*.yaml"):
        try:
            with p.open("r", encoding="utf-8") as f:
                doc = yaml.safe_load(f) or {}
        except Exception:
            continue
        qids = doc.get("qids") or {}
        wd = qids.get("wikidata")
        if wd:
            out[wd] = doc.get("slug") or p.stem
    return out


def iter_queue() -> Iterator[tuple[str, str]]:
    """Itère ``(qid, image_url)`` pour chaque entrée wikidata avec photo."""
    with WD_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            img = d.get("image_url")
            qid = d.get("source_id")
            if img and qid:
                yield qid, img


# ---------------------------------------------------------------------------
# Progress / errors
# ---------------------------------------------------------------------------


def load_progress() -> dict[str, Any]:
    if PROGRESS_PATH.exists():
        with PROGRESS_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"done_slugs": [], "failed_slugs": [], "skipped_nonfree": [], "last_index": 0}


def save_progress(state: dict[str, Any]) -> None:
    META_DIR.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(PROGRESS_PATH)


def log_error(slug: str, msg: str) -> None:
    META_DIR.mkdir(parents=True, exist_ok=True)
    with ERROR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{int(time.time())}\t{slug}\t{msg}\n")


def load_failed_slugs() -> set[str]:
    if not ERROR_LOG.exists():
        return set()
    out: set[str] = set()
    with ERROR_LOG.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                out.add(parts[1])
    return out


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


def update_stats(stats_update: dict[str, Any]) -> None:
    data: dict[str, Any] = {}
    if STATS_PATH.exists():
        with STATS_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f) or {}
    data.update(stats_update)
    with STATS_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def compute_photos_size_mb() -> float:
    total = 0
    if PHOTOS_DIR.exists():
        for p in PHOTOS_DIR.iterdir():
            if p.is_file():
                total += p.stat().st_size
    return round(total / (1024 * 1024), 2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


# état global pour signal handler
_INTERRUPTED = False


def _handle_sigint(signum, frame) -> None:  # noqa: ARG001
    global _INTERRUPTED
    _INTERRUPTED = True
    print("\n[signal] arrêt demandé — flush en cours…", file=sys.stderr)


def run(limit: int | None, retry_failed: bool, no_yaml_update: bool) -> None:
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)

    print("[phase-7] mapping QID → slug …", flush=True)
    qid_to_slug = build_qid_to_slug()
    print(f"[phase-7] {len(qid_to_slug)} évêques sur disque indexés.", flush=True)

    print("[phase-7] construction de la queue depuis wikidata.jsonl …", flush=True)
    queue: list[tuple[str, str, str]] = []  # (slug, qid, image_url)
    for qid, img in iter_queue():
        slug = qid_to_slug.get(qid)
        if not slug:
            continue
        queue.append((slug, qid, img))
    print(f"[phase-7] {len(queue)} photos candidates.", flush=True)

    failed = load_failed_slugs()
    if retry_failed:
        queue = [t for t in queue if t[0] in failed]
        print(f"[phase-7] --retry-failed → {len(queue)} candidates.", flush=True)

    if limit is not None:
        queue = queue[:limit]
        print(f"[phase-7] --limit {limit} → {len(queue)} candidates.", flush=True)

    state = load_progress()
    done_set: set[str] = set(state.get("done_slugs", []))
    nonfree_set: set[str] = set(state.get("skipped_nonfree", []))

    license_counter: Counter[str] = Counter()
    n_downloaded = 0
    n_skipped_local = 0
    n_skipped_nonfree = 0
    n_404 = 0
    n_errors = 0
    n_no_filename = 0

    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"}
    with httpx.Client(headers=headers, http2=True, timeout=30.0) as client:
        for idx, (slug, qid, image_url) in enumerate(queue):
            if _INTERRUPTED:
                break

            # idempotence : photo déjà locale + YAML à jour
            existing = list(PHOTOS_DIR.glob(f"{slug}.*"))
            if existing and slug in done_set:
                n_skipped_local += 1
                continue

            filename = extract_filename(image_url)
            if not filename:
                n_no_filename += 1
                log_error(slug, f"no-filename:{image_url}")
                continue

            # Tentatives avec backoff
            info: ImageInfo | None = None
            err_msg = ""
            for attempt, delay in enumerate((1, 4, 16), start=1):
                try:
                    info = fetch_image_info(client, filename)
                    break
                except httpx.HTTPStatusError as e:
                    code = e.response.status_code
                    err_msg = f"http-{code}"
                    if code == 404:
                        n_404 += 1
                        break
                    if attempt < 3:
                        time.sleep(delay)
                except (httpx.HTTPError, json.JSONDecodeError) as e:
                    err_msg = type(e).__name__
                    if attempt < 3:
                        time.sleep(delay)

            if info is None:
                n_errors += 1
                log_error(slug, f"imageinfo-failed:{filename}:{err_msg}")
            else:
                if not info.is_free:
                    n_skipped_nonfree += 1
                    nonfree_set.add(slug)
                    log_error(slug, f"non-free:{info.license_code}")
                else:
                    # Download
                    raw: bytes | None = None
                    dl_err = ""
                    for attempt, delay in enumerate((1, 4, 16), start=1):
                        try:
                            raw = download_bytes(client, info.thumb_url)
                            break
                        except httpx.HTTPStatusError as e:
                            dl_err = f"http-{e.response.status_code}"
                            if e.response.status_code == 404:
                                n_404 += 1
                                break
                            if attempt < 3:
                                time.sleep(delay)
                        except httpx.HTTPError as e:
                            dl_err = type(e).__name__
                            if attempt < 3:
                                time.sleep(delay)

                    if raw is None:
                        n_errors += 1
                        log_error(slug, f"download-failed:{info.thumb_url}:{dl_err}")
                    else:
                        processed = process_image(raw, info.mime)
                        if processed is None:
                            n_errors += 1
                            log_error(slug, f"image-decode-failed:{info.mime}")
                        else:
                            data, ext, w, h = processed
                            # supprimer d'éventuelles versions précédentes avec un autre ext
                            for old in PHOTOS_DIR.glob(f"{slug}.*"):
                                if old.suffix.lower() != f".{ext}":
                                    old.unlink()
                            out_path = PHOTOS_DIR / f"{slug}.{ext}"
                            out_path.write_bytes(data)

                            if not no_yaml_update:
                                update_eveque_yaml(
                                    slug=slug,
                                    fichier=f"{slug}.{ext}",
                                    source_url=info.descriptionurl,
                                    licence=info.license_code,
                                    auteur=info.author,
                                    largeur=w,
                                    hauteur=h,
                                )
                            license_counter[info.license_code] += 1
                            done_set.add(slug)
                            n_downloaded += 1

            # checkpoint
            if (idx + 1) % PROGRESS_FLUSH_EVERY == 0:
                state["done_slugs"] = sorted(done_set)
                state["skipped_nonfree"] = sorted(nonfree_set)
                state["last_index"] = idx + 1
                save_progress(state)
                print(
                    f"[phase-7] {idx + 1}/{len(queue)} | dl={n_downloaded} "
                    f"non-free={n_skipped_nonfree} 404={n_404} err={n_errors}",
                    flush=True,
                )

    # flush final
    state["done_slugs"] = sorted(done_set)
    state["skipped_nonfree"] = sorted(nonfree_set)
    state["last_index"] = state.get("last_index", 0)
    save_progress(state)

    # licences existantes accumulées
    prev_licences: dict[str, int] = {}
    if STATS_PATH.exists():
        with STATS_PATH.open("r", encoding="utf-8") as f:
            prev_licences = (json.load(f) or {}).get("photos_par_licence", {}) or {}
    merged = Counter(prev_licences)
    merged.update(license_counter)

    update_stats(
        {
            "photos_telechargees": len(list(PHOTOS_DIR.glob("*.*"))),
            "photos_par_licence": dict(merged.most_common()),
            "photos_taille_totale_mb": compute_photos_size_mb(),
            "photos_echecs_log": str(ERROR_LOG.relative_to(ROOT)),
            "photos_skipped_nonfree": len(nonfree_set),
        }
    )

    print(
        "\n[phase-7] terminé."
        f"\n  téléchargées (session)   : {n_downloaded}"
        f"\n  skip déjà locales        : {n_skipped_local}"
        f"\n  skip non-libre           : {n_skipped_nonfree}"
        f"\n  404                      : {n_404}"
        f"\n  autres erreurs           : {n_errors}"
        f"\n  sans filename extractable: {n_no_filename}"
        f"\n  taille totale photos/    : {compute_photos_size_mb()} MB"
        f"\n  log d'erreurs            : {ERROR_LOG}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Traite au plus N images (debug).",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retraite uniquement les slugs présents dans photo_errors.log.",
    )
    parser.add_argument(
        "--no-yaml-update",
        action="store_true",
        help="Télécharge sans modifier les YAML évêques.",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    run(
        limit=args.limit,
        retry_failed=args.retry_failed,
        no_yaml_update=args.no_yaml_update,
    )


if __name__ == "__main__":
    main()
