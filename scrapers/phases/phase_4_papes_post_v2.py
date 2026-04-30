"""Phase 4 — Solemn magisterial documents of post-Vatican II popes (1958 →).

Scope (priority 1): Encyclicals of all popes from John XXIII to today.
Priority 2+ (apost_exhortations, apost_constitutions, motu_proprio,
apost_letters) is available via env var PHASE4_EXTRA=1 — for this pass we
focus on encyclicals and include Leo XIV's single apostolic exhortation as
an explicit item since he has no encyclicals yet.

Language strategy: for each document we prefer /la/ (Latin editio typica)
when the page exists, then /it/, then /fr/. The *first* 200-OK URL wins and
its language is recorded in meta.langue_originale and langues_disponibles.

The list of encyclicals is hand-curated (from vatican.va indexes) rather
than re-scraped live, so the phase is deterministic and re-runs the same
way. Sources:
  - https://www.vatican.va/content/{slug}/la/encyclicals/index.html
  - /it/encyclicals/index.html for John-Paul II (Redemptoris Mater is only
    listed in the /it/ index but the /la/ document exists).
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

BASE = "https://www.vatican.va/content"

# --- Data model --------------------------------------------------------------

# (pope_slug, date_iso, incipit, titre_fr, filename_in_documents, [subjects])
# filename_in_documents is the final path component without .html, e.g.
#   "hf_j-xxiii_enc_11041963_pacem" or "papa-francesco_20201003_enciclica-fratelli-tutti"
# URL pattern: {BASE}/{pope_slug}/{lang}/encyclicals/documents/{filename}.html
EncRow = tuple[str, str, str, str | None, str, list[str]]

ENCYCLICALS: list[EncRow] = [
    # --- John XXIII (1958-1963), 8 encyclicals ---
    ("john-xxiii", "1959-06-29", "Ad Petri cathedram",
     "Sur la vérité, l'unité et la paix, dans un esprit de charité",
     "hf_j-xxiii_enc_29061959_ad-petri", ["unite", "paix"]),
    ("john-xxiii", "1959-08-01", "Sacerdotii Nostri Primordia",
     "Sur saint Jean-Marie Vianney", "hf_j-xxiii_enc_19590801_sacerdotii",
     ["sacerdoce"]),
    ("john-xxiii", "1959-09-26", "Grata Recordatio",
     "Sur le rosaire", "hf_j-xxiii_enc_26091959_grata-recordatio",
     ["rosaire"]),
    ("john-xxiii", "1959-11-28", "Princeps Pastorum",
     "Sur les missions", "hf_j-xxiii_enc_28111959_princeps",
     ["mission"]),
    ("john-xxiii", "1961-05-15", "Mater et Magistra",
     "Sur les récents développements de la question sociale",
     "hf_j-xxiii_enc_15051961_mater", ["doctrine-sociale"]),
    ("john-xxiii", "1961-11-11", "Aeterna Dei Sapientia",
     "À l'occasion du XVe centenaire de la mort de saint Léon le Grand",
     "hf_j-xxiii_enc_11111961_aeterna-dei", ["unite-eglise"]),
    ("john-xxiii", "1962-07-01", "Paenitentiam Agere",
     "Sur la pratique de la pénitence intérieure et extérieure",
     "hf_j-xxiii_enc_01071962_paenitentiam", ["penitence"]),
    ("john-xxiii", "1963-04-11", "Pacem in Terris",
     "Sur la paix entre toutes les nations, dans la vérité, la justice, la charité et la liberté",
     "hf_j-xxiii_enc_11041963_pacem", ["paix", "doctrine-sociale"]),

    # --- Paul VI (1963-1978), 7 encyclicals ---
    ("paul-vi", "1964-08-06", "Ecclesiam Suam",
     "Sur les chemins de l'Église", "hf_p-vi_enc_06081964_ecclesiam",
     ["ecclesiologie", "dialogue"]),
    ("paul-vi", "1965-04-29", "Mense Maio",
     "Au mois de mai, pour prier pour la paix et le concile",
     "hf_p-vi_enc_29041965_mense-maio", ["priere", "marie"]),
    ("paul-vi", "1965-09-03", "Mysterium Fidei",
     "Sur la doctrine et le culte de la sainte Eucharistie",
     "hf_p-vi_enc_03091965_mysterium", ["eucharistie"]),
    ("paul-vi", "1966-09-15", "Christi Matri",
     "Pour la paix, invoquant l'intercession de Marie",
     "hf_p-vi_enc_15091966_christi-matri", ["paix", "marie"]),
    ("paul-vi", "1967-03-26", "Populorum Progressio",
     "Sur le développement des peuples",
     "hf_p-vi_enc_26031967_populorum", ["doctrine-sociale", "developpement"]),
    ("paul-vi", "1967-06-24", "Sacerdotalis Caelibatus",
     "Sur le célibat sacerdotal",
     "hf_p-vi_enc_24061967_sacerdotalis", ["sacerdoce", "celibat"]),
    ("paul-vi", "1968-07-25", "Humanae Vitae",
     "Sur le mariage et la régulation des naissances",
     "hf_p-vi_enc_25071968_humanae-vitae", ["mariage", "sexualite", "bioethique"]),

    # --- John Paul I (1978) : aucune encyclique (33 jours) ---

    # --- John Paul II (1978-2005), 14 encyclicals ---
    ("john-paul-ii", "1979-03-04", "Redemptor Hominis",
     "Sur le Christ Rédempteur de l'homme",
     "hf_jp-ii_enc_04031979_redemptor-hominis", ["christologie", "anthropologie"]),
    ("john-paul-ii", "1980-11-30", "Dives in Misericordia",
     "Sur la miséricorde divine",
     "hf_jp-ii_enc_30111980_dives-in-misericordia", ["misericorde"]),
    ("john-paul-ii", "1981-09-14", "Laborem Exercens",
     "Sur le travail humain",
     "hf_jp-ii_enc_14091981_laborem-exercens", ["doctrine-sociale", "travail"]),
    ("john-paul-ii", "1985-06-02", "Slavorum Apostoli",
     "Pour le XIe centenaire de l'œuvre évangélisatrice de Cyrille et Méthode",
     "hf_jp-ii_enc_19850602_slavorum-apostoli", ["mission", "saints"]),
    ("john-paul-ii", "1986-05-18", "Dominum et Vivificantem",
     "Sur l'Esprit Saint dans la vie de l'Église et du monde",
     "hf_jp-ii_enc_18051986_dominum-et-vivificantem", ["esprit-saint", "trinite"]),
    ("john-paul-ii", "1987-03-25", "Redemptoris Mater",
     "Sur la bienheureuse Vierge Marie dans la vie de l'Église en marche",
     "hf_jp-ii_enc_25031987_redemptoris-mater", ["marie", "mariologie"]),
    ("john-paul-ii", "1987-12-30", "Sollicitudo Rei Socialis",
     "Pour le XXe anniversaire de Populorum Progressio",
     "hf_jp-ii_enc_30121987_sollicitudo-rei-socialis", ["doctrine-sociale"]),
    ("john-paul-ii", "1990-12-07", "Redemptoris Missio",
     "Sur la valeur permanente du précepte missionnaire",
     "hf_jp-ii_enc_07121990_redemptoris-missio", ["mission"]),
    ("john-paul-ii", "1991-05-01", "Centesimus Annus",
     "Pour le centenaire de Rerum Novarum",
     "hf_jp-ii_enc_01051991_centesimus-annus", ["doctrine-sociale"]),
    ("john-paul-ii", "1993-08-06", "Veritatis Splendor",
     "Sur quelques questions fondamentales de l'enseignement moral de l'Église",
     "hf_jp-ii_enc_06081993_veritatis-splendor", ["morale", "theologie-morale"]),
    ("john-paul-ii", "1995-03-25", "Evangelium Vitae",
     "Sur la valeur et l'inviolabilité de la vie humaine",
     "hf_jp-ii_enc_25031995_evangelium-vitae", ["bioethique", "vie", "morale"]),
    ("john-paul-ii", "1995-05-25", "Ut Unum Sint",
     "Sur l'engagement œcuménique",
     "hf_jp-ii_enc_25051995_ut-unum-sint", ["oecumenisme"]),
    ("john-paul-ii", "1998-09-14", "Fides et Ratio",
     "Sur les rapports entre la foi et la raison",
     "hf_jp-ii_enc_14091998_fides-et-ratio", ["foi-et-raison", "philosophie"]),
    ("john-paul-ii", "2003-04-17", "Ecclesia de Eucharistia",
     "Sur l'Eucharistie dans son rapport à l'Église",
     "hf_jp-ii_enc_20030417_eccl-de-euch", ["eucharistie", "ecclesiologie"]),

    # --- Benedict XVI (2005-2013), 3 encyclicals ---
    ("benedict-xvi", "2005-12-25", "Deus Caritas Est",
     "Sur l'amour chrétien",
     "hf_ben-xvi_enc_20051225_deus-caritas-est", ["charite", "amour"]),
    ("benedict-xvi", "2007-11-30", "Spe Salvi",
     "Sur l'espérance chrétienne",
     "hf_ben-xvi_enc_20071130_spe-salvi", ["esperance"]),
    ("benedict-xvi", "2009-06-29", "Caritas in Veritate",
     "Sur le développement humain intégral dans la charité et la vérité",
     "hf_ben-xvi_enc_20090629_caritas-in-veritate", ["doctrine-sociale", "charite"]),

    # --- Francis (2013-2025), 4 encyclicals ---
    # Note: filenames under Francis follow a different scheme (no "hf_" prefix).
    ("francesco", "2013-06-29", "Lumen Fidei",
     "Sur la foi (rédigée principalement par Benoît XVI)",
     "papa-francesco_20130629_enciclica-lumen-fidei", ["foi"]),
    ("francesco", "2015-05-24", "Laudato Si'",
     "Sur la sauvegarde de la maison commune",
     "papa-francesco_20150524_enciclica-laudato-si", ["ecologie", "doctrine-sociale"]),
    ("francesco", "2020-10-03", "Fratelli Tutti",
     "Sur la fraternité et l'amitié sociale",
     "papa-francesco_20201003_enciclica-fratelli-tutti", ["fraternite", "doctrine-sociale"]),
    ("francesco", "2024-10-24", "Dilexit Nos",
     "Sur l'amour humain et divin du Cœur de Jésus-Christ",
     "20241024-enciclica-dilexit-nos", ["coeur-de-jesus", "spiritualite"]),

    # --- Leo XIV (2025-) : aucune encyclique à ce jour (élu 8 mai 2025) ---
]

# --- Extra: Leo XIV's sole apostolic exhortation (high-profile solemn doc) ---
# (pope_slug, date_iso, incipit, titre_fr, filename, doc_type, [subjects])
EXTRA_SOLEMN: list[tuple[str, str, str, str | None, str, str, list[str]]] = [
    (
        "leo-xiv", "2025-10-04", "Dilexi Te",
        "Sur l'amour pour les pauvres",
        "20251004-dilexi-te", "exhortation-apostolique",
        ["pauvres", "doctrine-sociale"],
    ),
    (
        "paul-vi", "1968-06-18", "Pontificalis Romani",
        "Sur la révision du rite des ordinations du diacre, du prêtre et de l'évêque",
        "hf_p-vi_apc_19680618_pontificalis-romani", "constitution-apostolique",
        ["sacrements", "ordre", "matiere-forme", "ordinations",
         "rite-episcopal", "validite-sacramentelle", "reforme-liturgique"],
    ),
]


# --- Pope display names & directory slugs -----------------------------------

POPE_INFO: dict[str, tuple[str, str]] = {
    # vatican_slug -> (display_name, directory_slug)
    "john-xxiii":    ("Jean XXIII",     "1958-jean-xxiii"),
    "paul-vi":       ("Paul VI",        "1963-paul-vi"),
    "john-paul-i":   ("Jean-Paul Ier",  "1978-jean-paul-i"),
    "john-paul-ii":  ("Jean-Paul II",   "1978-jean-paul-ii"),
    "benedict-xvi":  ("Benoît XVI",     "2005-benoit-xvi"),
    "francesco":     ("François",       "2013-francois"),
    "leo-xiv":       ("Léon XIV",       "2025-leon-xiv"),
}

LANG_PREFERENCE = ["la", "it", "fr"]


# --- Language probe ---------------------------------------------------------

@dataclass
class ProbeResult:
    lang: str
    url: str


async def _probe_lang(
    client: httpx.AsyncClient, pope_slug: str, section: str,
    filename: str,
) -> ProbeResult | None:
    """HEAD-probe /la/, /it/, /fr/ for a given document; return first 200."""
    for lang in LANG_PREFERENCE:
        url = f"{BASE}/{pope_slug}/{lang}/{section}/documents/{filename}.html"
        try:
            r = await client.head(url, follow_redirects=True)
            if r.status_code < 400:
                return ProbeResult(lang=lang, url=url)
            # Some vatican.va paths 405 HEAD — fall back to GET
            if r.status_code in (403, 405):
                r2 = await client.get(url, follow_redirects=True)
                if r2.status_code < 400:
                    return ProbeResult(lang=lang, url=url)
        except httpx.HTTPError:
            continue
    return None


async def _resolve_all_langs(
    items: list[tuple[str, str, str]],  # (pope_slug, section, filename)
) -> dict[tuple[str, str], ProbeResult]:
    """Return mapping (pope_slug, filename) -> ProbeResult.

    A single client with a tiny concurrency budget to avoid hammering
    vatican.va (pipeline rate-limiter is per-request-through-fetcher, which
    probes don't use — so we self-throttle here).
    """
    sem = asyncio.Semaphore(4)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; MagisteriumArchiver/1.0; "
            "+https://github.com/realitix/catholique)"
        ),
        "Accept-Language": "la,it,fr,en;q=0.8",
    }
    async with httpx.AsyncClient(
        http2=True, timeout=30.0, headers=headers, follow_redirects=True,
    ) as client:
        async def one(pope_slug: str, section: str, filename: str):
            async with sem:
                return (pope_slug, filename), await _probe_lang(
                    client, pope_slug, section, filename,
                )
        results = await asyncio.gather(*(
            one(*t) for t in items
        ))
    return {k: v for k, v in results if v is not None}


# --- Build DocRefs ----------------------------------------------------------

def _slug_tail(filename: str) -> str:
    """Extract the incipit-slug portion from a vatican.va document filename.

    Examples:
      hf_j-xxiii_enc_11041963_pacem -> pacem
      hf_jp-ii_enc_04031979_redemptor-hominis -> redemptor-hominis
      papa-francesco_20201003_enciclica-fratelli-tutti -> fratelli-tutti
      20241024-enciclica-dilexit-nos -> dilexit-nos
    """
    # papa-francesco-style: ...enciclica-<slug>
    if "enciclica-" in filename:
        return filename.rsplit("enciclica-", 1)[1]
    # hf_-style : date is 8 digits, slug follows
    parts = filename.split("_")
    if len(parts) >= 4 and len(parts[-2]) == 8 and parts[-2].isdigit():
        return parts[-1]
    # Leading YYYYMMDD- prefix (Leo XIV, recent Francis)
    if len(filename) > 9 and filename[:8].isdigit() and filename[8] == "-":
        return filename[9:]
    # fallback: last _-separated chunk, or last -separated chunk
    if "_" in filename:
        return filename.split("_")[-1]
    return filename


async def build_refs() -> list[DocRef]:
    post_v2_root = MAGISTERIUM_ROOT / "C-post-vatican-ii" / "papes"

    # Collect probe list
    probe_list: list[tuple[str, str, str]] = []
    for pope_slug, _d, _i, _t, filename, _s in ENCYCLICALS:
        probe_list.append((pope_slug, "encyclicals", filename))
    # Map doc_type to its vatican.va section subpath
    section_for = {
        "exhortation-apostolique": "apost_exhortations",
        "constitution-apostolique": "apost_constitutions",
        "motu-proprio": "motu_proprio",
        "lettre-apostolique": "apost_letters",
    }
    for pope_slug, _d, _i, _t, filename, doc_type, _s in EXTRA_SOLEMN:
        probe_list.append((pope_slug, section_for[doc_type], filename))

    probed = await _resolve_all_langs(probe_list)

    refs: list[DocRef] = []

    # Encyclicals
    for pope_slug, date_iso, incipit, titre_fr, filename, sujets in ENCYCLICALS:
        pope_name, pope_dir = POPE_INFO[pope_slug]
        pr = probed.get((pope_slug, filename))
        if pr is None:
            # Fallback to Latin URL even if probe failed; pipeline will log.
            lang = "la"
            url = f"{BASE}/{pope_slug}/la/encyclicals/documents/{filename}.html"
        else:
            lang = pr.lang
            url = pr.url

        incipit_slug = _slug_tail(filename)
        slug = f"{date_iso}_{incipit_slug}_encyclique"

        refs.append(DocRef(
            url=url,
            target_dir=post_v2_root / pope_dir / "encycliques",
            slug=slug,
            lang=lang,
            meta_hints={
                "incipit": incipit,
                "titre_fr": titre_fr,
                "auteur": pope_name,
                "periode": "post-vatican-ii",
                "type": "encyclique",
                "date": date.fromisoformat(date_iso),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": lang,
                "langues_disponibles": [lang],
                "sujets": sujets,
            },
        ))

    # Extra solemn (Leo XIV exhortation etc.)
    for pope_slug, date_iso, incipit, titre_fr, filename, doc_type, sujets in EXTRA_SOLEMN:
        pope_name, pope_dir = POPE_INFO[pope_slug]
        pr = probed.get((pope_slug, filename))
        if pr is None:
            lang = "la"
            section = section_for[doc_type]
            url = (
                f"{BASE}/{pope_slug}/la/{section}/documents/"
                f"{filename}.html"
            )
        else:
            lang = pr.lang
            url = pr.url

        incipit_slug = _slug_tail(filename)
        type_short = {
            "exhortation-apostolique": "exhortation",
            "constitution-apostolique": "const-apost",
            "motu-proprio": "motu-proprio",
            "lettre-apostolique": "lettre-apost",
        }.get(doc_type, doc_type)
        slug = f"{date_iso}_{incipit_slug}_{type_short}"

        type_dir = {
            "exhortation-apostolique": "exhortations-apostoliques",
            "constitution-apostolique": "constitutions-apostoliques",
            "motu-proprio": "motu-proprio",
            "lettre-apostolique": "lettres-apostoliques",
        }.get(doc_type, doc_type)
        refs.append(DocRef(
            url=url,
            target_dir=post_v2_root / pope_dir / type_dir,
            slug=slug,
            lang=lang,
            meta_hints={
                "incipit": incipit,
                "titre_fr": titre_fr,
                "auteur": pope_name,
                "periode": "post-vatican-ii",
                "type": doc_type,
                "date": date.fromisoformat(date_iso),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": lang,
                "langues_disponibles": [lang],
                "sujets": sujets,
            },
        ))

    return refs


async def main() -> int:
    import os
    refresh = os.environ.get("REFRESH") == "1"
    refs = await build_refs()
    print(f"Phase 4 — {len(refs)} documents magistériels post-Vatican II")
    # Log language distribution
    by_lang: dict[str, int] = {}
    for r in refs:
        by_lang[r.lang] = by_lang.get(r.lang, 0) + 1
    print(f"  langues résolues: {by_lang}")

    result = await run_pipeline(refs, phase="phase-4-post-v2", refresh=refresh)
    print(
        f"ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
