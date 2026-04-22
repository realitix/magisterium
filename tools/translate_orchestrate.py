"""Orchestrateur de batch pour le skill `translate-corpus`.

Gère un state file `/tmp/translate_state.json` qui liste les unités de
travail (full doc ou chunk), leur statut, leur prompt, leur output_path.

Le workflow de l'orchestrateur haut niveau (Claude lui-même, via Agent
opus) est :

1. `--init <jobs.jsonl>` : charge tous les jobs, crée les sous-unités
   "chunk" pour les gros docs (découpe sur les frontières `##`).
2. `--next-batch N` : sort N prochaines unités prêtes (statut `todo`),
   les passe en `running`. Sortie JSON (une ligne par unité) avec le
   prompt complet et le fichier d'écriture.
3. `--complete <unit_id>` : marque une unité terminée ; si c'est un chunk
   et que tous les chunks du (slug, lang) sont `done`, assemble la
   traduction finale à `<target_path>`, puis appelle `translate_apply` ;
   pour un full, applique direct.
4. `--fail <unit_id>` : repasse l'unité en `todo` (incrément retry).
5. `--status` : stats globales sur stderr.

State file = JSON avec champs :

    {
      "jobs": [ { job: {...src JSONL...}, units: [ {unit_id, status,
        kind: full|chunk, chunk_idx?, chunk_total?, body, output_path},
        ... ] }, ... ]
    }
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"
SKILL_DIR = ROOT / ".claude" / "skills" / "translate-corpus"
GLOSSAIRES_DIR = SKILL_DIR / "glossaires"
STATE_PATH = Path("/tmp/translate_state.json")
CHUNK_OUT_DIR = Path("/tmp/translate_chunks")
MODEL_NAME = "claude-opus-4-7"

# Tokens approximés par chunk max. On vise ~10_000 tokens / chunk pour rester
# rapide et stable en opus.
CHUNK_TARGET_TOKENS = 10_000
CHUNK_TARGET_CHARS = CHUNK_TARGET_TOKENS * 4  # ~4 bytes/token en UTF-8 latin

LANG_NAMES = {
    "la": "latin",
    "fr": "français",
    "en": "anglais",
    "it": "italien",
    "de": "allemand",
    "es": "espagnol",
    "pt": "portugais",
    "pl": "polonais",
    "grc": "grec ancien",
}


def _read_state() -> dict:
    if not STATE_PATH.exists():
        return {"jobs": []}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def _write_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    tmp.replace(STATE_PATH)


def _split_markdown_on_h2(text: str, target_chars: int) -> list[str]:
    """Découpe sur les frontières `## ` quand le bloc dépasse target_chars.

    Stratégie simple : itère les lignes, commence un nouveau chunk quand on
    voit un `## ` ET que le buffer courant dépasse target_chars.
    Les `# ` de niveau 1 (titre du doc) restent dans le premier chunk.
    """
    lines = text.splitlines(keepends=True)
    chunks: list[list[str]] = [[]]
    current_len = 0
    for line in lines:
        is_h2 = bool(re.match(r"^## [^#]", line)) or line.startswith("## ")
        if is_h2 and current_len >= target_chars and chunks[-1]:
            chunks.append([])
            current_len = 0
        chunks[-1].append(line)
        current_len += len(line)
    out = ["".join(c) for c in chunks if c]
    # Si un seul chunk ressort alors qu'on visait du découpage, fallback :
    # découpe brutale par paragraphes (blank lines).
    if len(out) == 1 and len(out[0]) > target_chars * 2:
        return _split_by_paragraphs(out[0], target_chars)
    return out


def _split_by_paragraphs(text: str, target_chars: int) -> list[str]:
    paras = re.split(r"(\n\s*\n)", text)
    chunks: list[list[str]] = [[]]
    cur = 0
    for p in paras:
        if cur >= target_chars and chunks[-1]:
            chunks.append([])
            cur = 0
        chunks[-1].append(p)
        cur += len(p)
    return ["".join(c) for c in chunks if c]


def _glossaire(lang: str) -> str:
    f = GLOSSAIRES_DIR / f"{lang}.md"
    if not f.exists():
        return "(Pas de glossaire spécifique. Applique les conventions théologiques catholiques standard.)"
    return f.read_text(encoding="utf-8")


def _build_prompt(
    source_lang: str,
    target_lang: str,
    body: str,
    output_path: str,
    unit_id: str,
    chunk_info: str | None = None,
) -> str:
    src_name = LANG_NAMES.get(source_lang, source_lang)
    tgt_name = LANG_NAMES.get(target_lang, target_lang)
    glossaire = _glossaire(target_lang)
    chunk_block = ""
    if chunk_info:
        chunk_block = f"\n\nNOTE DÉCOUPE : {chunk_info}\n"
    return f"""Tu es un théologien catholique traducteur. Traduis le markdown ci-dessous depuis le {src_name} vers la langue cible : {tgt_name}.

CONSIGNES STRICTES (non négociables) :

1. Conserve EXACTEMENT la structure markdown : titres (# ##), listes, blocs de citation (>), emphase (* _), liens, code inline.
2. Préserve la numérotation interne des documents (canons, articles, paragraphes) sans la renuméroter.
3. Ne traduis PAS les références scripturaires elles-mêmes (« Mt 5, 3 » reste « Mt 5, 3 »), mais traduis le texte cité.
4. Respecte impérativement le glossaire théologique fourni ci-dessous.
5. Pour les termes latins intraduisibles (motu proprio, ex cathedra, Novus Ordo, subsistit in, anathema sit…), utilise la forme latine et ajoute une traduction entre parenthèses en première occurrence seulement, puis garde le latin seul.
6. Ne paraphrase pas. Ne reformule pas les canons dogmatiques. Traduction littérale privilégiée.
7. Ne modernise pas les formulations dogmatiques. « Qu'il soit anathème » ne devient pas « qu'il soit exclu » ou « qu'il soit rejeté ».
8. Si tu rencontres un passage ambigu, traduis littéralement sans deviner.

PROCÉDURE DE LIVRAISON :

- N'inclus PAS la traduction dans ta réponse texte.
- Écris la traduction via ton outil Write à ce chemin EXACT :
    {output_path}
- Le fichier doit contenir UNIQUEMENT le markdown traduit, sans préambule, sans commentaire.
- Après écriture, réponds seulement `OK {unit_id}` (une ligne, rien d'autre).

GLOSSAIRE IMPOSÉ :

{glossaire}
{chunk_block}
MARKDOWN SOURCE :

{body}
"""


def cmd_init(jobs_jsonl: Path) -> None:
    CHUNK_OUT_DIR.mkdir(parents=True, exist_ok=True)
    jobs_data = []
    for line in jobs_jsonl.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        job = json.loads(line)
        source_path = Path(job["source_path"])
        body = source_path.read_text(encoding="utf-8")
        slug = job["slug"]
        lang = job["lang_target"]
        units: list[dict] = []
        if job["needs_chunking"]:
            pieces = _split_markdown_on_h2(body, CHUNK_TARGET_CHARS)
            total = len(pieces)
            for idx, piece in enumerate(pieces):
                unit_id = f"{slug}::{lang}::chunk-{idx+1:02d}of{total:02d}"
                out_path = CHUNK_OUT_DIR / f"{slug}.{lang}.chunk-{idx+1:02d}of{total:02d}.md"
                units.append({
                    "unit_id": unit_id,
                    "kind": "chunk",
                    "chunk_idx": idx + 1,
                    "chunk_total": total,
                    "body": piece,
                    "output_path": str(out_path),
                    "status": "todo",
                    "retries": 0,
                })
        else:
            unit_id = f"{slug}::{lang}::full"
            units.append({
                "unit_id": unit_id,
                "kind": "full",
                "body": body,
                "output_path": job["target_path"],
                "status": "todo",
                "retries": 0,
            })
        jobs_data.append({"job": job, "units": units})
    state = {"jobs": jobs_data}
    _write_state(state)
    _print_status(state)


def _print_status(state: dict) -> None:
    counts = {"todo": 0, "running": 0, "done": 0, "failed": 0}
    total = 0
    doc_done = 0
    doc_total = len(state["jobs"])
    for jd in state["jobs"]:
        all_done = True
        for u in jd["units"]:
            counts[u["status"]] = counts.get(u["status"], 0) + 1
            total += 1
            if u["status"] != "done":
                all_done = False
        if all_done:
            doc_done += 1
    print(
        f"[orchestrate] units: {counts} / total {total}  "
        f"| docs complets: {doc_done}/{doc_total}",
        file=sys.stderr,
    )


def _iter_units(state: dict):
    for jd in state["jobs"]:
        for u in jd["units"]:
            yield jd, u


def cmd_next_batch(n: int) -> None:
    state = _read_state()
    taken: list[tuple[dict, dict]] = []
    for jd, u in _iter_units(state):
        if len(taken) >= n:
            break
        if u["status"] == "todo":
            u["status"] = "running"
            taken.append((jd, u))
    _write_state(state)
    for jd, u in taken:
        prompt = _build_prompt(
            source_lang=jd["job"]["source_lang"],
            target_lang=jd["job"]["lang_target"],
            body=u["body"],
            output_path=u["output_path"],
            unit_id=u["unit_id"],
            chunk_info=(
                f"Ce document est découpé en {u['chunk_total']} chunks. "
                f"Tu traduis le chunk {u['chunk_idx']}/{u['chunk_total']}. "
                "Ne commence pas par un texte d'introduction et ne conclus pas artificiellement : "
                "le chunk commence et finit au milieu d'un document plus grand (sauf si tu es le premier ou le dernier)."
                if u["kind"] == "chunk"
                else None
            ),
        )
        out = {
            "unit_id": u["unit_id"],
            "slug": jd["job"]["slug"],
            "lang_target": jd["job"]["lang_target"],
            "source_lang": jd["job"]["source_lang"],
            "output_path": u["output_path"],
            "kind": u["kind"],
            "prompt": prompt,
        }
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _apply_translation(job: dict) -> None:
    """Appelle tools.translate_apply pour persister la traduction."""
    subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-m",
            "tools.translate_apply",
            "--meta-path",
            job["meta_path"],
            "--lang",
            job["lang_target"],
            "--source-lang",
            job["source_lang"],
            "--source-sha256",
            job["source_sha256"],
            "--model",
            MODEL_NAME,
        ],
        check=True,
        cwd=ROOT,
    )


def _maybe_assemble(jd: dict) -> bool:
    """Si tous les chunks du job sont done, concatène dans target_path et apply.

    Retourne True si un assemblage + apply a été fait.
    """
    if jd["units"][0]["kind"] != "chunk":
        return False
    if any(u["status"] != "done" for u in jd["units"]):
        return False
    # Concatène les chunks dans l'ordre chunk_idx.
    ordered = sorted(jd["units"], key=lambda u: u["chunk_idx"])
    merged = "".join(Path(u["output_path"]).read_text(encoding="utf-8") for u in ordered)
    # Ajoute un saut de ligne terminal propre si absent.
    if not merged.endswith("\n"):
        merged += "\n"
    target = Path(jd["job"]["target_path"])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(merged, encoding="utf-8")
    _apply_translation(jd["job"])
    # cleanup temp chunks
    for u in ordered:
        try:
            Path(u["output_path"]).unlink()
        except FileNotFoundError:
            pass
    return True


def cmd_complete(unit_ids: list[str]) -> None:
    state = _read_state()
    touched_jobs: set[int] = set()
    id_to_jd = {}
    for idx, jd in enumerate(state["jobs"]):
        for u in jd["units"]:
            id_to_jd[u["unit_id"]] = (idx, u)
    for unit_id in unit_ids:
        if unit_id not in id_to_jd:
            print(f"[orchestrate] unknown unit_id: {unit_id}", file=sys.stderr)
            continue
        idx, u = id_to_jd[unit_id]
        if not Path(u["output_path"]).exists():
            print(
                f"[orchestrate] output_path absent pour {unit_id} : {u['output_path']}",
                file=sys.stderr,
            )
            continue
        # Garde-fou : n'accepte que des markdowns non vides.
        if Path(u["output_path"]).stat().st_size < 10:
            print(
                f"[orchestrate] output suspect (trop petit) pour {unit_id}",
                file=sys.stderr,
            )
            continue
        u["status"] = "done"
        touched_jobs.add(idx)
    # Pour les docs non-chunked, apply tout de suite. Pour chunked, tente
    # l'assemblage.
    for idx in touched_jobs:
        jd = state["jobs"][idx]
        if jd["units"][0]["kind"] == "full":
            u = jd["units"][0]
            if u["status"] == "done":
                _apply_translation(jd["job"])
        else:
            _maybe_assemble(jd)
    _write_state(state)
    _print_status(state)


def cmd_fail(unit_ids: list[str]) -> None:
    state = _read_state()
    for jd, u in _iter_units(state):
        if u["unit_id"] in unit_ids:
            u["status"] = "todo"
            u["retries"] = u.get("retries", 0) + 1
    _write_state(state)
    _print_status(state)


def cmd_status() -> None:
    _print_status(_read_state())


def cmd_reset_running() -> None:
    """Remet toutes les unités `running` en `todo` (utile après crash)."""
    state = _read_state()
    n = 0
    for jd, u in _iter_units(state):
        if u["status"] == "running":
            u["status"] = "todo"
            n += 1
    _write_state(state)
    print(f"[orchestrate] reset {n} running → todo", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)

    p_init = sp.add_parser("init")
    p_init.add_argument("jobs_jsonl")

    p_next = sp.add_parser("next-batch")
    p_next.add_argument("n", type=int)

    p_done = sp.add_parser("complete")
    p_done.add_argument("unit_ids", nargs="+")

    p_fail = sp.add_parser("fail")
    p_fail.add_argument("unit_ids", nargs="+")

    sp.add_parser("status")
    sp.add_parser("reset-running")

    args = p.parse_args()
    if args.cmd == "init":
        cmd_init(Path(args.jobs_jsonl))
    elif args.cmd == "next-batch":
        cmd_next_batch(args.n)
    elif args.cmd == "complete":
        cmd_complete(args.unit_ids)
    elif args.cmd == "fail":
        cmd_fail(args.unit_ids)
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "reset-running":
        cmd_reset_running()
    return 0


if __name__ == "__main__":
    sys.exit(main())
