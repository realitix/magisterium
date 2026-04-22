"""Helper : pull un batch via l'orchestrateur, dump les prompts sur disque,
et imprime sur stdout UNE ligne par unité au format :

    <unit_id>|<prompt_file_path>

Aucune sortie volumineuse, juste des paths. L'agent orchestrateur (Claude)
ne touche jamais au contenu des prompts — il les passe aux sous-agents qui
les lisent eux-mêmes via Read.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PROMPT_DIR = Path("/tmp/translate_prompts")


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    PROMPT_DIR.mkdir(parents=True, exist_ok=True)
    # Appelle l'orchestrateur pour récupérer les N prochaines unités.
    r = subprocess.run(
        ["uv", "run", "python", "-m", "tools.translate_orchestrate", "next-batch", str(n)],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in r.stdout.splitlines():
        if not line.strip():
            continue
        j = json.loads(line)
        unit_id = j["unit_id"]
        # Sanitize unit_id pour nom de fichier
        safe = unit_id.replace("::", "__").replace("/", "_")
        pf = PROMPT_DIR / f"{safe}.prompt.txt"
        pf.write_text(j["prompt"], encoding="utf-8")
        print(f"{unit_id}|{pf}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
