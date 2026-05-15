#!/usr/bin/env bash
# Post-hook : attend la fin du scrape catholic-hierarchy complet, puis
# enchaîne réconciliation (phase 4) + annotation rite (phase 5) + rebuild site.
#
# Lancement détaché :
#   nohup bash tools/clerge_post_ch_run.sh > /tmp/clerge-post-ch.log 2>&1 &
#   disown
#
# Idempotent : peut être relancé sans risque.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

log() {
    printf '[post-ch %s] %s\n' "$(date -u +%FT%TZ)" "$*"
}

# 1. Attendre la fin du process catholic-hierarchy (s'il tourne encore)
if pgrep -f "scrapers.clerge.sources.catholic_hierarchy" >/dev/null; then
    log "scrape CH encore en cours, attente…"
    while pgrep -f "scrapers.clerge.sources.catholic_hierarchy" >/dev/null; do
        sleep 60
    done
    log "scrape CH terminé."
else
    log "scrape CH déjà terminé."
fi

# Vérification : combien de fiches CH récupérées ?
CH_LINES=$(wc -l < clerge/_raw/catholic_hierarchy.jsonl)
log "catholic_hierarchy.jsonl : ${CH_LINES} fiches"

# 2. Phase 4 — réconciliation
log "phase 4 — réconciliation des 3 sources"
uv run python -m tools.clerge_reconcile 2>&1 | tee -a /tmp/clerge-post-ch.log | tail -5
log "phase 4 OK"

# 3. Phase 5 — annotation rite + lignées
log "phase 5 — annotation rite"
uv run python -m tools.clerge_annotate_rite 2>&1 | tee -a /tmp/clerge-post-ch.log | tail -5
log "phase 5 OK"

# 3bis. Finalisation prêtres (résout ordinateurs, calcule tampons, met à jour clerics.jsonl)
log "finalisation prêtres"
uv run python -m tools.clerge_finalize_pretres 2>&1 | tee -a /tmp/clerge-post-ch.log | tail -5
log "finalisation prêtres OK"

# 4. Rebuild site
log "rebuild site Astro"
cd site
npm run build 2>&1 | tee -a /tmp/clerge-post-ch.log | tail -5
cd "$REPO"
log "rebuild OK"

# 5. Stats finales
log "stats finales:"
if [[ -f clerge/_metadata/stats.json ]]; then
    python3 -c "
import json
s = json.load(open('clerge/_metadata/stats.json'))
print(f\"  total_eveques: {s.get('total_eveques', '?')}\")
print(f\"  par_tampon:    {s.get('par_tampon', {})}\")
print(f\"  par_rite:      {s.get('par_rite', {})}\")
print(f\"  avec_photo:    {s.get('avec_photo', '?')}\")
"
fi

log "POST-HOOK TERMINÉ"
