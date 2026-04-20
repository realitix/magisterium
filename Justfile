default:
    @just --list

setup:
    uv sync
    @command -v pandoc >/dev/null || (echo "pandoc missing" && exit 1)

test-connectivity:
    uv run python -m scrapers.phases.phase_0_5_connectivity

phase-1:
    uv run python -m scrapers.phases.phase_1_conciles

phase-2:
    uv run python -m scrapers.phases.phase_2_vatican_ii

phase-3:
    uv run python -m scrapers.phases.phase_3_papes_pre_v2

phase-4:
    uv run python -m scrapers.phases.phase_4_papes_post_v2

phase-9:
    uv run python -m scrapers.phases.phase_9_fsspx

resume:
    uv run python -m scrapers.cli resume

refresh SOURCE:
    uv run python -m scrapers.cli refresh {{SOURCE}}

build-index:
    uv run python -m tools.build_index

build-concordance:
    uv run python -m tools.build_concordance

validate:
    uv run python -m tools.validate

stats:
    uv run python -m tools.stats
