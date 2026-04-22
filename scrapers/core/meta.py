"""Pydantic model for per-document .meta.yaml sidecar."""
from datetime import date as _date, datetime as _datetime
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field

Period = Literal["pre-vatican-ii", "vatican-ii", "post-vatican-ii", "fsspx"]
TraductionKind = Literal["originale", "officielle", "ia"]


class Source(BaseModel):
    url: str
    site: str
    langue: str
    fetch_method: str


class Traduction(BaseModel):
    """Per-language provenance record.

    `kind` distinguishes the three statuses a text can have:
      * `originale`  — langue source du document (ce que l'auteur a écrit).
      * `officielle` — traduction publiée par la source faisant autorité
                        (Saint-Siège, dicastère, congrégation, éditeur officiel).
      * `ia`         — traduction générée automatiquement par un modèle IA,
                        à partir de l'originale (ou, à défaut, d'une officielle).

    For `originale` / `officielle` : `source_url`, `fetched_at`, `sha256` sont
    obligatoires.
    For `ia` : `model`, `translated_from`, `source_sha256`, `translated_at`,
    `sha256` sont obligatoires.
    """

    kind: TraductionKind

    # Champs communs
    sha256: str

    # Champs des traductions scrapées (originale / officielle)
    source_url: Optional[str] = None
    fetched_at: Optional[_datetime] = None
    fetch_method: Optional[str] = None

    # Champs des traductions IA
    model: Optional[str] = None
    translated_from: Optional[str] = None  # code langue de la source utilisée
    source_sha256: Optional[str] = None    # sha256 de la source au moment de la traduction
    translated_at: Optional[_datetime] = None


class DocMeta(BaseModel):
    incipit: str
    titre_fr: Optional[str] = None
    titre_original: Optional[str] = None
    auteur: str
    periode: Period
    type: str
    date: Optional[_date] = None
    autorite_magisterielle: Optional[str] = None
    langue_originale: Optional[str] = None
    denzinger: list[str] = Field(default_factory=list)
    sujets: list[str] = Field(default_factory=list)
    themes_doctrinaux: list[str] = Field(default_factory=list)
    references_anterieures: list[str] = Field(default_factory=list)
    references_posterieures: list[str] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)

    # Nouveau bloc multi-langue : une entrée par langue disponible avec
    # sa provenance (originale / officielle / ia). Source de vérité unique.
    traductions: dict[str, Traduction] = Field(default_factory=dict)

    # Champs historiques — conservés pour la compatibilité descendante avec
    # les consommateurs existants (build_index, loadDocuments). Dérivables
    # depuis `traductions` (langues_disponibles = liste des clés, sha256 =
    # map lang→hash).
    langues_disponibles: list[str] = Field(default_factory=list)
    sha256: dict[str, str] = Field(default_factory=dict)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.model_dump(mode="json", exclude_none=True)
        path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )

    @classmethod
    def read(cls, path: Path) -> "DocMeta":
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return cls.model_validate(data)

    def sync_legacy_fields(self) -> None:
        """Reconstruit `langues_disponibles` et `sha256` depuis `traductions`.

        Appelé après chaque mise à jour de `traductions` pour garder les
        champs historiques cohérents sans duplication de code.
        """
        if not self.traductions:
            return
        self.langues_disponibles = list(self.traductions.keys())
        self.sha256 = {lang: t.sha256 for lang, t in self.traductions.items()}
