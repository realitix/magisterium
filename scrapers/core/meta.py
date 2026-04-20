"""Pydantic model for per-document .meta.yaml sidecar."""
from datetime import date as _date
from pathlib import Path
from typing import Literal, Optional

import yaml
from pydantic import BaseModel, Field

Period = Literal["pre-vatican-ii", "vatican-ii", "post-vatican-ii", "fsspx"]


class Source(BaseModel):
    url: str
    site: str
    langue: str
    fetch_method: str


class DocMeta(BaseModel):
    incipit: str
    titre_fr: Optional[str] = None
    auteur: str
    periode: Period
    type: str
    date: Optional[_date] = None
    autorite_magisterielle: Optional[str] = None
    langues_disponibles: list[str] = Field(default_factory=list)
    langue_originale: Optional[str] = None
    denzinger: list[str] = Field(default_factory=list)
    sujets: list[str] = Field(default_factory=list)
    themes_doctrinaux: list[str] = Field(default_factory=list)
    references_anterieures: list[str] = Field(default_factory=list)
    references_posterieures: list[str] = Field(default_factory=list)
    sources: list[Source] = Field(default_factory=list)
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
