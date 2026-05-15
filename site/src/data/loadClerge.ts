/**
 * Loader prosopographique — lit `../clerge/eveques/*.yaml` + `_metadata/*.jsonl|json`.
 * Tout en cache mémoire, parsé une seule fois au premier appel.
 */
import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import { getDocumentBySlug } from './loadDocuments.js';
import type { Document } from './types.js';

export const CLERGE_ROOT: string = path.resolve(process.cwd(), '../clerge');
export const CLERGE_METADATA: string = path.join(CLERGE_ROOT, '_metadata');
export const CLERGE_EVEQUES: string = path.join(CLERGE_ROOT, 'eveques');
export const CLERGE_PRETRES: string = path.join(CLERGE_ROOT, 'pretres');

const CLERICS_JSONL = path.join(CLERGE_METADATA, 'clerics.jsonl');
const CONSECRATIONS_JSONL = path.join(CLERGE_METADATA, 'consecrations.jsonl');
const LINEAGES_JSON = path.join(CLERGE_METADATA, 'lineages.json');
const STATS_JSON = path.join(CLERGE_METADATA, 'stats.json');
const CLERIC_DOCUMENTS_JSON = path.join(CLERGE_METADATA, 'cleric_documents.json');
const DOCUMENT_AUTHORS_JSONL = path.join(CLERGE_METADATA, 'document_authors.jsonl');

export type Rite = 'ancien' | 'nouveau' | 'mixte' | 'inconnu';
export type Tampon = 'ordo_validus' | 'ordo_dubius' | 'ordo_incertus';

export interface ClericIndex {
  slug: string;
  nom: string;
  naissance_annee: number | null;
  deces_annee: number | null;
  fraternite: string | null;
  rang: string | null;
  pays: string | null;
  photo_disponible: boolean;
  wikidata_qid: string | null;
}

export interface SacreYaml {
  date?: string | null;
  lieu?: string | null;
  rite?: Rite;
  rite_source?: string;
  consecrateur_principal?: string | null;
  co_consecrateurs?: string[];
  source_urls?: string[];
}

export interface Obedience {
  du?: string | null;
  au?: string | null;
  statut?: string | null;
}

export interface Fonction {
  titre?: string | null;
  siege?: string | null;
  institut?: string | null;
  du?: string | null;
  au?: string | null;
}

export interface Photo {
  fichier?: string | null;
  source?: string | null;
  source_url?: string | null;
  licence?: string | null;
  auteur?: string | null;
}

export interface Ordination {
  date?: string | null;
  lieu?: string | null;
  rite?: Rite;
  rite_source?: string;
  ordinateur?: string | null;          // libellé brut ou slug
  ordinateur_slug?: string | null;     // slug résolu si ordinateur dans le corpus
  source_urls?: string[];
}

export interface Cleric {
  slug: string;
  nom: string;
  nom_complet?: string | null;
  naissance?: string | null;
  deces?: string | null;
  naissance_lieu?: string | null;
  deces_lieu?: string | null;
  rang?: string | null;
  nationalite?: string | null;
  tampon?: Tampon | null;              // calculé en phase 5 (évêques) ou finalize_pretres (prêtres)
  sacre?: SacreYaml | null;            // pour les évêques
  ordination?: Ordination | null;      // pour les prêtres
  obediences?: Obedience[];
  fonctions?: Fonction[];
  fraternite?: string | null;
  photo?: Photo | null;
  qids?: Record<string, string>;
  sources?: Array<{ source: string; fetched_at?: string; completeness?: number; url?: string }>;
  notes?: string | null;
}

export interface Consecration {
  consacre: string;
  consecrateur_principal: string | null;
  co_consecrateurs: string[];
  date: string | null;
  lieu: string | null;
  rite?: Rite;
  rite_source?: string;
  sources?: string[];
}

export interface Lineage {
  slug: string;
  tampon: Tampon;
  ancestors: string[]; // du clerc vers la racine (slug du clerc → consécrateur → ... → ancêtre le plus ancien)
  oldest_anchor?: string | null;
  count_nouveau: number;
  count_inconnu: number;
  count_ancien: number;
}

export interface Stats {
  total_eveques: number;
  par_fraternite: Record<string, number>;
  par_siecle_naissance: Record<string, number>;
  avec_photo: number;
  avec_consecrateur_connu: number;
  avec_date_sacre: number;
  cross_source_matches?: number;
  anomalies?: Record<string, number>;
}

// ───────────── Caches lazy ───────────────────────────────────────────

let _index: ClericIndex[] | null = null;
let _indexBySlug: Map<string, ClericIndex> | null = null;
let _consBySlug: Map<string, Consecration> | null = null;
let _descendantsBySlug: Map<string, string[]> | null = null;
let _lineages: Map<string, Lineage> | null = null;
let _stats: Stats | null = null;
const _clericCache: Map<string, Cleric | null> = new Map();
let _docsByCleric: Map<string, string[]> | null = null;
let _clericByDocSlug: Map<string, string> | null = null;

/** Lit chaque ligne du JSONL en parsant en JSON. */
function readJsonl<T>(p: string): T[] {
  if (!fs.existsSync(p)) return [];
  const raw = fs.readFileSync(p, 'utf8');
  const out: T[] = [];
  for (const line of raw.split('\n')) {
    const s = line.trim();
    if (s.length === 0) continue;
    out.push(JSON.parse(s) as T);
  }
  return out;
}

function loadIndex(): ClericIndex[] {
  if (_index !== null) return _index;
  _index = readJsonl<ClericIndex>(CLERICS_JSONL);
  _indexBySlug = new Map(_index.map((c) => [c.slug, c]));
  return _index;
}

function loadConsecrations(): void {
  if (_consBySlug !== null) return;
  const rows = readJsonl<Consecration>(CONSECRATIONS_JSONL);
  _consBySlug = new Map();
  _descendantsBySlug = new Map();
  for (const row of rows) {
    _consBySlug.set(row.consacre, row);
    if (row.consecrateur_principal !== null && row.consecrateur_principal !== undefined) {
      const list = _descendantsBySlug.get(row.consecrateur_principal) ?? [];
      list.push(row.consacre);
      _descendantsBySlug.set(row.consecrateur_principal, list);
    }
    for (const co of row.co_consecrateurs ?? []) {
      if (typeof co !== 'string' || co.length === 0) continue;
      const list = _descendantsBySlug.get(co) ?? [];
      // Évite les doublons (un évêque ne consacre pas le même nouvel évêque deux fois)
      if (!list.includes(row.consacre)) list.push(row.consacre);
      _descendantsBySlug.set(co, list);
    }
  }
}

function loadLineages(): void {
  if (_lineages !== null) return;
  _lineages = new Map();
  if (fs.existsSync(LINEAGES_JSON)) {
    const raw = JSON.parse(fs.readFileSync(LINEAGES_JSON, 'utf8'));
    if (Array.isArray(raw)) {
      for (const l of raw) _lineages.set(l.slug, l);
    } else if (typeof raw === 'object' && raw !== null) {
      for (const [k, v] of Object.entries(raw as Record<string, Lineage>)) {
        _lineages.set(k, v);
      }
    }
  }
}

/** Retourne tous les clercs (depuis l'index plat). */
export function getAllClerics(): ClericIndex[] {
  return loadIndex();
}

/** Index plat par slug. */
export function getClericIndex(slug: string): ClericIndex | null {
  loadIndex();
  return _indexBySlug?.get(slug) ?? null;
}

/** Charge la fiche YAML complète. `null` si manquante. */
export function getClericBySlug(slug: string): Cleric | null {
  if (_clericCache.has(slug)) return _clericCache.get(slug) ?? null;
  // Cherche d'abord dans eveques/, fallback sur pretres/.
  let p = path.join(CLERGE_EVEQUES, `${slug}.yaml`);
  if (!fs.existsSync(p)) {
    p = path.join(CLERGE_PRETRES, `${slug}.yaml`);
    if (!fs.existsSync(p)) {
      _clericCache.set(slug, null);
      return null;
    }
  }
  const raw = fs.readFileSync(p, 'utf8');
  const parsed = yaml.load(raw) as Cleric;
  _clericCache.set(slug, parsed);
  return parsed;
}

/** Sacre du clerc (= entrée de `consecrations.jsonl` où `consacre = slug`). */
export function getConsecrationOf(slug: string): Consecration | null {
  loadConsecrations();
  return _consBySlug?.get(slug) ?? null;
}

/** Slugs des évêques consacrés par ce slug (principal ou co-). */
export function getDescendants(slug: string): string[] {
  loadConsecrations();
  return _descendantsBySlug?.get(slug) ?? [];
}

/**
 * Lignée ascendante. Si le fichier `lineages.json` n'existe pas encore (phase 5
 * non terminée), on calcule un fallback minimal à la volée en remontant la
 * chaîne `consecrateur_principal` jusqu'à 12 niveaux ou jusqu'à un cycle.
 */
export function getLineage(slug: string): Lineage | null {
  loadLineages();
  const cached = _lineages?.get(slug);
  if (cached !== undefined) return cached;
  return fallbackLineage(slug);
}

function fallbackLineage(slug: string): Lineage {
  loadConsecrations();
  const seen = new Set<string>();
  const ancestors: string[] = [];
  let cur: string | null = slug;
  let count_nouveau = 0;
  let count_inconnu = 0;
  let count_ancien = 0;
  let depth = 0;
  while (cur !== null && !seen.has(cur) && depth < 20) {
    seen.add(cur);
    ancestors.push(cur);
    const cons = _consBySlug?.get(cur) ?? null;
    if (cons === null) break;
    const rite = inferRite(cons);
    if (rite === 'ancien') count_ancien += 1;
    else if (rite === 'nouveau') count_nouveau += 1;
    else count_inconnu += 1;
    const next = cons.consecrateur_principal;
    if (next === null || next === undefined || next.startsWith('non-indexe-')) break;
    cur = next;
    depth += 1;
  }
  let tampon: Tampon = 'ordo_incertus';
  if (count_nouveau > 0) tampon = 'ordo_dubius';
  else if (count_ancien > 0 && count_inconnu === 0) tampon = 'ordo_validus';
  return {
    slug,
    tampon,
    ancestors,
    oldest_anchor: ancestors[ancestors.length - 1] ?? null,
    count_nouveau,
    count_inconnu,
    count_ancien,
  };
}

/** Détermine le rite si non explicite : règle de date 1968-06-29. */
export function inferRite(cons: Consecration | null | undefined): Rite {
  if (cons === null || cons === undefined) return 'inconnu';
  if (cons.rite !== undefined && cons.rite !== null) return cons.rite;
  if (cons.date !== null && cons.date !== undefined) {
    const m = /^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?/.exec(cons.date);
    if (m !== null) {
      const y = Number.parseInt(m[1], 10);
      const mo = m[2] !== undefined ? Number.parseInt(m[2], 10) : 1;
      const d = m[3] !== undefined ? Number.parseInt(m[3], 10) : 1;
      if (y < 1968 || (y === 1968 && (mo < 6 || (mo === 6 && d <= 29)))) {
        return 'ancien';
      }
      return 'nouveau';
    }
  }
  return 'inconnu';
}

/** Tampon d'un clerc à partir de sa lignée (ou par défaut `ordo_incertus`). */
export function tamponOf(slug: string): Tampon {
  const l = getLineage(slug);
  return l?.tampon ?? 'ordo_incertus';
}

/** Statistiques globales. */
export function getStats(): Stats {
  if (_stats !== null) return _stats;
  if (fs.existsSync(STATS_JSON)) {
    _stats = JSON.parse(fs.readFileSync(STATS_JSON, 'utf8')) as Stats;
  } else {
    _stats = {
      total_eveques: 0,
      par_fraternite: {},
      par_siecle_naissance: {},
      avec_photo: 0,
      avec_consecrateur_connu: 0,
      avec_date_sacre: 0,
    };
  }
  return _stats;
}

/** Renvoie un siècle (1..21) à partir d'une année (1..2100). */
export function siecleDe(annee: number | null | undefined): number | null {
  if (annee === null || annee === undefined) return null;
  if (annee <= 0) return null;
  return Math.floor((annee - 1) / 100) + 1;
}

// ───────────── Pont documents ↔ clerc ─────────────────────────────────
// Produit par `tools/clerge_link_documents.py`. Si les fichiers sont
// absents (premier build avant exécution de l'outil), tout est vide et
// les fonctions retournent silencieusement [].

interface DocumentAuthorRow {
  document_slug: string;
  doc_path: string;
  cleric_slug: string;
  confidence: number;
  method: string;
}

function loadDocumentAuthors(): void {
  if (_docsByCleric !== null) return;
  _docsByCleric = new Map();
  _clericByDocSlug = new Map();
  // Index inverse (map JSON) — c'est lui qui sert pour /clerge/{slug}.
  if (fs.existsSync(CLERIC_DOCUMENTS_JSON)) {
    const raw = JSON.parse(fs.readFileSync(CLERIC_DOCUMENTS_JSON, 'utf8')) as Record<
      string,
      string[]
    >;
    for (const [cleric, slugs] of Object.entries(raw)) {
      _docsByCleric.set(cleric, slugs);
    }
  }
  // Index direct (document → clerc) — sert pour /documents/{slug}.
  if (fs.existsSync(DOCUMENT_AUTHORS_JSONL)) {
    const raw = fs.readFileSync(DOCUMENT_AUTHORS_JSONL, 'utf8');
    for (const line of raw.split('\n')) {
      const s = line.trim();
      if (s.length === 0) continue;
      const row = JSON.parse(s) as DocumentAuthorRow;
      _clericByDocSlug.set(row.document_slug, row.cleric_slug);
    }
  }
}

/**
 * Documents associés à un clerc (encycliques, motu proprio, lettres, etc.).
 * Résout les slugs en `Document` complets via `loadDocuments`. Les slugs
 * orphelins (présents dans le mapping mais absents de l'index documentaire)
 * sont silencieusement filtrés.
 */
export function getDocumentsByCleric(slug: string): Document[] {
  loadDocumentAuthors();
  const slugs = _docsByCleric?.get(slug) ?? [];
  const docs: Document[] = [];
  for (const docSlug of slugs) {
    const d = getDocumentBySlug(docSlug);
    if (d !== null) docs.push(d);
  }
  // Tri chronologique décroissant (plus récent d'abord) avec slugs sans
  // date en fin.
  docs.sort((a, b) => {
    const aDate = a.date ?? '';
    const bDate = b.date ?? '';
    if (aDate === '' && bDate === '') return a.slug.localeCompare(b.slug);
    if (aDate === '') return 1;
    if (bDate === '') return -1;
    return bDate.localeCompare(aDate);
  });
  return docs;
}

/** Slug du clerc auteur d'un document, ou `null` si non mappé / collectif. */
export function getClericOfDocument(documentSlug: string): string | null {
  loadDocumentAuthors();
  return _clericByDocSlug?.get(documentSlug) ?? null;
}
