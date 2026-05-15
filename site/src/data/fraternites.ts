/**
 * Table des fraternités traditionalistes et sédévacantistes exposées dans
 * `/clerge/fraternite/{slug}/`. Sortie dans un module à part pour pouvoir
 * être importée à la fois depuis le frontmatter et depuis `getStaticPaths`
 * (les deux scopes sont isolés en Astro).
 */
export interface FraterniteInfo {
  slug: string;
  label: string;
  pleinNom: string;
  intro: string;
  matches: string[];
}

export const FRATERNITES: FraterniteInfo[] = [
  {
    slug: 'fsspx',
    label: 'FSSPX',
    pleinNom: 'Fraternité Sacerdotale Saint-Pie X',
    intro:
      "Fondée par Mgr Marcel Lefebvre le 1ᵉʳ novembre 1970, supprimée canoniquement par Mgr Mamie le 6 mai 1975 — décision contestée par la FSSPX. Sacres d'Écône du 30 juin 1988 : Mgr Lefebvre, assisté de Mgr de Castro Mayer, consacre quatre évêques (Tissier de Mallerais, Williamson, Galarreta, Fellay) selon l'ancien rite. Rome déclare la cérémonie « acte schismatique » ; la FSSPX maintient agir au titre de l'état de nécessité. Statut canonique disputé depuis. Annonce du 2 février 2026 par l'abbé Pagliarani de nouveaux sacres à Écône le 1ᵉʳ juillet 2026.",
    matches: ['fsspx', 'fsspx-fondateur', 'fsspx-allie'],
  },
  {
    slug: 'resistance',
    label: 'Résistance',
    pleinNom: 'Mouvement de la Résistance catholique',
    intro:
      "Issu de l'exclusion de Mgr Richard Williamson de la FSSPX le 4 octobre 2012, dans le contexte des discussions doctrinales Rome-FSSPX. Sacres successifs par Williamson : Mgr Jean-Michel Faure (19 mars 2015, monastère Santa Cruz, Brésil), Mgr Tomás de Aquino Ferreira da Costa OSB (19 mars 2016). Mouvement non-una-cum dans sa majorité, en rupture avec la FSSPX qu'il accuse de modération doctrinale.",
    matches: ['resistance'],
  },
  {
    slug: 'fssp',
    label: 'FSSP',
    pleinNom: 'Fraternité Sacerdotale Saint-Pierre',
    intro:
      "Fondée le 18 juillet 1988 par d'anciens prêtres FSSPX désireux de demeurer en pleine communion avec Rome après les sacres d'Écône. Société de vie apostolique de droit pontifical, autorisée à célébrer exclusivement selon les livres liturgiques de 1962 (Ecclesia Dei). Renforcement de ce statut par Summorum Pontificum (2007), assouplissement remis en cause par Traditionis Custodes (2021) — la FSSP a obtenu un rescrit conservant ses prérogatives.",
    matches: ['fssp'],
  },
  {
    slug: 'icrsp',
    label: 'ICRSP',
    pleinNom: 'Institut du Christ-Roi Souverain Prêtre',
    intro:
      "Société de vie apostolique de droit pontifical fondée en 1990 par Mgr Gilles Wach et l'abbé Philippe Mora à Gricigliano (Italie). Liturgie tridentine de 1962, rituel exclusif. Apostolat principal en Europe et aux États-Unis. Communion pleine avec Rome.",
    matches: ['icrsp', 'icr'],
  },
  {
    slug: 'ibp',
    label: 'IBP',
    pleinNom: 'Institut du Bon Pasteur',
    intro:
      "Société de vie apostolique de droit pontifical fondée le 8 septembre 2006 sous Benoît XVI par d'anciens prêtres FSSPX, autour de l'abbé Philippe Laguérie. Liturgie tridentine exclusive. Centre principal à Bordeaux. Communion avec Rome ; certaines tensions doctrinales persistent.",
    matches: ['ibp'],
  },
  {
    slug: 'cmri',
    label: 'CMRI',
    pleinNom: 'Congregation of Mary Immaculate Queen',
    intro:
      "Communauté sédévacantiste fondée en 1967 (initialement à Spokane, Washington). Lignée épiscopale : Mgr Mark Pivarunas, sacré en 1991 par Mgr Moisés Carmona (lui-même sacré par Mgr Thục en 1981). Position « siège vacant » depuis Vatican II — refus de reconnaître la légitimité des papes post-conciliaires.",
    matches: ['cmri'],
  },
  {
    slug: 'sede-thuc-line',
    label: 'Lignée Thục',
    pleinNom: 'Successions sédévacantistes issues de Mgr Pierre Martin Ngô Đình Thục',
    intro:
      "Mgr Thục, ancien archevêque de Huế (Vietnam), sacre en 1976 puis 1981 plusieurs évêques en ancien rite hors juridiction canonique. Lignée principale : Carmona, Zamora (1981) puis Musey (1982), McKenna (1986), Pivarunas (1991), Sanborn (2002). Excommunié par Jean-Paul II en 1983 ; rétractations et réaffirmations sédévacantistes contradictoires en fin de vie.",
    matches: ['sede-thuc-line'],
  },
  {
    slug: 'sede-cassiciacum',
    label: 'Cassiciacum',
    pleinNom: 'Thèse de Cassiciacum',
    intro:
      "Thèse théologique formulée par le P. Michel-Louis Guérard des Lauriers OP (sacré par Mgr Thục en 1981) : les papes post-conciliaires sont « papes matériellement » mais non « formellement » — ils détiennent l'élection sans détenir l'autorité. Distincte du sédévacantisme pur. Représentants actuels : Mgr Donald Sanborn, sacré en 2002 par Mgr McKenna.",
    matches: ['sede-cassiciacum'],
  },
  {
    slug: 'palmar',
    label: 'Palmar',
    pleinNom: 'Église catholique palmarienne',
    intro:
      "Schisme issu des apparitions présumées de Palmar de Troya (Espagne, 1968-1976). Clemente Domínguez y Gómez, sacré par Mgr Thục en janvier 1976, se proclame « Pape Grégoire XVII » à la mort de Paul VI (août 1978). Hiérarchie ecclésiastique parallèle. Position considérée hors de la communion catholique par Rome.",
    matches: ['palmar'],
  },
  {
    slug: 'sgg',
    label: 'SGG-Most Holy Trinity Seminary',
    pleinNom: 'École sédévacantiste du Most Holy Trinity Seminary',
    intro:
      "Mouvance sédévacantiste américaine animée par Mgr Daniel Dolan († 2022, sacré 1993 par Pivarunas) et l'abbé Anthony Cekada († 2020). Centre à Brooksville (Floride). Position : sede vacante depuis Vatican II.",
    matches: ['sgg-school', 'cmri-allie'],
  },
];
