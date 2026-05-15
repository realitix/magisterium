import type { APIRoute } from 'astro';
import { getAllClerics, tamponOf, siecleDe } from '../../data/loadClerge.ts';

/**
 * Index plat servi à MiniSearch côté client. Ajoute le tampon précalculé
 * et le siècle de naissance pour les facettes.
 */
export const GET: APIRoute = () => {
  const clerics = getAllClerics();
  const items = clerics.map((c) => ({
    slug: c.slug,
    nom: c.nom,
    naissance: c.naissance_annee,
    deces: c.deces_annee,
    siecle: siecleDe(c.naissance_annee),
    fraternite: c.fraternite,
    rang: c.rang,
    pays: c.pays,
    photo: c.photo_disponible,
    tampon: tamponOf(c.slug),
  }));
  return new Response(JSON.stringify({ items }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
  });
};
