import { defineCollection, z } from 'astro:content';

const voiceSchema = z.object({
  tagline: z.string(),
  body: z.string(),
  punchline: z.string().optional(),
});

const questions = defineCollection({
  type: 'content',
  schema: z.object({
    title: z.string(),
    question: z.string(),
    date: z.string().optional(),
    summary: z.string().optional(),
    tags: z.array(z.string()).default([]),
    related_documents: z.array(z.string()).default([]),
    related_themes: z.array(z.string()).default([]),
    posture: z.enum(['traditionnelle', 'neutre', 'pastorale']).default('traditionnelle'),
    tampon: z
      .enum([
        'casus_gravissimus',
        'quaestio_disputata',
        'de_fide_definita',
        'traditio_constans',
        'disciplina',
        'ad_mentem_pii_x',
      ])
      .optional(),
    voices: z
      .object({
        conciliaire: voiceSchema.optional(),
        ecclesia_dei: voiceSchema.optional(),
        fsspx: voiceSchema.optional(),
        sedevacantiste: voiceSchema.optional(),
      })
      .optional(),
  }),
});

export const collections = { questions };
