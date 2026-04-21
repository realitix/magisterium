import { defineCollection, z } from 'astro:content';

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
  }),
});

export const collections = { questions };
