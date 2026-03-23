import express from 'express';
import db from '../db.js';
import * as llm from '../services/llm.js';

const router = express.Router();

// GET /api/companies - List
router.get('/', (req, res) => {
  try {
    const companies = db.prepare('SELECT * FROM companies ORDER BY name').all();
    res.json(companies);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/companies/:id - Single
router.get('/:id', (req, res) => {
  try {
    const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(req.params.id);
    if (!company) {
      return res.status(404).json({ error: 'Company not found' });
    }
    res.json(company);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/companies/:id/dossier - Generate dossier
router.post('/:id/dossier', async (req, res) => {
  try {
    const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(req.params.id);
    if (!company) {
      return res.status(404).json({ error: 'Company not found' });
    }

    const dossier = await llm.generateCompanyDossier(company);

    db.prepare(`
      UPDATE companies SET
        tech_stack = ?,
        glassdoor_rating = ?,
        recent_news = ?,
        connections_to_eric = ?,
        dossier_generated_at = ?,
        dossier_raw = ?
      WHERE id = ?
    `).run(
      JSON.stringify(dossier.tech_stack),
      dossier.rating,
      JSON.stringify(dossier.news),
      JSON.stringify(dossier.connections),
      new Date().toISOString(),
      JSON.stringify(dossier),
      req.params.id
    );

    const updated = db.prepare('SELECT * FROM companies WHERE id = ?').get(req.params.id);
    res.json(updated);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// PATCH /api/companies/:id - Update
router.patch('/:id', (req, res) => {
  const { name, website, size, industry, tech_stack, glassdoor_rating, description } = req.body;

  const updates = [];
  const params = [];

  if (name !== undefined) { updates.push('name = ?'); params.push(name); }
  if (website !== undefined) { updates.push('website = ?'); params.push(website); }
  if (size !== undefined) { updates.push('size = ?'); params.push(size); }
  if (industry !== undefined) { updates.push('industry = ?'); params.push(industry); }
  if (tech_stack !== undefined) { updates.push('tech_stack = ?'); params.push(JSON.stringify(tech_stack)); }
  if (glassdoor_rating !== undefined) { updates.push('glassdoor_rating = ?'); params.push(glassdoor_rating); }
  if (description !== undefined) { updates.push('description = ?'); params.push(description); }

  if (updates.length === 0) {
    return res.status(400).json({ error: 'No fields to update' });
  }

  params.push(req.params.id);

  try {
    db.prepare(`UPDATE companies SET ${updates.join(', ')} WHERE id = ?`).run(...params);
    const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(req.params.id);
    res.json(company);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
