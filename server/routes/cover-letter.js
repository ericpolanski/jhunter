import express from 'express';
import { getProfile } from '../lib/profile.js';
import db from '../db.js';
import * as llm from '../services/llm.js';
import { generateCoverLetterDocx } from '../services/cover-letter-gen.js';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { existsSync, readFileSync } from 'fs';

const router = express.Router();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const GENERATED_DIR = join(__dirname, '..', '..', 'generated', 'cover-letters');

// POST /api/cover-letter/generate/:jobId - Generate letter
router.post('/generate/:jobId', async (req, res) => {
  try {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(req.params.jobId);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const profile = getProfile();
    if (!profile) {
      return res.status(404).json({ error: 'Profile not found' });
    }

    // Get company dossier if available
    let companyDossier = null;
    if (job.company_id) {
      const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(job.company_id);
      companyDossier = company ? company.dossier_raw : null;
    }

    const coverLetter = await llm.generateCoverLetter(profile, job.description || '', companyDossier);

    const now = new Date().toISOString();
    const result = db.prepare(`
      INSERT INTO cover_letters (job_id, content, created_at)
      VALUES (?, ?, ?)
    `).run(req.params.jobId, coverLetter.content, now);

    res.json({
      id: result.lastInsertRowid,
      job_id: req.params.jobId,
      content: coverLetter.content,
      created_at: now
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/cover-letter/download/:id - Generate PDF, return download URL
router.post('/download/:id', (req, res) => {
  try {
    const coverLetter = db.prepare('SELECT * FROM cover_letters WHERE id = ?').get(req.params.id);
    if (!coverLetter) {
      return res.status(404).json({ error: 'Cover letter not found' });
    }

    // Stub - would generate PDF in production
    res.json({
      id: coverLetter.id,
      download_url: `/generated/cover-letters/${coverLetter.id}.pdf`
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/cover-letter/:jobId - Get existing
router.get('/:jobId', (req, res) => {
  try {
    const coverLetter = db.prepare('SELECT * FROM cover_letters WHERE job_id = ? ORDER BY created_at DESC').get(req.params.jobId);
    if (!coverLetter) {
      return res.status(404).json({ error: 'Cover letter not found' });
    }
    res.json(coverLetter);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
