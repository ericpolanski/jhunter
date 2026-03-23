import express from 'express';
import { getProfile } from '../lib/profile.js';
import db from '../db.js';
import * as llm from '../services/llm.js';
import * as resumeTailor from '../services/resume-tailor.js';

const router = express.Router();

// GET /api/resume/base - Eric's structured profile
router.get('/base', (req, res) => {
  const profile = getProfile();
  if (!profile) {
    return res.status(404).json({ error: 'Profile not found' });
  }
  res.json(profile);
});

// POST /api/resume/tailor/:jobId - Generate tailoring suggestions
router.post('/tailor/:jobId', async (req, res) => {
  try {
    const suggestions = await resumeTailor.getTailoringSuggestions(req.params.jobId);
    res.json(suggestions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/resume/generate/:jobId - Generate DOCX+PDF with tailored content
router.post('/generate/:jobId', async (req, res) => {
  const { suggestions } = req.body;

  try {
    if (!suggestions || !Array.isArray(suggestions)) {
      return res.status(400).json({ error: 'Suggestions array required' });
    }

    const result = await resumeTailor.generateTailoredResume(req.params.jobId, suggestions);

    res.json({
      id: result.id,
      job_id: result.job_id,
      file_name: result.file_name,
      docx_path: result.docx_path,
      pdf_path: result.pdf_path,
      download_url: result.pdf_path ? `/generated/resumes/${result.file_name}` : null,
      created_at: result.created_at
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/resume/download/:id - Download resume file
router.get('/download/:id', (req, res) => {
  try {
    const resume = db.prepare('SELECT * FROM resume_versions WHERE id = ?').get(req.params.id);
    if (!resume) {
      return res.status(404).json({ error: 'Resume not found' });
    }

    // Determine which file to serve (prefer PDF over DOCX)
    const filePath = resume.pdf_path || resume.docx_path;
    if (!filePath) {
      return res.status(404).json({ error: 'No file generated yet' });
    }

    const fileName = resume.file_name || resume.pdf_path?.split('/').pop() || 'resume.pdf';
    res.download(filePath, fileName);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/resume/versions - List all resume versions
router.get('/versions', (req, res) => {
  try {
    const jobId = req.query.jobId;
    let query = 'SELECT * FROM resume_versions';
    const params = [];

    if (jobId) {
      query += ' WHERE job_id = ?';
      params.push(jobId);
    }
    query += ' ORDER BY created_at DESC';

    const versions = db.prepare(query).all(...params);
    res.json(versions);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/resume/version/:id - Get specific resume version
router.get('/version/:id', (req, res) => {
  try {
    const resume = db.prepare('SELECT * FROM resume_versions WHERE id = ?').get(req.params.id);
    if (!resume) {
      return res.status(404).json({ error: 'Resume not found' });
    }
    res.json(resume);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
