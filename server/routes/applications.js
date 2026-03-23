import express from 'express';
import db from '../db.js';
import { generateInterviewPrepForJob } from '../services/interview-prep-gen.js';

const router = express.Router();

// GET /api/applications - List
router.get('/', (req, res) => {
  const { status } = req.query;

  let query = 'SELECT a.*, j.title, j.company_name, j.location, j.fit_score, j.source_url FROM applications a LEFT JOIN jobs j ON a.job_id = j.id';
  const params = [];

  if (status) {
    query += ' WHERE a.status = ?';
    params.push(status);
  }

  query += ' ORDER BY a.created_at DESC';

  try {
    const applications = db.prepare(query).all(...params);
    res.json(applications);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/applications/stats - Stats
router.get('/stats', (req, res) => {
  try {
    const total = db.prepare('SELECT COUNT(*) as count FROM applications').get().count;

    const byStatus = db.prepare(`
      SELECT status, COUNT(*) as count
      FROM applications
      GROUP BY status
    `).all();

    const recentActivity = db.prepare(`
      SELECT DATE(status_updated_at) as date, COUNT(*) as count
      FROM applications
      WHERE status_updated_at >= DATE('now', '-30 days')
      GROUP BY DATE(status_updated_at)
      ORDER BY date DESC
    `).all();

    res.json({
      total,
      byStatus: byStatus.reduce((acc, row) => { acc[row.status] = row.count; return acc; }, {}),
      recentActivity
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/applications - Create
router.post('/', (req, res) => {
  const { job_id, status = 'saved', notes } = req.body;

  if (!job_id) {
    return res.status(400).json({ error: 'job_id is required' });
  }

  const now = new Date().toISOString();

  try {
    // Check if job exists
    const job = db.prepare('SELECT id FROM jobs WHERE id = ?').get(job_id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    // Check if application already exists
    const existing = db.prepare('SELECT id FROM applications WHERE job_id = ?').get(job_id);
    if (existing) {
      return res.status(400).json({ error: 'Application already exists for this job' });
    }

    const result = db.prepare(`
      INSERT INTO applications (job_id, status, applied_date, status_updated_at, notes, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(job_id, status, status === 'applied' ? now : null, now, notes || null, now);

    const application = db.prepare('SELECT * FROM applications WHERE id = ?').get(result.lastInsertRowid);
    res.status(201).json(application);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// PATCH /api/applications/:id - Update
router.patch('/:id', async (req, res) => {
  const { status, notes } = req.body;

  try {
    const existing = db.prepare('SELECT * FROM applications WHERE id = ?').get(req.params.id);
    if (!existing) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const updates = ['status_updated_at = ?'];
    const params = [new Date().toISOString()];

    if (status !== undefined) {
      updates.push('status = ?');
      params.push(status);

      // Set applied_date if status changed to applied
      if (status === 'applied' && !existing.applied_date) {
        updates.push('applied_date = ?');
        params.push(new Date().toISOString());
      }
    }

    if (notes !== undefined) {
      updates.push('notes = ?');
      params.push(notes);
    }

    params.push(req.params.id);

    db.prepare(`UPDATE applications SET ${updates.join(', ')} WHERE id = ?`).run(...params);

    const application = db.prepare(`
      SELECT a.*, j.title, j.company_name, j.location, j.fit_score, j.description
      FROM applications a
      LEFT JOIN jobs j ON a.job_id = j.id
      WHERE a.id = ?
    `).get(req.params.id);

    // Auto-generate interview prep when status changes to "interview"
    if (status === 'interview' && existing.status !== 'interview') {
      try {
        await generateInterviewPrepForJob(application.job_id);
      } catch (err) {
        console.error('Failed to generate interview prep:', err.message);
      }
    }

    res.json(application);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DELETE /api/applications/:id - Delete
router.delete('/:id', (req, res) => {
  try {
    const result = db.prepare('DELETE FROM applications WHERE id = ?').run(req.params.id);
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }
    res.status(204).send();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
