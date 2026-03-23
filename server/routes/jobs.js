import express from 'express';
import db from '../db.js';
import { scoreJob, getFitScore } from '../services/fit-scorer.js';

const router = express.Router();

// GET /api/jobs - List with filters
router.get('/', (req, res) => {
  const { source, minScore, location, search, sort, limit = 50, offset = 0 } = req.query;

  let query = `
    SELECT j.*,
      a.id as application_id,
      a.status as application_status,
      a.created_at as application_created_at
    FROM jobs j
    LEFT JOIN applications a ON j.id = a.job_id
    WHERE j.is_active = 1
  `;
  const params = [];

  if (source) {
    query += ' AND source = ?';
    params.push(source);
  }

  if (minScore) {
    query += ' AND fit_score >= ?';
    params.push(parseInt(minScore));
  }

  if (location) {
    query += ' AND (location LIKE ? OR is_remote = 1)';
    params.push(`%${location}%`);
  }

  if (search) {
    query += ' AND (title LIKE ? OR company_name LIKE ? OR description LIKE ?)';
    params.push(`%${search}%`, `%${search}%`, `%${search}%`);
  }

  // Sorting
  if (sort === 'score_desc') {
    query += ' ORDER BY fit_score DESC';
  } else if (sort === 'score_asc') {
    query += ' ORDER BY fit_score ASC';
  } else if (sort === 'date_desc') {
    query += ' ORDER BY posted_date DESC';
  } else {
    query += ' ORDER BY scraped_at DESC';
  }

  query += ' LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  try {
    const jobs = db.prepare(query).all(...params);
    res.json(jobs.map(job => ({
      ...job,
      fit_breakdown: job.fit_breakdown ? JSON.parse(job.fit_breakdown) : null,
      requirements: job.requirements ? JSON.parse(job.requirements) : null
    })));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/jobs/:id - Single job
router.get('/:id', (req, res) => {
  try {
    const job = db.prepare(`
      SELECT j.*,
        a.id as application_id,
        a.status as application_status,
        a.applied_date,
        a.notes as application_notes,
        a.created_at as application_created_at
      FROM jobs j
      LEFT JOIN applications a ON j.id = a.job_id
      WHERE j.id = ?
    `).get(req.params.id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }
    res.json({
      ...job,
      fit_breakdown: job.fit_breakdown ? JSON.parse(job.fit_breakdown) : null,
      requirements: job.requirements ? JSON.parse(job.requirements) : null
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/jobs/:id/dossier - Company dossier (generates if missing)
router.get('/:id/dossier', async (req, res) => {
  try {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(req.params.id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    let company = db.prepare('SELECT * FROM companies WHERE id = ?').get(job.company_id);

    // If no company exists by ID, look up or create by name
    if (!company && job.company_name) {
      company = db.prepare('SELECT * FROM companies WHERE name = ?').get(job.company_name);
      if (!company) {
        try {
          const result = db.prepare('INSERT INTO companies (name) VALUES (?)').run(job.company_name);
          company = db.prepare('SELECT * FROM companies WHERE id = ?').get(result.lastInsertRowid);
        } catch (err) {
          // Handle race condition: another request inserted the same company name
          if (err.message.includes('UNIQUE')) {
            company = db.prepare('SELECT * FROM companies WHERE name = ?').get(job.company_name);
          } else {
            throw err;
          }
        }
      }
    }

    if (!company) {
      return res.json(null);
    }

    // If dossier is missing, generate it
    if (!company.dossier_generated_at) {
      const { default: companiesRouter } = await import('./companies.js');
      // Call the dossier generation inline to avoid routing issues
      try {
        const dossier = await import('../services/llm.js').then(m => m.generateCompanyDossier(company));

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
          company.id
        );

        company = db.prepare('SELECT * FROM companies WHERE id = ?').get(company.id);
      } catch (e) {
        console.error(`Dossier generation error for company ${company.id}:`, e.message);
        // Return company as-is if generation fails
      }
    }

    res.json(company);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/jobs/:id/score - Re-score
router.post('/:id/score', async (req, res) => {
  try {
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(req.params.id);
    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    // Score the job using heuristic + LLM
    const { fit_score, fit_breakdown } = await scoreJob(req.params.id, db);

    res.json({
      id: job.id,
      fit_score,
      fit_breakdown
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// PATCH /api/jobs/:id - Update
router.patch('/:id', (req, res) => {
  const { title, location, is_remote, salary_min, salary_max, salary_text, description, requirements, posted_date, is_active } = req.body;

  const updates = [];
  const params = [];

  if (title !== undefined) { updates.push('title = ?'); params.push(title); }
  if (location !== undefined) { updates.push('location = ?'); params.push(location); }
  if (is_remote !== undefined) { updates.push('is_remote = ?'); params.push(is_remote ? 1 : 0); }
  if (salary_min !== undefined) { updates.push('salary_min = ?'); params.push(salary_min); }
  if (salary_max !== undefined) { updates.push('salary_max = ?'); params.push(salary_max); }
  if (salary_text !== undefined) { updates.push('salary_text = ?'); params.push(salary_text); }
  if (description !== undefined) { updates.push('description = ?'); params.push(description); }
  if (requirements !== undefined) { updates.push('requirements = ?'); params.push(JSON.stringify(requirements)); }
  if (posted_date !== undefined) { updates.push('posted_date = ?'); params.push(posted_date); }
  if (is_active !== undefined) { updates.push('is_active = ?'); params.push(is_active ? 1 : 0); }

  if (updates.length === 0) {
    return res.status(400).json({ error: 'No fields to update' });
  }

  params.push(req.params.id);

  try {
    db.prepare(`UPDATE jobs SET ${updates.join(', ')} WHERE id = ?`).run(...params);
    const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(req.params.id);
    res.json(job);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
