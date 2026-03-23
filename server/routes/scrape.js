import express from 'express';
import db from '../db.js';
import { scrapeAll } from '../services/scraper.js';

const router = express.Router();

// GET /api/scrape/runs - List scrape runs
router.get('/runs', (req, res) => {
  try {
    const runs = db.prepare('SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 20').all();
    res.json(runs);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/scrape/runs/:id - Single scrape run
router.get('/runs/:id', (req, res) => {
  try {
    const run = db.prepare('SELECT * FROM scrape_runs WHERE id = ?').get(req.params.id);
    if (!run) {
      return res.status(404).json({ error: 'Scrape run not found' });
    }
    res.json(run);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/scrape/run - Trigger a scrape
router.post('/run', async (req, res) => {
  try {
    const { sources = ['linkedin', 'indeed', 'glassdoor'] } = req.body;
    const now = new Date().toISOString();

    const result = db.prepare(`
      INSERT INTO scrape_runs (started_at, status, source)
      VALUES (?, ?, ?)
    `).run(now, 'running', Array.isArray(sources) ? sources.join(',') : sources);

    const runId = result.lastInsertRowid;

    // Run scraping in background (don't block the response)
    const runIdCopy = runId;
    (async () => {
      try {
        const outcome = await scrapeAll({ sources, maxDetailPages: 500, runId: runIdCopy });
        db.prepare(`
          UPDATE scrape_runs SET
            status = 'completed',
            completed_at = ?,
            jobs_found = ?,
            jobs_new = ?,
            jobs_scored = ?
          WHERE id = ?
        `).run(new Date().toISOString(), outcome.jobs_found, outcome.jobs_new, outcome.jobs_scored, runIdCopy);
        console.log(`Scrape run ${runIdCopy} complete: ${outcome.jobs_new} new jobs`);
      } catch (e) {
        db.prepare(`
          UPDATE scrape_runs SET status = 'failed', error_message = ? WHERE id = ?
        `).run(e.message, runIdCopy);
        console.error(`Scrape run ${runIdCopy} failed:`, e.message);
      }
    })();

    res.json({
      id: runId,
      status: 'running',
      message: `Scraping initiated for ${Array.isArray(sources) ? sources.join(', ') : sources}`
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/scrape/status - Get scrape status
router.get('/status', (req, res) => {
  try {
    const latest = db.prepare('SELECT * FROM scrape_runs ORDER BY started_at DESC LIMIT 1').get();
    res.json({
      last_run: latest,
      next_scheduled: '1:00 AM daily'
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
