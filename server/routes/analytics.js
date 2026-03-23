import express from 'express';
import db from '../db.js';

const router = express.Router();

// GET /api/analytics/funnel - Application funnel (count by status)
router.get('/funnel', (req, res) => {
  try {
    const data = db.prepare(`
      SELECT status, COUNT(*) as count
      FROM applications
      GROUP BY status
      ORDER BY count DESC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/timeline - Applications over time
router.get('/timeline', (req, res) => {
  try {
    const data = db.prepare(`
      SELECT DATE(applied_date) as date, COUNT(*) as count
      FROM applications
      WHERE applied_date IS NOT NULL
      GROUP BY DATE(applied_date)
      ORDER BY date ASC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/fit-score-distribution - Fit score distribution
router.get('/fit-score-distribution', (req, res) => {
  try {
    // Bucket: 0-20, 21-40, 41-60, 61-80, 81-100
    const data = db.prepare(`
      SELECT
        CASE
          WHEN fit_score <= 20 THEN '0-20'
          WHEN fit_score <= 40 THEN '21-40'
          WHEN fit_score <= 60 THEN '41-60'
          WHEN fit_score <= 80 THEN '61-80'
          ELSE '81-100'
        END as bucket,
        COUNT(*) as count
      FROM jobs
      WHERE fit_score IS NOT NULL
      GROUP BY bucket
      ORDER BY bucket ASC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/salary-distribution - Salary distribution
router.get('/salary-distribution', (req, res) => {
  try {
    // Bucket salaries into ranges
    const data = db.prepare(`
      SELECT
        CASE
          WHEN salary_min < 50000 THEN '<50k'
          WHEN salary_min < 75000 THEN '50k-75k'
          WHEN salary_min < 100000 THEN '75k-100k'
          WHEN salary_min < 125000 THEN '100k-125k'
          WHEN salary_min < 150000 THEN '125k-150k'
          ELSE '150k+'
        END as bucket,
        COUNT(*) as count
      FROM jobs
      WHERE salary_min IS NOT NULL
      GROUP BY bucket
      ORDER BY bucket ASC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/skill-gap - Skill gap analysis
router.get('/skill-gap', (req, res) => {
  try {
    // Extract skills from job requirements and compare with Eric's profile
    const jobs = db.prepare(`
      SELECT requirements
      FROM jobs
      WHERE requirements IS NOT NULL AND requirements != '[]'
    `).all();

    // Count skill occurrences
    const skillCounts = {};
    jobs.forEach(job => {
      if (job.requirements) {
        const reqs = JSON.parse(job.requirements);
        reqs.forEach(req => {
          const skill = req.toLowerCase().trim();
          skillCounts[skill] = (skillCounts[skill] || 0) + 1;
        });
      }
    });

    // Convert to array and sort by count
    const data = Object.entries(skillCounts)
      .map(([skill, count]) => ({ skill, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 20); // Top 20 skills

    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/response-rate - Response rate by source
router.get('/response-rate', (req, res) => {
  try {
    // Get applications with job source info
    const data = db.prepare(`
      SELECT
        j.source,
        COUNT(a.id) as total_applications,
        SUM(CASE WHEN a.status IN ('phone_screen', 'interview', 'offer') THEN 1 ELSE 0 END) as positive_responses,
        ROUND(
          CAST(SUM(CASE WHEN a.status IN ('phone_screen', 'interview', 'offer') THEN 1 ELSE 0 END) AS FLOAT) /
          CAST(COUNT(a.id) AS FLOAT) * 100,
          1
        ) as response_rate
      FROM applications a
      JOIN jobs j ON a.job_id = j.id
      GROUP BY j.source
      ORDER BY response_rate DESC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/analytics/status-breakdown - Status breakdown (pie chart)
router.get('/status-breakdown', (req, res) => {
  try {
    const data = db.prepare(`
      SELECT status, COUNT(*) as value
      FROM applications
      GROUP BY status
      ORDER BY value DESC
    `).all();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
