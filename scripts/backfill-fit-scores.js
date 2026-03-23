/**
 * Backfill fit scores for all existing jobs in the database
 * Usage: node scripts/backfill-fit-scores.js
 */
import db from '../server/db.js';
import { scoreJob } from '../server/services/fit-scorer.js';

async function backfillScores() {
  console.log('Starting fit score backfill...');

  // Get all jobs without fit scores
  const jobs = db.prepare(`
    SELECT id, title, company_name, description
    FROM jobs
    WHERE fit_score IS NULL OR fit_score = 0
  `).all();

  console.log(`Found ${jobs.length} jobs needing fit scores`);

  let scored = 0;
  let failed = 0;

  for (const job of jobs) {
    try {
      const result = await scoreJob(job.id, db);
      console.log(`Job ${job.id} (${job.title} @ ${job.company_name}): score=${result.fit_score}`);
      scored++;
    } catch (error) {
      console.error(`Job ${job.id} failed: ${error.message}`);
      failed++;
    }
  }

  console.log(`\nBackfill complete: ${scored} scored, ${failed} failed`);
}

backfillScores().catch(console.error);