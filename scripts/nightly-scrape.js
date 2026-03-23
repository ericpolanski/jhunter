#!/usr/bin/env node
/**
 * JHunter Nightly Scrape Script
 * Run via cron at 1:00 AM daily.
 * Performs a full scrape across all configured sources, scores jobs, and creates reminders.
 */

import { scrapeAll } from '../server/services/scraper.js';
import db from '../server/db.js';
import { getProfile } from '../server/lib/profile.js';
import { scoreJob } from '../server/services/fit-scorer.js';

const LOG_DIR = '/home/eric/ai-company/logs/jhunter';
const GENERATED_DIR = '/home/eric/ai-company/projects/jhunter/generated';

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { mkdirSync, appendFileSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Ensure log directory exists
mkdirSync(LOG_DIR, { recursive: true });

async function main() {
  const startedAt = new Date().toISOString();
  const logFile = `${LOG_DIR}/scrape-${startedAt.split('T')[0]}.log`;

  console.log(`[${startedAt}] JHunter nightly scrape starting...`);

  // Create a scrape run record
  const result = db.prepare(`
    INSERT INTO scrape_runs (started_at, status, source)
    VALUES (?, 'running', 'all')
  `).run(startedAt);

  const runId = result.lastInsertRowid;
  const sources = ['linkedin', 'indeed', 'glassdoor'];

  try {
    // Run the scrape
    const outcome = await scrapeAll({ sources, maxDetailPages: 500, runId });

    // Mark scrape run complete
    const completedAt = new Date().toISOString();
    db.prepare(`
      UPDATE scrape_runs SET
        status = 'completed',
        completed_at = ?,
        jobs_found = ?,
        jobs_new = ?,
        jobs_scored = ?
      WHERE id = ?
    `).run(completedAt, outcome.jobs_found, outcome.jobs_new, outcome.jobs_scored, runId);

    console.log(`[${completedAt}] Scrape complete: ${outcome.jobs_found} found, ${outcome.jobs_new} new, ${outcome.jobs_scored} scored`);

    // Generate reminders for applications that hit follow-up intervals
    const reminderIntervals = [7, 14]; // days
    const today = new Date();

    const appliedApps = db.prepare(`
      SELECT a.*, j.title, j.company_name
      FROM applications a
      JOIN jobs j ON a.job_id = j.id
      WHERE a.status = 'applied'
        AND a.follow_up_sent = 0
        AND NOT EXISTS (
          SELECT 1 FROM reminders r
          WHERE r.application_id = a.id
            AND r.reminder_type IN ('follow_up_7', 'follow_up_14')
        )
    `).all();

    let remindersCreated = 0;

    for (const app of appliedApps) {
      const appliedDate = new Date(app.applied_date || app.created_at);
      const daysSince = Math.floor((today - appliedDate) / (1000 * 60 * 60 * 24));

      for (const interval of reminderIntervals) {
        if (daysSince >= interval) {
          const dueDate = new Date(appliedDate);
          dueDate.setDate(dueDate.getDate() + interval);

          db.prepare(`
            INSERT INTO reminders (application_id, reminder_type, due_date, created_at)
            VALUES (?, ?, ?, ?)
          `).run(
            app.id,
            interval === 7 ? 'follow_up_7' : 'follow_up_14',
            dueDate.toISOString(),
            today.toISOString()
          );

          remindersCreated++;
        }
      }
    }

    console.log(`Reminders created: ${remindersCreated}`);

    const logEntry = `[${completedAt}] Scrape complete: ${outcome.jobs_new} new, ${outcome.jobs_scored} scored, ${remindersCreated} reminders\n`;
    appendFileSync(logFile, logEntry);

  } catch (error) {
    console.error(`Scrape run ${runId} failed:`, error.message);
    db.prepare(`
      UPDATE scrape_runs SET status = 'failed', error_message = ? WHERE id = ?
    `).run(error.message, runId);

    appendFileSync(logFile, `[${new Date().toISOString()}] ERROR: ${error.message}\n`);
    process.exit(1);
  }
}

main().catch(console.error);
