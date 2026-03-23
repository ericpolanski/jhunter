import db from '../db.js';
import { generateFollowUpDraft } from './llm.js';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load Eric's profile
function loadProfile() {
  const profilePath = join(__dirname, '..', '..', 'data', 'eric-profile.json');
  return JSON.parse(readFileSync(profilePath, 'utf-8'));
}

export function getRemindersForApplication(applicationId) {
  return db.prepare(`
    SELECT * FROM reminders
    WHERE application_id = ?
    ORDER BY due_date ASC
  `).all(applicationId);
}

export function getAllActiveReminders() {
  return db.prepare(`
    SELECT r.*, a.status, a.job_id, a.applied_date,
           j.title, j.company_name, j.location
    FROM reminders r
    JOIN applications a ON r.application_id = a.id
    JOIN jobs j ON a.job_id = j.id
    WHERE r.is_dismissed = 0
    ORDER BY r.due_date ASC
  `).all();
}

export function getDueReminders() {
  const today = new Date().toISOString().split('T')[0];
  return db.prepare(`
    SELECT r.*, a.status, a.job_id, a.applied_date,
           j.title, j.company_name, j.location
    FROM reminders r
    JOIN applications a ON r.application_id = a.id
    JOIN jobs j ON a.job_id = j.id
    WHERE r.is_dismissed = 0 AND r.due_date <= ?
    ORDER BY r.due_date ASC
  `).all(today);
}

export function createReminder(applicationId, reminderType, dueDate) {
  // Check if reminder already exists
  const existing = db.prepare(`
    SELECT id FROM reminders
    WHERE application_id = ? AND reminder_type = ?
  `).get(applicationId, reminderType);

  if (existing) {
    return existing;
  }

  const result = db.prepare(`
    INSERT INTO reminders (application_id, reminder_type, due_date, created_at)
    VALUES (?, ?, ?, ?)
  `).run(applicationId, reminderType, dueDate, new Date().toISOString());

  return { id: result.lastInsertRowid };
}

export function dismissReminder(reminderId) {
  db.prepare('UPDATE reminders SET is_dismissed = 1 WHERE id = ?').run(reminderId);
}

export function generateFollowUpForReminder(reminderId) {
  const reminder = db.prepare(`
    SELECT r.*, a.job_id
    FROM reminders r
    JOIN applications a ON r.application_id = a.id
    WHERE r.id = ?
  `).get(reminderId);

  if (!reminder) {
    throw new Error('Reminder not found');
  }

  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(reminder.job_id);
  const company = db.prepare('SELECT * FROM companies WHERE name = ?').get(job.company_name);
  const application = db.prepare('SELECT * FROM applications WHERE id = ?').get(reminder.application_id);

  return generateFollowUpDraft(application, job, company);
}

export function updateReminderFollowUp(reminderId, followUpDraft) {
  db.prepare('UPDATE reminders SET follow_up_draft = ? WHERE id = ?')
    .run(JSON.stringify(followUpDraft), reminderId);
}

export function deleteReminder(reminderId) {
  const result = db.prepare('DELETE FROM reminders WHERE id = ?').run(reminderId);
  return result.changes > 0;
}

// Nightly reminder generation - call this from the scrape script
export async function generateRemindersForAppliedApplications() {
  const appliedApps = db.prepare(`
    SELECT a.*, j.title, j.company_name, j.location
    FROM applications a
    JOIN jobs j ON a.job_id = j.id
    WHERE a.status = 'applied' AND a.applied_date IS NOT NULL
  `).all();

  const today = new Date();
  const results = [];

  for (const app of appliedApps) {
    const appliedDate = new Date(app.applied_date);
    const daysSinceApplied = Math.floor((today - appliedDate) / (1000 * 60 * 60 * 24));

    // Check for 7-day reminder
    if (daysSinceApplied === 7) {
      const dueDate = new Date(today);
      dueDate.setDate(dueDate.getDate() + 7); // Due in 7 days from now (14 days total from application)

      createReminder(app.id, 'follow_up_14', dueDate.toISOString().split('T')[0]);
      results.push({ applicationId: app.id, type: 'follow_up_14', daysSinceApplied });
    }

    // Check for 14-day reminder (7 days after 7-day reminder was created)
    if (daysSinceApplied >= 14) {
      const dueDate = new Date(today);
      dueDate.setDate(dueDate.getDate() + 7); // Due in 7 days

      createReminder(app.id, 'follow_up_21', dueDate.toISOString().split('T')[0]);
      results.push({ applicationId: app.id, type: 'follow_up_21', daysSinceApplied });
    }
  }

  return results;
}

export default {
  getRemindersForApplication,
  getAllActiveReminders,
  getDueReminders,
  createReminder,
  dismissReminder,
  generateFollowUpForReminder,
  updateReminderFollowUp,
  deleteReminder,
  generateRemindersForAppliedApplications
};
