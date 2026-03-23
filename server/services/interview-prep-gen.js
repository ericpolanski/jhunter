import db from '../db.js';
import { generateInterviewPrep } from './llm.js';
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

export async function generateInterviewPrepForJob(jobId) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) {
    throw new Error('Job not found');
  }

  const profile = loadProfile();

  // Get company info if exists
  let companyDossier = null;
  if (job.company_id) {
    const company = db.prepare('SELECT * FROM companies WHERE id = ?').get(job.company_id);
    if (company && company.dossier_raw) {
      companyDossier = company.dossier_raw;
    }
  }

  // Generate interview prep via LLM
  const prep = await generateInterviewPrep(profile, job.description, companyDossier);

  // Store in database
  const existing = db.prepare('SELECT id FROM interview_prep WHERE job_id = ?').get(jobId);

  if (existing) {
    db.prepare(`
      UPDATE interview_prep
      SET likely_questions = ?, talking_points = ?, role_specific_prep = ?
      WHERE job_id = ?
    `).run(
      JSON.stringify(prep.likely_questions),
      JSON.stringify(prep.talking_points),
      prep.role_specific_prep,
      jobId
    );
  } else {
    db.prepare(`
      INSERT INTO interview_prep (job_id, likely_questions, talking_points, role_specific_prep, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(
      jobId,
      JSON.stringify(prep.likely_questions),
      JSON.stringify(prep.talking_points),
      prep.role_specific_prep,
      new Date().toISOString()
    );
  }

  return prep;
}

export function getInterviewPrepForJob(jobId) {
  const prep = db.prepare('SELECT * FROM interview_prep WHERE job_id = ?').get(jobId);
  if (!prep) return null;

  return {
    ...prep,
    likely_questions: JSON.parse(prep.likely_questions || '[]'),
    talking_points: JSON.parse(prep.talking_points || '[]')
  };
}

export function getAllInterviewPrep() {
  const preps = db.prepare(`
    SELECT ip.*, j.title, j.company_name, j.location
    FROM interview_prep ip
    JOIN jobs j ON ip.job_id = j.id
    ORDER BY ip.created_at DESC
  `).all();

  return preps.map(prep => ({
    ...prep,
    likely_questions: JSON.parse(prep.likely_questions || '[]'),
    talking_points: JSON.parse(prep.talking_points || '[]')
  }));
}

export function deleteInterviewPrep(jobId) {
  const result = db.prepare('DELETE FROM interview_prep WHERE job_id = ?').run(jobId);
  return result.changes > 0;
}

export default { generateInterviewPrepForJob, getInterviewPrepForJob, getAllInterviewPrep, deleteInterviewPrep };
