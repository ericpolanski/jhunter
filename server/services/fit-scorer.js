import { getProfile } from '../lib/profile.js';
import * as llm from './llm.js';

/**
 * Fit Scoring Service
 *
 * Two-phase scoring:
 * 1. Heuristic: fast scoring based on profile vs job description
 * 2. LLM Refinement: for jobs scoring >= 40
 */

const WEIGHTS = {
  skill_match: 0.40,
  location_match: 0.20,
  experience_match: 0.15,
  role_match: 0.15,
  salary_match: 0.10
};

// Keywords for matching
const ROLE_KEYWORDS = {
  ai_engineer: ['ai engineer', 'artificial intelligence', 'machine learning engineer', 'ml engineer', 'ai/ml'],
  software_engineer: ['software engineer', 'software developer', 'swe', 'full stack', 'backend', 'frontend', 'full-stack'],
  data_scientist: ['data scientist', 'data analyst', 'analytics', 'data engineer'],
  devops: ['devops', 'sre', 'site reliability', 'platform engineer']
};

const LOCATION_KEYWORDS = ['chicago', 'il', 'illinois', 'evanston', 'remote', 'hybrid'];
const SKILL_KEYWORDS = {
  python: ['python', 'pandas', 'numpy', 'scipy'],
  ml: ['machine learning', 'ml', 'deep learning', 'neural network', 'pytorch', 'tensorflow', 'keras'],
  data: ['data pipeline', 'etl', 'data processing', 'data engineering', 'sql'],
  cloud: ['aws', 'gcp', 'azure', 'google cloud'],
  api: ['api', 'rest', 'restful', 'http'],
  docker: ['docker', 'kubernetes', 'k8s', 'container'],
  git: ['git', 'github', 'version control'],
  nlp: ['nlp', 'natural language', 'text', 'llm', 'gpt', 'transformers']
};

/**
 * Calculate heuristic fit score
 * @param {Object} profile - Eric's profile
 * @param {Object} job - Job record
 * @returns {Object} - { score, breakdown }
 */
export function calculateHeuristicScore(profile, job) {
  const breakdown = {
    skill_match: 0,
    location_match: 0,
    experience_match: 0,
    role_match: 0,
    salary_match: 0
  };

  const jobText = `${job.title} ${job.description} ${job.location}`.toLowerCase();

  // 1. Skill Match (40%)
  const profileSkills = [
    ...(profile.skills?.languages || []),
    ...(profile.skills?.frameworks || []),
    ...(profile.skills?.tools || [])
  ].map(s => s.toLowerCase());

  let matchedSkills = 0;
  let totalSkills = 0;

  for (const [category, keywords] of Object.entries(SKILL_KEYWORDS)) {
    const profileHas = profileSkills.some(s => s.includes(category) || keywords.some(k => s.includes(k)));
    const jobHas = keywords.some(k => jobText.includes(k));

    if (jobHas) {
      totalSkills++;
      if (profileHas) matchedSkills++;
    }
  }

  breakdown.skill_match = totalSkills > 0 ? Math.round((matchedSkills / totalSkills) * 100) : 50;

  // 2. Location Match (20%)
  const profileLocation = (profile.location || '').toLowerCase();
  const jobLocation = (job.location || '').toLowerCase();

  // Check if job is in Chicago area or remote
  const isChicagoArea = jobLocation.includes('chicago') ||
                        jobLocation.includes('il') ||
                        jobLocation.includes('evanston') ||
                        jobLocation.includes('northwestern');
  const isRemote = jobLocation.includes('remote') || job.is_remote;

  if (isRemote) {
    breakdown.location_match = 100;
  } else if (isChicagoArea && (profileLocation.includes('chicago') || profileLocation.includes('il') || profileLocation.includes('evanston'))) {
    breakdown.location_match = 100;
  } else if (isChicagoArea || profileLocation.includes('chicago') || profileLocation.includes('il')) {
    breakdown.location_match = 70;
  } else {
    breakdown.location_match = 30;
  }

  // 3. Experience Match (15%) - Entry level preferred
  const experienceYears = estimateExperienceYears(profile.experience);
  const jobRequiresSenior = jobText.includes('senior') ||
                              jobText.includes('5+ years') ||
                              jobText.includes('7+ years') ||
                              jobText.includes('principal') ||
                              jobText.includes('staff engineer');

  if (jobRequiresSenior && experienceYears < 3) {
    breakdown.experience_match = 30;
  } else if (experienceYears >= 1) {
    breakdown.experience_match = 80;
  } else {
    breakdown.experience_match = 60;
  }

  // 4. Role Match (15%)
  let roleMatches = 0;
  let totalRoleChecks = 0;

  for (const [role, keywords] of Object.entries(ROLE_KEYWORDS)) {
    const jobHasRole = keywords.some(k => jobText.includes(k));
    if (jobHasRole) {
      totalRoleChecks++;
      // Check if profile has relevant background
      if (role === 'ai_engineer' && (profileSkills.some(s => s.includes('ml') || s.includes('ai') || s.includes('pytorch')))) {
        roleMatches++;
      } else if (role === 'software_engineer' && (profileSkills.some(s => s.includes('python') || s.includes('javascript') || s.includes('java')))) {
        roleMatches++;
      } else if (role === 'data_scientist' && (profileSkills.some(s => s.includes('python') || s.includes('sql') || s.includes('statistics')))) {
        roleMatches++;
      } else {
        roleMatches++; // Generic match
      }
    }
  }

  breakdown.role_match = totalRoleChecks > 0 ? Math.round((roleMatches / totalRoleChecks) * 100) : 50;

  // 5. Salary Match (10%)
  const profileMinSalary = 50000;
  const profileMaxSalary = 120000;

  if (job.salary_min && job.salary_max) {
    const jobMid = (job.salary_min + job.salary_max) / 2;
    if (jobMid >= profileMinSalary && jobMid <= profileMaxSalary) {
      breakdown.salary_match = 100;
    } else if (jobMid < profileMinSalary) {
      breakdown.salary_match = Math.max(0, 100 - Math.round((profileMinSalary - jobMid) / 1000));
    } else {
      breakdown.salary_match = Math.max(0, 100 - Math.round((jobMid - profileMaxSalary) / 2000));
    }
  } else if (job.salary_text) {
    // Try to parse from text
    breakdown.salary_match = 50; // Unknown
  } else {
    breakdown.salary_match = 50; // No salary info
  }

  // Calculate weighted score
  const score = Math.round(
    breakdown.skill_match * WEIGHTS.skill_match +
    breakdown.location_match * WEIGHTS.location_match +
    breakdown.experience_match * WEIGHTS.experience_match +
    breakdown.role_match * WEIGHTS.role_match +
    breakdown.salary_match * WEIGHTS.salary_match
  );

  return { score, breakdown };
}

/**
 * Estimate years of experience from profile
 */
function estimateExperienceYears(experience) {
  if (!experience || !Array.isArray(experience)) return 0;

  let totalMonths = 0;
  for (const exp of experience) {
    if (exp.roles && Array.isArray(exp.roles)) {
      for (const role of exp.roles) {
        // Parse dates if available, otherwise estimate
        totalMonths += 3; // Assume ~3 months per internship/role
      }
    }
  }

  return Math.round(totalMonths / 12);
}

/**
 * Get or calculate fit score for a job
 * Uses LLM refinement for scores >= 40
 * @param {number} jobId - Job ID
 * @returns {Object} - { fit_score, fit_breakdown }
 */
export async function getFitScore(jobId, db) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) {
    throw new Error('Job not found');
  }

  // Return existing score if already calculated with LLM
  if (job.fit_score && job.fit_breakdown && job.fit_score >= 40) {
    try {
      return {
        fit_score: job.fit_score,
        fit_breakdown: JSON.parse(job.fit_breakdown)
      };
    } catch {
      // If parsing fails, recalculate
    }
  }

  // Calculate heuristic score
  const profile = getProfile();
  const { score, breakdown } = calculateHeuristicScore(profile, job);

  // Use LLM refinement if score >= 40
  if (score >= 40 && job.description) {
    try {
      const llmResult = await llm.generateFitScore(profile, job.description);
      return {
        fit_score: llmResult.score,
        fit_breakdown: llmResult.breakdown
      };
    } catch (error) {
      console.warn('LLM refinement failed, using heuristic:', error.message);
    }
  }

  return { fit_score: score, fit_breakdown: breakdown };
}

/**
 * Score a job and update in database
 */
export async function scoreJob(jobId, db) {
  const { fit_score, fit_breakdown } = await getFitScore(jobId, db);

  db.prepare(`
    UPDATE jobs SET fit_score = ?, fit_breakdown = ? WHERE id = ?
  `).run(fit_score, JSON.stringify(fit_breakdown), jobId);

  return { fit_score, fit_breakdown };
}

export default {
  calculateHeuristicScore,
  getFitScore,
  scoreJob
};
