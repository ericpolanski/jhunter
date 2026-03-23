import { getProfile } from '../lib/profile.js';
import * as llm from './llm.js';
import { generateResumeDocx, convertDocxToPdf } from './docx-generator.js';
import db from '../db.js';

/**
 * Resume Tailoring Service
 *
 * Orchestrates the resume tailoring workflow:
 * 1. Get profile + job description
 * 2. Generate tailoring suggestions via LLM
 * 3. Apply suggestions to create tailored content
 * 4. Generate DOCX and convert to PDF
 * 5. Store in database
 */

export async function getTailoringSuggestions(jobId) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) {
    throw new Error('Job not found');
  }

  const profile = getProfile();
  if (!profile) {
    throw new Error('Profile not found');
  }

  // Generate suggestions from LLM
  const suggestions = await llm.generateResumeSuggestions(profile, job.description || '');

  return {
    job: {
      id: job.id,
      title: job.title,
      company_name: job.company_name
    },
    suggestions: suggestions.suggestions || []
  };
}

export async function generateTailoredResume(jobId, acceptedSuggestions) {
  const job = db.prepare('SELECT * FROM jobs WHERE id = ?').get(jobId);
  if (!job) {
    throw new Error('Job not found');
  }

  const profile = getProfile();
  if (!profile) {
    throw new Error('Profile not found');
  }

  const now = new Date().toISOString();
  const fileName = `Eric Polanski Resume - ${job.company_name}.pdf`;

  // Apply accepted suggestions to create tailored sections
  const tailoredSections = applySuggestionsToProfile(profile, acceptedSuggestions);

  try {
    // Generate DOCX - tailoredSections is the profile with suggestions already applied
    const { docxPath, fileName: docxFileName } = await generateResumeDocx(
      tailoredSections,
      null,
      job.company_name
    );

    // Convert to PDF
    let pdfPath = docxPath.replace(/\.docx$/, '.pdf');
    try {
      const { pdfPath: convertedPdf } = await convertDocxToPdf(docxPath);
      pdfPath = convertedPdf;
    } catch (pdfError) {
      console.warn('PDF conversion failed, using DOCX:', pdfError.message);
      // Use DOCX if PDF fails
      pdfPath = docxPath;
    }

    // Store in database
    const result = db.prepare(`
      INSERT INTO resume_versions (
        job_id, suggestions, tailored_sections, docx_path, pdf_path, file_name, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      jobId,
      JSON.stringify(acceptedSuggestions),
      JSON.stringify(tailoredSections),
      docxPath,
      pdfPath,
      fileName,
      now
    );

    return {
      id: result.lastInsertRowid,
      job_id: jobId,
      file_name: fileName,
      docx_path: docxPath,
      pdf_path: pdfPath,
      created_at: now
    };

  } catch (error) {
    console.error('Resume generation error:', error);
    throw error;
  }
}

/**
 * Apply accepted suggestions to profile to create tailored sections
 */
function applySuggestionsToProfile(profile, acceptedSuggestions) {
  const tailored = JSON.parse(JSON.stringify(profile));

  // Map suggestions to profile sections
  for (const suggestion of acceptedSuggestions) {
    const sectionName = suggestion.section?.toLowerCase();

    // Find matching section in profile
    if (sectionName?.includes('experience') || sectionName?.includes('abbvie')) {
      // Apply to experience
      for (const exp of tailored.experience || []) {
        if (exp.company?.toLowerCase().includes(suggestion.section.toLowerCase()) ||
            suggestion.original && exp.roles) {
          for (const role of exp.roles || []) {
            if (role.bullets) {
              role.bullets = role.bullets.map(bullet =>
                bullet === suggestion.original ? suggestion.suggested : bullet
              );
            }
          }
        }
      }
    } else if (sectionName?.includes('project')) {
      // Apply to projects
      for (const proj of tailored.projects || []) {
        if (proj.name?.toLowerCase().includes(suggestion.section.toLowerCase()) ||
            suggestion.original && proj.bullets) {
          proj.bullets = proj.bullets.map(bullet =>
            bullet === suggestion.original ? suggestion.suggested : bullet
          );
        }
      }
    } else if (sectionName?.includes('skill')) {
      // Skills already tailored in suggestions
    }
  }

  return tailored;
}

export default {
  getTailoringSuggestions,
  generateTailoredResume
};
