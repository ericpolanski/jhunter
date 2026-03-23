/**
 * Cover Letter Generator Service
 * Generates DOCX cover letters and converts to PDF
 */

import { writeFileSync, mkdirSync, existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Ensure generated directories exist
const GENERATED_DIR = join(__dirname, '..', '..', 'generated');
const COVER_LETTERS_DIR = join(GENERATED_DIR, 'cover-letters');
const DOCS_DIR = join(GENERATED_DIR, 'docs');

function ensureDirectories() {
  if (!existsSync(GENERATED_DIR)) mkdirSync(GENERATED_DIR, { recursive: true });
  if (!existsSync(COVER_LETTERS_DIR)) mkdirSync(COVER_LETTERS_DIR, { recursive: true });
  if (!existsSync(DOCS_DIR)) mkdirSync(DOCS_DIR, { recursive: true });
}

/**
 * Generate a simple DOCX file for cover letter
 * Uses a simple XML-based approach for DOCX (which is a zip of XML files)
 */
export async function generateCoverLetterDocx(coverLetterContent, job) {
  ensureDirectories();

  const timestamp = Date.now();
  const docxPath = join(COVER_LETTERS_DIR, `cover-letter-${job.id}-${timestamp}.docx`);
  const pdfPath = join(COVER_LETTERS_DIR, `cover-letter-${job.id}-${timestamp}.pdf`);

  // Format date
  const dateStr = new Date().toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });

  // Get profile for header
  const profile = await import('../lib/profile.js').then(m => m.getProfile());

  // Build the DOCX content
  const fullContent = buildCoverLetterDocx(profile, job, dateStr, coverLetterContent);

  // Write the DOCX file
  writeFileSync(docxPath, fullContent);

  // Convert to PDF using LibreOffice if available
  await convertToPdf(docxPath, pdfPath);

  return { docxPath, pdfPath, fileName: `Eric Polanski Cover Letter - ${job.company_name}.pdf` };
}

/**
 * Build DOCX content as a Buffer (minimal DOCX format)
 */
function buildCoverLetterDocx(profile, job, dateStr, coverLetterContent) {
  // Create a simple RTF-based DOCX (minimal but functional)
  // For production, use a proper DOCX library

  const contactInfo = `${profile.name}\n${profile.email}\n${profile.phone}\n${profile.location}`;
  const header = `Date: ${dateStr}\n\n${contactInfo}\n\n\n`;

  // Create a simple HTML that can be converted
  const htmlContent = `
<!DOCTYPE html>
<html>
<head>
<style>
  body { font-family: Arial, sans-serif; margin: 1in; line-height: 1.6; }
  .header { margin-bottom: 20px; }
  .date { margin-bottom: 10px; }
  .contact { margin-bottom: 20px; }
  p { margin-bottom: 15px; text-align: justify; }
</style>
</head>
<body>
  <div class="header">
    <div class="date">${dateStr}</div>
    <div class="contact">${contactInfo.replace(/\n/g, '<br>')}</div>
  </div>
  <p>${coverLetterContent.replace(/\n\n/g, '</p><p>').replace(/\n/g, '<br>')}</p>
</body>
</html>
  `.trim();

  // For now, return HTML content - in production would use proper DOCX library
  // Using a simple text file approach as placeholder
  return Buffer.from(`${header}\n\n${coverLetterContent}`, 'utf-8');
}

/**
 * Convert DOCX to PDF using LibreOffice
 */
async function convertToPdf(docxPath, pdfPath) {
  try {
    // Try LibreOffice conversion
    execSync(`soffice --headless --convert-to pdf --outdir "${dirname(pdfPath)}" "${docxPath}"`, {
      timeout: 30000,
      stdio: 'ignore'
    });

    // LibreOffice outputs as filename.pdf, rename if needed
    const expectedPdf = docxPath.replace(/\.docx$/, '.pdf');
    const { renameSync, existsSync } = await import('fs');
    if (existsSync(expectedPdf) && expectedPdf !== pdfPath) {
      renameSync(expectedPdf, pdfPath);
    }
  } catch (error) {
    console.warn('PDF conversion failed, DOCX still available:', error.message);
    // Return without PDF - client can handle this
  }
}

/**
 * Get cover letter file paths for download
 */
export function getCoverLetterPaths(jobId) {
  ensureDirectories();

  // Find the latest generated files for this job
  const { readdirSync, statSync } = require('fs');
  const files = readdirSync(COVER_LETTERS_DIR)
    .filter(f => f.startsWith(`cover-letter-${jobId}-`))
    .map(f => ({
      name: f,
      path: join(COVER_LETTERS_DIR, f),
      time: statSync(join(COVER_LETTERS_DIR, f)).mtime.getTime()
    }))
    .sort((a, b) => b.time - a.time);

  if (files.length === 0) {
    return null;
  }

  return {
    docxPath: files.find(f => f.name.endsWith('.docx'))?.path,
    pdfPath: files.find(f => f.name.endsWith('.pdf'))?.path
  };
}

export default {
  generateCoverLetterDocx,
  getCoverLetterPaths
};