import { spawn } from 'child_process';
import { promisify } from 'util';
import { writeFileSync, unlinkSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createWriteStream } from 'fs';
import { pipeline } from 'stream/promises';

const exec = promisify(spawn);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, '..', '..');

const GENERATED_DIR = join(projectRoot, 'generated', 'resumes');
const DOCX_CLI = 'dotnet';
const DOCX_PROJECT = join(projectRoot, 'scripts', 'dotnet', 'MiniMaxAIDocx.Cli');

/**
 * Generate a tailored resume DOCX from profile data
 * @param {Object} profile - Eric's profile data
 * @param {Array} tailoredSections - Array of tailored sections with original and suggested text
 * @param {string} companyName - Company name for the filename
 * @returns {Promise<{docxPath: string, fileName: string}>}
 */
export async function generateResumeDocx(profile, tailoredSections, companyName) {
  const fileName = `Eric Polanski Resume - ${companyName.replace(/[^a-zA-Z0-9]/g, '_')}.docx`;
  const docxPath = join(GENERATED_DIR, fileName.replace('.docx', '.docx'));

  // Build resume content JSON for minimax-docx
  const content = buildResumeContent(profile, tailoredSections);
  const configPath = join(GENERATED_DIR, `config_${Date.now()}.json`);
  writeFileSync(configPath, JSON.stringify(content, null, 2));

  try {
    // Use minimax-docx CLI to create the document
    const args = [
      'run', '--project', DOCX_PROJECT,
      '--', 'create',
      '--type', 'report',
      '--output', docxPath,
      '--config', configPath,
      '--page-size', 'letter',
      '--margins', 'narrow'
    ];

    const result = await exec(DOCX_CLI, args, {
      cwd: projectRoot,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    if (result.stderr && result.stderr.toString().includes('error')) {
      throw new Error(result.stderr.toString());
    }

    console.log(`Generated DOCX: ${docxPath}`);
    return { docxPath, fileName };
  } catch (error) {
    console.error('DOCX generation error:', error.message);
    // Fallback: create a simple text-based DOCX
    return createSimpleDocx(profile, tailoredSections, companyName);
  } finally {
    // Clean up temp config
    try { unlinkSync(configPath); } catch {}
  }
}

/**
 * Build resume content structure for minimax-docx
 */
function buildResumeContent(profile, tailoredSections) {
  const sections = [];

  // Header with name and contact info
  sections.push({
    type: 'heading',
    level: 1,
    text: `${profile.name || 'Eric Polanski'}`
  });

  // Contact info
  const contact = [];
  if (profile.email) contact.push(profile.email);
  if (profile.phone) contact.push(profile.phone);
  if (profile.location) contact.push(profile.location);
  if (profile.linkedin) contact.push(profile.linkedin);

  sections.push({
    type: 'paragraph',
    text: contact.join(' | ')
  });

  // Summary
  if (profile.summary) {
    sections.push({
      type: 'heading',
      level: 2,
      text: 'Professional Summary'
    });
    sections.push({
      type: 'paragraph',
      text: profile.summary
    });
  }

  // Tailored sections (experience, projects, skills)
  const sectionNames = {
    'experience': 'Professional Experience',
    'project': 'Projects',
    'skills': 'Technical Skills',
    'education': 'Education'
  };

  for (const [key, title] of Object.entries(sectionNames)) {
    const sectionData = profile[key] || profile[key + 's'];
    if (sectionData && sectionData.length > 0) {
      sections.push({
        type: 'heading',
        level: 2,
        text: title
      });

      for (const item of sectionData) {
        // Check if there's a tailored version for this item (only if tailoredSections is an array)
        let desc;
        if (Array.isArray(tailoredSections)) {
          const tailored = tailoredSections.find(t =>
            t.section === item.name || t.section === key
          );
          desc = tailored?.suggested || item.description || item.details;
        } else {
          // Suggestions already applied to profile, use description directly
          desc = item.description || item.details;
        }

        const title = item.title || item.name;
        const org = item.org || item.company || item.school;
        const date = item.date || item.period;

        sections.push({
          type: 'paragraph',
          text: `${title}${org ? ` | ${org}` : ''}${date ? ` | ${date}` : ''}`
        });

        if (Array.isArray(desc)) {
          for (const d of desc) {
            sections.push({
              type: 'bullet',
              text: d
            });
          }
        } else if (desc) {
          sections.push({
            type: 'paragraph',
            text: typeof desc === 'object' ? JSON.stringify(desc) : desc
          });
        }
      }
    }
  }

  return {
    title: 'Resume',
    author: 'Eric Polanski',
    sections
  };
}

/**
 * Fallback: Create simple DOCX using direct approach
 */
async function createSimpleDocx(profile, tailoredSections, companyName) {
  const fileName = `Eric Polanski Resume - ${companyName.replace(/[^a-zA-Z0-9]/g, '_')}.docx`;
  const docxPath = join(GENERATED_DIR, fileName);

  // Simple content JSON
  const content = {
    title: 'Resume',
    author: 'Eric Polanski',
    sections: [
      { type: 'heading', level: 1, text: profile.name || 'Eric Polanski' },
      { type: 'paragraph', text: `${profile.email || ''} | ${profile.phone || ''} | ${profile.location || ''}` }
    ]
  };

  const configPath = join(GENERATED_DIR, `config_simple_${Date.now()}.json`);
  writeFileSync(configPath, JSON.stringify(content));

  try {
    await exec(DOCX_CLI, [
      'run', '--project', DOCX_PROJECT, '--', 'create',
      '--type', 'report',
      '--output', docxPath,
      '--config', configPath,
      '--page-size', 'letter'
    ], { cwd: projectRoot });
  } finally {
    try { unlinkSync(configPath); } catch {}
  }

  return { docxPath, fileName };
}

/**
 * Convert DOCX to PDF using LibreOffice
 * @param {string} docxPath - Path to DOCX file
 * @returns {Promise<{pdfPath: string}>}
 */
export async function convertDocxToPdf(docxPath) {
  const pdfPath = docxPath.replace(/\.docx?$/i, '.pdf');

  return new Promise((resolve, reject) => {
    const soffice = spawn('soffice', [
      '--headless',
      '--convert-to', 'pdf',
      '--outdir', dirname(docxPath),
      docxPath
    ], {
      stdio: ['pipe', 'pipe', 'pipe']
    });

    soffice.on('close', (code) => {
      if (code === 0) {
        console.log(`Converted to PDF: ${pdfPath}`);
        resolve({ pdfPath });
      } else {
        reject(new Error(`LibreOffice conversion failed with code ${code}`));
      }
    });

    soffice.on('error', reject);
  });
}

export default { generateResumeDocx, convertDocxToPdf };
