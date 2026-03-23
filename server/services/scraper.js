import crypto from 'crypto';
import db from '../db.js';
import { scoreJob } from './fit-scorer.js';

// Lazy-load the API key at function call time to avoid ES module import ordering issues
const getScrapflyKey = () => process.env.SCRAPFLY_API_KEY;
const SCRAPFLY_BASE = 'https://api.scrapfly.io';

const CHICAGO_LOCATIONS = ['chicago', 'evanston', 'gurnee', 'north chicago', 'lincolnshire', 'grayslake', 'waukegan', 'lake county'];
const ROLES = ['AI Engineer', 'Software Engineer', 'Machine Learning Engineer'];
const SOURCES = ['linkedin', 'indeed', 'glassdoor'];

// Target companies in Chicagoland area for direct career page scraping
const DIRECT_COMPANIES = [
  'AbbVie', 'Allstate', 'Aon', 'United Airlines', 'Boeing',
  'CDK Global', 'Morningstar', 'CNA Insurance', 'Kemper',
  'Grubhub', 'SpotHero', 'Base', 'Sprout Social', 'Inventive',
  'Kaplan', 'Allstate', 'US Foods', 'Molex', 'Horizon Therapeutics'
];

function scrapflyHeaders() {
  return {
    'Content-Type': 'application/json'
  };
}

async function scrapflyScrape(url, config = {}) {
  const params = new URLSearchParams({
    key: getScrapflyKey(),
    url,
    asp: 'true',
    render_js: 'true',
    country: 'US',
    ...config
  });

  const response = await fetch(`${SCRAPFLY_BASE}/scrape?${params}`);

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Scrapfly error ${response.status}: ${error}`);
  }

  return response.json();
}

async function scrapflyExtract(body, url, extractionPrompt) {
  const params = new URLSearchParams({
    key: getScrapflyKey(),
    extraction_prompt: extractionPrompt
  });
  const response = await fetch(`${SCRAPFLY_BASE}/extraction?${params}`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/html' },
    body
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Scrapfly extraction error ${response.status}: ${error}`);
  }

  return response.json();
}

function hashUrl(url) {
  // Normalize URL: lowercase, strip tracking params
  const normalized = url.split('?')[0].toLowerCase().trim();
  return crypto.createHash('sha256').update(normalized).digest('hex');
}

function decodeHtmlEntities(text) {
  const entities = {
    '&nbsp;': ' ', '&nbsp': ' ', '&#160;': ' ', '&#xA0;': ' ', '\u00A0': ' ',
    '&amp;': '&', '&#38;': '&',
    '&lt;': '<', '&#60;': '<', '&#x3C;': '<',
    '&gt;': '>', '&#62;': '>', '&#x3E;': '>',
    '&quot;': '"', '&#34;': '"', '&#x22;': '"',
    '&apos;': "'", '&#39;': "'", '&#x27;': "'",
    '&mdash;': '\u2014', '&#8211;': '\u2013', '&ndash;': '\u2013',
    '&lsquo;': '\u2018', '&rsquo;': '\u2019', '&ldquo;': '\u201C', '&rdquo;': '\u201D',
    '&hellip;': '\u2026', '&#8230;': '\u2026',
    '&copy;': '\u00A9', '&reg;': '\u00AE', '&trade;': '\u2122',
    '&deg;': '\u00B0', '&plusmn;': '\u00B1', '&times;': '\u00D7', '&divide;': '\u00F7',
    '&cent;': '\u00A2', '&pound;': '\u00A3', '&euro;': '\u20AC', '&yen;': '\u00A5',
  };
  // First handle numeric/hex entities
  let result = text.replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code, 10)));
  result = result.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
  // Then handle named entities (case-insensitive)
  for (const [entity, char] of Object.entries(entities)) {
    result = result.replace(new RegExp(entity.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'), char);
  }
  return result;
}

function parseSalary(text) {
  if (!text) return { min: null, max: null, text: null };
  // Handle "$120K - $180K", "$80,000 - $120,000", "120000", etc.
  const clean = text.replace(/[$,kK]/g, '').trim();
  const numbers = clean.match(/[\d]+/g);
  if (!numbers) return { min: null, max: null, text };

  const vals = numbers.map(n => parseInt(n, 10));
  // If values look like they're in thousands (e.g., 80-120), assume K
  const isThousands = vals.some(v => v < 500);

  const min = isThousands ? vals[0] * 1000 : vals[0];
  const max = vals.length > 1 ? (isThousands ? vals[1] * 1000 : vals[1]) : min;

  return { min, max, text };
}

function isChicagoland(location) {
  if (!location) return false;
  const loc = location.toLowerCase();
  return CHICAGO_LOCATIONS.some(c => loc.includes(c)) ||
    /\b(il|illinois)\b/i.test(loc) ||
    /\b(chicago|northbrook|evanston|gurnee|waukegan)\b/i.test(loc);
}

function isRemote(location) {
  if (!location) return false;
  return /\bremote\b/i.test(location);
}

function jobMatchesTarget(job) {
  const loc = (job.location || '').toLowerCase();
  const title = (job.title || '').toLowerCase();
  // Must be in Illinois or remote OR company is known Chicagoland
  const isIL = /\bil\b/i.test(loc) || isChicagoland(loc);
  const isRemoteJob = isRemote(loc);
  if (!isIL && !isRemoteJob) return false;

  // Title should match target roles
  const roleMatch = ROLES.some(r => title.includes(r.toLowerCase()));
  return roleMatch;
}

function stripHtmlWithNewlines(html) {
  if (!html) return '';
  return html
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, '')
    .replace(/<header[^>]*>[\s\S]*?<\/header>/gi, '')
    .replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, '')
    .replace(/<aside[^>]*>[\s\S]*?<\/aside>/gi, '')
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<\/(p|div|h[1-6]|li|tr|article|section)>/gi, '\n\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(parseInt(c, 10)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/[ \t]+/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// ─── LinkedIn Scraper ───────────────────────────────────────────────────────

async function scrapeLinkedIn() {
  const jobs = [];

  for (const role of ROLES) {
    // Search 1: Chicago/Illinois area
    const query = encodeURIComponent(`${role} entry level`);
    const chicagoUrl = `https://www.linkedin.com/jobs/search/?keywords=${query}&location=Chicago%2C%20IL&f_E=2&f_TPR=r604800&locationId=`;

    // Search 2: Remote jobs (nationwide)
    const remoteUrl = `https://www.linkedin.com/jobs/search/?keywords=${encodeURIComponent(role)}&location=United%20States&f_WT=2&f_TPR=r604800`;

    const urls = [
      { url: chicagoUrl, defaultLoc: 'Chicago, IL' },
      { url: remoteUrl, defaultLoc: 'Remote, USA' }
    ];

    for (const { url, defaultLoc } of urls) {
      try {
        const result = await scrapflyScrape(url, { rendering_wait: 5000 });

        const extracted = await scrapflyExtract(
          result.result.content,
          url,
          `Extract all job listings from this LinkedIn search results page. For each job return a JSON array with: title, company_name, location, source_url (the job detail link), posted_date (as YYYY-MM-DD or relative like "2 days ago"), salary_text (if visible). Return ONLY valid JSON array.`
        );

        let listings = [];
        try {
          // extraction API returns data directly as array, not as extracted_content property
          listings = Array.isArray(extracted.data) ? extracted.data : JSON.parse(extracted.data?.extracted_content || '[]');
        } catch {
          // Fallback: try to extract from markdown
          const markdown = extracted.data?.extracted_content || '';
          // Try common patterns
          const lines = markdown.split('\n').filter(l => l.trim());
          for (const line of lines) {
            const titleMatch = line.match(/\[([^\]]+)\]/);
            const companyMatch = line.match(/at\s+([^(]+)/);
            if (titleMatch) {
              listings.push({
                title: titleMatch[1],
                company_name: companyMatch ? companyMatch[1].trim() : 'Unknown',
                location: 'Remote',
                source_url: '',
                posted_date: null,
                salary_text: null
              });
            }
          }
        }

        for (const job of listings) {
          if (!job.source_url || job.source_url === '#') continue;
          jobs.push({
            source: 'linkedin',
            source_url: job.source_url,
            title: job.title,
            company_name: job.company_name,
            location: job.location ? decodeHtmlEntities(job.location) : defaultLoc,
            salary_text: job.salary_text,
            posted_date: job.posted_date,
            description: '',
            requirements: '[]'
          });
        }
      } catch (e) {
        console.error(`LinkedIn scrape error for ${role} (${defaultLoc}):`, e.message);
      }

      // Rate limit between requests
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  return jobs;
}

// ─── Indeed Scraper ─────────────────────────────────────────────────────────

async function scrapeIndeed() {
  const jobs = [];

  for (const role of ROLES) {
    const query = encodeURIComponent(role);

    // Search 1: Gurnee, IL area (Chicagoland)
    const location = encodeURIComponent('Gurnee, IL');
    const chicagoUrl = `https://www.indeed.com/jobs?q=${query}&l=${location}&radius=30&fromage=3&sort=date`;

    // Search 2: Remote jobs
    const remoteUrl = `https://www.indeed.com/jobs?q=${query}&l=remote&fromage=3&sort=date`;

    const urls = [
      { url: chicagoUrl, defaultLoc: 'Gurnee, IL' },
      { url: remoteUrl, defaultLoc: 'Remote' }
    ];

    for (const { url, defaultLoc } of urls) {
      try {
        const result = await scrapflyScrape(url, { rendering_wait: 4000 });

        const extracted = await scrapflyExtract(
          result.result.content,
          url,
          `Extract all job listings from this Indeed search results page. For each job return a JSON array with: title, company_name, location, source_url (the complete indeed job URL), posted_date (as YYYY-MM-DD or relative like "3 days ago"), salary_text (if visible). Return ONLY valid JSON array.`
        );

        let listings = [];
        try {
          // extraction API returns data directly as array
          listings = Array.isArray(extracted.data) ? extracted.data : JSON.parse(extracted.data?.extracted_content || '[]');
        } catch {
          // Try HTML parsing from result if extraction fails
          const html = result.result.content;
          // Simple regex-based fallback for job cards
          const cardRegex = /<a[^>]+href="(\/rc\/clk\?[^"]+)"[^>]*>.*?<女星[^>]*>([^<]+)<\/span>.*?<div[^>]*class="[^"]*company[^"]*"[^>]*>([^<]+)<\/div>/gs;
          let match;
          while ((match = cardRegex.exec(html)) !== null && listings.length < 30) {
            const href = match[1];
            const title = match[2]?.trim();
            const company = match[3]?.trim();
            if (title && company) {
              listings.push({
                title,
                company_name: company,
                location: 'Remote',
                source_url: href.startsWith('http') ? href : `https://www.indeed.com${href}`,
                posted_date: null,
                salary_text: null
              });
            }
          }
        }

        for (const job of listings) {
          if (!job.source_url) continue;
          const fullUrl = job.source_url.startsWith('http')
            ? job.source_url
            : `https://www.indeed.com${job.source_url}`;

          jobs.push({
            source: 'indeed',
            source_url: fullUrl,
            title: job.title,
            company_name: job.company_name,
            location: job.location ? decodeHtmlEntities(job.location) : defaultLoc,
            salary_text: job.salary_text,
            posted_date: job.posted_date,
            description: '',
            requirements: '[]'
          });
        }
      } catch (e) {
        console.error(`Indeed scrape error for ${role}:`, e.message);
      }

      await new Promise(r => setTimeout(r, 2000));
    }
  }

  return jobs;
}

// ─── Glassdoor Scraper ──────────────────────────────────────────────────────

async function scrapeGlassdoor() {
  const jobs = [];

  for (const role of ROLES) {
    const query = encodeURIComponent(role);

    // Search 1: Chicago area
    const chicagoUrl = `https://www.glassdoor.com/Job/chicago-${query}-jobs-SRCH_IL.0_7.htm`;

    // Search 2: Remote jobs (using "Remote" in search)
    const remoteUrl = `https://www.glassdoor.com/Job/remote-${query}-jobs-SRCH_IL.0_7.htm`;

    const urls = [
      { url: chicagoUrl, defaultLoc: 'Chicago, IL' },
      { url: remoteUrl, defaultLoc: 'Remote' }
    ];

    for (const { url, defaultLoc } of urls) {
      try {
        const result = await scrapflyScrape(url, { rendering_wait: 5000 });

        const extracted = await scrapflyExtract(
          result.result.content,
          url,
          `Extract all job listings from this Glassdoor search results page. For each job return a JSON array with: title, company_name, location, source_url (the job detail URL), posted_date, salary_text. Return ONLY valid JSON array.`
        );

        let listings = [];
        try {
          // extraction API returns data directly as array
          listings = Array.isArray(extracted.data) ? extracted.data : JSON.parse(extracted.data?.extracted_content || '[]');
        } catch {
          // Fallback
        }

        for (const job of listings) {
          if (!job.source_url) continue;
          const fullUrl = job.source_url.startsWith('http')
            ? job.source_url
            : `https://www.glassdoor.com${job.source_url}`;

          jobs.push({
            source: 'glassdoor',
            source_url: fullUrl,
            title: job.title,
            company_name: job.company_name,
            location: job.location ? decodeHtmlEntities(job.location) : defaultLoc,
            salary_text: job.salary_text,
            posted_date: job.posted_date,
            description: '',
            requirements: '[]'
          });
        }
      } catch (e) {
        console.error(`Glassdoor scrape error for ${role}:`, e.message);
      }

      await new Promise(r => setTimeout(r, 2000));
    }
  }

  return jobs;
}

// ─── Scrape detail page for a single job ───────────────────────────────────

async function scrapeJobDetail(job) {
  if (!job.source_url) return null;

  try {
    const result = await scrapflyScrape(job.source_url, { rendering_wait: 3000 });

    const extracted = await scrapflyExtract(
      result.result.content,
      job.source_url,
      `Extract the full job details from this job posting page. Return JSON with: title, company_name, location, salary_text, description (full job description text), requirements (array of requirement strings), apply_url (the direct link to apply).`
    );

    let detail = {};
    try {
      // extraction API returns data directly as object for single items
      detail = (typeof extracted.data === 'object' && !Array.isArray(extracted.data)) ? extracted.data : JSON.parse(extracted.data?.extracted_content || '{}');
    } catch {
      detail = {};
    }

    // Bug 4: Extract salary from JSON-LD structured data and data-salary attributes
    const rawHtml = result.result.content || '';
    let salaryMin = null;
    let salaryMax = null;
    let salaryText = detail.salary_text || job.salary_text;

    // Try JSON-LD first
    const jsonLdMatch = rawHtml.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
    if (jsonLdMatch) {
      try {
        const jsonLd = JSON.parse(jsonLdMatch[1]);
        if (jsonLd.baseSalary) {
          const bs = jsonLd.baseSalary;
          if (bs.minValue !== undefined) salaryMin = Math.round(bs.minValue);
          if (bs.maxValue !== undefined) salaryMax = Math.round(bs.maxValue);
          if (bs.currency) salaryText = `${bs.currency} ${salaryMin}-${salaryMax}`;
        }
      } catch {}
    }

    // Try data-salary attribute
    const dataSalaryMatch = rawHtml.match(/data-(?:salary|compensation|pay)=["']([^"']+)["']/i);
    if (dataSalaryMatch && dataSalaryMatch[1]) {
      const p = parseSalary(dataSalaryMatch[1]);
      if (p.min !== null) salaryMin = p.min;
      if (p.max !== null) salaryMax = p.max;
      if (p.text) salaryText = p.text;
    }

    const salary = parseSalary(salaryText);
    salaryMin = salaryMin ?? salary.min;
    salaryMax = salaryMax ?? salary.max;

    // Bug 1 & 2: Description capture with 20000 char limit, stop at apply/footer
    let description = detail.description || '';
    if (rawHtml && !description) {
      // Bug 2: HTML-based description capture with 20000 char limit
      const descPatterns = [
        /<div[^>]+class=["'][^"']*(?:description|job-description|jobdetail|jd|job-content|job-body)[^"']*["'][^>]*>([\s\S]{500,20000}?)(?=(?:<div[^>]+class=["'][^"']*(?:apply|application|submit)[^"']*["']|<footer|<\/article>))/i,
        /<article[^>]*>([\s\S]{500,20000}?)<(?:aside|footer|nav|div[^>]+class=["'][^"']*(?:apply|application)[^"']*["'])/i,
      ];
      for (const pattern of descPatterns) {
        const match = rawHtml.match(pattern);
        if (match && match[1]) {
          description = match[1];
          break;
        }
      }
    }

    // Bug 1: Location fallback to description body text
    let extractedLocation = detail.location ? decodeHtmlEntities(detail.location) : (job.location ? decodeHtmlEntities(job.location) : null);
    if (!extractedLocation && description) {
      const bodyLocationMatch = description.match(/(?:based\s+(?:in|near)|location[:]\s*|role\s+is\s+(?:in|near))\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2}(?:\s+\d{5})?)/i);
      if (bodyLocationMatch && bodyLocationMatch[1]) {
        extractedLocation = bodyLocationMatch[1].trim();
      }
    }

    // Bug 7: Preserve paragraph breaks
    if (description) {
      description = stripHtmlWithNewlines(description);
    }

    // Bug 3: Requirements from clean HTML
    let requirements = job.requirements;
    if (detail.requirements && Array.isArray(detail.requirements)) {
      requirements = JSON.stringify(detail.requirements);
    } else if (rawHtml) {
      // Extract requirements from clean HTML
      const cleanHtml = rawHtml
        .replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, '')
        .replace(/<header[^>]*>[\s\S]*?<\/header>/gi, '')
        .replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, '')
        .replace(/<aside[^>]*>[\s\S]*?<\/aside>/gi, '')
        .replace(/<!--[\s\S]*?-->/g, '');
      const reqPatterns = [
        /<li[^>]*>([\s\S]{10,300}?)<\/li>/gi,
        /<p[^>]*>([\s\S]{20,500}?)<\/p>/gi,
      ];
      const reqs = [];
      const seenReqs = new Set();
      for (const pattern of reqPatterns) {
        const matches = [...cleanHtml.matchAll(pattern)];
        for (const match of matches) {
          const text = stripHtmlWithNewlines(match[1]).trim();
          if (text.length > 15 && text.length < 300 &&
              !seenReqs.has(text) &&
              !text.match(/^(Job|Apply|Career|Amazon|Google|Microsoft|Company|Job categories|My career|Sign\s+in|Account|Categories|Navigation|Browse)/i) &&
              !text.includes('http') && !text.includes('javascript')) {
            seenReqs.add(text);
            reqs.push(text);
          }
        }
      }
      if (reqs.length > 0) {
        requirements = JSON.stringify(reqs);
      }
    }

    // Bug 6: Benefits extracted as structured field (=== BENEFITS === section)
    let benefitsSection = '';
    if (rawHtml) {
      const benefitsPatterns = [
        /<li[^>]+class=["'][^"']*(?:benefit|perk)[^"']*["'][^>]*>([\s\S]{10,500}?)<\/li>/gi,
        /<(?:ul|ol)[^>]*class=["'][^"']*(?:benefits|perks|what-we-offer)[^"']*["'][^>]*>([\s\S]{100,5000}?)<\/(?:ul|ol)>/gi,
      ];
      const benefits = [];
      for (const pattern of benefitsPatterns) {
        const matches = [...rawHtml.matchAll(pattern)];
        for (const match of matches) {
          const text = stripHtmlWithNewlines(match[1]).trim();
          if (text.length > 5 && text.length < 300 && !benefits.includes(text)) {
            benefits.push(text);
          }
        }
      }
      if (benefits.length > 0) {
        benefitsSection = '\n\n=== BENEFITS ===\n' + benefits.map(b => '- ' + b).join('\n');
      }
    }

    const finalDescription = description + benefitsSection;

    return {
      ...job,
      title: detail.title || job.title,
      company_name: detail.company_name || job.company_name,
      location: extractedLocation,
      salary_text: salary.text,
      salary_min: salaryMin,
      salary_max: salaryMax,
      description: finalDescription,
      requirements: requirements,
      source_url: detail.apply_url || job.source_url
    };
  } catch (e) {
    // Return original job data if detail scrape fails
    return {
      ...job,
      salary_text: parseSalary(job.salary_text).text,
      salary_min: parseSalary(job.salary_text).min,
      salary_max: parseSalary(job.salary_text).max
    };
  }
}

// ─── Upsert company ─────────────────────────────────────────────────────────

function upsertCompany(name) {
  const existing = db.prepare('SELECT id FROM companies WHERE name = ?').get(name);
  if (existing) return existing.id;

  const result = db.prepare('INSERT INTO companies (name) VALUES (?)').run(name);
  return result.lastInsertRowid;
}

// ─── Main scrapeAll function ────────────────────────────────────────────────

export async function scrapeAll(options = {}) {
  const { sources = SOURCES, maxDetailPages = 500, runId = null } = options;

  const allListings = [];

  if (sources.includes('linkedin')) {
    try {
      const linkedinJobs = await scrapeLinkedIn();
      allListings.push(...linkedinJobs);
      console.log(`LinkedIn: found ${linkedinJobs.length} listings`);
    } catch (e) {
      console.error('LinkedIn scrape failed:', e.message);
    }
  }

  if (sources.includes('indeed')) {
    try {
      const indeedJobs = await scrapeIndeed();
      allListings.push(...indeedJobs);
      console.log(`Indeed: found ${indeedJobs.length} listings`);
    } catch (e) {
      console.error('Indeed scrape failed:', e.message);
    }
  }

  if (sources.includes('glassdoor')) {
    try {
      const glassdoorJobs = await scrapeGlassdoor();
      allListings.push(...glassdoorJobs);
      console.log(`Glassdoor: found ${glassdoorJobs.length} listings`);
    } catch (e) {
      console.error('Glassdoor scrape failed:', e.message);
    }
  }

  console.log(`Total listings before dedup: ${allListings.length}`);

  // Deduplicate by URL hash
  const seen = new Set();
  const unique = allListings.filter(job => {
    const hash = hashUrl(job.source_url);
    if (seen.has(hash)) return false;
    // Also check if already in DB
    const existing = db.prepare('SELECT id FROM jobs WHERE url_hash = ?').get(hash);
    if (existing) return false;
    seen.add(hash);
    return true;
  });

  console.log(`Unique new listings: ${unique.length}`);

  // Filter to Chicagoland-relevant
  const filtered = unique.filter(job => jobMatchesTarget(job) || isRemote(job.location));
  console.log(`After Chicagoland filter: ${filtered.length}`);

  // Scrape detail pages (limited)
  let jobsNew = 0;
  let jobsScored = 0;
  const now = new Date().toISOString();

  for (let i = 0; i < Math.min(filtered.length, maxDetailPages); i++) {
    const listing = filtered[i];

    try {
      const detail = await scrapeJobDetail(listing);
      if (!detail) continue;

      const companyId = upsertCompany(detail.company_name);
      const urlHash = hashUrl(detail.source_url);
      const salary = parseSalary(detail.salary_text);

      db.prepare(`
        INSERT INTO jobs (
          source, source_url, url_hash, title, company_name, company_id,
          location, is_remote, salary_min, salary_max, salary_text,
          description, requirements, posted_date, scraped_at,
          is_active, scrape_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
      `).run(
        detail.source,
        detail.source_url,
        urlHash,
        detail.title,
        detail.company_name,
        companyId,
        detail.location,
        isRemote(detail.location) ? 1 : 0,
        salary.min,
        salary.max,
        salary.text,
        detail.description,
        detail.requirements,
        detail.posted_date || null,
        now,
        runId
      );

      jobsNew++;

      // Score the job
      const jobId = db.prepare('SELECT last_insert_rowid() as id').get().id;
      try {
        await scoreJob(jobId, db);
        jobsScored++;
      } catch (e) {
        console.error(`Scoring error for job ${jobId}:`, e.message);
      }

      // Rate limit between detail scrapes
      await new Promise(r => setTimeout(r, 1500));
    } catch (e) {
      console.error(`Detail scrape error for ${listing.source_url}:`, e.message);
    }
  }

  return { jobs_found: allListings.length, jobs_new: jobsNew, jobs_scored: jobsScored };
}

export { scrapeLinkedIn, scrapeIndeed, scrapeGlassdoor, scrapeJobDetail };
