import Database from 'better-sqlite3';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const dbPath = join(__dirname, '..', 'jhunter.db');
const db = new Database(dbPath);

// Enable foreign keys
db.pragma('foreign_keys = ON');

// Create all 9 tables

// 1. companies table
db.exec(`
  CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    website TEXT,
    size TEXT,
    industry TEXT,
    tech_stack TEXT,
    glassdoor_rating REAL,
    description TEXT,
    recent_news TEXT,
    connections_to_eric TEXT,
    dossier_generated_at TEXT,
    dossier_raw TEXT
  )
`);

// 2. jobs table
db.exec(`
  CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    url_hash TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    company_id INTEGER,
    location TEXT,
    is_remote INTEGER DEFAULT 0,
    salary_min INTEGER,
    salary_max INTEGER,
    salary_text TEXT,
    description TEXT,
    requirements TEXT,
    posted_date TEXT,
    scraped_at TEXT NOT NULL,
    fit_score INTEGER,
    fit_breakdown TEXT,
    is_active INTEGER DEFAULT 1,
    is_hidden INTEGER DEFAULT 0,
    raw_html TEXT,
    scrape_run_id INTEGER,
    FOREIGN KEY (company_id) REFERENCES companies(id)
  )
`);

// 3. applications table
db.exec(`
  CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'saved',
    applied_date TEXT,
    status_updated_at TEXT NOT NULL,
    notes TEXT,
    follow_up_sent INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id)
  )
`);

// 4. resume_versions table
db.exec(`
  CREATE TABLE IF NOT EXISTS resume_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    suggestions TEXT,
    tailored_sections TEXT,
    docx_path TEXT,
    pdf_path TEXT,
    file_name TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id)
  )
`);

// 5. cover_letters table
db.exec(`
  CREATE TABLE IF NOT EXISTS cover_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    content TEXT,
    docx_path TEXT,
    pdf_path TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id)
  )
`);

// 6. interview_prep table
db.exec(`
  CREATE TABLE IF NOT EXISTS interview_prep (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    likely_questions TEXT,
    talking_points TEXT,
    role_specific_prep TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id)
  )
`);

// 7. reminders table
db.exec(`
  CREATE TABLE IF NOT EXISTS reminders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    application_id INTEGER NOT NULL,
    reminder_type TEXT,
    due_date TEXT,
    is_dismissed INTEGER DEFAULT 0,
    follow_up_draft TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (application_id) REFERENCES applications(id)
  )
`);

// 8. scrape_runs table
db.exec(`
  CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT,
    source TEXT,
    jobs_found INTEGER DEFAULT 0,
    jobs_new INTEGER DEFAULT 0,
    jobs_scored INTEGER DEFAULT 0,
    error_message TEXT
  )
`);

// 9. scrape_results table - stores ALL scraped listings per run (not deduplicated)
db.exec(`
  CREATE TABLE IF NOT EXISTS scrape_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_run_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT NOT NULL,
    url_hash TEXT NOT NULL,
    title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    location TEXT,
    salary_text TEXT,
    posted_date TEXT,
    is_duplicate INTEGER DEFAULT 0,
    scraped_at TEXT NOT NULL,
    FOREIGN KEY (scrape_run_id) REFERENCES scrape_runs(id)
  )
`);

// 10. settings table
db.exec(`
  CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
  )
`);

// Migration: add is_hidden column if it doesn't exist (existing databases may not have it)
try {
  db.exec(`ALTER TABLE jobs ADD COLUMN is_hidden INTEGER DEFAULT 0`);
} catch (e) {
  // Column may already exist (new database), ignore error
}

// Migration: add idx_jobs_is_hidden index if it doesn't exist
try {
  db.exec(`CREATE INDEX idx_jobs_is_hidden ON jobs(is_hidden)`);
} catch (e) {
  // Index may already exist, ignore error
}

// Create indexes for performance
db.exec(`
  CREATE INDEX IF NOT EXISTS idx_jobs_company_id ON jobs(company_id);
  CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
  CREATE INDEX IF NOT EXISTS idx_jobs_fit_score ON jobs(fit_score);
  CREATE INDEX IF NOT EXISTS idx_applications_job_id ON applications(job_id);
  CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status);
  CREATE INDEX IF NOT EXISTS idx_reminders_application_id ON reminders(application_id);
  CREATE INDEX IF NOT EXISTS idx_scrape_results_run_id ON scrape_results(scrape_run_id);
  CREATE INDEX IF NOT EXISTS idx_scrape_results_url_hash ON scrape_results(url_hash);
`);

export default db;
