# Plan: JHunter — Personal Job Hunting Application

## Context

Eric is a Dec 2025 Northwestern CS grad (AI concentration) looking for entry-level AI Engineering and Software Engineering roles in the Chicagoland area. He wants a self-hosted web app that scrapes jobs nightly, scores them against his profile, and provides AI-powered tools (resume tailoring, cover letters, interview prep, company research) — all in one place.

This will be built by the Autono engineering team via a Paperclip ticket assigned to the Senior Engineer, who orchestrates the full pipeline: spec → UI/UX design → backend + frontend implementation → QA → deploy.

---

## Tech Stack

- **Frontend:** React 18 + Vite + Tailwind CSS + React Router v6 + Recharts
- **Backend:** Node.js + Express (port 4200)
- **Database:** SQLite via better-sqlite3
- **Scraping:** Scrapfly API (LinkedIn, Indeed, Glassdoor)
- **AI/LLM:** MiniMax API (Anthropic messages format) for fit scoring, resume tailoring, cover letters, interview prep, company dossiers
- **Document generation:** minimax-docx (.NET/OpenXML) → LibreOffice PDF conversion
- **Deployment:** Cloudflare tunnel → jhunter.ericpolanski.com
- **Resume source:** `/home/eric/Downloads/Eric Polanski Resume.pdf`

---

## Features (10 total)

1. **Nightly Job Scraping** — Scrape LinkedIn, Indeed, Glassdoor for AI Eng / SWE / ML Eng entry-level roles near Chicagoland. Deduplicate by URL hash + title/company combo
2. **Fit Score (0-100)** — Two-phase: fast heuristic (skill match 40%, location 20%, experience level 15%, role type 15%, salary 10%), then LLM refinement for jobs scoring ≥40
3. **Job Information Display** — Title, company, location, salary, description, requirements, application link, source, posted date
4. **Resume Tailoring** — LLM suggests 3-7 bullet/skill tweaks per job. Accept suggestions → generate one-page DOCX → PDF. Download as "Eric Polanski Resume - [Company Name].pdf"
5. **Cover Letter Generation** — 3-4 paragraph cover letter per job, downloadable as PDF
6. **Application Tracking** — Kanban + table view. Statuses: saved → applied → phone screen → interview → offer → rejected/withdrawn
7. **Company Research Dossiers** — Scrape company site + Glassdoor, synthesize via LLM: size, tech stack, ratings, news, connections to Eric's background
8. **Interview Prep Module** — Auto-generates when status = "interview": 8-10 likely questions with answer frameworks, 5-7 talking points mapped to Eric's experience, role-specific prep
9. **Analytics Dashboard** — 7 charts: application funnel, timeline, fit score distribution, salary distribution, skill gap analysis, response rate by source, status breakdown
10. **Follow-Up Reminders (app only)** — Track days since application, surface reminders at 7/14 days, auto-draft follow-up emails

---

## Project Structure

```
~/ai-company/projects/jhunter/
├── package.json
├── jhunter.db                    # SQLite (gitignored)
├── data/
│   ├── eric-profile.json         # Structured resume data
│   └── scrape-sources.json       # Job board search configs
├── server/
│   ├── index.js                  # Express entry: static serving, SPA catch-all
│   ├── db.js                     # SQLite init + table creation
│   ├── routes/                   # jobs, applications, resume, cover-letter, companies,
│   │                             # interview-prep, analytics, reminders, scrape, settings
│   ├── services/
│   │   ├── llm.js                # MiniMax API client
│   │   ├── scraper.js            # Scrapfly job scraping
│   │   ├── fit-scorer.js         # Heuristic + LLM scoring
│   │   ├── resume-tailor.js      # Resume suggestion generation
│   │   ├── cover-letter-gen.js   # Cover letter generation
│   │   ├── company-research.js   # Company dossier generation
│   │   ├── interview-prep-gen.js # Interview prep generation
│   │   └── docx-generator.js     # DOCX creation + PDF conversion
│   └── lib/
│       ├── dedup.js              # URL hash deduplication
│       └── profile.js            # Eric's profile loader
├── client/
│   └── src/
│       ├── pages/                # Dashboard, JobBoard, JobDetail, Applications,
│       │                         # Analytics, Settings
│       ├── components/           # Layout, Sidebar, JobCard, FitScoreBadge,
│       │                         # StatusPipeline, ResumeTailor, CoverLetterPanel,
│       │                         # CompanyDossier, InterviewPrepPanel, ReminderList,
│       │                         # FilterBar, ChartContainer, Modal, etc.
│       ├── hooks/                # useJobs, useApplications, useAnalytics
│       └── utils/                # api.js, formatters.js, constants.js
├── scripts/
│   └── nightly-scrape.js         # Cron-triggered scrape + score + reminder generation
└── generated/                    # Resumes + cover letters (gitignored)
```

---

## Database Schema (SQLite)

**9 tables:** `jobs`, `companies`, `applications`, `resume_versions`, `cover_letters`, `interview_prep`, `reminders`, `scrape_runs`, `settings`

Key relationships:
- `jobs.company_id` → `companies.id`
- `applications.job_id` → `jobs.id` (unique — one application per job)
- `resume_versions.job_id` → `jobs.id`
- `cover_letters.job_id` → `jobs.id`
- `interview_prep.job_id` → `jobs.id`
- `reminders.application_id` → `applications.id`

### jobs table
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | Auto-increment |
| source | TEXT NOT NULL | linkedin, indeed, glassdoor, direct |
| source_url | TEXT NOT NULL | Original posting URL |
| url_hash | TEXT NOT NULL UNIQUE | SHA-256 for dedup |
| title | TEXT NOT NULL | Job title |
| company_name | TEXT NOT NULL | |
| company_id | INTEGER | FK → companies.id (nullable) |
| location | TEXT | e.g. "Chicago, IL", "Remote" |
| is_remote | INTEGER DEFAULT 0 | |
| salary_min | INTEGER | Annual low end (nullable) |
| salary_max | INTEGER | Annual high end (nullable) |
| salary_text | TEXT | Raw salary string |
| description | TEXT | Full JD text |
| requirements | TEXT | JSON array |
| posted_date | TEXT | |
| scraped_at | TEXT NOT NULL | |
| fit_score | INTEGER | 0-100 |
| fit_breakdown | TEXT | JSON: {skill_match, location_match, experience_match, role_match, salary_match, explanation} |
| is_active | INTEGER DEFAULT 1 | |
| raw_html | TEXT | For re-processing |
| scrape_run_id | INTEGER | FK → scrape_runs.id |

### companies table
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | |
| name | TEXT NOT NULL UNIQUE | |
| website | TEXT | |
| size | TEXT | startup/small/mid/large/enterprise |
| industry | TEXT | |
| tech_stack | TEXT | JSON array |
| glassdoor_rating | REAL | |
| description | TEXT | |
| recent_news | TEXT | JSON array |
| connections_to_eric | TEXT | JSON |
| dossier_generated_at | TEXT | |
| dossier_raw | TEXT | Full LLM dossier text |

### applications table
| Column | Type | Notes |
|--------|------|-------|
| id | INTEGER PK | |
| job_id | INTEGER NOT NULL UNIQUE | FK → jobs.id |
| status | TEXT NOT NULL DEFAULT 'saved' | saved/applied/phone_screen/interview/offer/rejected/withdrawn |
| applied_date | TEXT | |
| status_updated_at | TEXT NOT NULL | |
| notes | TEXT | |
| follow_up_sent | INTEGER DEFAULT 0 | |
| created_at | TEXT NOT NULL | |

### Other tables
- **resume_versions**: job_id, suggestions (JSON), tailored_sections (JSON), docx_path, pdf_path, file_name, created_at
- **cover_letters**: job_id, content (text), docx_path, pdf_path, created_at
- **interview_prep**: job_id, likely_questions (JSON), talking_points (JSON), role_specific_prep (markdown), created_at
- **reminders**: application_id, reminder_type (follow_up_7/14/custom), due_date, is_dismissed, follow_up_draft, created_at
- **scrape_runs**: started_at, completed_at, status, source, jobs_found, jobs_new, jobs_scored, error_message
- **settings**: key (PK), value (JSON-encoded)

---

## API Endpoints

### Jobs
- `GET /api/jobs` — List with filters: source, minScore, location, search, sort, limit, offset
- `GET /api/jobs/:id` — Single job with fit breakdown
- `GET /api/jobs/:id/dossier` — Get/generate company dossier
- `POST /api/jobs/:id/score` — Re-score via LLM
- `PATCH /api/jobs/:id` — Update fields

### Applications
- `GET /api/applications` — List with status filter
- `POST /api/applications` — Create {job_id, status?, notes?}
- `PATCH /api/applications/:id` — Update status/notes
- `DELETE /api/applications/:id` — Remove tracking
- `GET /api/applications/stats` — Aggregate stats

### Resume
- `GET /api/resume/base` — Eric's structured profile
- `POST /api/resume/tailor/:jobId` — Generate tailoring suggestions
- `POST /api/resume/generate/:jobId` — Generate DOCX + PDF from accepted suggestions
- `GET /api/resume/download/:id` — Download PDF

### Cover Letter
- `POST /api/cover-letter/generate/:jobId` — Generate letter
- `POST /api/cover-letter/download/:id` — Generate PDF, return download URL
- `GET /api/cover-letter/:jobId` — Get existing

### Companies, Interview Prep, Analytics, Reminders, Scrape, Settings
- Standard CRUD + generation endpoints as detailed in the full spec

---

## Scraping Pipeline

**Sources:** LinkedIn (entry-level filter), Indeed (24h recency, 30mi from Gurnee), Glassdoor
**Queries:** "AI Engineer", "Software Engineer", "Machine Learning Engineer"

**Flow per source:**
1. Scrape search results via Scrapfly (`asp: true`, `render_js: true`)
2. Extract job URLs via Scrapfly extraction API with structured prompts
3. Dedup by URL hash — skip existing
4. Scrape each new job detail page, extract structured data
5. Insert into `jobs` table
6. Batch fit-score new jobs (heuristic first, LLM for ≥40)
7. Rate limit: max 20 detail pages per source per run

**Cron:** `~/ai-company/scripts/schedule-jhunter-scrape.sh` at 1:00 AM daily

---

## AI Integration Points (6 LLM calls via MiniMax)

1. **Fit Scoring** — Profile + JD → `{score, breakdown, explanation}`
2. **Resume Tailoring** — Profile + JD → `{suggestions: [{section, original, suggested, reason, impact}]}`
3. **Cover Letter** — Profile + JD + dossier → plain text (3-4 paragraphs, <400 words)
4. **Company Dossier** — Scraped data → `{overview, size, tech_stack, rating, news, connections}`
5. **Interview Prep** — Profile + JD + company → `{questions, talking_points, role_specific_prep}`
6. **Follow-Up Draft** — Application context → email subject + body

---

## Document Generation

**Resume:** Accept suggestions → apply to `eric-profile.json` → DOCX via minimax-docx (.NET/OpenXML, one-page, narrow margins) → PDF via LibreOffice → "Eric Polanski Resume - [Company Name].pdf"

**Cover Letter:** LLM text → DOCX (header with contact info + date + body) → PDF via LibreOffice

---

## Frontend Pages

| Page | Route | Key Components |
|------|-------|----------------|
| Dashboard | `/` | StatsCards, ReminderList, top new jobs, pipeline mini-view |
| Job Board | `/jobs` | FilterBar, JobCard list, FitScoreBadge, pagination |
| Job Detail | `/jobs/:id` | Full JD, fit breakdown, ResumeTailor, CoverLetterPanel, CompanyDossier, InterviewPrepPanel |
| Applications | `/applications` | Table + Kanban views, drag-to-change-status, StatusBadge |
| Analytics | `/analytics` | 7 Recharts: funnel, timeline, fit distribution, salary, skill gaps, source rates, status pie |
| Settings | `/settings` | Scrape config, reminder intervals, base resume |

---

## Implementation Order (10 phases)

1. **Foundation** — Scaffold project, DB schema, `eric-profile.json`, Express server, LLM client, frontend shell with routing
2. **Job Scraping** — Scrapfly integration, dedup, scrape routes, JobBoard page, nightly cron
3. **Fit Scoring** — Heuristic + LLM scorer, FitScoreBadge, breakdown on JobDetail
4. **Application Tracking** — CRUD, Applications page (table + kanban), status on JobDetail
5. **Dashboard** — Stats endpoints, Dashboard page with cards + charts + reminders
6. **Resume Tailoring** — Tailor service, DOCX generation (.NET), PDF conversion, download
7. **Cover Letters + Company Dossiers** — Generation services, panels on JobDetail
8. **Interview Prep + Reminders** — Prep generation, reminder system in nightly scrape
9. **Analytics** — All 7 chart endpoints + Recharts visualizations
10. **Polish + Deploy** — Settings, loading/error states, responsive, build, deploy to jhunter.ericpolanski.com, activate cron

---

## Ticket Creation

Create a Paperclip project "JHunter" and a root ticket assigned to Senior Engineer with this spec. The Senior Engineer will:
1. Write the detailed engineering spec → commit as SPEC.md
2. Create UI/UX Designer ticket for design spec
3. Create Backend + Frontend implementation tickets referencing both specs
4. Route through QA → DevOps for deployment

---

## Verification

1. Manual scrape → jobs appear in JobBoard with fit scores
2. Save job → track through application pipeline → Kanban updates
3. Generate tailored resume → download PDF → verify one page, correct filename
4. Generate cover letter → download PDF
5. Move to "interview" → interview prep auto-generates
6. Analytics shows correct charts
7. Nightly cron runs and deduplicates correctly
8. jhunter.ericpolanski.com accessible

---

## Key Reference Files

- `~/ai-company/projects/nfl-mock-draft-2026/server/index.js` — Express server pattern
- `~/ai-company/skills/minimax-docx/SKILL.md` — DOCX generation pipeline
- `~/ai-company/scripts/schedule-air-healthcheck.sh` — Cron script pattern
- `~/ai-company/scripts/deploy.sh` — Cloudflare tunnel deployment
