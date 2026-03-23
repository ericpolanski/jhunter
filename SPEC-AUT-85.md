# SPEC-AUT-85: JHunter Phase 2 - Polish & Bug Fixes

## Context

JHunter Phase 2 is a polish and bug-fix iteration to address remaining issues before production readiness.

## Bugs Found

### 1. GET /api/jobs/:id/dossier fails with UNIQUE constraint
**Severity:** High
**File:** `server/routes/jobs.js` lines 96-119

**Problem:** When `job.company_id` is NULL but `job.company_name` exists in the companies table, the endpoint tries to INSERT a new company with the same name, causing `UNIQUE constraint failed: companies.name`.

**Current Code:**
```javascript
let company = db.prepare('SELECT * FROM companies WHERE id = ?').get(job.company_id);
// If no company exists, create one
if (!company && job.company_name) {
  const result = db.prepare('INSERT INTO companies (name) VALUES (?)').run(job.company_name);
  // ...
}
```

**Fix Required:**
1. First try to find company by `job.company_id`
2. If not found, try to find company by `job.company_name` (look up by name)
3. If still not found and `job.company_name` exists, create new company
4. If company has no `dossier_generated_at`, trigger dossier generation via `company-research.js`

### 2. DOCX Generator receives wrong data type
**Severity:** High
**File:** `server/services/docx-generator.js` line 124

**Problem:** The `buildResumeContent` function expects `tailoredSections` to be an array of suggestion objects with `{section, original, suggested}` fields, but `applySuggestionsToProfile` returns a modified profile object.

**Current Flow:**
1. `generateTailoredResume` calls `applySuggestionsToProfile(profile, acceptedSuggestions)` → returns modified profile object
2. Passes this to `generateResumeDocx(profile, tailoredSections, ...)`
3. `buildResumeContent` calls `tailoredSections.find(...)` → fails because object doesn't have `.find()`

**Fix Required:**
Option A: Change `buildResumeContent` to work with the modified profile object directly
Option B: Change `applySuggestionsToProfile` to return an array of changes instead of modifying the profile

Recommend Option A - the tailoring is already applied in the profile object, so `buildResumeContent` should just use the profile directly without trying to look up suggestions.

## Verification Completed

### ✅ Fit Scoring - WORKING
- Both existing jobs have fit scores (50 and 82)
- `POST /api/jobs/:id/score` works correctly
- Scraper correctly scores new jobs after insertion

### ✅ Reminders System - WORKING
- `POST /api/reminders/generate-nightly` creates reminders for apps 7+ days old
- `GET /api/reminders` returns active reminders with job/company info
- Reminders have proper `due_date`, `reminder_type`, `follow_up_draft` fields

### ✅ Company Dossier Generation - WORKING
- `POST /api/companies/:id/dossier` correctly generates dossiers via LLM
- Dossier stored with `dossier_raw` JSON and `dossier_generated_at` timestamp

## Items Needing Verification

### ⚠️ Real Scrape Run
- Last scrape run returned 0 new jobs
- Likely due to job boards blocking scrapers (not a code bug)
- Scraping code structure is correct

### ⚠️ DOCX → PDF Conversion
- `convertDocxToPdf()` uses LibreOffice (`soffice`) which is installed
- Fallback to DOCX if PDF fails is implemented
- Full E2E test with actual DOCX generation pending bug #2 fix

## Out of Scope (Known Limitations)

1. **Company data population** - Companies table has name but no other data for most entries. Dossier generation populates some fields but company websites, size, etc. require separate scraping.
2. **Scraping effectiveness** - Job boards actively block scraping. Real job data would need Indeed/LinkedIn accounts with API access or manual entry.

## Test Commands

```bash
# Test job dossier endpoint (after fix)
curl http://localhost:4200/api/jobs/7/dossier

# Test resume generation (after DOCX bug fix)
curl -X POST http://localhost:4200/api/resume/tailor/7
curl -X POST http://localhost:4200/api/resume/generate/7 -H "Content-Type: application/json" -d '{"suggestions":[{"section":"skills","original":"...","suggested":"..."}]}'

# Check reminders
curl http://localhost:4200/api/reminders
```

## Implementation Notes

- No database schema changes needed
- No frontend changes needed for bug fixes
- Both fixes are in backend route/service files