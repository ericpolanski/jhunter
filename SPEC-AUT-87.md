# SPEC-AUT-87: JHunter Scraper — 7 Critical Parsing/Extraction Bug Fixes

## Context

The JHunter scraper (`server/services/scraper.js`) extracts job data from Amazon/Twitch/other company career pages but has 7 critical bugs causing incorrect or incomplete data. This spec defines exact fixes for each bug.

---

## Bug 1: Location Hardcoded Instead of Parsed from Body

**File:** `server/services/scraper.js`
**Severity:** High — Fit Score uses wrong location (shows Chicago, actual is San Francisco)

### Root Cause
In `scrapeCompanyCareerPages()` (line 198), location is hardcoded:
```javascript
location: 'Chicago, IL',
```
The `scrapeJobDetail()` function does extract location from the detail page HTML (lines 289–305), but the initial listing object already has the wrong location baked in.

### Fix
In `scrapeJobDetail()`, the location extraction logic at lines 289–305 is correct — the issue is primarily that we also need to look **inside the job description body text** for location mentions that the HTML-level patterns miss.

Add a fallback: after HTML pattern matching, search the raw description text for location mentions using a city-name extraction approach. Additionally, enhance the HTML location patterns to handle Amazon's specific location markup (`data-job-location`, `locationValue` attributes).

```javascript
// Enhanced: also search description body for explicit location statements
// e.g., "This role is based in San Francisco, CA"
const bodyLocationMatch = description.match(/(?:based\s+(?:in|near)|location[:]\s*|role\s+is\s+(?:in|near))\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2}(?:\s+\d{5})?)/i);
if (bodyLocationMatch && bodyLocationMatch[1]) {
  extractedLocation = bodyLocationMatch[1].trim();
}
```

---

## Bug 2: Job Description Truncated After Basic Qualifications

**File:** `server/services/scraper.js`, `scrapeJobDetail()`
**Severity:** High — Preferred Qualifications and all subsequent content missing

### Root Cause
The `descPatterns` regex at line 251 terminates on the first occurrence of a `div` with class `benefits|perks|qualifications|requirements|apply`:
```javascript
/<div[^>]+class=["'][^"']*(?:description|job-description|jobdetail|jd|job-content|job-body)[^"']*["'][^>]*>([\s\S]{500,15000}?)(?=<div[^>]+class=["'][^"']*(?:benefits|perks|qualifications|requirements|apply)[^"']*["'][^-])/i
```

This is a look-ahead that stops at the first qualifying div, truncating the content.

### Fix
Replace the termination look-ahead with a pattern that captures more content. The new approach:
1. Use a broader initial capture that gets the main content div
2. Don't terminate on the first benefits div — instead, continue until `apply` section or end of content
3. Use a more inclusive end-marker: stop at `apply` button/link OR `footer` OR end of article

```javascript
const descPatterns = [
  // Pattern 1: Capture full content until the apply section or footer
  /<div[^>]+class=["'][^"']*(?:description|job-description|jobdetail|jd|job-content|job-body)[^"']*["'][^>]*>([\s\S]{500,20000}?)(?=(?:<div[^>]+class=["'][^"']*(?:apply|application|submit)[^"']*["']|<footer|<\\/article))/i,
  // Pattern 2: article tag with broad capture
  /<article[^>]*>([\s\S]{500,25000}?)<(?:aside|footer|nav|div[^>]+class=["'][^"']*(?:apply|application)[^"']*["'])/i,
  // Pattern 3: section-based with longer capture
  /<section[^>]+(?:id|class)=["'][^"']*(?:description|job-description|jobdetail|job-content|about|role)[^"']*["'][^>]*>([\s\S]{500,25000}?)<(?:section|aside|footer|nav|div[^>]+class=["'][^"']*(?:apply|application)[^"']*["'])/i,
];
```

---

## Bug 3: Requirements Parsing Captures Nav/Metadata Links

**File:** `server/services/scraper.js`, `scrapeJobDetail()`
**Severity:** High — "Job categories", "My career My applications" scraped as requirements

### Root Cause
The `reqPatterns` at lines 272–275 use `ul/ol` and `li` patterns with class qualifiers, but they can still match:
- Navigation `<ul>` elements with the same class structure
- Footer link lists

The filter at line 282 checks `!text.match(/^(Job|Apply|Career|Amazon|Google|Microsoft|Company)/i)` but misses many nav-related strings.

### Fix
Add negative patterns to exclude nav/metadata content:
```javascript
// Before extracting, remove nav, header, footer, sidebar from HTML
const cleanHtml = html
  .replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, '')
  .replace(/<header[^>]*>[\s\S]*?<\/header>/gi, '')
  .replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, '')
  .replace(/<aside[^>]*>[\s\S]*?<\/aside>/gi, '')
  .replace(/<!--[\s\S]*?-->/g, '');

// Then extract requirements from cleanHtml
// Also add more negative filters
if (text.length > 15 && text.length < 300 &&
    !seenReqs.has(text) &&
    !text.match(/^(Job|Apply|Career|Amazon|Google|Microsoft|Company|Job categories|My career|Sign\s+in|Account|Categories|Navigation|Browse)/i) &&
    !text.includes('http') &&
    !text.includes('javascript'))
```

---

## Bug 4: Salary Information Not Used

**File:** `server/services/scraper.js`, `scrapeJobDetail()`
**Severity:** High — Fit Score shows "$ Not specified" and placeholder 50

### Root Cause
The `salaryPatterns` (lines 234–246) run on the raw HTML, but Amazon/company career pages often embed salary in:
- JSON-LD structured data (`<script type="application/ld+json">`)
- `data-salary` attributes
- Complex structured markup

Additionally, the fallback in `scrapeAll()` at line 460 only uses `parseSalary(detail.salary_text)` which may be null.

### Fix
1. Extract salary from JSON-LD structured data first:
```javascript
const jsonLdMatch = html.match(/<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/i);
if (jsonLdMatch) {
  try {
    const jsonLd = JSON.parse(jsonLdMatch[1]);
    const salary = jsonLd.baseSalary || jsonLd.salaryCurrency || null;
    // Parse from JSON-LD structure
  } catch {}
}
```

2. Also check `data-salary` and `data-compensation` attributes:
```javascript
const dataSalaryMatch = html.match(/data-(?:salary|compensation|pay)=["']([^"']+)["']/i);
```

3. Fix `scrapeAll()` line 460: use `detail.salary_min` directly if already set:
```javascript
const salary = detail.salary_min !== null || detail.salary_max !== null
  ? { min: detail.salary_min, max: detail.salary_max, text: detail.salary_text }
  : parseSalary(detail.salary_text);
```

---

## Bug 5: HTML Entities Not Decoded

**File:** `server/services/scraper.js`, `stripHtmlWithNewlines()`
**Severity:** Medium — "LinkedIn&nbsp;and&nbsp;X,&nbsp;" instead of "LinkedIn and X,"

### Root Cause
The `stripHtmlWithNewlines()` function (lines 322–348) has HTML entity decoding at lines 337–343:
```javascript
.replace(/&nbsp;/gi, ' ')
.replace(/&amp;/gi, '&')
```

But these only handle lowercase entities. The raw HTML may contain:
- Uppercase: `&NBSP;`, `&AMP;`
- Numeric: `&#160;`, `&#38;`, `&#xA0;`
- Other entities: `&mdash;`, `&ndash;`, `&lsquo;`, `&rsquo;`, `&quot;`, `&apos;`

### Fix
Replace all explicit entity replacements with a full decoder using a DOMParser-like approach, or handle all common entities comprehensively:

```javascript
function decodeHtmlEntities(text) {
  const entities = {
    '&nbsp;': ' ', '&nbsp': ' ', '&#160;': ' ', '&#xA0;': ' ', '\u00A0': ' ',
    '&amp;': '&', '&#38;': '&', '&': '&',
    '&lt;': '<', '&#60;': '<', '&#x3C;': '<',
    '&gt;': '>', '&#62;': '>', '&#x3E;': '>',
    '&quot;': '"', '&#34;': '"', '&#x22;': '"',
    '&apos;': "'", '&#39;': "'", '&#x27;': "'",
    '&mdash;': '—', '&#8211;': '–', '&ndash;': '–',
    '&lsquo;': ''', '&rsquo;': ''', '&ldquo;': '"', '&rdquo;': '"',
    '&hellip;': '…', '&#8230;': '…',
    '&copy;': '©', '&reg;': '®', '&trade;': '™',
    '&deg;': '°', '&plusmn;': '±', '&times;': '×', '&divide;': '÷',
    '&cent;': '¢', '&pound;': '£', '&euro;': '€', '&yen;': '¥',
  };
  // First handle numeric/hex entities
  let result = text.replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code, 10)));
  result = result.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
  // Then handle named entities
  for (const [entity, char] of Object.entries(entities)) {
    result = result.replace(new RegExp(entity.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'), char);
  }
  return result;
}
```

And integrate into `stripHtmlWithNewlines()` after the tag-stripping step.

---

## Bug 6: Benefits Section Not Extracted as Structured Field

**File:** `server/services/scraper.js`, `scrapeJobDetail()`
**Severity:** Medium — Benefits buried in raw text, not structured

### Root Cause
No dedicated benefits extraction exists. Benefits appear in raw description text.

### Fix
Add a benefits extraction pattern in `scrapeJobDetail()`:

```javascript
// Extract benefits section
const benefitsPatterns = [
  /<li[^>]+class=["'][^"']*(?:benefit|perk)[^"']*["'][^>]*>([\s\S]{10,500}?)<\/li>/gi,
  /<(?:ul|ol)[^>]*class=["'][^"']*(?:benefits|perks|what-we-offer)[^"']*["'][^>]*>([\s\S]{100,5000}?)<\/(?:ul|ol)>/gi,
  /<(?:div|p)[^>]*class=["'][^"']*(?:benefits|perks|what-we-offer)[^"']*["'][^>]*>([\s\S]{100,3000}?)(?=<(?:div|p)[^>]+class=["'][^"']*(?:apply|about-us|company)[^"']*["']|<footer)/gi,
];

const benefits = [];
for (const pattern of benefitsPatterns) {
  const matches = [...html.matchAll(pattern)];
  for (const match of matches) {
    const text = stripHtml(match[1]).trim();
    if (text.length > 5 && text.length < 300 && !benefits.includes(text)) {
      benefits.push(text);
    }
  }
}
```

Return `benefits: JSON.stringify(benefits)` in the job object.
Note: The `jobs` table schema needs checking — if no `benefits` column exists, store in `description` as a separate section instead.

---

## Bug 7: No Newlines in Job Description

**File:** `server/services/scraper.js`, `stripHtmlWithNewlines()`
**Severity:** Medium — Job description is a wall of text

### Root Cause
The newline preservation in `stripHtmlWithNewlines()` (lines 332–334) converts some block elements to `\n`:
```javascript
.replace(/<\/(?:p|div|h[1-6]|li|tr)>/gi, '\n')
.replace(/<br\s*\/?>/gi, '\n')
.replace(/<p[^>]*>/gi, '\n')
```

But the subsequent HTML tag stripping `.replace(/<[^>]+>/g, ' ')` collapses the newlines. Also, nested divs create extra whitespace.

### Fix
Refactor `stripHtmlWithNewlines()` to preserve paragraph structure better:

```javascript
function stripHtmlWithNewlines(html) {
  return html
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<nav[^>]*>[\s\S]*?<\/nav>/gi, '')
    .replace(/<header[^>]*>[\s\S]*?<\/header>/gi, '')
    .replace(/<footer[^>]*>[\s\S]*?<\/footer>/gi, '')
    .replace(/<aside[^>]*>[\s\S]*?<\/aside>/gi, '')
    .replace(/<!--[\s\S]*?-->/g, '')
    // First: replace block-level elements with explicit newlines
    .replace(/<\/(p|div|h[1-6]|li|tr|article|section)>/gi, '\n\n')
    .replace(/<br\s*\/?>/gi, '\n')
    // Then remove remaining tags
    .replace(/<[^>]+>/g, ' ')
    // Decode entities
    .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(parseInt(c, 10)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    // Clean: collapse spaces but preserve intentional line breaks
    .replace(/[ \t]+/g, ' ')
    .replace(/\n[ \t]+/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    // Collapse multiple blank lines
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}
```

---

## Database Schema Check

Before implementing Bug 6 (benefits), verify the `jobs` table schema:

```bash
sqlite3 jhunter.db ".schema jobs"
```

If no `benefits` column exists, the fix for Bug 6 should instead format benefits as a clearly delimited section within the `description` field (e.g., `=== BENEFITS ===\n...`).

---

## Test Plan

### Unit Tests for `scraper.js`
1. `stripHtmlWithNewlines` — verify entity decoding, newline preservation
2. `parseSalary` — verify ranges, currency symbols, K suffix handling
3. Location extraction — verify fallback to body text

### Integration Tests
1. Run a scrape of an Amazon jobs page and verify all 7 bugs are fixed
2. Verify benefits are extracted (or delimited in description)

### Manual Verification Commands
```bash
# Check a scraped job's description for newlines
curl -s http://localhost:4200/api/jobs/1 | jq '.description' | head -c 500

# Check salary is populated
curl -s http://localhost:4200/api/jobs/1 | jq '.salary_min, .salary_max, .salary_text'

# Check benefits field
curl -s http://localhost:4200/api/jobs/1 | jq '.benefits'
```

---

## Implementation Notes

- All changes are confined to `server/services/scraper.js`
- No database migration needed (Bug 6 workaround: embed in description if no column)
- No changes to API routes needed
- Fixes can be verified independently with unit tests
