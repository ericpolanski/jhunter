# SPEC-AUT-106: JHunter Hide Jobs Feature

## Overview

Add the ability to hide jobs from the Job Board view. Hidden jobs remain in the database but are filtered out of the board.

## Context

Enhancement 1 of ticket AUT-106. Enhancement 2 (default sort to score_desc) is already implemented.

## Design

### Database Changes (db.js)
- Add `is_hidden INTEGER DEFAULT 0` column to the `jobs` table
- Add index on `is_hidden` for efficient filtering

### API Changes (routes/jobs.js)
1. **GET /api/jobs** - Add filter to exclude hidden jobs:
   - Add condition: `AND j.is_hidden = 0`

2. **PATCH /api/jobs/:id** - Allow updating `is_hidden`:
   - Add `is_hidden` to allowed fields in PATCH handler

### Frontend Changes (JobBoard.jsx)

1. **Hide Button on Job Card**:
   - Add a small "Hide" or "X" button on each job card
   - Position: Top-right corner of the job card
   - On click: Call PATCH /api/jobs/:id with is_hidden=true
   - After successful API call: Remove job from local state (optimistic update)

2. **Visual Design**:
   - Hide button: Small, subtle, text or icon button
   - Hover state: Slight opacity change to indicate interactivity
   - No confirmation dialog needed (can be undone via job detail view later if needed)

## Implementation Details

### Files to Modify

1. **jhunter/server/db.js**
   - Add `is_hidden INTEGER DEFAULT 0` to jobs table schema

2. **jhunter/server/routes/jobs.js**
   - Add `is_hidden = 0` filter in GET /api/jobs query
   - Add `is_hidden` to PATCH endpoint allowed fields

3. **jhunter/client/src/pages/JobBoard.jsx**
   - Add hide button to job card
   - Handle click: PATCH job with is_hidden=true, remove from list

### API Contract

**PATCH /api/jobs/:id**
```json
Request: { "is_hidden": true }
Response: Updated job object
```

**GET /api/jobs** (existing, add filter)
- Automatically filters `is_hidden = 0` - no API change needed, handled in SQL

## Testing

1. Backend:
   - Verify hidden jobs don't appear in GET /api/jobs
   - Verify PATCH can set is_hidden=true/false

2. Frontend:
   - Click hide button → job disappears from list
   - Hidden jobs don't reappear on filter change or refresh

## Priority

Medium