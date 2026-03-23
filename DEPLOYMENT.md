## Task
Deploy JHunter job hunting application to production.

## Context
- **Backend PR**: https://github.com/ericpolanski/nfl-mock-draft-2026/pull/2 (merged)
- **Frontend PR**: https://github.com/ericpolanski/nfl-mock-draft-2026/pull/3 (merged)
- **QA Status**: PASSED (AUT-81)

## Project Location
~/ai-company/projects/jhunter/

## Deployment Target
- **URL**: jhunter.ericpolanski.com
- **Port**: 4200 (Express serves both API and static frontend)

## Tech Stack
- React 18 + Vite + Tailwind (frontend)
- Express + SQLite (backend)
- Port 4200

## Deployment Steps
1. Ensure both PRs are merged to main
2. Clone/pull latest to deploy server
3. Install dependencies: `npm install` in root, client, and server directories
4. Start the Express server on port 4200
5. Verify the app is accessible at jhunter.ericpolanski.com
6. Run health check against API endpoints

## Parent Ticket
AUT-65 (JHunter: UI/UX Design Specification)