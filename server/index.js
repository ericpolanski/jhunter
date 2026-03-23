import express from 'express';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFileSync } from 'fs';

// Load environment variables from ~/.ai-company/.env BEFORE importing modules that need them
const envFile = readFileSync('/home/eric/.ai-company/.env', 'utf8');
for (const line of envFile.split('\n')) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx < 0) continue;
  const key = trimmed.slice(0, eqIdx).trim();
  const val = trimmed.slice(eqIdx + 1).trim();
  if (key && !process.env[key]) process.env[key] = val;
}

// Import modules that depend on environment variables
import db from './db.js';
import jobsRouter from './routes/jobs.js';
import applicationsRouter from './routes/applications.js';
import resumeRouter from './routes/resume.js';
import coverLetterRouter from './routes/cover-letter.js';
import companiesRouter from './routes/companies.js';
import interviewPrepRouter from './routes/interview-prep.js';
import analyticsRouter from './routes/analytics.js';
import remindersRouter from './routes/reminders.js';
import scrapeRouter from './routes/scrape.js';
import settingsRouter from './routes/settings.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());

// Serve generated files (resumes, cover letters)
app.use('/generated', express.static(join(__dirname, '..', 'generated')));

// API Routes
app.use('/api/jobs', jobsRouter);
app.use('/api/applications', applicationsRouter);
app.use('/api/resume', resumeRouter);
app.use('/api/cover-letter', coverLetterRouter);
app.use('/api/companies', companiesRouter);
app.use('/api/interview-prep', interviewPrepRouter);
app.use('/api/analytics', analyticsRouter);
app.use('/api/reminders', remindersRouter);
app.use('/api/scrape', scrapeRouter);
app.use('/api/settings', settingsRouter);

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Temp debug endpoint
app.get('/api/debug/key', (req, res) => {
  res.json({ key: process.env.SCRAPFLY_API_KEY ? 'set (' + process.env.SCRAPFLY_API_KEY.slice(0, 10) + '...)' : 'NOT SET' });
});

// Serve static frontend files
app.use('/assets', express.static(join(__dirname, '..', 'client', 'dist', 'assets'), {
  maxAge: '1y',
  immutable: true
}));
app.use(express.static(join(__dirname, '..', 'client', 'dist'), {
  setHeaders: (res, path) => {
    if (path.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
  }
}));

// SPA catch-all
app.get('*', (req, res) => {
  res.sendFile(join(__dirname, '..', 'client', 'dist', 'index.html'));
});

const PORT = 4200;
app.listen(PORT, () => {
  console.log(`JHunter Server running on port ${PORT}`);
});
