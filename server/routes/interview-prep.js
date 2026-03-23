import express from 'express';
import { generateInterviewPrepForJob, getInterviewPrepForJob, getAllInterviewPrep, deleteInterviewPrep } from '../services/interview-prep-gen.js';

const router = express.Router();

// GET /api/interview-prep - List all interview prep
router.get('/', (req, res) => {
  try {
    const preps = getAllInterviewPrep();
    res.json(preps);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/interview-prep/:jobId - Get prep for a specific job
router.get('/:jobId', (req, res) => {
  try {
    const jobId = parseInt(req.params.jobId);
    const prep = getInterviewPrepForJob(jobId);
    if (!prep) {
      return res.status(404).json({ error: 'Interview prep not found' });
    }
    res.json(prep);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/interview-prep/generate/:jobId - Generate interview prep for a job
router.post('/generate/:jobId', async (req, res) => {
  try {
    const jobId = parseInt(req.params.jobId);
    const prep = await generateInterviewPrepForJob(jobId);
    res.json(prep);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DELETE /api/interview-prep/:jobId - Delete interview prep
router.delete('/:jobId', (req, res) => {
  try {
    const jobId = parseInt(req.params.jobId);
    const deleted = deleteInterviewPrep(jobId);
    if (!deleted) {
      return res.status(404).json({ error: 'Interview prep not found' });
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
