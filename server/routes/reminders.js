import express from 'express';
import {
  getRemindersForApplication,
  getAllActiveReminders,
  getDueReminders,
  createReminder,
  dismissReminder,
  generateFollowUpForReminder,
  updateReminderFollowUp,
  deleteReminder,
  generateRemindersForAppliedApplications
} from '../services/reminder-service.js';

const router = express.Router();

// GET /api/reminders - Get all active reminders
router.get('/', (req, res) => {
  try {
    const reminders = getAllActiveReminders();
    res.json(reminders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/reminders/due - Get reminders due today or overdue
router.get('/due', (req, res) => {
  try {
    const reminders = getDueReminders();
    res.json(reminders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/reminders/application/:applicationId - Get reminders for specific application
router.get('/application/:applicationId', (req, res) => {
  try {
    const applicationId = parseInt(req.params.applicationId);
    const reminders = getRemindersForApplication(applicationId);
    res.json(reminders);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/reminders - Create a new reminder
router.post('/', (req, res) => {
  try {
    const { application_id, reminder_type, due_date } = req.body;
    if (!application_id || !reminder_type || !due_date) {
      return res.status(400).json({ error: 'application_id, reminder_type, and due_date are required' });
    }
    const reminder = createReminder(application_id, reminder_type, due_date);
    res.json(reminder);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/reminders/:id/dismiss - Dismiss a reminder
router.post('/:id/dismiss', (req, res) => {
  try {
    const id = parseInt(req.params.id);
    dismissReminder(id);
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/reminders/:id/follow-up - Generate follow-up email draft
router.post('/:id/follow-up', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const followUp = await generateFollowUpForReminder(id);
    updateReminderFollowUp(id, followUp);
    res.json(followUp);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DELETE /api/reminders/:id - Delete a reminder
router.delete('/:id', (req, res) => {
  try {
    const id = parseInt(req.params.id);
    const deleted = deleteReminder(id);
    if (!deleted) {
      return res.status(404).json({ error: 'Reminder not found' });
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// POST /api/reminders/generate-nightly - Generate reminders for all applied apps (for nightly cron)
router.post('/generate-nightly', async (req, res) => {
  try {
    const results = await generateRemindersForAppliedApplications();
    res.json({ generated: results.length, details: results });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
