import express from 'express';
import db from '../db.js';

const router = express.Router();

// GET /api/settings - Get all settings
router.get('/', (req, res) => {
  try {
    const settings = db.prepare('SELECT * FROM settings').all();
    const result = {};
    settings.forEach(s => {
      try {
        result[s.key] = JSON.parse(s.value);
      } catch {
        result[s.key] = s.value;
      }
    });
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// GET /api/settings/:key - Get single setting
router.get('/:key', (req, res) => {
  try {
    const setting = db.prepare('SELECT * FROM settings WHERE key = ?').get(req.params.key);
    if (!setting) {
      return res.status(404).json({ error: 'Setting not found' });
    }
    try {
      res.json({ key: setting.key, value: JSON.parse(setting.value) });
    } catch {
      res.json({ key: setting.key, value: setting.value });
    }
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// PUT /api/settings/:key - Update or create setting
router.put('/:key', (req, res) => {
  const { value } = req.body;
  const key = req.params.key;

  if (value === undefined) {
    return res.status(400).json({ error: 'value is required' });
  }

  const stringValue = typeof value === 'string' ? value : JSON.stringify(value);

  try {
    db.prepare(`
      INSERT INTO settings (key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value
    `).run(key, stringValue);

    res.json({ key, value });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DELETE /api/settings/:key - Delete setting
router.delete('/:key', (req, res) => {
  try {
    const result = db.prepare('DELETE FROM settings WHERE key = ?').run(req.params.key);
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Setting not found' });
    }
    res.status(204).send();
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
