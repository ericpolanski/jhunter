import { useEffect, useState } from 'react';
import { Save, RefreshCw } from 'lucide-react';
import { resumeApi, settingsApi } from '../utils/api';

export default function Settings() {
  const [profile, setProfile] = useState(null);
  const [settings, setSettings] = useState({
    scrape_sources: ['linkedin', 'indeed', 'glassdoor'],
    scrape_location: 'Chicago, IL',
    follow_up_days: 7,
    no_response_days: 14,
  });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    setLoading(true);
    try {
      const [profileData, allSettings] = await Promise.all([
        resumeApi.getProfile(),
        settingsApi.getAll(),
      ]);
      setProfile(profileData);

      // Parse settings
      const parsedSettings = {
        scrape_sources: JSON.parse(allSettings.scrape_sources?.value || '["linkedin", "indeed", "glassdoor"]'),
        scrape_location: allSettings.scrape_location?.value || 'Chicago, IL',
        follow_up_days: parseInt(allSettings.follow_up_days?.value || '7'),
        no_response_days: parseInt(allSettings.no_response_days?.value || '14'),
      };
      setSettings(parsedSettings);
    } catch (error) {
      console.error('Failed to load settings:', error);
    } finally {
      setLoading(false);
    }
  }

  async function handleSaveSettings() {
    setSaving(true);
    try {
      await settingsApi.set('scrape_sources', JSON.stringify(settings.scrape_sources));
      await settingsApi.set('scrape_location', settings.scrape_location);
      await settingsApi.set('follow_up_days', settings.follow_up_days.toString());
      await settingsApi.set('no_response_days', settings.no_response_days.toString());
    } catch (error) {
      console.error('Failed to save settings:', error);
    } finally {
      setSaving(false);
    }
  }

  function handleSourceToggle(source) {
    setSettings(prev => {
      const sources = prev.scrape_sources.includes(source)
        ? prev.scrape_sources.filter(s => s !== source)
        : [...prev.scrape_sources, source];
      return { ...prev, scrape_sources: sources };
    });
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold">Settings</h1>
        <div className="space-y-4">
          <div className="h-32 skeleton rounded-md"></div>
          <div className="h-32 skeleton rounded-md"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-text-primary">Settings</h1>
        <button className="btn btn-primary flex items-center gap-2" onClick={handleSaveSettings} disabled={saving}>
          <Save size={16} />
          {saving ? 'Saving...' : 'Save Settings'}
        </button>
      </div>

      {/* Scraping Config */}
      <div className="card">
        <h2 className="text-lg font-semibold mb-4">Scraping Configuration</h2>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-2">Job Sources</label>
            <div className="flex gap-4">
              {['linkedin', 'indeed', 'glassdoor'].map(source => (
                <label key={source} className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={settings.scrape_sources.includes(source)}
                    onChange={() => handleSourceToggle(source)}
                    className="w-4 h-4 text-primary rounded border-border focus:ring-primary"
                  />
                  <span className="text-sm capitalize">{source}</span>
                </label>
              ))}
            </div>
          </div>
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-2">Location</label>
            <input
              type="text"
              className="input w-full max-w-md"
              value={settings.scrape_location}
              onChange={e => setSettings(prev => ({ ...prev, scrape_location: e.target.value }))}
            />
          </div>
        </div>
      </div>

      {/* Reminder Settings */}
      <div className="card">
        <h2 className="text-lg font-semibold mb-4">Reminder Settings</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 max-w-md">
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-2">Follow-up Reminder (days)</label>
            <input
              type="number"
              className="input"
              value={settings.follow_up_days}
              onChange={e => setSettings(prev => ({ ...prev, follow_up_days: parseInt(e.target.value) || 7 }))}
              min={1}
              max={30}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-text-secondary mb-2">No Response Reminder (days)</label>
            <input
              type="number"
              className="input"
              value={settings.no_response_days}
              onChange={e => setSettings(prev => ({ ...prev, no_response_days: parseInt(e.target.value) || 14 }))}
              min={1}
              max={60}
            />
          </div>
        </div>
      </div>

      {/* Profile Preview */}
      <div className="card">
        <h2 className="text-lg font-semibold mb-4">Profile Preview</h2>
        {profile && (
          <div className="space-y-4">
            <div>
              <h3 className="font-medium">{profile.name}</h3>
              <p className="text-sm text-text-secondary">{profile.email} | {profile.phone}</p>
              <p className="text-sm text-text-muted">{profile.location}</p>
            </div>
            <div>
              <h4 className="text-sm font-medium text-text-secondary mb-2">Education</h4>
              {profile.education?.map((edu, i) => (
                <div key={i} className="text-sm">
                  <p className="font-medium">{edu.school}</p>
                  <p className="text-text-muted">{edu.degree}</p>
                </div>
              ))}
            </div>
            <div>
              <h4 className="text-sm font-medium text-text-secondary mb-2">Skills</h4>
              <div className="flex flex-wrap gap-2">
                {Object.values(profile.skills || {}).flat().map((skill, i) => (
                  <span key={i} className="px-2 py-1 bg-background-secondary rounded text-xs">
                    {skill}
                  </span>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
