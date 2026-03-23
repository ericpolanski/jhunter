const API_BASE = '/api';

async function request(endpoint, options = {}) {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: 'Request failed' }));
    throw new Error(error.error || 'Request failed');
  }

  if (response.status === 204) {
    return null;
  }

  return response.json();
}

// Jobs
export const jobsApi = {
  getAll: (params = {}) => {
    const query = new URLSearchParams(params).toString();
    return request(`/jobs${query ? `?${query}` : ''}`);
  },
  getById: (id) => request(`/jobs/${id}`),
  create: (data) => request('/jobs', { method: 'POST', body: JSON.stringify(data) }),
  update: (id, data) => request(`/jobs/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  delete: (id) => request(`/jobs/${id}`, { method: 'DELETE' }),
  score: (id) => request(`/jobs/${id}/score`, { method: 'POST' }),
  getDossier: (id) => request(`/jobs/${id}/dossier`),
};

// Applications
export const applicationsApi = {
  getAll: (params = {}) => {
    const query = new URLSearchParams(params).toString();
    return request(`/applications${query ? `?${query}` : ''}`);
  },
  getStats: () => request('/applications/stats'),
  create: (data) => request('/applications', { method: 'POST', body: JSON.stringify(data) }),
  update: (id, data) => request(`/applications/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  delete: (id) => request(`/applications/${id}`, { method: 'DELETE' }),
};

// Analytics
export const analyticsApi = {
  getFunnel: () => request('/analytics/funnel'),
  getTimeline: () => request('/analytics/timeline'),
  getFitScoreDistribution: () => request('/analytics/fit-score-distribution'),
  getSalaryDistribution: () => request('/analytics/salary-distribution'),
  getSkillGap: () => request('/analytics/skill-gap'),
  getResponseRate: () => request('/analytics/response-rate'),
  getStatusBreakdown: () => request('/analytics/status-breakdown'),
};

// Resume
export const resumeApi = {
  getProfile: () => request('/resume/base'),
  tailor: (jobId) => request(`/resume/tailor/${jobId}`, { method: 'POST' }),
  generate: (jobId, data) => request(`/resume/generate/${jobId}`, { method: 'POST', body: JSON.stringify(data) }),
  download: (id) => request(`/resume/download/${id}`),
};

// Cover Letter
export const coverLetterApi = {
  generate: (jobId) => request(`/cover-letter/generate/${jobId}`, { method: 'POST' }),
  get: (jobId) => request(`/cover-letter/${jobId}`),
  download: (id) => request(`/cover-letter/download/${id}`, { method: 'POST' }),
};

// Interview Prep
export const interviewPrepApi = {
  get: (jobId) => request(`/interview-prep/${jobId}`),
  generate: (jobId) => request(`/interview-prep/generate/${jobId}`, { method: 'POST' }),
};

// Reminders
export const remindersApi = {
  getAll: () => request('/reminders'),
  dismiss: (id) => request(`/reminders/${id}`, { method: 'PATCH', body: JSON.stringify({ is_dismissed: true }) }),
};

// Settings
export const settingsApi = {
  get: (key) => request(`/settings/${key}`),
  set: (key, value) => request(`/settings/${key}`, { method: 'PUT', body: JSON.stringify({ value }) }),
  getAll: () => request('/settings'),
};

export default {
  jobs: jobsApi,
  applications: applicationsApi,
  analytics: analyticsApi,
  resume: resumeApi,
  coverLetter: coverLetterApi,
  interviewPrep: interviewPrepApi,
  reminders: remindersApi,
  settings: settingsApi,
};
