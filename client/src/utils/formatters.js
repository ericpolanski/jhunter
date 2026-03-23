// Format relative time (e.g., "2h ago", "3d ago")
export function formatRelativeTime(dateString) {
  if (!dateString) return '';

  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now - date;
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffSecs < 60) return 'Just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
  return `${Math.floor(diffDays / 30)}mo ago`;
}

// Format date
export function formatDate(dateString) {
  if (!dateString) return '';
  return new Date(dateString).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

// Format salary
export function formatSalary(min, max) {
  if (!min && !max) return 'Not specified';
  if (!max) return `$${(min / 1000).toFixed(0)}k+`;
  if (!min) return `Up to $${(max / 1000).toFixed(0)}k`;
  return `$${(min / 1000).toFixed(0)}k - $${(max / 1000).toFixed(0)}k`;
}

// Get fit score color
export function getFitScoreColor(score) {
  if (score >= 80) return 'text-fit-80 bg-fit-80/10';
  if (score >= 60) return 'text-fit-60 bg-fit-60/10';
  if (score >= 40) return 'text-fit-40 bg-fit-40/10';
  if (score >= 20) return 'text-fit-20 bg-fit-20/10';
  return 'text-fit-0 bg-fit-0/10';
}

// Get status color
export function getStatusColor(status) {
  const colors = {
    saved: 'bg-gray-500',
    applied: 'bg-primary',
    phone_screen: 'bg-accent',
    interview: 'bg-purple-500',
    offer: 'bg-success',
    rejected: 'bg-error',
    withdrawn: 'bg-text-muted',
  };
  return colors[status] || 'bg-gray-500';
}

// Get status label
export function getStatusLabel(status) {
  const labels = {
    saved: 'Saved',
    applied: 'Applied',
    phone_screen: 'Phone Screen',
    interview: 'Interview',
    offer: 'Offer',
    rejected: 'Rejected',
    withdrawn: 'Withdrawn',
  };
  return labels[status] || status;
}

// Truncate text
export function truncate(text, length = 100) {
  if (!text) return '';
  if (text.length <= length) return text;
  return text.slice(0, length) + '...';
}

// Source badge color
export function getSourceBadgeColor(source) {
  const colors = {
    linkedin: 'bg-blue-500',
    indeed: 'bg-green-500',
    glassdoor: 'bg-purple-500',
    direct: 'bg-gray-500',
  };
  return colors[source] || 'bg-gray-500';
}
