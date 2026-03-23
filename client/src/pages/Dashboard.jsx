import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Briefcase, Users, Phone, Award, ArrowRight, RefreshCw } from 'lucide-react';
import { jobsApi, applicationsApi, remindersApi } from '../utils/api';
import { formatRelativeTime, getFitScoreColor, getStatusColor, getStatusLabel, truncate } from '../utils/formatters';

export default function Dashboard() {
  const [stats, setStats] = useState({ total: 0, byStatus: {} });
  const [recentJobs, setRecentJobs] = useState([]);
  const [reminders, setReminders] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const [appsStats, jobs, rems] = await Promise.all([
          applicationsApi.getStats(),
          jobsApi.getAll({ limit: 4, sort: 'date_desc' }),
          remindersApi.getAll(),
        ]);
        setStats(appsStats);
        setRecentJobs(jobs);
        setReminders(rems.filter(r => !r.is_dismissed));
      } catch (error) {
        console.error('Failed to load dashboard data:', error);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="h-8 w-32 skeleton rounded"></div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="h-24 skeleton rounded-md"></div>
          ))}
        </div>
      </div>
    );
  }

  const statusCounts = stats.byStatus || {};
  const appliedCount = statusCounts.applied || 0;
  const interviewCount = (statusCounts.interview || 0) + (statusCounts.phone_screen || 0);
  const offerCount = statusCounts.offer || 0;
  const responseRate = stats.total > 0 ? Math.round((interviewCount + offerCount) / stats.total * 100) : 0;

  const statCards = [
    { label: 'Applied', value: appliedCount, icon: Briefcase, color: 'text-primary' },
    { label: 'Response Rate', value: `${responseRate}%`, icon: Users, color: 'text-accent' },
    { label: 'Interviews', value: interviewCount, icon: Phone, color: 'text-purple-500' },
    { label: 'Offers', value: offerCount, icon: Award, color: 'text-success' },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-bold text-text-primary">Dashboard</h1>
        <div className="flex gap-2">
          <button className="btn btn-secondary flex items-center gap-2">
            <RefreshCw size={16} />
            Sync
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {statCards.map(({ label, value, icon: Icon, color }) => (
          <div key={label} className="card">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-text-secondary">{label}</p>
                <p className="text-3xl font-bold text-text-primary">{value}</p>
              </div>
              <div className={`p-3 rounded-full bg-background-secondary ${color}`}>
                <Icon size={24} />
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Pipeline Overview & Reminders */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Pipeline Overview */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Pipeline Overview</h2>
          <div className="space-y-3">
            {['saved', 'applied', 'phone_screen', 'interview', 'offer', 'rejected'].map(status => {
              const count = statusCounts[status] || 0;
              const total = stats.total || 1;
              const percent = Math.round((count / total) * 100);
              return (
                <div key={status} className="flex items-center gap-3">
                  <div className={`w-3 h-3 rounded-full ${getStatusColor(status)}`}></div>
                  <span className="flex-1 text-sm text-text-secondary">{getStatusLabel(status)}</span>
                  <span className="text-sm font-medium">{count}</span>
                  <div className="w-24 h-2 bg-background-secondary rounded-full overflow-hidden">
                    <div
                      className={`h-full ${getStatusColor(status)}`}
                      style={{ width: `${percent}%` }}
                    ></div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Reminders */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Reminders</h2>
          {reminders.length === 0 ? (
            <p className="text-text-secondary text-sm">No active reminders</p>
          ) : (
            <div className="space-y-3">
              {reminders.slice(0, 5).map(reminder => (
                <div key={reminder.id} className="flex items-center gap-3 p-3 bg-background-secondary rounded-md">
                  <div className="flex-1">
                    <p className="text-sm font-medium">{reminder.reminder_type.replace('_', ' ')}</p>
                    <p className="text-xs text-text-secondary">Due: {formatRelativeTime(reminder.due_date)}</p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Recent Jobs */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">New Jobs</h2>
          <Link to="/jobs" className="text-primary text-sm flex items-center gap-1 hover:underline">
            See All <ArrowRight size={14} />
          </Link>
        </div>
        {recentJobs.length === 0 ? (
          <p className="text-text-secondary text-sm">No jobs yet. Start by adding some jobs!</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {recentJobs.map(job => (
              <Link
                key={job.id}
                to={`/jobs/${job.id}`}
                className="flex items-center gap-4 p-4 bg-background-secondary rounded-md hover:bg-border/50"
              >
                <div className={`w-12 h-12 rounded-full flex items-center justify-center text-white font-bold ${getFitScoreColor(job.fit_score)}`}>
                  {job.fit_score || '-'}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-text-primary truncate">{job.title}</p>
                  <p className="text-sm text-text-secondary truncate">{job.company_name}</p>
                </div>
                <span className="text-xs text-text-muted">{formatRelativeTime(job.scraped_at)}</span>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
