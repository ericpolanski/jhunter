import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Table, LayoutGrid, ChevronDown, Plus } from 'lucide-react';
import { applicationsApi } from '../utils/api';
import { formatDate, getStatusColor, getStatusLabel, getFitScoreColor } from '../utils/formatters';

const STATUSES = ['saved', 'applied', 'phone_screen', 'interview', 'offer', 'rejected', 'withdrawn'];

export default function Applications() {
  const [applications, setApplications] = useState([]);
  const [view, setView] = useState('table'); // 'table' or 'kanban'
  const [loading, setLoading] = useState(true);
  const [filterStatus, setFilterStatus] = useState('');

  useEffect(() => {
    loadApplications();
  }, [filterStatus]);

  async function loadApplications() {
    setLoading(true);
    try {
      const params = filterStatus ? { status: filterStatus } : {};
      const data = await applicationsApi.getAll(params);
      setApplications(data);
    } catch (error) {
      console.error('Failed to load applications:', error);
    } finally {
      setLoading(false);
    }
  }

  async function handleStatusChange(appId, newStatus) {
    try {
      await applicationsApi.update(appId, { status: newStatus });
      loadApplications();
    } catch (error) {
      console.error('Failed to update status:', error);
    }
  }

  const applicationsByStatus = STATUSES.reduce((acc, status) => {
    acc[status] = applications.filter(app => app.status === status);
    return acc;
  }, {});

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-bold text-text-primary">Applications</h1>
        <div className="flex gap-2">
          {/* View Toggle */}
          <div className="flex bg-background-secondary rounded-md p-1">
            <button
              className={`p-2 rounded ${view === 'table' ? 'bg-surface shadow-sm' : ''}`}
              onClick={() => setView('table')}
            >
              <Table size={18} />
            </button>
            <button
              className={`p-2 rounded ${view === 'kanban' ? 'bg-surface shadow-sm' : ''}`}
              onClick={() => setView('kanban')}
            >
              <LayoutGrid size={18} />
            </button>
          </div>
          {/* Status Filter */}
          <select
            className="input w-40"
            value={filterStatus}
            onChange={e => setFilterStatus(e.target.value)}
          >
            <option value="">All Statuses</option>
            {STATUSES.map(status => (
              <option key={status} value={status}>{getStatusLabel(status)}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Loading */}
      {loading ? (
        <div className="space-y-4">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-16 skeleton rounded-md"></div>
          ))}
        </div>
      ) : applications.length === 0 ? (
        <div className="card text-center py-12">
          <p className="text-text-secondary mb-4">No applications yet</p>
          <Link to="/jobs" className="btn btn-primary">Browse Jobs</Link>
        </div>
      ) : view === 'table' ? (
        /* Table View */
        <div className="card overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-3 px-4 text-sm font-medium text-text-secondary">Job</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-text-secondary">Company</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-text-secondary">Score</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-text-secondary">Status</th>
                <th className="text-left py-3 px-4 text-sm font-medium text-text-secondary">Applied</th>
              </tr>
            </thead>
            <tbody>
              {applications.map(app => (
                <tr key={app.id} className="border-b border-border hover:bg-background-secondary">
                  <td className="py-3 px-4">
                    <Link to={`/jobs/${app.job_id}`} className="font-medium text-text-primary hover:text-primary">
                      {app.title}
                    </Link>
                  </td>
                  <td className="py-3 px-4 text-text-secondary">{app.company_name}</td>
                  <td className="py-3 px-4">
                    <span className={`px-2 py-1 rounded text-sm font-medium ${getFitScoreColor(app.fit_score)}`}>
                      {app.fit_score || '-'}
                    </span>
                  </td>
                  <td className="py-3 px-4">
                    <select
                      value={app.status}
                      onChange={e => handleStatusChange(app.id, e.target.value)}
                      className="input w-36 h-9 text-sm"
                    >
                      {STATUSES.map(status => (
                        <option key={status} value={status}>{getStatusLabel(status)}</option>
                      ))}
                    </select>
                  </td>
                  <td className="py-3 px-4 text-text-secondary text-sm">
                    {app.applied_date ? formatDate(app.applied_date) : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        /* Kanban View */
        <div className="flex gap-4 overflow-x-auto pb-4">
          {STATUSES.map(status => (
            <div key={status} className="flex-shrink-0 w-72">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <div className={`w-3 h-3 rounded-full ${getStatusColor(status)}`}></div>
                  <span className="font-medium text-sm">{getStatusLabel(status)}</span>
                </div>
                <span className="text-sm text-text-muted">{applicationsByStatus[status].length}</span>
              </div>
              <div className="space-y-3 min-h-[200px] p-2 bg-background-secondary rounded-lg">
                {applicationsByStatus[status].map(app => (
                  <div key={app.id} className="bg-surface p-3 rounded-md shadow-sm">
                    <Link to={`/jobs/${app.job_id}`} className="font-medium text-text-primary hover:text-primary text-sm">
                      {app.title}
                    </Link>
                    <p className="text-xs text-text-muted mt-1">{app.company_name}</p>
                    <div className="flex items-center justify-between mt-2">
                      <span className={`text-xs px-2 py-0.5 rounded ${getFitScoreColor(app.fit_score)}`}>
                        {app.fit_score || '-'}
                      </span>
                      {app.applied_date && (
                        <span className="text-xs text-text-muted">{formatDate(app.applied_date)}</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
