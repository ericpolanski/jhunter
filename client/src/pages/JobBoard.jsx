import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Search, Filter, ChevronLeft, ChevronRight, MapPin, ExternalLink } from 'lucide-react';
import { jobsApi, applicationsApi } from '../utils/api';
import { formatRelativeTime, formatSalary, getFitScoreColor, getSourceBadgeColor } from '../utils/formatters';

export default function JobBoard() {
  const [jobs, setJobs] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    search: '',
    source: '',
    minScore: '',
    location: '',
    sort: 'date_desc',
  });
  const [pagination, setPagination] = useState({ limit: 20, offset: 0 });

  useEffect(() => {
    loadJobs();
  }, [filters, pagination]);

  async function loadJobs() {
    setLoading(true);
    try {
      const params = { ...filters, ...pagination };
      // Remove empty values
      Object.keys(params).forEach(key => {
        if (!params[key]) delete params[key];
      });
      const data = await jobsApi.getAll(params);
      // API returns array directly
      setJobs(Array.isArray(data) ? data : []);
      setTotal(Array.isArray(data) ? data.length : 0);
    } catch (error) {
      console.error('Failed to load jobs:', error);
    } finally {
      setLoading(false);
    }
  }

  function handleFilterChange(key, value) {
    setFilters(prev => ({ ...prev, [key]: value }));
    setPagination(prev => ({ ...prev, offset: 0 }));
  }

  const totalPages = Math.ceil(total / pagination.limit);
  const currentPage = Math.floor(pagination.offset / pagination.limit) + 1;

  async function handleSaveJob(job) {
    try {
      await applicationsApi.create({ job_id: job.id, status: 'saved' });
      loadJobs();
    } catch (error) {
      console.error('Failed to save job:', error);
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <h1 className="text-2xl font-bold text-text-primary">Job Board</h1>
      </div>

      {/* Filters */}
      <div className="card">
        <div className="flex flex-col lg:flex-row gap-4">
          {/* Search */}
          <div className="flex-1">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-text-muted" size={18} />
              <input
                type="text"
                placeholder="Search jobs..."
                className="input pl-10"
                value={filters.search}
                onChange={e => handleFilterChange('search', e.target.value)}
              />
            </div>
          </div>

          {/* Source Filter */}
          <select
            className="input w-full lg:w-40"
            value={filters.source}
            onChange={e => handleFilterChange('source', e.target.value)}
          >
            <option value="">All Sources</option>
            <option value="linkedin">LinkedIn</option>
            <option value="indeed">Indeed</option>
            <option value="glassdoor">Glassdoor</option>
          </select>

          {/* Min Score Filter */}
          <select
            className="input w-full lg:w-40"
            value={filters.minScore}
            onChange={e => handleFilterChange('minScore', e.target.value)}
          >
            <option value="">Any Score</option>
            <option value="80">80+</option>
            <option value="60">60+</option>
            <option value="40">40+</option>
          </select>

          {/* Sort */}
          <select
            className="input w-full lg:w-40"
            value={filters.sort}
            onChange={e => handleFilterChange('sort', e.target.value)}
          >
            <option value="date_desc">Newest</option>
            <option value="date_asc">Oldest</option>
            <option value="score_desc">Highest Score</option>
            <option value="score_asc">Lowest Score</option>
          </select>
        </div>
      </div>

      {/* Results count */}
      <div className="text-sm text-text-secondary">
        {total} jobs found
      </div>

      {/* Job List */}
      {loading ? (
        <div className="space-y-4">
          {[...Array(5)].map((_, i) => (
            <div key={i} className="h-32 skeleton rounded-md"></div>
          ))}
        </div>
      ) : jobs.length === 0 ? (
        <div className="card text-center py-12">
          <p className="text-text-secondary">No jobs found. Try adjusting your filters.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {jobs.map(job => (
            <div key={job.id} className="card flex flex-col sm:flex-row sm:items-center gap-4">
              {/* Fit Score */}
              <div className={`flex-shrink-0 w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold ${getFitScoreColor(job.fit_score)}`}>
                {job.fit_score || '-'}
              </div>

              {/* Job Info */}
              <div className="flex-1 min-w-0">
                <div className="flex flex-col sm:flex-row sm:items-center gap-2">
                  <Link to={`/jobs/${job.id}`} className="font-semibold text-text-primary hover:text-primary">
                    {job.title}
                  </Link>
                  <span className={`px-2 py-0.5 rounded text-xs text-white ${getSourceBadgeColor(job.source)}`}>
                    {job.source}
                  </span>
                </div>
                <p className="text-text-secondary">{job.company_name}</p>
                <div className="flex flex-wrap items-center gap-3 mt-1 text-sm text-text-muted">
                  <span className="flex items-center gap-1">
                    <MapPin size={14} />
                    {job.is_remote ? 'Remote' : job.location || 'Unknown'}
                  </span>
                  <span>{formatSalary(job.salary_min, job.salary_max)}</span>
                  <span>{formatRelativeTime(job.posted_date || job.scraped_at)}</span>
                </div>
              </div>

              {/* Actions */}
              <div className="flex gap-2 flex-shrink-0">
                <Link to={`/jobs/${job.id}`} className="btn btn-secondary">
                  View
                </Link>
                {job.source_url && (
                  <a
                    href={job.source_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="btn btn-ghost"
                  >
                    <ExternalLink size={18} />
                  </a>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-text-secondary">
            Page {currentPage} of {totalPages}
          </p>
          <div className="flex gap-2">
            <button
              className="btn btn-ghost"
              disabled={currentPage === 1}
              onClick={() => setPagination(prev => ({ ...prev, offset: prev.offset - prev.limit }))}
            >
              <ChevronLeft size={18} />
            </button>
            <button
              className="btn btn-ghost"
              disabled={currentPage === totalPages}
              onClick={() => setPagination(prev => ({ ...prev, offset: prev.offset + prev.limit }))}
            >
              <ChevronRight size={18} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
