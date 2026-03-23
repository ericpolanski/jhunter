import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ArrowLeft, MapPin, DollarSign, Calendar, ExternalLink, Building, FileText, Mail, MessageSquare } from 'lucide-react';
import { jobsApi, applicationsApi, coverLetterApi, interviewPrepApi } from '../utils/api';
import { formatDate, formatSalary, getFitScoreColor } from '../utils/formatters';

export default function JobDetail() {
  const { id } = useParams();
  const [job, setJob] = useState(null);
  const [activeTab, setActiveTab] = useState('details');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [coverLetter, setCoverLetter] = useState(null);
  const [interviewPrep, setInterviewPrep] = useState(null);

  useEffect(() => {
    loadJob();
  }, [id]);

  async function loadJob() {
    setLoading(true);
    try {
      const data = await jobsApi.getById(id);
      setJob(data);
    } catch (error) {
      console.error('Failed to load job:', error);
    } finally {
      setLoading(false);
    }
  }

  async function handleSaveJob() {
    setSaving(true);
    try {
      await applicationsApi.create({ job_id: parseInt(id), status: 'saved' });
      loadJob();
    } catch (error) {
      console.error('Failed to save job:', error);
    } finally {
      setSaving(false);
    }
  }

  async function handleApply() {
    setSaving(true);
    try {
      if (job.application) {
        await applicationsApi.update(job.application.id, { status: 'applied' });
      } else {
        await applicationsApi.create({ job_id: parseInt(id), status: 'applied' });
      }
      loadJob();
    } catch (error) {
      console.error('Failed to apply:', error);
    } finally {
      setSaving(false);
    }
  }

  async function loadCoverLetter() {
    try {
      const data = await coverLetterApi.get(id);
      setCoverLetter(data);
    } catch (error) {
      console.error('Failed to load cover letter:', error);
    }
  }

  async function loadInterviewPrep() {
    try {
      const data = await interviewPrepApi.get(id);
      setInterviewPrep(data);
    } catch (error) {
      console.error('Failed to load interview prep:', error);
    }
  }

  useEffect(() => {
    if (activeTab === 'coverletter') loadCoverLetter();
    if (activeTab === 'prep') loadInterviewPrep();
  }, [activeTab]);

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="h-8 w-24 skeleton rounded"></div>
        <div className="h-64 skeleton rounded-md"></div>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-center py-12">
        <p className="text-text-secondary">Job not found</p>
        <Link to="/jobs" className="text-primary hover:underline mt-2 inline-block">
          Back to Job Board
        </Link>
      </div>
    );
  }

  const tabs = [
    { id: 'details', label: 'Details' },
    { id: 'coverletter', label: 'Cover Letter' },
    { id: 'prep', label: 'Interview Prep' },
  ];

  return (
    <div className="space-y-6">
      {/* Back button */}
      <Link to="/jobs" className="inline-flex items-center gap-2 text-text-secondary hover:text-text-primary">
        <ArrowLeft size={18} />
        Back to Jobs
      </Link>

      {/* Job Header */}
      <div className="card">
        <div className="flex flex-col lg:flex-row lg:items-start gap-6">
          {/* Job Info */}
          <div className="flex-1">
            <h1 className="text-2xl font-bold text-text-primary">{job.title}</h1>
            <div className="flex items-center gap-2 mt-2">
              <Building size={18} className="text-text-muted" />
              <span className="text-lg text-text-secondary">{job.company_name}</span>
            </div>
            <div className="flex flex-wrap gap-4 mt-3 text-sm text-text-muted">
              <span className="flex items-center gap-1">
                <MapPin size={16} />
                {job.is_remote ? 'Remote' : job.location || 'Unknown'}
              </span>
              <span className="flex items-center gap-1">
                <DollarSign size={16} />
                {formatSalary(job.salary_min, job.salary_max)}
              </span>
              <span className="flex items-center gap-1">
                <Calendar size={16} />
                {formatDate(job.posted_date)}
              </span>
            </div>
          </div>

          {/* Fit Score */}
          <div className={`flex-shrink-0 w-24 h-24 rounded-lg flex flex-col items-center justify-center ${getFitScoreColor(job.fit_score)}`}>
            <span className="text-3xl font-bold">{job.fit_score || '-'}</span>
            <span className="text-xs">Fit Score</span>
          </div>
        </div>

        {/* Actions */}
        <div className="flex gap-3 mt-6">
          {job.application ? (
            <button
              className="btn btn-primary"
              onClick={handleApply}
              disabled={saving || job.application.status === 'applied'}
            >
              {job.application.status === 'applied' ? 'Applied' : 'Mark as Applied'}
            </button>
          ) : (
            <button className="btn btn-primary" onClick={handleSaveJob} disabled={saving}>
              Save Job
            </button>
          )}
          {job.source_url && (
            <a
              href={job.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="btn btn-secondary flex items-center gap-2"
            >
              <ExternalLink size={16} />
              Apply on {job.source}
            </a>
          )}
        </div>
      </div>

      {/* Fit Score Breakdown */}
      {job.fit_breakdown && (
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Fit Score Breakdown</h2>
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-4">
            {Object.entries(job.fit_breakdown).map(([key, value]) => (
              <div key={key} className="text-center">
                <div className={`text-2xl font-bold ${getFitScoreColor(value)}`}>{value}</div>
                <div className="text-xs text-text-muted capitalize">{key.replace('_', ' ')}</div>
              </div>
            ))}
          </div>
          {job.fit_breakdown.explanation && (
            <p className="mt-4 text-sm text-text-secondary">{job.fit_breakdown.explanation}</p>
          )}
        </div>
      )}

      {/* Tabs */}
      <div className="border-b border-border">
        <div className="flex gap-6">
          {tabs.map(tab => (
            <button
              key={tab.id}
              className={`pb-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-primary text-primary'
                  : 'border-transparent text-text-secondary hover:text-text-primary'
              }`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab Content */}
      <div className="card">
        {activeTab === 'details' && (
          <div className="prose max-w-none">
            <h3 className="text-lg font-semibold mb-3">Job Description</h3>
            <div className="whitespace-pre-wrap text-text-secondary text-sm">
              {job.description || 'No description available.'}
            </div>
            {job.requirements && job.requirements.length > 0 && (
              <>
                <h3 className="text-lg font-semibold mt-6 mb-3">Requirements</h3>
                <ul className="list-disc list-inside space-y-1 text-text-secondary text-sm">
                  {job.requirements.map((req, i) => (
                    <li key={i}>{req}</li>
                  ))}
                </ul>
              </>
            )}
          </div>
        )}

        {activeTab === 'coverletter' && (
          <div>
            {coverLetter ? (
              <div className="whitespace-pre-wrap text-text-secondary text-sm">
                {coverLetter.content}
              </div>
            ) : (
              <div className="text-center py-8">
                <MessageSquare size={48} className="mx-auto text-text-muted mb-4" />
                <p className="text-text-secondary mb-4">Generate a cover letter for this job</p>
                <button className="btn btn-primary">Generate Cover Letter</button>
              </div>
            )}
          </div>
        )}

        {activeTab === 'prep' && (
          <div>
            {interviewPrep ? (
              <div className="space-y-6">
                <div>
                  <h3 className="text-lg font-semibold mb-3">Likely Questions</h3>
                  <div className="space-y-4">
                    {interviewPrep.likely_questions?.map((q, i) => (
                      <div key={i} className="p-4 bg-background-secondary rounded-md">
                        <p className="font-medium">{q.question}</p>
                        <p className="text-sm text-text-muted mt-1">{q.type}</p>
                        <p className="text-sm text-text-secondary mt-2">{q.answer_framework}</p>
                      </div>
                    ))}
                  </div>
                </div>
                {interviewPrep.role_specific_prep && (
                  <div>
                    <h3 className="text-lg font-semibold mb-3">Role-Specific Prep</h3>
                    <div className="p-4 bg-background-secondary rounded-md text-text-secondary text-sm">
                      {interviewPrep.role_specific_prep}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="text-center py-8">
                <FileText size={48} className="mx-auto text-text-muted mb-4" />
                <p className="text-text-secondary mb-4">Generate interview prep for this job</p>
                <button className="btn btn-primary">Generate Interview Prep</button>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
