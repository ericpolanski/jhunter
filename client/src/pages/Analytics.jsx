import { useEffect, useState } from 'react';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import { analyticsApi } from '../utils/api';

const COLORS = ['#0D7377', '#F5A623', '#E85A4F', '#10B981', '#8B5CF6', '#6B7280', '#F97316'];

export default function Analytics() {
  const [data, setData] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadAnalytics();
  }, []);

  async function loadAnalytics() {
    setLoading(true);
    try {
      const [funnel, timeline, fitDist, salaryDist, skillGap, responseRate, statusBreakdown] = await Promise.all([
        analyticsApi.getFunnel(),
        analyticsApi.getTimeline(),
        analyticsApi.getFitScoreDistribution(),
        analyticsApi.getSalaryDistribution(),
        analyticsApi.getSkillGap(),
        analyticsApi.getResponseRate(),
        analyticsApi.getStatusBreakdown(),
      ]);
      setData({ funnel, timeline, fitDist, salaryDist, skillGap, responseRate, statusBreakdown });
    } catch (error) {
      console.error('Failed to load analytics:', error);
    } finally {
      setLoading(false);
    }
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold">Analytics</h1>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {[...Array(7)].map((_, i) => (
            <div key={i} className="h-80 skeleton rounded-md"></div>
          ))}
        </div>
      </div>
    );
  }

  // Transform data for charts
  const funnelData = (data.funnel || []).map(item => ({
    name: item.status.replace('_', ' '),
    value: item.count,
  }));

  const statusData = (data.statusBreakdown || []).map(item => ({
    name: item.status.replace('_', ' '),
    value: item.value,
  }));

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold text-text-primary">Analytics</h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Application Funnel */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Application Funnel</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={funnelData} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis type="category" dataKey="name" width={100} />
                <Tooltip />
                <Bar dataKey="value" fill="#0D7377" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Status Breakdown */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Status Breakdown</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={statusData}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={80}
                  paddingAngle={5}
                  dataKey="value"
                >
                  {statusData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Application Timeline */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Application Timeline</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.timeline || []}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="count" stroke="#0D7377" strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Fit Score Distribution */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Fit Score Distribution</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.fitDist || []}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="bucket" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="count" fill="#F5A623" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Salary Distribution */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Salary Distribution</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.salaryDist || []}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="bucket" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="count" fill="#10B981" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Skill Gap Analysis */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Skill Gap Analysis</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.skillGap || []} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis type="category" dataKey="skill" width={100} />
                <Tooltip />
                <Bar dataKey="count" fill="#E85A4F" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Response Rate by Source */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Response Rate by Source</h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.responseRate || []}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="source" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="response_rate" fill="#8B5CF6" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
