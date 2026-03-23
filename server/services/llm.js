import db from '../db.js';

const API_BASE_URL = process.env.ANTHROPIC_BASE_URL || 'https://api.minimax.io/anthropic';
const API_KEY = process.env.ANTHROPIC_AUTH_TOKEN || process.env.MINIMAX_API_KEY;

export async function callLLM(messages, options = {}) {
  const { model = 'MiniMax-M2.7', maxTokens = 4096, temperature = 0.7 } = options;

  const response = await fetch(`${API_BASE_URL}/v1/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model,
      messages,
      max_tokens: maxTokens,
      temperature
    })
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`LLM API error: ${response.status} - ${error}`);
  }

  const data = await response.json();
  // MiniMax returns multiple content blocks: thinking (reasoning) first, then text
  // Always extract from the text block, not the thinking block
  let textContent = data.content?.find(c => c.type === 'text')?.text || '';
  if (!textContent) {
    throw new Error(`LLM returned no text content. Response: ${JSON.stringify(data).slice(0, 200)}`);
  }

  // Strip markdown code fences if present (LLM often wraps JSON in ```json ... ```)
  textContent = textContent.replace(/^```json\s*/i, '').replace(/```\s*$/i, '').trim();

  return textContent;
}

export async function generateFitScore(profile, jobDescription) {
  const systemPrompt = `You are a job fit analyzer. Given a candidate profile and a job description, analyze how well the candidate fits the role.

Return your analysis as a JSON object with this exact structure:
{
  "score": <number 0-100>,
  "breakdown": {
    "skill_match": <number 0-100>,
    "location_match": <number 0-100>,
    "experience_match": <number 0-100>,
    "role_match": <number 0-100>,
    "salary_match": <number 0-100>
  },
  "explanation": "<2-3 sentence explanation of the score>"
}

Be strict but fair. Focus on:
- Skills match (Python, ML frameworks, data pipelines, APIs)
- Location (Chicago area preferred)
- Experience level (entry-level appropriate)
- Role type (AI Engineer, Software Engineer, ML Engineer)
- Salary expectations
`;

  const userMessage = `Candidate Profile:
${JSON.stringify(profile, null, 2)}

Job Description:
${jobDescription}`;

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userMessage }
  ], { temperature: 0.3 });

  try {
    return JSON.parse(result);
  } catch {
    return {
      score: 50,
      breakdown: { skill_match: 50, location_match: 50, experience_match: 50, role_match: 50, salary_match: 50 },
      explanation: 'Unable to parse LLM response, defaulting to average score.'
    };
  }
}

export async function generateResumeSuggestions(profile, jobDescription) {
  const systemPrompt = `You are a resume tailoring expert. Given a candidate profile and a job description, suggest 3-7 ways to tailor the resume for the specific role.

Return your analysis as a JSON object with this exact structure:
{
  "suggestions": [
    {
      "section": "<experience/project name>",
      "original": "<original text>",
      "suggested": "<improved text>",
      "reason": "<why this change improves fit>",
      "impact": "<high/medium/low>"
    }
  ]
}

Focus on:
- Highlighting relevant skills and technologies
- Emphasizing relevant projects or experience
- Using job-specific keywords
- Quantifying achievements where possible
`;

  const userMessage = `Candidate Profile:
${JSON.stringify(profile, null, 2)}

Job Description:
${jobDescription}`;

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userMessage }
  ], { temperature: 0.5 });

  try {
    return JSON.parse(result);
  } catch {
    return { suggestions: [] };
  }
}

export async function generateCoverLetter(profile, jobDescription, companyDossier = null) {
  const systemPrompt = `You are a professional cover letter writer. Write a concise, compelling 3-4 paragraph cover letter for a job application.

Return your response as a JSON object:
{
  "content": "<full cover letter text, 300-400 words>"
}

The letter should:
- Be professional but personable
- Highlight 2-3 key qualifications matching the job
- Show genuine interest in the company
- End with a call to action
- NOT include placeholder text like [Company Name]
`;

  let context = `Candidate Profile:
${JSON.stringify(profile, null, 2)}

Job Description:
${jobDescription}`;

  if (companyDossier) {
    context += `\n\nCompany Dossier:\n${companyDossier}`;
  }

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: context }
  ], { temperature: 0.5 });

  try {
    return JSON.parse(result);
  } catch {
    return { content: result };
  }
}

export async function generateCompanyDossier(companyData) {
  const systemPrompt = `You are a company research analyst. Given company information, create a comprehensive dossier.

Return your response as a JSON object:
{
  "overview": "<2-3 sentence company overview>",
  "size": "<startup/small/mid/large/enterprise/unknown>",
  "tech_stack": ["<list of technologies>"],
  "rating": <Glassdoor rating or null>,
  "news": ["<recent news items>"],
  "connections": ["<connections to candidate's background>"]
}

Be concise and informative.
`;

  const userMessage = `Company Information:
${JSON.stringify(companyData, null, 2)}`;

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userMessage }
  ], { temperature: 0.3 });

  try {
    return JSON.parse(result);
  } catch {
    return {
      overview: 'Unable to generate dossier',
      size: 'unknown',
      tech_stack: [],
      rating: null,
      news: [],
      connections: []
    };
  }
}

export async function generateInterviewPrep(profile, jobDescription, companyDossier = null) {
  const systemPrompt = `You are an interview preparation expert. Given a candidate's profile, job description, and company info, generate comprehensive interview preparation materials.

Return your response as a JSON object:
{
  "likely_questions": [
    {
      "question": "<question text>",
      "type": "<technical/behavioral/role-specific>",
      "answer_framework": "<how to structure the answer>"
    }
  ],
  "talking_points": [
    {
      "point": "<key point about candidate>",
      "evidence": "<specific example from profile>",
      "when_to_use": "<which questions this maps to>"
    }
  ],
  "role_specific_prep": "<2-3 paragraphs of role-specific preparation advice>"
}

Generate 8-10 likely questions with answer frameworks.
`;

  let context = `Candidate Profile:
${JSON.stringify(profile, null, 2)}

Job Description:
${jobDescription}`;

  if (companyDossier) {
    context += `\n\nCompany Info:\n${companyDossier}`;
  }

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: context }
  ], { temperature: 0.5 });

  try {
    return JSON.parse(result);
  } catch {
    return { likely_questions: [], talking_points: [], role_specific_prep: '' };
  }
}

export async function generateFollowUpDraft(application, job, company) {
  const systemPrompt = `You are a professional email writer. Given an application context, draft a follow-up email.

Return your response as a JSON object:
{
  "subject": "<email subject>",
  "body": "<full email body>"
}

Keep it concise, professional, and friendly. Reference the specific role and company.
`;

  const userMessage = `Application:
- Status: ${application.status}
- Applied date: ${application.applied_date || 'N/A'}
- Notes: ${application.notes || 'N/A'}

Job:
- Title: ${job.title}
- Company: ${job.company_name}
- Location: ${job.location}

Company: ${company ? company.name : job.company_name}`;

  const result = await callLLM([
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userMessage }
  ], { temperature: 0.5 });

  try {
    return JSON.parse(result);
  } catch {
    return { subject: 'Following up on application', body: result };
  }
}

export default { callLLM, generateFitScore, generateResumeSuggestions, generateCoverLetter, generateCompanyDossier, generateInterviewPrep, generateFollowUpDraft };
