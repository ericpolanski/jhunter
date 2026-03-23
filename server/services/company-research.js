/**
 * Company Research Service
 * Scrapes company websites and Glassdoor for research data
 */

const SCRAPFLY_API_KEY = process.env.SCRAPFLY_API_KEY;
const SCRAPFLY_BASE_URL = 'https://api.scrapfly.com';

export async function scrapeCompanyWebsite(websiteUrl) {
  if (!websiteUrl) {
    return { success: false, error: 'No website URL provided' };
  }

  try {
    const response = await fetch(
      `${SCRAPFLY_BASE_URL}/scrape?key=${SCRAPFLY_API_KEY}&url=${encodeURIComponent(websiteUrl)}&asp=true&render_js=true`,
      { method: 'GET' }
    );

    if (!response.ok) {
      throw new Error(`Scrapfly error: ${response.status}`);
    }

    const data = await response.json();
    return {
      success: true,
      content: data.result?.text_content || data.result?.content || '',
      html: data.result?.content || ''
    };
  } catch (error) {
    console.error('Company website scrape error:', error);
    return { success: false, error: error.message };
  }
}

export async function scrapeGlassdoor(companyName) {
  if (!companyName) {
    return { success: false, error: 'No company name provided' };
  }

  const glassdoorUrl = `https://www.glassdoor.com/Reviews/${companyName.replace(/\s+/g, '-')}-reviews-SRCH.htm`;

  try {
    const response = await fetch(
      `${SCRAPFLY_BASE_URL}/scrape?key=${SCRAPFLY_API_KEY}&url=${encodeURIComponent(glassdoorUrl)}&asp=true&render_js=true`,
      { method: 'GET' }
    );

    if (!response.ok) {
      throw new Error(`Scrapfly error: ${response.status}`);
    }

    const data = await response.json();
    return {
      success: true,
      content: data.result?.text_content || data.result?.content || '',
      html: data.result?.content || ''
    };
  } catch (error) {
    console.error('Glassdoor scrape error:', error);
    return { success: false, error: error.message };
  }
}

/**
 * Research a company - scrape website and Glassdoor
 * Returns structured data for LLM synthesis
 */
export async function researchCompany(company) {
  const results = {
    website: null,
    glassdoor: null,
    company_name: company.name,
    website_url: company.website
  };

  // Scrape company website if available
  if (company.website) {
    results.website = await scrapeCompanyWebsite(company.website);
  }

  // Scrape Glassdoor
  results.glassdoor = await scrapeGlassdoor(company.name);

  return results;
}

/**
 * Extract structured info from scraped content
 * This would normally use LLM to parse, but we provide the raw data here
 */
export function extractCompanyInfo(scrapedData) {
  const info = {
    size: null,
    industry: null,
    description: null,
    tech_stack: [],
    glassdoor_rating: null
  };

  // Basic extraction from text content
  if (scrapedData.website?.success && scrapedData.website.content) {
    const text = scrapedData.website.content.toLowerCase();

    // Look for size indicators
    if (text.includes('enterprise') || text.includes('500+ employees')) {
      info.size = 'enterprise';
    } else if (text.includes('100-500') || text.includes('50-200')) {
      info.size = 'mid';
    } else if (text.includes('10-50') || text.includes('startup')) {
      info.size = 'startup';
    }

    // Look for tech stack keywords
    const techKeywords = ['react', 'angular', 'vue', 'node', 'python', 'aws', 'gcp', 'azure', 'kubernetes', 'docker', 'graphql', 'typescript'];
    info.tech_stack = techKeywords.filter(t => text.includes(t));

    // Description - first few sentences
    const sentences = scrapedData.website.content.split(/[.!?]/);
    if (sentences.length > 0) {
      info.description = sentences.slice(0, 3).join('. ').substring(0, 500);
    }
  }

  // Glassdoor rating extraction
  if (scrapedData.glassdoor?.success && scrapedData.glassdoor.content) {
    const ratingMatch = scrapedData.glassdoor.content.match(/(\d+\.\d+)\s*(?:out of)?\s*5(?:\s*stars?)?/i);
    if (ratingMatch) {
      info.glassdoor_rating = parseFloat(ratingMatch[1]);
    }
  }

  return info;
}

export default {
  scrapeCompanyWebsite,
  scrapeGlassdoor,
  researchCompany,
  extractCompanyInfo
};