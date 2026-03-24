#!/usr/bin/env python3
"""
Fortune 100 Career Page Scraper using Scrapfly.
Scrapes job listings from Fortune 100 company career pages.
Outputs normalized JSON for consumption by Node.js parent process.
"""

import sys
import json
import os
import argparse
import time
import re
from datetime import datetime, timedelta

# Scrapfly SDK
from scrapfly import ScrapflyClient, ScrapflyError

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMPANIES_FILE = os.path.join(os.path.dirname(SCRIPT_DIR), "..", "..", "data", "fortune100_companies.json")

ROLES = ['AI Engineer', 'Software Engineer', 'Machine Learning Engineer']
EXCLUDE_TITLES = ['senior', 'principal', 'staff', 'manager', 'director', 'vp ', 'head of', 'chief']
MAX_PAGES_PER_COMPANY = 5
REQUEST_DELAY = 5  # seconds between requests


def load_companies():
    """Load Fortune 100 companies from the default JSON file."""
    with open(COMPANIES_FILE, 'r') as f:
        return json.load(f)


def parse_date(date_str):
    """Parse various date formats to YYYY-MM-DD."""
    if not date_str:
        return None

    date_str = str(date_str).lower().strip()

    # Handle relative dates
    try:
        if 'day' in date_str and 'ago' in date_str:
            num = int(re.search(r'\d+', date_str).group() or '1')
            d = datetime.now() - timedelta(days=num)
            return d.strftime('%Y-%m-%d')
        elif 'hour' in date_str and 'ago' in date_str:
            return datetime.now().strftime('%Y-%m-%d')
        elif 'week' in date_str and 'ago' in date_str:
            num = int(re.search(r'\d+', date_str).group() or '1')
            d = datetime.now() - timedelta(weeks=num)
            return d.strftime('%Y-%m-%d')
        elif 'month' in date_str and 'ago' in date_str:
            num = int(re.search(r'\d+', date_str).group() or '1')
            d = datetime.now() - timedelta(days=num * 30)
            return d.strftime('%Y-%m-%d')
    except Exception:
        pass

    # Try direct parse
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    except Exception:
        pass

    return None


def parse_salary(text):
    """Parse salary text to min/max values."""
    if not text:
        return None, None, None

    text = str(text).replace('$', '').replace(',', '').strip()

    # Handle ranges like "120K - 180K" or "80,000 - 120,000"
    numbers = re.findall(r'[\d]+', text)
    if not numbers:
        return None, None, text

    vals = [int(n) for n in numbers]
    is_thousands = any(v < 500 for v in vals)

    if is_thousands:
        vals = [v * 1000 for v in vals]

    min_sal = vals[0] if vals else None
    max_sal = vals[1] if len(vals) > 1 else min_sal

    return min_sal, max_sal, text if min_sal or max_sal else None


def should_exclude_title(title):
    """Check if job title should be excluded (senior/principal/staff)."""
    if not title:
        return False
    title_lower = title.lower()
    return any(excl in title_lower for excl in EXCLUDE_TITLES)


def matches_target_roles(title):
    """Check if job title matches target roles."""
    if not title:
        return False
    title_lower = title.lower()
    return any(role.lower() in title_lower for role in ROLES)


def normalize_job(job_data, company_name, source_url):
    """Normalize a job dict to the standard output format."""
    title = job_data.get('title', job_data.get('job_title', ''))
    location = job_data.get('location', job_data.get('job_location', 'United States'))
    salary_text = job_data.get('salary', job_data.get('salary_text', ''))
    posted_date = job_data.get('posted_date', job_data.get('date_posted', job_data.get('posted', '')))
    description = job_data.get('description', job_data.get('job_description', ''))
    requirements = job_data.get('requirements', [])

    min_sal, max_sal, salary_text = parse_salary(salary_text) if salary_text else (None, None, None)
    if salary_text:
        parsed = parse_salary(salary_text)
        if parsed[0]: min_sal = parsed[0]
        if parsed[1]: max_sal = parsed[1]

    posted_date = parse_date(posted_date)

    return {
        'source': 'fortune100',
        'source_url': source_url or job_data.get('url', job_data.get('apply_url', '')),
        'title': title,
        'company_name': company_name,
        'location': location,
        'is_remote': 'remote' in location.lower() if location else False,
        'salary_text': salary_text,
        'salary_min': min_sal,
        'salary_max': max_sal,
        'posted_date': posted_date,
        'description': description,
        'requirements': json.dumps(requirements) if isinstance(requirements, list) else (requirements or '[]')
    }


async def scrape_company_career_page(client, company, roles, results_wanted):
    """Scrape a single company's career page for all target roles."""
    company_name = company['name']
    job_listing_pattern = company.get('job_listing_pattern', company.get('career_url', ''))
    all_jobs = []

    print(f"[fortune100] Scraping {company_name}...", file=sys.stderr)

    for role in roles:
        if len(all_jobs) >= results_wanted:
            break

        # Build the search URL with role
        search_url = job_listing_pattern.replace('{role}', role.replace(' ', '+'))

        # For companies without placeholder, build URL differently
        if '{role}' not in search_url:
            base_url = company.get('career_url', '')
            if '?' in base_url:
                search_url = base_url + '&search=' + role.replace(' ', '+')
            else:
                search_url = base_url + '?search=' + role.replace(' ', '+')

        try:
            # Scrape the search results page
            result = await client.scrape(
                url=search_url,
                asp=True,
                render_js=True,
                proxy_pool='public_residential_pool',
                country='us',
                rendering_wait=5000
            )

            # Extract job listings using the extraction API
            extracted = await client.extract(
                content=result.content,
                extraction_prompt=f"""Extract all job listings from this {company_name} career page.
                For each job return a JSON array with: title, location, salary (if visible), posted_date (as YYYY-MM-DD or relative like "3 days ago"), url (the job detail link).
                Return ONLY a valid JSON array."""
            )

            listings = []
            if extracted and 'data' in extracted:
                data = extracted['data']
                if isinstance(data, list):
                    listings = data
                elif isinstance(data, dict) and 'extracted_content' in data:
                    try:
                        listings = json.loads(data['extracted_content'])
                    except json.JSONDecodeError:
                        pass

            for job in listings:
                if len(all_jobs) >= results_wanted:
                    break

                title = job.get('title', '')
                if should_exclude_title(title):
                    continue
                if not matches_target_roles(title):
                    continue

                source_url = job.get('url', job.get('apply_url', ''))
                if not source_url:
                    continue

                normalized = normalize_job(job, company_name, source_url)
                if normalized['title'] and normalized['source_url']:
                    all_jobs.append(normalized)

            print(f"[fortune100]   {company_name}/{role}: found {len(listings)} listings, {len(all_jobs)} matched", file=sys.stderr)

        except ScrapflyError as e:
            print(f"[fortune100]   {company_name}/{role}: Scrapfly error: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[fortune100]   {company_name}/{role}: Error: {e}", file=sys.stderr)

        # Rate limit
        await asyncio.sleep(REQUEST_DELAY)

    return all_jobs


async def scrape_all_fortune100(companies, roles, results_wanted, timeout):
    """Scrape all Fortune 100 companies for target roles."""
    api_key = os.environ.get('SCRAPFLY_API_KEY')
    if not api_key:
        print("[fortune100] Error: SCRAPFLY_API_KEY not set", file=sys.stderr)
        return []

    client = ScrapflyClient(key=api_key)
    all_jobs = []
    start_time = time.time()

    for company in companies:
        if time.time() - start_time >= timeout:
            print(f"[fortune100] Timeout reached, stopping.", file=sys.stderr)
            break

        jobs = await scrape_company_career_page(client, company, roles, results_wanted)
        all_jobs.extend(jobs)

        if len(all_jobs) >= results_wanted:
            break

    print(f"[fortune100] Total: scraped {len(all_jobs)} jobs from Fortune 100 companies", file=sys.stderr)
    return all_jobs


def main():
    parser = argparse.ArgumentParser(description='Fortune 100 Career Page Scraper')
    parser.add_argument('--companies', default=None,
                        help='Comma-separated company names to scrape (optional, defaults to all in fortune100_companies.json)')
    parser.add_argument('--roles', default='AI Engineer,Software Engineer,Machine Learning Engineer',
                        help='Comma-separated target roles')
    parser.add_argument('--results-wanted', type=int, default=50,
                        help='Maximum number of results per company')
    parser.add_argument('--timeout', type=int, default=600,
                        help='Overall timeout in seconds')

    args = parser.parse_args()

    # Load companies from default file path
    companies = load_companies()

    # Filter by specified companies if provided
    if args.companies:
        requested = [c.strip() for c in args.companies.split(',')]
        companies = [c for c in companies if c['name'] in requested]

    roles = [r.strip() for r in args.roles.split(',')]

    import asyncio
    jobs = asyncio.run(scrape_all_fortune100(companies, roles, args.results_wanted, args.timeout))

    # Output as JSON
    print(json.dumps(jobs, ensure_ascii=False))


if __name__ == '__main__':
    main()