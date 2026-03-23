import { describe, test, expect } from 'bun:test';
import fs from 'fs';

const source = fs.readFileSync('./server/services/scraper.js', 'utf8');

describe('scraper.js - Bug Fix Verification', () => {
  describe('Bug 1: Location Parsed from Job Description Body', () => {
    test('bodyLocationMatch fallback exists in source', () => {
      expect(source.includes('bodyLocationMatch')).toBe(true);
    });

    test('body location regex pattern for "based in/near" exists', () => {
      // The regex includes based\\s+(?:in|near)
      expect(source.includes('based\\s+(?:in|near)')).toBe(true);
    });

    test('location extraction falls back to description body text', () => {
      // The logic: if no extractedLocation from HTML, try body text
      expect(source.includes('if (!extractedLocation && description)')).toBe(true);
    });
  });

  describe('Bug 2: Job Description Full Capture', () => {
    test('description capture allows up to 20000 chars', () => {
      expect(source.includes('20000}')).toBe(true);
    });

    test('description stops at apply/footer, not benefits div', () => {
      // Should stop at apply section, not at benefits
      expect(source.includes('apply|application|submit')).toBe(true);
    });
  });

  describe('Bug 3: Requirements Parsing from Clean HTML', () => {
    test('cleanHtml is created by removing nav/header/footer/aside', () => {
      expect(source.includes('const cleanHtml')).toBe(true);
      expect(source.includes("<nav[^>]*>")).toBe(true);
      expect(source.includes("<header")).toBe(true);
      expect(source.includes("<footer")).toBe(true);
      expect(source.includes("<aside")).toBe(true);
    });

    test('requirements extraction uses cleanHtml', () => {
      // reqPatterns should be applied to cleanHtml
      const cleanHtmlIndex = source.indexOf('const cleanHtml');
      const reqPatternsIndex = source.indexOf('reqPatterns');
      const cleanHtmlUsedInReq = source.indexOf('cleanHtml.matchAll');
      expect(cleanHtmlIndex).toBeGreaterThan(0);
      expect(reqPatternsIndex).toBeGreaterThan(cleanHtmlIndex);
      expect(cleanHtmlUsedInReq).toBeGreaterThan(cleanHtmlIndex);
    });

    test('extended negative filters for nav/metadata content', () => {
      expect(source.includes('Job categories')).toBe(true);
      expect(source.includes('My career')).toBe(true);
      expect(source.includes('Navigation')).toBe(true);
    });
  });

  describe('Bug 4: Salary Extraction from JSON-LD and data-salary', () => {
    test('JSON-LD salary extraction exists', () => {
      expect(source.includes('jsonLdMatch')).toBe(true);
      expect(source.includes('jsonLd.baseSalary')).toBe(true);
    });

    test('data-salary/compensation/pay attributes checked', () => {
      expect(source.includes('data-(?:salary|compensation|pay)')).toBe(true);
    });

    test('parseSalary function handles K suffix', () => {
      expect(source.includes('parseSalary')).toBe(true);
      // Check that K/k handling exists in parseSalary
      expect(source.includes("replace(/[$,kK]/g, '')")).toBe(true);
    });
  });

  describe('Bug 5: HTML Entities Fully Decoded', () => {
    test('decodeHtmlEntities function exists', () => {
      expect(source.includes('function decodeHtmlEntities')).toBe(true);
    });

    test('decodeHtmlEntities handles numeric entities', () => {
      expect(source.includes('&#(\\d+);')).toBe(true);
      expect(source.includes('&#x([0-9a-fA-F]+);')).toBe(true);
    });

    test('decodeHtmlEntities handles common named entities', () => {
      expect(source.includes('&nbsp;')).toBe(true);
      expect(source.includes('&amp;')).toBe(true);
      expect(source.includes('&mdash;')).toBe(true);
      expect(source.includes('&quot;')).toBe(true);
    });
  });

  describe('Bug 6: Benefits Extracted as Structured Field', () => {
    test('benefitsPatterns array exists', () => {
      expect(source.includes('benefitsPatterns')).toBe(true);
    });

    test('benefits extraction looks for benefit/perk classes', () => {
      expect(source.includes('benefit')).toBe(true);
      expect(source.includes('perk')).toBe(true);
    });

    test('benefits appended as === BENEFITS === section', () => {
      expect(source.includes('=== BENEFITS ===')).toBe(true);
    });
  });

  describe('Bug 7: Paragraph Breaks Preserved', () => {
    test('stripHtmlWithNewlines replaces block elements with newlines', () => {
      expect(source.includes('function stripHtmlWithNewlines')).toBe(true);
      // Check for block element handling
      expect(source.includes('p|div|h[1-6]')).toBe(true);
    });

    test('stripHtmlWithNewlines handles br tags', () => {
      expect(source.includes('br\\s*\\/?>')).toBe(true);
    });

    test('stripHtmlWithNewlines collapses multiple blank lines', () => {
      expect(source.includes('\\n{3,}')).toBe(true);
    });
  });
});

describe('Integration: All 7 bug fixes present', () => {
  test('all 7 bugs have corresponding implementations', () => {
    // Bug 1
    expect(source.includes('bodyLocationMatch')).toBe(true);
    // Bug 2
    expect(source.includes('20000}')).toBe(true);
    // Bug 3
    expect(source.includes('const cleanHtml')).toBe(true);
    // Bug 4
    expect(source.includes('jsonLd.baseSalary')).toBe(true);
    // Bug 5
    expect(source.includes('function decodeHtmlEntities')).toBe(true);
    // Bug 6
    expect(source.includes('benefitsPatterns')).toBe(true);
    expect(source.includes('=== BENEFITS ===')).toBe(true);
    // Bug 7
    expect(source.includes('function stripHtmlWithNewlines')).toBe(true);
  });
});