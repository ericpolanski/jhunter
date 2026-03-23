import { describe, test, expect, beforeEach, vi } from 'bun:test';
import { calculateHeuristicScore } from '../services/fit-scorer.js';

const mockProfile = {
  name: 'Eric Polanski',
  location: 'Gurnee, IL',
  skills: {
    languages: ['Python', 'SQL', 'C++', 'JavaScript', 'React'],
    frameworks: ['PyTorch', 'TensorFlow', 'Scikit-learn', 'Hugging Face'],
    tools: ['Git', 'Docker', 'Google Cloud', 'RESTful APIs']
  },
  experience: [
    {
      company: 'AbbVie',
      roles: [
        { title: 'AI Engineer Intern', dates: 'Sep. 2025 – Dec. 2025' }
      ]
    }
  ]
};

describe('fit-scorer', () => {
  describe('calculateHeuristicScore', () => {
    test('should score AI engineer role in Chicago highly', () => {
      const job = {
        title: 'AI Engineer',
        location: 'Chicago, IL',
        description: 'Python PyTorch machine learning AI engineer role',
        salary_min: 80000,
        salary_max: 110000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.score).toBeGreaterThan(70);
      expect(result.breakdown).toHaveProperty('skill_match');
      expect(result.breakdown).toHaveProperty('location_match');
      expect(result.breakdown).toHaveProperty('experience_match');
      expect(result.breakdown).toHaveProperty('role_match');
      expect(result.breakdown).toHaveProperty('salary_match');
    });

    test('should score remote job with matching skills', () => {
      const job = {
        title: 'Machine Learning Engineer',
        location: 'Remote',
        description: 'Python TensorFlow scikit-learn machine learning',
        salary_min: 90000,
        salary_max: 130000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.score).toBeGreaterThan(60);
      expect(result.breakdown.location_match).toBe(100);
    });

    test('should score non-Chicago job lower', () => {
      const job = {
        title: 'Software Engineer',
        location: 'New York, NY',
        description: 'Java software development',
        salary_min: 70000,
        salary_max: 100000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.breakdown.location_match).toBeLessThan(70);
    });

    test('should score senior role lower for entry-level candidate', () => {
      const job = {
        title: 'Senior Machine Learning Engineer',
        location: 'Chicago, IL',
        description: 'Python machine learning 5+ years experience required',
        salary_min: 150000,
        salary_max: 200000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.breakdown.experience_match).toBeLessThan(50);
    });

    test('should handle job without salary', () => {
      const job = {
        title: 'Junior Software Engineer',
        location: 'Chicago, IL',
        description: 'Python JavaScript React developer'
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.breakdown.salary_match).toBe(50);
    });

    test('should handle job with low salary', () => {
      const job = {
        title: 'Software Engineer',
        location: 'Chicago, IL',
        description: 'Python developer',
        salary_min: 30000,
        salary_max: 45000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.breakdown.salary_match).toBeLessThan(50);
    });

    test('should handle job with very high salary', () => {
      const job = {
        title: 'Senior Software Engineer',
        location: 'Chicago, IL',
        description: 'Python developer',
        salary_min: 180000,
        salary_max: 250000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.breakdown.salary_match).toBeLessThan(80);
    });

    test('should handle job without description', () => {
      const job = {
        title: 'Software Engineer',
        location: 'Chicago, IL',
        description: null
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.score).toBeGreaterThan(0);
      expect(result.breakdown.skill_match).toBe(50);
    });

    test('should handle edge case empty job', () => {
      const job = {
        title: '',
        location: '',
        description: ''
      };

      const result = calculateHeuristicScore(mockProfile, job);

      expect(result.score).toBeGreaterThanOrEqual(0);
      expect(result.score).toBeLessThanOrEqual(100);
    });

    test('should weight skill_match at 40%', () => {
      const job = {
        title: 'Python Engineer',
        location: 'Remote',
        description: 'Python pandas numpy',
        salary_min: 80000,
        salary_max: 100000
      };

      const result = calculateHeuristicScore(mockProfile, job);

      // With matching skills, skill_match should be high
      expect(result.breakdown.skill_match).toBeGreaterThan(70);
    });

    test('should weight location_match at 20%', () => {
      const chicagoJob = {
        title: 'Engineer',
        location: 'Chicago, IL',
        description: 'Python'
      };

      const nyJob = {
        title: 'Engineer',
        location: 'New York, NY',
        description: 'Python'
      };

      const chicagoResult = calculateHeuristicScore(mockProfile, chicagoJob);
      const nyResult = calculateHeuristicScore(mockProfile, nyJob);

      expect(chicagoResult.breakdown.location_match).toBe(100);
      expect(nyResult.breakdown.location_match).toBeLessThan(70);
    });
  });
});