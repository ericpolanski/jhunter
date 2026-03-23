import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const profilePath = join(__dirname, '..', '..', 'data', 'eric-profile.json');

let profileCache = null;

export function loadProfile() {
  if (profileCache) {
    return profileCache;
  }

  try {
    const data = readFileSync(profilePath, 'utf-8');
    profileCache = JSON.parse(data);
    return profileCache;
  } catch (error) {
    console.error('Error loading profile:', error);
    return null;
  }
}

export function getProfile() {
  return loadProfile();
}

export default { loadProfile, getProfile };
