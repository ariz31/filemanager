import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

const requiredMarkers = [
  '<!doctype html>',
  'id="app-script"',
  'indexedDB.open',
  'New folder',
  'Upload files',
  'Upload folder',
  'Export backup',
  'Import backup',
  'Trash',
  'Favorites',
  'serviceWorker',
  '</html>'
];

for (const marker of requiredMarkers) {
  assert.ok(html.includes(marker), `Expected index.html to include ${marker}`);
}

const scriptMatch = html.match(/<script id="app-script">([\s\S]*?)<\/script>/);
assert.ok(scriptMatch, 'Expected an inline app script');
assert.ok(scriptMatch[1].includes('async function init()'), 'Expected app bootstrap function');
assert.ok(scriptMatch[1].includes('function render()'), 'Expected render function');
assert.ok(scriptMatch[1].includes('function showModal'), 'Expected modal function');

console.log('Static smoke test passed.');
