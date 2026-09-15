const assert = require('node:assert/strict');
const test = require('node:test');
const path = require('node:path');

test('public frontend serves the current dashboard, assets, and settings API before legacy routes', async () => {
  process.env.BACKEND_URL = 'http://127.0.0.1:8083';
  const config = require(path.join(__dirname, '../web/next.config.js'));
  const rules = await config.rewrites();
  const before = Object.fromEntries(rules.beforeFiles.map(rule => [rule.source, rule.destination]));
  assert.equal(before['/'], 'http://127.0.0.1:8083/');
  assert.equal(before['/assets/:path*'], 'http://127.0.0.1:8083/assets/:path*');
  assert.equal(before['/api/:path*'], 'http://127.0.0.1:8083/api/:path*');
});
