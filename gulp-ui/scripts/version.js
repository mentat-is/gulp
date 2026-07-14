const { execSync } = require('child_process');
const { writeFileSync } = require('fs');
const { join } = require('path');

function sh(cmd) { return execSync(cmd, { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim(); }

let version = process.env.APP_VERSION || process.env._VERSION || '';
if (!version) {
  try { version = sh('git describe --tags --always'); } catch { version = 'v0.0.0'; }
}

let commit = '';
try { commit = sh('git rev-parse --short HEAD'); } catch { commit = ''; }

const builtAt = new Date().toISOString();
writeFileSync(join(process.cwd(), 'public', 'version'), version);
console.log(`[version] ${version} (${commit}) ${builtAt}`);
