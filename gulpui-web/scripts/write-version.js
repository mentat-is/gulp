#!/usr/bin/env node
const { execSync } = require('child_process')
const { writeFileSync, mkdirSync } = require('fs')
const { join, dirname } = require('path')

function sh(cmd) {
  try { return execSync(cmd, { stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim() } catch { return '' }
}

let version = process.env.APP_VERSION || process.env._VERSION || ''
if (!version) version = sh('git describe --tags --always') || 'unknown'

const commit = sh('git rev-parse --short HEAD')
const builtAt = new Date().toISOString()

const publicFile = join(process.cwd(), 'public', 'version')
mkdirSync(dirname(publicFile), { recursive: true })
writeFileSync(publicFile, `${version}`)

const tsFile = join(process.cwd(), 'src', 'version.ts')
mkdirSync(dirname(tsFile), { recursive: true })
writeFileSync(tsFile, `export const APP_VERSION = ${JSON.stringify(version)};\nexport const APP_COMMIT = ${JSON.stringify(commit)};\nexport const APP_BUILT_AT = ${JSON.stringify(builtAt)};\n`)

process.stdout.write(`${version}\n`)
