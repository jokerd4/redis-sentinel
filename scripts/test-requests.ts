/**
 * Load & Testing Script for Redis Sentinel Time Endpoint
 *
 * Usage:
 *   bun scripts/test-requests.ts
 *   bun scripts/test-requests.ts 20
 *   bun scripts/test-requests.ts --rps=25 --url=http://localhost:3000/time
 *   RPS=50 bun scripts/test-requests.ts
 */

const targetUrl =
  process.argv.find((arg) => arg.startsWith('--url='))?.split('=')[1] ||
  process.env.TARGET_URL ||
  'http://localhost:3000/time';

const rawRps =
  process.argv.find((arg) => arg.startsWith('--rps='))?.split('=')[1] ||
  (!process.argv[2]?.startsWith('-') ? process.argv[2] : null) ||
  process.env.RPS ||
  '10';

const rps = Math.max(1, Number.parseInt(rawRps, 10) || 10);
const intervalMs = 1000 / rps;

// ANSI colors for readable terminal output
const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
};

let requestCount = 0;
let successCount = 0;
let failureCount = 0;
let maxDuration = 0;

console.log(`${colors.bold}${colors.cyan}=== Redis Sentinel Load Tester ===${colors.reset}`);
console.log(`Target URL : ${colors.bold}${targetUrl}${colors.reset}`);
console.log(`Rate       : ${colors.bold}${rps} req/sec${colors.reset} (every ${intervalMs.toFixed(1)} ms)`);
console.log(`Press ${colors.bold}Ctrl+C${colors.reset} to stop and see summary.\n`);

async function sendRequest(id: number): Promise<void> {
  const startTime = performance.now();
  const timeStr = new Date().toISOString().substring(11, 23);

  try {
    const res = await fetch(targetUrl);
    const duration = Math.round(performance.now() - startTime);
    maxDuration = Math.max(maxDuration, duration);
    const body = await res.text();

    if (res.ok) {
      successCount++;
      console.log(
        `${colors.dim}[#${id}] [${timeStr}]${colors.reset} ` +
          `${colors.green}Status: ${res.status}${colors.reset} ` +
          `(${duration}ms) | ` +
          `Response: ${body}`,
      );
    } else {
      failureCount++;
      console.log(
        `${colors.dim}[#${id}] [${timeStr}]${colors.reset} ` +
          `${colors.red}Status: ${res.status}${colors.reset} ` +
          `(${duration}ms) | ` +
          `Response: ${body}`,
      );
    }
  } catch (error) {
    failureCount++;
    const duration = Math.round(performance.now() - startTime);
    maxDuration = Math.max(maxDuration, duration);
    const errMsg = error instanceof Error ? error.message : String(error);
    console.log(
      `${colors.dim}[#${id}] [${timeStr}]${colors.reset} ` +
        `${colors.red}Status: ERROR${colors.reset} ` +
        `(${duration}ms) | ` +
        `Error: ${errMsg}`,
    );
  }
}

const timer = setInterval(() => {
  requestCount++;
  sendRequest(requestCount);
}, intervalMs);

function printSummary(): void {
  clearInterval(timer);
  console.log(`\n${colors.bold}${colors.cyan}=== Test Summary ===${colors.reset}`);
  console.log(`Total sent   : ${requestCount}`);
  console.log(`Successful   : ${colors.green}${successCount}${colors.reset}`);
  console.log(`Failed       : ${failureCount > 0 ? colors.red : colors.green}${failureCount}${colors.reset}`);
  console.log(`Max duration : ${colors.bold}${maxDuration} ms${colors.reset}`);
  process.exit(0);
}

process.on('SIGINT', printSummary);
process.on('SIGTERM', printSummary);
