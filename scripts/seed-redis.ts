import Redis from 'ioredis';

/**
 * Script to populate Redis with test data (~200MB)
 *
 * Usage:
 *   bun scripts/seed-redis.ts            # Fills ~200MB
 *   bun scripts/seed-redis.ts --mb=250   # Fills custom size in MB
 *   bun scripts/seed-redis.ts --clear    # Removes seeded keys
 */

const targetMbArg = process.argv.find((arg) => arg.startsWith('--mb='))?.split('=')[1];
const targetMb = Math.max(1, Number.parseInt(targetMbArg || '200', 10));
const isClearMode = process.argv.includes('--clear');

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = Number.parseInt(process.env.REDIS_PORT || '6379', 10);

async function getUsedMemory(client: Redis): Promise<string> {
  const info = await client.info('memory');
  const match = info.match(/used_memory_human:([^\r\n]+)/);
  return match ? match[1].trim() : 'unknown';
}

async function main() {
  console.log(`Connecting to Redis at ${REDIS_HOST}:${REDIS_PORT}...`);
  const client = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: 3,
  });

  client.on('error', (err) => {
    console.error(`Redis connection error: ${err.message}`);
  });

  try {
    await client.ping();
    const roleInfo = (await client.role())[0];
    console.log(`Connected! Current node role: [${roleInfo}]`);

    if (roleInfo !== 'master') {
      console.warn(`Warning: Connected node is "${roleInfo}", not master. Writes might fail.`);
    }

    const initialMemory = await getUsedMemory(client);
    console.log(`Current memory usage: ${initialMemory}`);

    if (isClearMode) {
      console.log('Clearing previously seeded keys (pattern: seed:chunk:*)...');
      const stream = client.scanStream({ match: 'seed:chunk:*', count: 100 });
      let deletedCount = 0;

      for await (const keys of stream) {
        if (keys.length > 0) {
          await client.del(...keys);
          deletedCount += keys.length;
        }
      }
      const afterClearMemory = await getUsedMemory(client);
      console.log(`Deleted ${deletedCount} keys.`);
      console.log(`Memory after clear: ${afterClearMemory}`);
      await client.quit();
      return;
    }

    console.log(`\nGenerating ~${targetMb} MB of test data in 1MB chunks...`);

    // Create a 1MB payload (random characters so Redis doesn't compress or deduplicate)
    const chunkSize = 1024 * 1024; // 1 MB
    const sampleBuffer = Buffer.alloc(chunkSize);
    for (let i = 0; i < chunkSize; i++) {
      sampleBuffer[i] = 65 + (i % 26); // ASCII letters A-Z
    }
    const samplePayload = sampleBuffer.toString('utf-8');

    const batchSize = 10;
    const totalChunks = targetMb;

    for (let i = 0; i < totalChunks; i += batchSize) {
      const pipeline = client.pipeline();
      const currentBatchCount = Math.min(batchSize, totalChunks - i);

      for (let j = 0; j < currentBatchCount; j++) {
        const keyIndex = i + j;
        pipeline.set(`seed:chunk:${keyIndex}`, samplePayload);
      }

      await pipeline.exec();
      const percent = Math.min(100, Math.round(((i + currentBatchCount) / totalChunks) * 100));
      process.stdout.write(`\rProgress: ${i + currentBatchCount}/${totalChunks} MB (${percent}%)`);
    }

    process.stdout.write('\n');
    const finalMemory = await getUsedMemory(client);
    console.log(`\nDone! Successfully seeded ~${totalChunks} MB.`);
    console.log(`Initial memory : ${initialMemory}`);
    console.log(`Final memory   : ${finalMemory}`);
  } catch (error) {
    console.error('Seeding failed:', error);
  } finally {
    await client.quit();
  }
}

main();
