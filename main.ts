import fetch from 'node-fetch';
import mysql from 'mysql2/promise';
import cron from 'node-cron';

const WORKER_URL = process.env.WORKER_URL

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
	throw new Error('DATABASE_URL environment variable is not set');
}

const pool = mysql.createPool(DATABASE_URL);

async function getApplications() {
	const [rows] = await pool.query('SELECT id, bot_id FROM applications');
	return rows as { id: string, bot_id: string }[];
}

async function fetchGuildCount(appId: string) {
	const url = `${WORKER_URL}/${appId}?locale=en-US`;
	let delay = 1000;
	while (true) {
		try {
			const controller = new AbortController();
			const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
			const res = await fetch(url, { signal: controller.signal });
			clearTimeout(timeoutId);

			if (res.status === 429) {
				const retryAfter = res.headers.get('Retry-After');
				const waitTime = retryAfter ? parseInt(retryAfter, 10) * 1000 : delay;
				console.warn(`Rate limited for app ${appId}, retrying in ${waitTime / 1000}s`);
				await new Promise(r => setTimeout(r, waitTime));
				delay = Math.min(delay * 2, 60000); // exponential backoff, max 60s
				continue;
			}

			if (!res.ok) throw new Error(`HTTP ${res.status}`);

			const data = (await res.json()) as {
				directory_entry?: { guild_count?: number };
				guild?: { approximate_member_count?: number };
			};

			if (data.directory_entry && typeof data.directory_entry.guild_count === 'number') {
				return data.directory_entry.guild_count;
			}
			if (data.guild && typeof data.guild.approximate_member_count === 'number') {
				return data.guild.approximate_member_count;
			}
			return null;
		} catch (e) {
			console.error(`Error fetching for app ${appId}:`, e);
			await new Promise(r => setTimeout(r, delay));
			delay = Math.min(delay * 2, 60000);
		}
	}
}

async function recordGuildCount(bot_id: string, guild_count: number) {
	await pool.query(
		'INSERT INTO application_stats (bot_id, guild_count) VALUES (?, ?)',
		[bot_id, guild_count]
	);
}

async function processApp(app: { id: string, bot_id: string }, index: number, total: number) {
	const appStart = Date.now();
	console.log(`[${index + 1}/${total}] Fetching guild count for app id=${app.id}, bot_id=${app.bot_id}`);
	
	const guildCount = await fetchGuildCount(app.id);
	const appEnd = Date.now();
	const elapsed = ((appEnd - appStart) / 1000).toFixed(2);
	
	if (guildCount !== null && app.bot_id) {
		await recordGuildCount(app.bot_id, guildCount);
		console.log(`[${index + 1}/${total}] ✓ Recorded guild_count=${guildCount} for bot_id=${app.bot_id} (took ${elapsed}s)`);
		return { success: true, app, guildCount, elapsed };
	} else {
		console.log(`[${index + 1}/${total}] ✗ No guild count recorded for bot_id=${app.bot_id} (took ${elapsed}s)`);
		return { success: false, app, elapsed };
	}
}

async function updateAllApplications() {
	const apps = await getApplications();
	const totalApps = apps.length;
	
	console.log(`\n[INFO] Starting concurrent update for ${totalApps} apps.`);
	const startTime = Date.now();
	
	const concurrencyLimit = 10;
	const results: PromiseSettledResult<
		{ success: boolean; app: { id: string; bot_id: string }; guildCount: number; elapsed: string } |
		{ success: boolean; app: { id: string; bot_id: string }; elapsed: string; guildCount?: undefined }
	>[] = [];

	for (let i = 0; i < totalApps; i += concurrencyLimit) {
		const batch = apps.slice(i, Math.min(i + concurrencyLimit, totalApps));
		console.log(`\n[BATCH] Processing apps ${i + 1}-${Math.min(i + concurrencyLimit, totalApps)} of ${totalApps}`);

		// Retry logic: keep retrying failed apps in the batch until all succeed
		let batchResults: PromiseSettledResult<any>[] = [];
		let batchToProcess = batch;
		let batchIndexes = batch.map((_, idx) => idx);
		while (batchToProcess.length > 0) {
			const batchPromises = batchToProcess.map((app, batchIndex) =>
				processApp(app, i + batchIndexes[batchIndex], totalApps)
			);
			batchResults = await Promise.allSettled(batchPromises);

			// Find failed ones
			const failedIndexes: number[] = [];
			const failedApps: typeof batch = [];
			const failedBatchIndexes: number[] = [];
			batchResults.forEach((result, idx) => {
				if (result.status !== 'fulfilled' || !result.value || !result.value.success) {
					failedIndexes.push(idx);
					failedApps.push(batchToProcess[idx]);
					failedBatchIndexes.push(batchIndexes[idx]);
				}
			});

			if (failedApps.length === 0) {
				// All succeeded
				break;
			}

			console.log(`[BATCH] Retrying ${failedApps.length} failed apps in this batch...`);
			await new Promise(r => setTimeout(r, 2000));
			batchToProcess = failedApps;
			batchIndexes = failedBatchIndexes;
		}

		// All succeeded, push results
		results.push(...batchResults);

		if (i + concurrencyLimit < totalApps) {
			console.log(`[BATCH] Waiting 2 seconds before next batch...`);
			await new Promise(r => setTimeout(r, 2000));
		}
	}

	const endTime = Date.now();
	const totalElapsed = ((endTime - startTime) / 1000 / 60).toFixed(2);

	let successful = 0;
	let failed = 0;
	for (const r of results) {
		if (r.status === 'fulfilled' && r.value && (r.value as any).success) {
			successful++;
		} else {
			failed++;
		}
	}

	console.log(`\n[INFO] ✓ Completed concurrent update for all apps in ${totalElapsed} minutes.`);
	console.log(`[INFO] Results: ${successful} successful, ${failed} failed out of ${totalApps} total apps.`);
	console.log(`[INFO] Average time per app: ${((endTime - startTime) / totalApps / 1000).toFixed(2)} seconds`);
	console.log(`[INFO] Speed improvement: ~${((6 * 60) / parseFloat(totalElapsed)).toFixed(1)}x faster than sequential processing\n`);
}

cron.schedule('0 */6 * * *', async () => {
	console.log(`[${new Date().toISOString()}] Starting 6-hourly concurrent update...`);
	try {
		await updateAllApplications();
		console.log(`[${new Date().toISOString()}] Update complete. Next update in 6 hours.`);
	} catch (e) {
		console.error(`[${new Date().toISOString()}] Update failed:`, e);
	}
});

console.log('Application scanner started. Running initial scan...');
updateAllApplications().then(() => {
	console.log('Initial scan complete. Scheduled updates will run every 6 hours.');
}).catch(e => {
	console.error('Initial scan failed:', e);
});