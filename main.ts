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
	
	try {
		const controller = new AbortController();
		const timeoutId = setTimeout(() => controller.abort(), 10000); // 10 second timeout
		const res = await fetch(url, { signal: controller.signal });
		clearTimeout(timeoutId);

		if (!res.ok) {
			console.warn(`HTTP ${res.status} for app ${appId}`);
			return null;
		}

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
		console.error(`Error fetching for app ${appId}:`, e.message);
		return null;
	}
}

async function recordGuildCount(bot_id: string, guild_count: number) {
	await pool.query(
		'INSERT INTO application_stats (bot_id, guild_count) VALUES (?, ?)',
		[bot_id, guild_count]
	);
}

async function updateAllApplications() {
	const apps = await getApplications();
	const totalApps = apps.length;
	
	console.log(`\n[INFO] Starting sequential update for ${totalApps} apps (1 per second).`);
	const startTime = Date.now();
	
	let successful = 0;
	let failed = 0;

	for (let i = 0; i < totalApps; i++) {
		const app = apps[i];
		const appStart = Date.now();
		
		console.log(`[${i + 1}/${totalApps}] Fetching guild count for app id=${app.id}, bot_id=${app.bot_id}`);
		
		const guildCount = await fetchGuildCount(app.id);
		const appEnd = Date.now();
		const elapsed = ((appEnd - appStart) / 1000).toFixed(2);
		
		if (guildCount !== null && app.bot_id) {
			await recordGuildCount(app.bot_id, guildCount);
			console.log(`[${i + 1}/${totalApps}] ✓ Recorded guild_count=${guildCount} for bot_id=${app.bot_id} (took ${elapsed}s)`);
			successful++;
		} else {
			console.log(`[${i + 1}/${totalApps}] ✗ No guild count recorded for bot_id=${app.bot_id} (took ${elapsed}s)`);
			failed++;
		}

		// Wait 1 second before next request (except for the last one)
		if (i < totalApps - 1) {
			await new Promise(r => setTimeout(r, 1000));
		}
	}

	const endTime = Date.now();
	const totalElapsed = ((endTime - startTime) / 1000 / 60).toFixed(2);

	console.log(`\n[INFO] ✓ Completed update for all apps in ${totalElapsed} minutes.`);
	console.log(`[INFO] Results: ${successful} successful, ${failed} failed out of ${totalApps} total apps.`);
	console.log(`[INFO] Average time per app: ${((endTime - startTime) / totalApps / 1000).toFixed(2)} seconds\n`);
}

cron.schedule('0 */6 * * *', async () => {
	console.log(`[${new Date().toISOString()}] Starting 6-hourly update...`);
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