

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
	const maxRetries = 5;
	let attempt = 0;
	let delay = 1000; // start with 1s
	while (attempt < maxRetries) {
		try {
			const res = await fetch(url);
			if (res.status === 429) {
				// Rate limited, check Retry-After header
				const retryAfter = res.headers.get('Retry-After');
				const waitTime = retryAfter ? parseInt(retryAfter, 10) * 1000 : delay;
				console.warn(`Rate limited for app ${appId}, retrying in ${waitTime / 1000}s (attempt ${attempt + 1}/${maxRetries})`);
				await new Promise(r => setTimeout(r, waitTime));
				attempt++;
				delay *= 2; // exponential backoff
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
			if (attempt >= maxRetries - 1) {
				console.error(`Failed to fetch for app ${appId} after ${maxRetries} attempts:`, e);
				return null;
			}
			// Wait before retrying on error
			await new Promise(r => setTimeout(r, delay));
			delay *= 2;
			attempt++;
		}
	}
	return null;
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
	const totalDurationMs = 6 * 60 * 60 * 1000; // 6 hours in ms
	const intervalMs = totalApps > 1 ? Math.floor(totalDurationMs / (totalApps - 1)) : 0;

	console.log(`\n[INFO] Starting update for ${totalApps} apps.`);
	console.log(`[INFO] Target total duration: ${(totalDurationMs / 1000 / 60 / 60).toFixed(2)} hours (${(totalDurationMs / 1000 / 60).toFixed(2)} minutes)`);
	console.log(`[INFO] Calculated interval between each app: ${(intervalMs / 1000).toFixed(2)} seconds (${(intervalMs / 1000 / 60).toFixed(2)} minutes)\n`);

	const startTime = Date.now();
	for (let i = 0; i < totalApps; i++) {
		const app = apps[i];
		const appStart = Date.now();
		console.log(`[${i+1}/${totalApps}] Fetching guild count for app id=${app.id}, bot_id=${app.bot_id}`);
		const guildCount = await fetchGuildCount(app.id);
		const appEnd = Date.now();
		const elapsed = ((appEnd - appStart) / 1000).toFixed(2);
		if (guildCount !== null && app.bot_id) {
			await recordGuildCount(app.bot_id, guildCount);
			console.log(`[${i+1}/${totalApps}] Recorded guild_count=${guildCount} for bot_id=${app.bot_id} (fetch+record took ${elapsed}s)`);
		} else {
			console.log(`[${i+1}/${totalApps}] No guild count recorded for bot_id=${app.bot_id} (fetch took ${elapsed}s)`);
		}
		if (i < totalApps - 1 && intervalMs > 0) {
			console.log(`[${i+1}/${totalApps}] Waiting ${(intervalMs / 1000).toFixed(2)} seconds before next app...`);
			await new Promise(r => setTimeout(r, intervalMs));
		}
	}
	const endTime = Date.now();
	const totalElapsed = ((endTime - startTime) / 1000 / 60).toFixed(2);
	console.log(`\n[INFO] Finished update for all apps. Actual elapsed time: ${totalElapsed} minutes.`);
}


cron.schedule('0 */6 * * *', async () => {
	console.log('Starting 6-hourly update...');
	try {
		await updateAllApplications();
		console.log('Update complete.');
	} catch (e) {
		console.error('Update failed:', e);
	}
});
