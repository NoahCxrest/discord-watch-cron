

import fetch from 'node-fetch';
import mysql from 'mysql2/promise';
import cron from 'node-cron';

const WORKER_URL = process.env.WORKER_URL

const DATABASE_URL = process.env.DATBASE_URL || process.env.DATABASE_URL;
if (!DATABASE_URL) {
	throw new Error('DATBASE_URL environment variable is not set');
}

const pool = mysql.createPool(DATABASE_URL);

async function getApplications() {
		const [rows] = await pool.query('SELECT id, bot_id FROM applications');
		return rows as { id: string, bot_id: string }[];
}

async function fetchGuildCount(appId: string) {
	const url = `${WORKER_URL}/${appId}?locale=en-US`;
	try {
		const res = await fetch(url);
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
		console.error(`Failed to fetch for app ${appId}:`, e);
		return null;
	}
}

async function recordGuildCount(bot_id: string, guild_count: number) {
		await pool.query(
			'INSERT INTO app_stats (bot_id, guild_count) VALUES (?, ?)',
			[bot_id, guild_count]
		);
}

async function updateAllApplications() {
	const apps = await getApplications();
	const concurrency = 10;
	let i = 0;
	async function next() {
		if (i >= apps.length) return;
		const app = apps[i++];
		const guildCount = await fetchGuildCount(app.id);
		if (guildCount !== null && app.bot_id) {
			await recordGuildCount(app.bot_id, guildCount);
			console.log(`Recorded guild_count=${guildCount} for bot_id=${app.bot_id}`);
		}
		await next();
	}
	await Promise.all(Array(concurrency).fill(0).map(next));
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

updateAllApplications().catch(console.error);
