// pointsbot.js - Professional Enhanced Version (FIXED)
import 'dotenv/config';
import {
  Client, GatewayIntentBits, REST, Routes,
  SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits
} from 'discord.js';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import nodeHtmlToImage from 'node-html-to-image';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/* =========================
   CONFIG & CONSTANTS
========================= */
const CONFIG = {
  appId: (process.env.APPLICATION_ID || '').trim(),
  token: (process.env.DISCORD_TOKEN || '').trim(),
  devGuildId: (process.env.DEV_GUILD_ID || '').trim(),
  dbFile: (process.env.DB_PATH || path.join(__dirname, 'data', 'points.db')).trim(),
};

const COOLDOWNS = {
  gym: 12 * 60 * 60 * 1000,
  badminton: 12 * 60 * 60 * 1000,
  cricket: 12 * 60 * 60 * 1000,
  exercise: 6 * 60 * 60 * 1000,
  swimming: 12 * 60 * 60 * 1000,
  yoga: 12 * 60 * 60 * 1000,
};

const POINTS = { 
  gym: 2, 
  badminton: 5, 
  cricket: 5, 
  exercise: 1,
  swimming: 3,
  yoga: 2,
};

const DISTANCE_RATES = {
  walking: 0.5,
  jogging: 0.6,
  running: 0.7,
};

const DEDUCTIONS = {
  chocolate: { points: 2, emoji: '🍫', label: 'Chocolate' },
  fries: { points: 3, emoji: '🍟', label: 'Fries' },
  soda: { points: 2, emoji: '🥤', label: 'Soda' },
  pizza: { points: 4, emoji: '🍕', label: 'Pizza' },
  burger: { points: 3, emoji: '🍔', label: 'Burger' },
  sweets: { points: 2, emoji: '🍬', label: 'Sweets' },
};

const RANKS = [
  { min: 0, name: "🆕 Rookie", color: 0x95a5a6, next: 20 },
  { min: 20, name: "🌟 Beginner", color: 0x3498db, next: 50 },
  { min: 50, name: "💪 Athlete", color: 0x9b59b6, next: 100 },
  { min: 100, name: "🥉 Pro", color: 0xf39c12, next: 200 },
  { min: 200, name: "🥈 Expert", color: 0xe67e22, next: 350 },
  { min: 350, name: "🥇 Champion", color: 0xf1c40f, next: 500 },
  { min: 500, name: "🏆 Legend", color: 0xe74c3c, next: 1000 },
  { min: 1000, name: "👑 Godlike", color: 0x8e44ad, next: null }
];

const ACHIEVEMENTS = [
  { id: 'first_points', name: '🎯 First Steps', requirement: (stats) => stats.total >= 1, description: 'Earn your first point' },
  { id: 'gym_rat', name: '💪 Gym Rat', requirement: (stats) => stats.gym >= 50, description: 'Earn 50 gym points' },
  { id: 'cardio_king', name: '🏃 Cardio King', requirement: (stats) => stats.exercise >= 100, description: 'Earn 100 exercise points' },
  { id: 'streak_7', name: '🔥 Week Warrior', requirement: (stats) => stats.current_streak >= 7, description: 'Maintain a 7-day streak' },
  { id: 'century_club', name: '💯 Century Club', requirement: (stats) => stats.total >= 100, description: 'Reach 100 total points' },
];

/* =========================
   DATABASE CLASS
========================= */
class PointsDatabase {
  constructor(dbPath) {
    try {
      fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') console.error('Failed to create DB directory:', err);
    }
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.initSchema();
    this.prepareStatements();
  }

  initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS points (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL, total REAL NOT NULL DEFAULT 0, 
        gym REAL NOT NULL DEFAULT 0, badminton REAL NOT NULL DEFAULT 0, cricket REAL NOT NULL DEFAULT 0,
        exercise REAL NOT NULL DEFAULT 0, swimming REAL NOT NULL DEFAULT 0, yoga REAL NOT NULL DEFAULT 0,
        current_streak INTEGER DEFAULT 0, longest_streak INTEGER DEFAULT 0, last_activity_date TEXT,
        created_at INTEGER DEFAULT (strftime('%s', 'now')), updated_at INTEGER DEFAULT (strftime('%s', 'now')),
        PRIMARY KEY (guild_id, user_id)
      );
      CREATE TABLE IF NOT EXISTS cooldowns ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, category TEXT NOT NULL, last_ms INTEGER NOT NULL, PRIMARY KEY (guild_id, user_id, category) );
      CREATE TABLE IF NOT EXISTS points_log ( id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, user_id TEXT NOT NULL, category TEXT NOT NULL, amount REAL NOT NULL, ts INTEGER NOT NULL, reason TEXT, notes TEXT );
      CREATE TABLE IF NOT EXISTS buddies ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, buddy_id TEXT, created_at INTEGER DEFAULT (strftime('%s', 'now')), PRIMARY KEY (guild_id, user_id) );
      CREATE TABLE IF NOT EXISTS achievements ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, achievement_id TEXT NOT NULL, unlocked_at INTEGER DEFAULT (strftime('%s', 'now')), PRIMARY KEY (guild_id, user_id, achievement_id) );
      CREATE INDEX IF NOT EXISTS idx_points_log_guild_ts ON points_log(guild_id, ts);
      CREATE INDEX IF NOT EXISTS idx_points_total ON points(guild_id, total DESC);
    `);
  }
  
  prepareStatements() {
    const stmts = {};
    stmts.upsertUser = this.db.prepare(`INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id) ON CONFLICT(guild_id, user_id) DO NOTHING`);
    stmts.addPoints = this.db.prepare(`UPDATE points SET total = total + @add, gym = CASE WHEN @category = 'gym' THEN gym + @add ELSE gym END, badminton = CASE WHEN @category = 'badminton' THEN badminton + @add ELSE badminton END, cricket = CASE WHEN @category = 'cricket' THEN cricket + @add ELSE cricket END, exercise = CASE WHEN @category = 'exercise' THEN exercise + @add ELSE exercise END, swimming = CASE WHEN @category = 'swimming' THEN swimming + @add ELSE swimming END, yoga = CASE WHEN @category = 'yoga' THEN yoga + @add ELSE yoga END, updated_at = strftime('%s', 'now') WHERE guild_id = @guild_id AND user_id = @user_id`);
    stmts.getUser = this.db.prepare(`SELECT * FROM points WHERE guild_id = ? AND user_id = ?`);
    stmts.updateStreak = this.db.prepare(`UPDATE points SET current_streak = @current_streak, longest_streak = @longest_streak, last_activity_date = @last_activity_date WHERE guild_id = @guild_id AND user_id = @user_id`);
    stmts.setCooldown = this.db.prepare(`INSERT INTO cooldowns (guild_id, user_id, category, last_ms) VALUES (@guild_id, @user_id, @category, @last_ms) ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms = excluded.last_ms`);
    stmts.getCooldown = this.db.prepare(`SELECT last_ms FROM cooldowns WHERE guild_id = ? AND user_id = ? AND category = ?`);
    stmts.logPoints = this.db.prepare(`INSERT INTO points_log (guild_id, user_id, category, amount, ts, reason, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`),
    stmts.getUserRankAllTime = this.db.prepare(`WITH Ranks AS (SELECT user_id, RANK() OVER (ORDER BY total DESC) as rank, total as score FROM points WHERE guild_id = ?) SELECT rank, score FROM Ranks WHERE user_id = ?`);
    stmts.getLeaderboardAllTime = this.db.prepare(`SELECT user_id as userId, total as score FROM points WHERE guild_id = ? AND total > 0 ORDER BY total DESC LIMIT 10`);
    stmts.getLeaderboardPeriodic = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id = ? AND ts >= ? AND amount > 0 GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
    stmts.getLeaderboardPeriodicCategory = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id = ? AND ts >= ? AND category = ? AND amount > 0 GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
    stmts.getTopStreaks = this.db.prepare(`SELECT user_id as userId, current_streak as score FROM points WHERE guild_id = ? AND current_streak > 0 ORDER BY current_streak DESC LIMIT 10`);
    stmts.getBuddy = this.db.prepare(`SELECT buddy_id FROM buddies WHERE guild_id = ? AND user_id = ?`);
    stmts.setBuddy = this.db.prepare(`INSERT INTO buddies (guild_id, user_id, buddy_id) VALUES (?, ?, ?) ON CONFLICT(guild_id, user_id) DO UPDATE SET buddy_id = excluded.buddy_id`);
    stmts.unlockAchievement = this.db.prepare(`INSERT OR IGNORE INTO achievements (guild_id, user_id, achievement_id) VALUES (?, ?, ?)`),
    stmts.getUserAchievements = this.db.prepare(`SELECT achievement_id FROM achievements WHERE guild_id = ? AND user_id = ?`);

    for(const category of Object.keys(POINTS)) {
        stmts[`getLeaderboard_${category}`] = this.db.prepare(`SELECT user_id as userId, ${category} as score FROM points WHERE guild_id = ? AND ${category} > 0 ORDER BY ${category} DESC LIMIT 10`);
    }

    this.stmts = stmts;
  }
  
  modifyPoints({ guildId, userId, category, amount, reason = null, notes = null }) {
    this.stmts.upsertUser.run({ guild_id: guildId, user_id: userId });
    const modAmount = Number(amount) || 0;
    if (modAmount === 0) return [];

    let targetCategory = category;
    if (modAmount < 0 && category === 'total') {
      const userPoints = this.stmts.getUser.get(guildId, userId) || {};
      targetCategory = ['exercise', 'gym', 'badminton', 'cricket', 'swimming', 'yoga'].sort((a, b) => (userPoints[b] || 0) - (userPoints[a] || 0))[0] || 'exercise';
    }

    this.stmts.addPoints.run({ guild_id: guildId, user_id: userId, category: targetCategory, add: modAmount });
    this.stmts.logPoints.run(guildId, userId, category, modAmount, Date.now(), reason, notes);

    if (modAmount > 0) {
        this.updateStreak(guildId, userId);
        return this.checkAchievements(guildId, userId);
    }
    return [];
  }

  updateStreak(guildId, userId) {
    const user = this.stmts.getUser.get(guildId, userId);
    if (!user) return;
    const today = new Date().toISOString().split('T')[0];
    const lastDate = user.last_activity_date;
    if (lastDate === today) return;

    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const currentStreak = lastDate === yesterday ? (user.current_streak || 0) + 1 : 1;
    const longestStreak = Math.max(user.longest_streak || 0, currentStreak);
    this.stmts.updateStreak.run({ guild_id: guildId, user_id: userId, current_streak: currentStreak, longest_streak: longestStreak, last_activity_date: today });
  }

  checkCooldown({ guildId, userId, category }) {
    const row = this.stmts.getCooldown.get(guildId, userId, category);
    const now = Date.now();
    const cooldownMs = COOLDOWNS[category];
    if (row && now - row.last_ms < cooldownMs) return cooldownMs - (now - row.last_ms);
    return 0;
  }

  commitCooldown({ guildId, userId, category }) {
    this.stmts.setCooldown.run({ guild_id: guildId, user_id: userId, category, last_ms: Date.now() });
  }

  checkAchievements(guildId, userId) {
    const user = this.stmts.getUser.get(guildId, userId);
    if (!user) return [];
    const unlocked = this.stmts.getUserAchievements.all(guildId, userId).map(r => r.achievement_id);
    const newAchievements = [];
    for (const achievement of ACHIEVEMENTS) {
      if (!unlocked.includes(achievement.id) && achievement.requirement(user)) {
        this.stmts.unlockAchievement.run(guildId, userId, achievement.id);
        newAchievements.push(achievement);
      }
    }
    return newAchievements;
  }
  
  close() { this.db.close(); }
}

/* =========================
   UTILITIES & RENDERER
========================= */
const formatNumber = (n) => (Math.round(n * 10) / 10).toLocaleString(undefined, { maximumFractionDigits: 1 });
const progressBar = (pct) => `${'█'.repeat(Math.floor(pct / 10))}${'░'.repeat(10 - Math.floor(pct / 10))} ${pct}%`;
const getUserRank = (total) => RANKS.reduce((acc, rank) => total >= rank.min ? rank : acc, RANKS[0]);
function nextRankProgress(total) {
  const cur = getUserRank(total);
  if (cur.next === null) return { pct: 100, cur, need: 0 };
  const span = cur.next - cur.min, done = total - cur.min;
  return { pct: Math.max(0, Math.min(100, Math.floor((done / span) * 100))), cur, need: cur.next - total };
}
const formatCooldown = (ms) => `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
function getPeriodStart(period = 'week') {
    const now = new Date();
    switch (period) {
        case 'day': return now.setHours(0, 0, 0, 0);
        case 'month': return new Date(now.getFullYear(), now.getMonth(), 1).getTime();
        case 'year': return new Date(now.getFullYear(), 0, 1).getTime();
        case 'week': default:
            const dayOfWeek = now.getDay();
            const diff = now.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1);
            return new Date(now.setDate(diff)).setHours(0, 0, 0, 0);
    }
}

async function renderLeaderboardCard({ title, rows, guild, userRank, subtitle = null }) {
    const userIds = [...rows.map(r => r.userId)];
    if (userRank) userIds.push(userRank.userId);
    const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
    const userRows = rows.map(row => {
        const member = members.get(row.userId);
        const user = member?.user;
        return { rank: row.rank, avatarUrl: user?.displayAvatarURL({ extension: 'png', size: 256 }) || 'https://cdn.discordapp.com/embed/avatars/0.png', displayName: (member?.displayName || user?.username || 'Unknown User').replace(/[<>]/g, ''), score: formatNumber(row.score) };
    });
    let userRankHtml = '';
    if (userRank && !rows.some(r => r.userId === userRank.userId)) {
        const member = members.get(userRank.userId);
        const user = member?.user;
        const avatarUrl = user?.displayAvatarURL({ extension: 'png', size: 256 }) || 'https://cdn.discordapp.com/embed/avatars/0.png';
        const displayName = (member?.displayName || user?.username || 'You').replace(/[<>]/g, '');
        userRankHtml = `<div class="row self"><div class="rank-badge self-badge">#${userRank.rank}</div><div class="avatar-container"><img class="avatar" src="${avatarUrl}" alt="${displayName}" /></div><div class="user-info"><div class="name">${displayName}</div><div class="user-tag">Your Rank</div></div><div class="score">${formatNumber(userRank.score)}</div></div>`;
    }
    const guildIcon = guild.iconURL({ extension: 'png', size: 256 });
    const finalSubtitle = subtitle || `Top ${rows.length} Players`;
    const html = `<!DOCTYPE html><html><head><meta charset="utf-8" /><style>@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');*{box-sizing:border-box;margin:0;padding:0;}body{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:linear-gradient(135deg, #0f172a 0%, #1e293b 100%);color:#e2e8f0;width:900px;padding:40px;-webkit-font-smoothing:antialiased;}.card{background:linear-gradient(180deg, #1e293b 0%, #0f172a 100%);border-radius:24px;overflow:hidden;box-shadow:0 25px 50px -12px rgba(0,0,0,0.5);border:1px solid rgba(148,163,184,0.1);}.header{background:linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);padding:32px;display:flex;align-items:center;gap:20px;border-bottom:3px solid rgba(59,130,246,0.3);}.guild-icon{width:72px;height:72px;border-radius:20px;box-shadow:0 10px 25px rgba(0,0,0,0.3);border:3px solid rgba(255,255,255,0.2);}.header-text{flex:1;}.title{font-size:32px;font-weight:900;color:#fff;text-shadow:0 2px 10px rgba(0,0,0,0.3);letter-spacing:-.5px;margin-bottom:4px;}.subtitle{font-size:16px;color:rgba(255,255,255,0.9);font-weight:500;text-transform:uppercase;letter-spacing:1px;}.leaderboard-content{padding:8px;}.row{display:flex;align-items:center;gap:16px;padding:16px 24px;margin:8px 0;background:rgba(30,41,59,0.5);border-radius:16px;transition:all .3s ease;border:1px solid transparent;}.row.top3{background:linear-gradient(135deg, rgba(59,130,246,0.15) 0%, rgba(139,92,246,0.15) 100%);border:1px solid rgba(59,130,246,0.3);}.rank-badge{min-width:48px;height:48px;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:18px;border-radius:12px;background:rgba(71,85,105,0.5);color:#94a3b8;border:2px solid rgba(148,163,184,0.2);}.rank-badge.gold{background:linear-gradient(135deg, #fbbf24 0%, #f59e0b 100%);color:#78350f;box-shadow:0 4px 15px rgba(251,191,36,0.4);border-color:#fde047;}.rank-badge.silver{background:linear-gradient(135deg, #e5e7eb 0%, #d1d5db 100%);color:#374151;box-shadow:0 4px 15px rgba(229,231,235,0.4);border-color:#f3f4f6;}.rank-badge.bronze{background:linear-gradient(135deg, #f97316 0%, #ea580c 100%);color:#7c2d12;box-shadow:0 4px 15px rgba(249,115,22,0.4);border-color:#fb923c;}.avatar{width:56px;height:56px;border-radius:50%;border:3px solid rgba(148,163,184,0.3);box-shadow:0 4px 12px rgba(0,0,0,0.3);}.top3 .avatar{border-color:rgba(59,130,246,0.6);box-shadow:0 4px 20px rgba(59,130,246,0.4);}.user-info{flex:1;min-width:0;}.name{font-weight:700;font-size:18px;color:#f1f5f9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px;}.user-tag{font-size:13px;color:#94a3b8;font-weight:500;}.score{font-weight:900;font-size:24px;background:linear-gradient(135deg, #10b981 0%, #059669 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;min-width:80px;text-align:right;}.row.self{margin-top:16px;background:linear-gradient(135deg, rgba(59,130,246,0.2) 0%, rgba(139,92,246,0.2) 100%);border:2px solid #3b82f6;box-shadow:0 8px 20px rgba(59,130,246,0.3);}.rank-badge.self-badge{background:linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);color:#fff;border-color:#60a5fa;}</style></head><body><div class="card"><div class="header">${guildIcon ? `<img src="${guildIcon}" class="guild-icon" alt="Server Icon" />` : ''}<div class="header-text"><h1 class="title">${title}</h1><div class="subtitle">${finalSubtitle} • ${guild.name}</div></div></div><div class="leaderboard-content">${userRows.map(u => `<div class="row ${u.rank <= 3 ? 'top3' : ''}"><div class="rank-badge ${u.rank === 1 ? 'gold' : u.rank === 2 ? 'silver' : u.rank === 3 ? 'bronze' : ''}">#${u.rank}</div><div class="avatar-container"><img class="avatar" src="${u.avatarUrl}" alt="${u.displayName}" /></div><div class="user-info"><div class="name">${u.displayName}</div><div class="user-tag">Rank #${u.rank}</div></div><div class="score">${u.score}</div></div>`).join('')}${userRankHtml}</div></div></body></html>`;
    return nodeHtmlToImage({ html, puppeteerArgs: { args: ['--no-sandbox', '--disable-setuid-sandbox'] } });
}

/* =========================
   COMMAND DEFINITIONS
========================= */
function buildCommands() {
    const activityChoices = Object.keys(POINTS).map(key => ({ name: key.charAt(0).toUpperCase() + key.slice(1), value: key }));
    return [
        ...Object.entries(POINTS).map(([name, points]) => new SlashCommandBuilder().setName(name).setDescription(`💪 Claim +${points} for ${name}`)),
        new SlashCommandBuilder().setName('walking').setDescription(`🚶 Log walking by distance (${DISTANCE_RATES.walking} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 2.5)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('jogging').setDescription(`🏃 Log jogging by distance (${DISTANCE_RATES.jogging} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 5)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('running').setDescription(`💨 Log running by distance (${DISTANCE_RATES.running} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 3)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('myscore').setDescription('🏆 Show your score, rank, and progress'),
        new SlashCommandBuilder().setName('leaderboard').setDescription('📊 Show the server leaderboard').addStringOption(o => o.setName('period').setDescription('Time period').setRequired(true).addChoices({ name: 'Today', value: 'day' }, { name: 'This Week', value: 'week' }, { name: 'This Month', value: 'month' }, { name: 'This Year', value: 'year' }, { name: 'All Time', value: 'all' })).addStringOption(o => o.setName('category').setDescription('Category to rank').addChoices({ name: 'All (total)', value: 'all' }, ...activityChoices, { name: 'Current Streak', value: 'streak' })),
        new SlashCommandBuilder().setName('junk').setDescription('🍕 Log junk food to deduct points').addStringOption(o => o.setName('item').setDescription('The junk food item').setRequired(true).addChoices(...Object.entries(DEDUCTIONS).map(([key, { emoji, label }]) => ({ name: `${emoji} ${label}`, value: key })))),
        new SlashCommandBuilder().setName('buddy').setDescription('👯 Set or view your workout buddy').addUserOption(o => o.setName('user').setDescription('Your buddy (leave empty to view)')),
        new SlashCommandBuilder().setName('admin').setDescription('🛠️ Admin commands').setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
            .addSubcommand(sub => sub.setName('award').setDescription('Award points to a user').addUserOption(o => o.setName('user').setRequired(true).setDescription('User to award')).addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points to award')).addStringOption(o => o.setName('category').setRequired(true).setDescription('Category').addChoices(...activityChoices)).addStringOption(o => o.setName('reason').setDescription('Reason for the award')))
            .addSubcommand(sub => sub.setName('deduct').setDescription('Deduct points from a user').addUserOption(o => o.setName('user').setRequired(true).setDescription('User to deduct from')).addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points to deduct')).addStringOption(o => o.setName('category').setRequired(true).setDescription('Category').addChoices(...activityChoices)).addStringOption(o => o.setName('reason').setDescription('Reason for the deduction'))),
    ].map(c => c.toJSON());
}

/* =========================
   COMMAND HANDLERS
========================= */
/* =========================
   COMMAND HANDLERS (FIXED)
========================= */
class CommandHandler {
    constructor(db) { this.db = db; }

    async handleClaim(interaction, category, cooldownKey, explicitAmount) {
        const { guild, user } = interaction;
        const amount = Number(explicitAmount ?? POINTS[category]) || 0;
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) return interaction.reply({ content: `⏳ Cooldown active for **${category}**. Try again in **${formatCooldown(remaining)}**.`, ephemeral: true });

        const newAchievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category, amount, reason: `claim:${category}` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`**+${formatNumber(amount)}** points for **${category}**!`).addFields({ name: "New Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Current Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL());
        if (need > 0) embed.setFooter({ text: `Only ${formatNumber(need)} points to the next rank!` });
        
        const replyPayload = { embeds: [embed], ephemeral: true };
        if (newAchievements.length > 0) {
            const achievementEmbed = new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement Unlocked!').setDescription(newAchievements.map(a => `**${a.name}**: ${a.description}`).join('\n'));
            replyPayload.embeds.push(achievementEmbed);
        }
        return interaction.reply(replyPayload);
    }

    async handleJunk(interaction) {
        const { guild, user, options } = interaction;
        const item = options.getString('item', true);
        const deduction = DEDUCTIONS[item];
        this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'total', amount: -deduction.points, reason: `junk:${item}` });
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        const embed = new EmbedBuilder().setColor(0xED4245).setDescription(`${deduction.emoji} **-${formatNumber(deduction.points)}** points for **${deduction.label}**!`).addFields({ name: "New Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }).setThumbnail(user.displayAvatarURL());
        return interaction.reply({ embeds: [embed], ephemeral: true });
    }

    async handleMyScore(interaction) {
        const { guild, user } = interaction;
        const userRow = this.db.stmts.getUser.get(guild.id, user.id) || { total: 0, current_streak: 0 };
        const { pct, cur, need } = nextRankProgress(userRow.total);
        const achievements = this.db.stmts.getUserAchievements.all(guild.id, user.id).map(r => r.achievement_id);
        const embed = new EmbedBuilder().setColor(cur.color).setAuthor({ name: user.displayName, iconURL: user.displayAvatarURL() }).setTitle(`Rank: ${cur.name}`).addFields(
            { name: 'Total Points', value: formatNumber(userRow.total), inline: true },
            { name: 'Current Streak', value: `🔥 ${userRow.current_streak || 0} days`, inline: true },
            { name: 'Progress to Next Rank', value: progressBar(pct), inline: false },
            { name: 'Achievements', value: achievements.length > 0 ? achievements.map(id => `**${ACHIEVEMENTS.find(a=>a.id===id)?.name || id}**`).join(', ') : 'None yet!' }
        );
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} points to the next rank!` });
        return interaction.reply({ embeds: [embed] });
    }

    async handleLeaderboard(interaction) {
        // --- DEFER MOVED HERE ---
        await interaction.deferReply();
        
        const { guild, user, options } = interaction;
        const period = options.getString('period');
        const cat = options.getString('category') ?? 'all';
        let rows = [], userRankRow = null, subtitle = '';

        try {
            subtitle = `Top ${cat === 'streak' ? 'Streaks' : 'Players'}`;
            
            if (period === 'all') {
                if (cat === 'streak') {
                    rows = this.db.stmts.getTopStreaks.all(guild.id, 10);
                } else if (cat === 'all') {
                    rows = this.db.stmts.getLeaderboardAllTime.all(guild.id);
                    userRankRow = this.db.stmts.getUserRankAllTime.get(guild.id, user.id);
                    if (userRankRow) userRankRow.userId = user.id;
                } else {
                    const stmtKey = `getLeaderboard_${cat}`;
                    if (this.db.stmts[stmtKey]) {
                        rows = this.db.stmts[stmtKey].all(guild.id);
                    }
                }
            } else {
                const periodName = { day: 'Today', week: 'This Week', month: 'This Month', year: 'This Year' }[period];
                subtitle = cat === 'all' ? periodName : `${periodName} - ${cat.charAt(0).toUpperCase() + cat.slice(1)}`;
                if (cat === 'streak') {
                    rows = this.db.stmts.getTopStreaks.all(guild.id);
                } else {
                    const since = getPeriodStart(period);
                    rows = cat === 'all' ? this.db.stmts.getLeaderboardPeriodic.all(guild.id, since) : this.db.stmts.getLeaderboardPeriodicCategory.all(guild.id, since, cat);
                }
            }

            if (!rows || rows.length === 0) {
                return interaction.editReply({ content: '📊 No data available for this leaderboard yet! Start earning points to appear here.' });
            }

            rows = rows.map((r, idx) => ({ ...r, rank: idx + 1 }));

            const imageBuffer = await renderLeaderboardCard({ title: 'Leaderboard', rows, guild, userRank: userRankRow, subtitle });
            return interaction.editReply({ files: [new AttachmentBuilder(imageBuffer, { name: 'leaderboard.png' })] });
        } catch (err) {
            console.error("❌ Leaderboard render failed:", err);
            return interaction.editReply({ content: `❌ Sorry, there was an error generating the leaderboard.` });
        }
    }

    async handleBuddy(interaction) {
        const { guild, user, options } = interaction;
        const targetUser = options.getUser('user');
        if (!targetUser) {
            const buddy = this.db.stmts.getBuddy.get(guild.id, user.id);
            return interaction.reply({ content: buddy?.buddy_id ? `Your workout buddy is <@${buddy.buddy_id}>.` : 'You haven\'t set a workout buddy yet!', ephemeral: true });
        }
        if (targetUser.id === user.id) return interaction.reply({ content: 'You cannot be your own buddy!', ephemeral: true });
        this.db.stmts.setBuddy.run(guild.id, user.id, targetUser.id);
        return interaction.reply({ content: `✨ You've set <@${targetUser.id}> as your new workout buddy!` });
    }

    async handleAdmin(interaction) {
        // --- DEFER MOVED HERE ---
        await interaction.deferReply({ ephemeral: true });

        const { guild, user, options } = interaction;
        const subcommand = options.getSubcommand();
        const targetUser = options.getUser('user', true);
        const amount = options.getNumber('amount', true);
        const category = options.getString('category', true);
        const reason = options.getString('reason') || `Admin action by ${user.tag}`;

        if (subcommand === 'award') {
            this.db.modifyPoints({ guildId: guild.id, userId: targetUser.id, category, amount, reason: 'admin:award', notes: reason });
            return interaction.editReply({ content: `✅ Awarded ${formatNumber(amount)} ${category} points to <@${targetUser.id}>.` });
        }
        if (subcommand === 'deduct') {
            this.db.modifyPoints({ guildId: guild.id, userId: targetUser.id, category, amount: -amount, reason: 'admin:deduct', notes: reason });
            return interaction.editReply({ content: `✅ Deducted ${formatNumber(amount)} ${category} points from <@${targetUser.id}>.` });
        }
    }
}

/* =========================
   MAIN BOT INITIALIZATION (FIXED)
========================= */
async function main() {
  if (!CONFIG.token || !CONFIG.appId) {
    console.error('❌ Missing required environment variables: DISCORD_TOKEN and APPLICATION_ID');
    process.exit(1);
  }

  const database = new PointsDatabase(CONFIG.dbFile);
  const handler = new CommandHandler(database);

  const rest = new REST({ version: '10' }).setToken(CONFIG.token);
  try {
    console.log('🔄 Registering slash commands...');
    const route = CONFIG.devGuildId ? Routes.applicationGuildCommands(CONFIG.appId, CONFIG.devGuildId) : Routes.applicationCommands(CONFIG.appId);
    await rest.put(route, { body: buildCommands() });
    console.log('✅ Registered slash commands.');
  } catch (err) {
    console.error('❌ Command registration failed:', err);
    process.exit(1);
  }

  const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers] });

  // Use the new 'clientReady' event to fix the deprecation warning
  client.once('clientReady', (c) => {
    console.log(`🤖 Logged in as ${c.user.tag}`);
    console.log(`📊 Serving ${c.guilds.cache.size} server(s)`);
  });

  client.on('interactionCreate', async (interaction) => {
    if (!interaction.isChatInputCommand() || !interaction.guild) return;
    const { commandName } = interaction;

    try {
      const claimCategories = Object.keys(POINTS);
      if (claimCategories.includes(commandName)) {
        await handler.handleClaim(interaction, commandName, commandName);
      } else {
        switch (commandName) {
          case 'walking':
          case 'jogging':
          case 'running':
            await handler.handleClaim(interaction, 'exercise', 'exercise', interaction.options.getNumber('km', true) * DISTANCE_RATES[commandName]);
            break;
          case 'junk': await handler.handleJunk(interaction); break;
          case 'myscore': await handler.handleMyScore(interaction); break;
          case 'leaderboard': await handler.handleLeaderboard(interaction); break;
          case 'buddy': await handler.handleBuddy(interaction); break;
          case 'admin': await handler.handleAdmin(interaction); break;
        }
      }
    } catch (err) {
      console.error(`❌ Error handling command ${commandName}:`, err);
      
      // --- ROBUST ERROR HANDLING ---
      // Discord error code 10062 is "Unknown Interaction", which means it timed out.
      // If we get this error, we can't reply, so we just log it.
      if (err.code === 10062) {
        console.log("Interaction timed out (likely a cold start). No reply sent.");
        return;
      }

      const reply = { content: `❌ An error occurred while processing your command.`, ephemeral: true };
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply(reply).catch(console.error); // Catch potential follow-up errors
      } else {
        await interaction.reply(reply).catch(console.error);
      }
    }
  });

  process.on('SIGINT', () => {
    console.log('\n🛑 SIGINT received, shutting down gracefully...');
    database.close();
    client.destroy();
    process.exit(0);
  });

  process.on('SIGTERM', () => {
    console.log('\n🛑 SIGTERM received, shutting down gracefully...');
    database.close();
    client.destroy();
    process.exit(0);
  });

  await client.login(CONFIG.token);
}

main().catch(err => {
  console.error('❌ Fatal error in main execution:', err);
  process.exit(1);
});