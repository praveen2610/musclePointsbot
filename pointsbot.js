// pointsbot.js - Professional Version (with new protein items and cooldown)
import 'dotenv/config';
import http from 'node:http';
import {
    Client, GatewayIntentBits, REST, Routes,
    SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits, MessageFlags
} from 'discord.js';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

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

// MODIFIED: Added dahl and mutton
const PROTEIN_SOURCES = {
    // -- Meats & Poultry (protein per gram, cooked) --
    chicken_breast: { name: 'Chicken Breast (Cooked)', unit: 'gram', protein_per_unit: 0.31 },
    chicken_thigh:  { name: 'Chicken Thigh (Cooked)', unit: 'gram', protein_per_unit: 0.26 },
    ground_beef:    { name: 'Ground Beef 85/15 (Cooked)', unit: 'gram', protein_per_unit: 0.26 },
    steak:          { name: 'Steak (Sirloin, Cooked)', unit: 'gram', protein_per_unit: 0.29 },
    pork_chop:      { name: 'Pork Chop (Cooked)', unit: 'gram', protein_per_unit: 0.27 },
    mutton:         { name: 'Mutton (Cooked)', unit: 'gram', protein_per_unit: 0.27 },

    // -- Seafood (protein per gram, cooked) --
    salmon: { name: 'Salmon (Cooked)', unit: 'gram', protein_per_unit: 0.25 },
    tuna:   { name: 'Tuna (Canned in water)', unit: 'gram', protein_per_unit: 0.23 },
    shrimp: { name: 'Shrimp (Cooked)', unit: 'gram', protein_per_unit: 0.24 },
    cod:    { name: 'Cod (Cooked)', unit: 'gram', protein_per_unit: 0.26 },

    // -- Dairy & Eggs --
    egg:            { name: 'Large Egg', unit: 'item', protein_per_unit: 6 },
    egg_white:      { name: 'Large Egg White', unit: 'item', protein_per_unit: 3.6 },
    greek_yogurt:   { name: 'Greek Yogurt', unit: 'gram', protein_per_unit: 0.10 },
    cottage_cheese: { name: 'Cottage Cheese', unit: 'gram', protein_per_unit: 0.11 },
    milk:           { name: 'Milk (Dairy)', unit: 'gram', protein_per_unit: 0.034 },

    // -- Plant-Based (protein per gram, cooked/prepared) --
    tofu:        { name: 'Tofu (Firm)', unit: 'gram', protein_per_unit: 0.08 },
    edamame:     { name: 'Edamame (Shelled)', unit: 'gram', protein_per_unit: 0.11 },
    lentils:     { name: 'Lentils (Cooked)', unit: 'gram', protein_per_unit: 0.09 },
    dahl:        { name: 'Dahl (Cooked Lentils)', unit: 'gram', protein_per_unit: 0.09 },
    chickpeas:   { name: 'Chickpeas (Cooked)', unit: 'gram', protein_per_unit: 0.09 },
    black_beans: { name: 'Black Beans (Cooked)', unit: 'gram', protein_per_unit: 0.08 },
    quinoa:      { name: 'Quinoa (Cooked)', unit: 'gram', protein_per_unit: 0.04 },
    almonds:     { name: 'Almonds', unit: 'gram', protein_per_unit: 0.21 },
    peanuts:     { name: 'Peanuts', unit: 'gram', protein_per_unit: 0.26 },
    
    // -- Supplements (protein per gram of powder) --
    protein_powder: { name: 'Protein Powder', unit: 'gram', protein_per_unit: 0.80 }
};

// MODIFIED: Changed exercise cooldown to 30 minutes
const COOLDOWNS = {
    gym: 12 * 60 * 60 * 1000,
    badminton: 12 * 60 * 60 * 1000,
    cricket: 12 * 60 * 60 * 1000,
    swimming: 12 * 60 * 60 * 1000,
    yoga: 12 * 60 * 60 * 1000,
    exercise: 30 * 60 * 1000, // 30 minutes
};

const POINTS = {
    gym: 2,
    badminton: 5,
    cricket: 5,
    swimming: 3,
    yoga: 2,
};

const EXERCISE_RATES = {
    yoga_per_minute: 0.2,
    per_rep: 0.002,
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
    samosa: { points: 3, emoji: '🥟', label: 'Samosa' },
    parotta: { points: 4, emoji: '🫓', label: 'Parotta' },
    vada_pav: { points: 3, emoji: '🍔', label: 'Vada Pav' },
    pani_puri: { points: 2, emoji: '🧆', label: 'Pani Puri / Golgappe' },
    jalebi: { points: 3, emoji: '🍥', label: 'Jalebi' },
    pakora: { points: 2, emoji: ' fritter', label: 'Pakora / Bhaji' },
    bonda: { points: 2, emoji: '🥔', label: 'Bonda' },
    murukku: { points: 2, emoji: '🥨', label: 'Murukku / Chakli' },
    kachori: { points: 3, emoji: '🍘', label: 'Kachori' },
    chaat: { points: 3, emoji: '🥣', label: 'Chaat' },
    gulab_jamun: { points: 3, emoji: '🍮', label: 'Gulab Jamun' },
    chips: { points: 2, emoji: '🥔', label: 'Chips (Packet)' },
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
          CREATE TABLE IF NOT EXISTS reminders ( id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT, user_id TEXT, activity TEXT, due_at INTEGER );
          CREATE INDEX IF NOT EXISTS idx_points_log_guild_ts ON points_log(guild_id, ts);
          CREATE INDEX IF NOT EXISTS idx_points_total ON points(guild_id, total DESC);
          CREATE TABLE IF NOT EXISTS protein_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            protein_grams REAL NOT NULL,
            timestamp INTEGER NOT NULL
          );
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
        stmts.getLeaderboardAllTime = this.db.prepare(`SELECT user_id as userId, total as score FROM points WHERE guild_id = ? AND total > 0 ORDER BY total DESC LIMIT 10`);
        stmts.getUserRankAllTime = this.db.prepare(`
            SELECT rank, score FROM (
                SELECT user_id, total as score, RANK() OVER (ORDER BY total DESC) as rank
                FROM points WHERE guild_id = ?
            ) WHERE user_id = ?
        `);
        stmts.getLeaderboardPeriodic = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id = ? AND ts >= ? AND amount > 0 GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
        stmts.getLeaderboardPeriodicCategory = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id = ? AND ts >= ? AND category = ? AND amount > 0 GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
        stmts.getTopStreaks = this.db.prepare(`SELECT user_id as userId, current_streak as score FROM points WHERE guild_id = ? AND current_streak > 0 ORDER BY current_streak DESC LIMIT 10`);
        stmts.getBuddy = this.db.prepare(`SELECT buddy_id FROM buddies WHERE guild_id = ? AND user_id = ?`);
        stmts.setBuddy = this.db.prepare(`INSERT INTO buddies (guild_id, user_id, buddy_id) VALUES (?, ?, ?) ON CONFLICT(guild_id, user_id) DO UPDATE SET buddy_id = excluded.buddy_id`);
        stmts.unlockAchievement = this.db.prepare(`INSERT OR IGNORE INTO achievements (guild_id, user_id, achievement_id) VALUES (?, ?, ?)`),
        stmts.getUserAchievements = this.db.prepare(`SELECT achievement_id FROM achievements WHERE guild_id = ? AND user_id = ?`);
        stmts.addReminder = this.db.prepare(`INSERT INTO reminders (guild_id, user_id, activity, due_at) VALUES (?, ?, ?, ?)`);
        stmts.getDueReminders = this.db.prepare(`SELECT id, guild_id, user_id, activity FROM reminders WHERE due_at <= ?`);
        stmts.deleteReminder = this.db.prepare(`DELETE FROM reminders WHERE id = ?`);
        stmts.addProteinLog = this.db.prepare(`INSERT INTO protein_log (guild_id, user_id, item_name, protein_grams, timestamp) VALUES (?, ?, ?, ?, ?)`);
        stmts.getDailyProtein = this.db.prepare(`SELECT SUM(protein_grams) AS total FROM protein_log WHERE guild_id = ? AND user_id = ? AND timestamp >= ?`);

        for (const category of Object.keys(POINTS)) {
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
        this.stmts.logPoints.run(guildId, userId, category, modAmount, Math.floor(Date.now() / 1000), reason, notes);
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
    UTILITIES
========================= */
const formatNumber = (n) => (Math.round(n * 1000) / 1000).toLocaleString(undefined, { maximumFractionDigits: 3 });
const progressBar = (pct) => `${'█'.repeat(Math.floor(pct / 10))}${'░'.repeat(10 - Math.floor(pct / 10))} ${pct}%`;
const getUserRank = (total) => RANKS.reduce((acc, rank) => total >= rank.min ? rank : acc, RANKS[0]);
function nextRankProgress(total) { const cur = getUserRank(total); if (cur.next === null) return { pct: 100, cur, need: 0 }; const span = cur.next - cur.min; const done = total - cur.min; return { pct: Math.max(0, Math.min(100, Math.floor((done / span) * 100))), cur, need: cur.next - total }; }
const formatCooldown = (ms) => `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
function getPeriodStart(period = 'week') { const now = new Date(); switch (period) { case 'day': now.setHours(0, 0, 0, 0); return Math.floor(now.getTime() / 1000); case 'month': return Math.floor(new Date(now.getFullYear(), now.getMonth(), 1).getTime() / 1000); case 'year': return Math.floor(new Date(now.getFullYear(), 0, 1).getTime() / 1000); case 'week': default: const dayOfWeek = now.getDay(); const diff = now.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1); now.setDate(diff); now.setHours(0, 0, 0, 0); return Math.floor(now.getTime() / 1000); } }
function createKeepAliveServer() { http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('Bot is alive and running.'); }).listen(process.env.PORT || 3000, () => { console.log('✅ Keep-alive server started.'); }); }

/* =========================
    COMMAND DEFINITIONS
========================= */
function buildCommands() {
    const activityChoices = Object.keys(POINTS).map(key => ({ name: key.charAt(0).toUpperCase() + key.slice(1), value: key }));
    return [
        ...Object.entries(POINTS).map(([name, points]) => new SlashCommandBuilder().setName(name).setDescription(`💪 Claim +${points} for ${name}`)),
        
        new SlashCommandBuilder().setName('exercise').setDescription('💪 Log a detailed exercise session')
            .addSubcommand(sub => sub.setName('yoga').setDescription(`🧘 Log a yoga session (${EXERCISE_RATES.yoga_per_minute} points/min)`)
                .addNumberOption(o => o.setName('minutes').setDescription('How many minutes did you do?').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('reps').setDescription(`💪 Log a per-rep exercise (${EXERCISE_RATES.per_rep} points/rep)`)
                .addNumberOption(o => o.setName('count').setDescription('How many total reps did you do?').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('dumbbells').setDescription(`🏋️ Log a dumbbell workout`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Number of sets').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('barbell').setDescription(`🏋️ Log a barbell workout`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Number of sets').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('pushup').setDescription(`💪 Log a pushup workout`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Number of sets').setRequired(true).setMinValue(1))),

        new SlashCommandBuilder().setName('protein').setDescription('🥩 Track your protein intake for the day')
            .addSubcommand(sub => sub.setName('add_item').setDescription('Add a protein source by item count (e.g., eggs)')
                .addStringOption(o => o.setName('item').setDescription('The food item you ate').setRequired(true).addChoices(
                    ...Object.entries(PROTEIN_SOURCES)
                        .filter(([key, val]) => val.unit === 'item')
                        .map(([key, val]) => ({ name: val.name, value: key }))
                ))
                .addIntegerOption(o => o.setName('quantity').setDescription('How many items? (e.g., 2 for 2 eggs)').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('add_grams').setDescription('Add a protein source by weight in grams')
                .addStringOption(o => o.setName('item').setDescription('The food item you ate').setRequired(true).addChoices(
                    ...Object.entries(PROTEIN_SOURCES)
                        .filter(([key, val]) => val.unit === 'gram')
                        .map(([key, val]) => ({ name: val.name, value: key }))
                ))
                .addNumberOption(o => o.setName('grams').setDescription('How many grams?').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('total').setDescription("View your total protein intake for today")
                .addUserOption(o => o.setName('user').setDescription('View another user\'s total (optional)'))),

        new SlashCommandBuilder().setName('walking').setDescription(`🚶 Log walking by distance (${DISTANCE_RATES.walking} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 2.5)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('jogging').setDescription(`🏃 Log jogging by distance (${DISTANCE_RATES.jogging} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 5)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('running').setDescription(`💨 Log running by distance (${DISTANCE_RATES.running} points/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 3)').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('myscore').setDescription('🏆 Show your score, rank, and progress'),
        new SlashCommandBuilder().setName('leaderboard').setDescription('📊 Show the server leaderboard').addStringOption(o => o.setName('period').setDescription('Time period').setRequired(true).addChoices({ name: 'Today', value: 'day' }, { name: 'This Week', value: 'week' }, { name: 'This Month', value: 'month' }, { name: 'This Year', value: 'year' }, { name: 'All Time', value: 'all' })).addStringOption(o => o.setName('category').setDescription('Category to rank').addChoices({ name: 'All (total)', value: 'all' }, ...activityChoices, { name: 'Current Streak', value: 'streak' })),
        new SlashCommandBuilder().setName('junk').setDescription('🍕 Log junk food to deduct points').addStringOption(o => o.setName('item').setDescription('The junk food item').setRequired(true).addChoices(...Object.entries(DEDUCTIONS).map(([key, { emoji, label }]) => ({ name: `${emoji} ${label}`, value: key })))),
        new SlashCommandBuilder().setName('buddy').setDescription('👯 Set or view your workout buddy').addUserOption(o => o.setName('user').setDescription('Your buddy (leave empty to view)')),
        new SlashCommandBuilder().setName('nudge').setDescription('👉 Nudge a user to work out').addUserOption(o => o.setName('user').setRequired(true).setDescription('The user to nudge')).addStringOption(o => o.setName('activity').setRequired(true).setDescription('The activity to remind them about')),
        new SlashCommandBuilder().setName('remind').setDescription('⏰ Set a personal workout reminder').addStringOption(o => o.setName('activity').setRequired(true).setDescription('What to remind you about')).addNumberOption(o => o.setName('hours').setRequired(true).setDescription('In how many hours?').setMinValue(1)),
        new SlashCommandBuilder().setName('admin').setDescription('🛠️ Admin commands').setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
            .addSubcommand(sub => sub.setName('award').setDescription('Award points to a user').addUserOption(o => o.setName('user').setRequired(true).setDescription('User to award')).addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points to award')).addStringOption(o => o.setName('category').setRequired(true).setDescription('Category').addChoices(...activityChoices, {name: 'Exercise', value: 'exercise'})).addStringOption(o => o.setName('reason').setDescription('Reason for the award')))
            .addSubcommand(sub => sub.setName('deduct').setDescription('Deduct points from a user').addUserOption(o => o.setName('user').setRequired(true).setDescription('User to deduct from')).addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points to deduct')).addStringOption(o => o.setName('category').setRequired(true).setDescription('Category').addChoices(...activityChoices, {name: 'Exercise', value: 'exercise'})).addStringOption(o => o.setName('reason').setDescription('Reason for the deduction'))),
    ].map(c => c.toJSON());
}

/* =========================
    COMMAND HANDLERS
========================= */
class CommandHandler {
    constructor(db) { this.db = db; }

    async handleClaim(interaction, category, cooldownKey, explicitAmount) {
        const { guild, user } = interaction;
        const amount = Number(explicitAmount) || 0;
        
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) {
            return interaction.editReply({ 
                content: `⏳ Cooldown active for **${category}**. Try again in **${formatCooldown(remaining)}**.`,
                flags: [MessageFlags.Ephemeral] 
            });
        }

        const newAchievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category, amount, reason: `claim:${category}` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) {
            return interaction.editReply({ content: 'There was an error updating your score. Please try again.', flags: [MessageFlags.Ephemeral] });
        }

        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`**+${formatNumber(amount)}** points for **${category}** claimed by ${user.toString()}!`).addFields({ name: "New Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Current Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL());
        if (need > 0) embed.setFooter({ text: `Only ${formatNumber(need)} points to the next rank!` });
        
        const embeds = [embed];
        
        if (newAchievements.length > 0) {
            const achievementEmbed = new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement Unlocked!').setDescription(newAchievements.map(a => `**${a.name}**: ${a.description}`).join('\n'));
            embeds.push(achievementEmbed);
        }

        return interaction.editReply({ embeds });
    }
    
    async handleExercise(interaction) {
        const { guild, user, options } = interaction;
        const subcommand = options.getSubcommand();
        let amount = 0;
        let description = '';

        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: 'exercise' });
        if (remaining > 0) {
            return interaction.editReply({ 
                content: `⏳ Cooldown active for **exercise**. Try again in **${formatCooldown(remaining)}**.`,
                flags: [MessageFlags.Ephemeral] 
            });
        }

        switch (subcommand) {
            case 'yoga': {
                const minutes = options.getNumber('minutes', true);
                amount = minutes * EXERCISE_RATES.yoga_per_minute;
                description = `**+${formatNumber(amount)}** points for **${minutes} minutes** of Yoga`;
                break;
            }
            case 'reps': {
                const count = options.getNumber('count', true);
                amount = count * EXERCISE_RATES.per_rep;
                description = `**+${formatNumber(amount)}** points for **${count} total reps**`;
                break;
            }
            case 'dumbbells':
            case 'barbell':
            case 'pushup': {
                const reps = options.getNumber('reps', true);
                const sets = options.getNumber('sets', true);
                const totalReps = reps * sets;
                amount = totalReps * EXERCISE_RATES.per_rep;
                description = `**+${formatNumber(amount)}** points for ${sets}x${reps} (${totalReps} total reps) of ${subcommand}`;
                break;
            }
        }

        const newAchievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'exercise', amount, reason: `exercise:${subcommand}` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: 'exercise' });
        
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) {
            return interaction.editReply({ content: 'There was an error updating your score. Please try again.', flags: [MessageFlags.Ephemeral] });
        }

        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder()
            .setColor(cur.color)
            .setDescription(`${description} claimed by ${user.toString()}!`)
            .addFields(
                { name: "New Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true },
                { name: "Current Rank", value: cur.name, inline: true }
            )
            .setThumbnail(user.displayAvatarURL());

        if (need > 0) embed.setFooter({ text: `Only ${formatNumber(need)} points to the next rank!` });
        
        const embeds = [embed];
        
        if (newAchievements.length > 0) {
            const achievementEmbed = new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement Unlocked!').setDescription(newAchievements.map(a => `**${a.name}**: ${a.description}`).join('\n'));
            embeds.push(achievementEmbed);
        }

        return interaction.editReply({ embeds });
    }

    async handleProtein(interaction) {
        const { guild, user, options } = interaction;
        const subcommand = options.getSubcommand();
        const targetUser = options.getUser('user') || user;

        if (subcommand === 'total') {
            const since = getPeriodStart('day');
            const result = this.db.stmts.getDailyProtein.get(guild.id, targetUser.id, since);
            const totalProtein = result?.total || 0;

            const embed = new EmbedBuilder()
                .setColor(0x5865F2)
                .setTitle(`🥩 Daily Protein Total for ${targetUser.displayName}`)
                .setDescription(`So far today, you have logged **${formatNumber(totalProtein)}g** of protein.`)
                .setThumbnail(targetUser.displayAvatarURL());
            
            return interaction.editReply({ embeds: [embed] });
        }

        let proteinGrams = 0;
        let itemName = '';
        const itemKey = options.getString('item', true);
        const source = PROTEIN_SOURCES[itemKey];

        if (subcommand === 'add_item') {
            const quantity = options.getInteger('quantity', true);
            proteinGrams = source.protein_per_unit * quantity;
            itemName = `${quantity} ${source.name}`;
        } else if (subcommand === 'add_grams') {
            const grams = options.getNumber('grams', true);
            proteinGrams = source.protein_per_unit * grams;
            itemName = `${grams}g of ${source.name}`;
        }

        this.db.stmts.addProteinLog.run(guild.id, user.id, itemName, proteinGrams, Math.floor(Date.now() / 1000));

        const since = getPeriodStart('day');
        const result = this.db.stmts.getDailyProtein.get(guild.id, user.id, since);
        const totalProtein = result?.total || 0;

        const embed = new EmbedBuilder()
            .setColor(0x2ECC71)
            .setTitle('✅ Protein Logged!')
            .setDescription(`${user.toString()} added **${formatNumber(proteinGrams)}g** of protein from **${itemName}**.`)
            .addFields({ name: 'Daily Total', value: `Your new total for today is **${formatNumber(totalProtein)}g** of protein.` })
            .setThumbnail(user.displayAvatarURL());
        
        return interaction.editReply({ embeds: [embed] });
    }

    async handleJunk(interaction) {
        const { guild, user, options } = interaction;
        const item = options.getString('item', true);
        const deduction = DEDUCTIONS[item];

        const positiveMessages = [
            "It's all about balance! Let's make the next meal a healthy one.",
            "A small setback is just a setup for a great comeback!",
            "Thanks for being honest! Every healthy choice from here on out counts.",
            "We all treat ourselves sometimes. Let's get back on track together!",
            "Acknowledging it is the first step! Let's aim for a healthy snack next time."
        ];
        const randomMessage = positiveMessages[Math.floor(Math.random() * positiveMessages.length)];

        this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'total', amount: -deduction.points, reason: `junk:${item}` });
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);

        const embed = new EmbedBuilder()
            .setColor(0xED4245)
            .setDescription(`${user.toString()} logged having some ${deduction.emoji} **${deduction.label}**, deducting **${formatNumber(deduction.points)}** points.`)
            .addFields({ name: "New Total", value: `🏆 ${formatNumber(userRow.total)}` })
            .setFooter({ text: randomMessage });
            
        return interaction.editReply({ embeds: [embed] });
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
            { name: 'Achievements', value: achievements.length > 0 ? achievements.map(id => `**${ACHIEVEMENTS.find(a => a.id === id)?.name || id}**`).join(', ') : 'None yet!' }
        );
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} points to the next rank!` });
        return interaction.editReply({ embeds: [embed] });
    }

    async handleLeaderboard(interaction) {
        const { guild, user, options } = interaction;
        const period = options.getString('period');
        const cat = options.getString('category') ?? 'all';
        let rows = [], userRankRow = null, subtitle = '';

        subtitle = `Top ${cat === 'streak' ? 'Streaks' : 'Players'}`;
        if (period === 'all') {
            subtitle += ' (All Time)';
            if (cat === 'streak') {
                rows = this.db.stmts.getTopStreaks.all(guild.id);
            } else if (cat === 'all') {
                rows = this.db.stmts.getLeaderboardAllTime.all(guild.id);
                userRankRow = this.db.stmts.getUserRankAllTime.get(guild.id, user.id);
            } else {
                const stmtKey = `getLeaderboard_${cat}`;
                if (this.db.stmts[stmtKey]) rows = this.db.stmts[stmtKey].all(guild.id);
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
            return interaction.editReply({ content: '📊 No data available for this leaderboard yet!' });
        }

        rows = rows.map((r, idx) => ({ ...r, rank: idx + 1 }));

        const userIds = rows.map(r => r.userId);
        const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());

        const leaderboardEntries = rows.map(row => {
            const member = members.get(row.userId);
            const name = member?.displayName || 'Unknown User';
            const score = formatNumber(row.score);
            const rankEmoji = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`;
            return `${rankEmoji} ${name} - \`${score}\``;
        });
        
        const embed = new EmbedBuilder()
            .setTitle(`🏆 Leaderboard: ${subtitle}`)
            .setColor(0x3498db)
            .setDescription(leaderboardEntries.join('\n'))
            .setTimestamp();
            
        if (userRankRow && !rows.some(r => r.userId === user.id)) {
            embed.setFooter({ text: `Your Rank: #${userRankRow.rank} with ${formatNumber(userRankRow.score)} points` });
        }

        return interaction.editReply({ embeds: [embed] });
    }

    async handleBuddy(interaction) {
        const { guild, user, options } = interaction;
        const targetUser = options.getUser('user');
        if (!targetUser) {
            const buddy = this.db.stmts.getBuddy.get(guild.id, user.id);
            return interaction.editReply({ content: buddy?.buddy_id ? `Your workout buddy is <@${buddy.buddy_id}>.` : 'You haven\'t set a workout buddy yet!' });
        }
        if (targetUser.id === user.id) return interaction.editReply({ content: 'You cannot be your own buddy!' });
        this.db.stmts.setBuddy.run(guild.id, user.id, targetUser.id);
        return interaction.editReply({ content: `✨ You've set <@${targetUser.id}> as your new workout buddy!` });
    }

    async handleNudge(interaction) {
        const { guild, user, options } = interaction;
        const targetUser = options.getUser('user', true);
        const activity = options.getString('activity', true);
    
        if (targetUser.bot || targetUser.id === user.id) {
            return interaction.editReply({ content: "You can't nudge a bot or yourself." });
        }
    
        const isAdmin = interaction.member.permissions.has(PermissionFlagsBits.ManageGuild);
        const buddy = this.db.stmts.getBuddy.get(guild.id, user.id);
        const isBuddy = buddy?.buddy_id === targetUser.id;
    
        if (!isAdmin && !isBuddy) {
            return interaction.editReply({ content: "You can only nudge your designated buddy or ask an admin to do it." });
        }
    
        try {
            await targetUser.send(`⏰ <@${user.id}> from **${guild.name}** is nudging you to do your **${activity}**!`);
            return interaction.editReply({ content: `✅ Nudge sent to <@${targetUser.id}>.` });
        } catch (err) {
            return interaction.editReply({ content: `❌ Could not send a DM to that user. They may have DMs disabled.` });
        }
    }
    
    async handleRemind(interaction) {
        const { guild, user, options } = interaction;
        const activity = options.getString('activity', true);
        const hours = options.getNumber('hours', true);
        const dueAt = Date.now() + hours * 60 * 60 * 1000;
    
        this.db.stmts.addReminder.run(guild.id, user.id, activity, dueAt);
    
        return interaction.editReply({ content: `⏰ Got it! I'll send you a DM to remind you about **${activity}** in ${hours} hour(s).` });
    }

    async handleAdmin(interaction) {
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
    MAIN BOT INITIALIZATION
========================= */
async function main() {
    createKeepAliveServer();
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

    client.once('clientReady', (c) => {
        console.log(`🤖 Logged in as ${c.user.tag}`);
        console.log(`📊 Serving ${c.guilds.cache.size} server(s)`);
        
        setInterval(async () => {
            try {
                const now = Date.now();
                const dueReminders = database.stmts.getDueReminders.all(now);

                for (const reminder of dueReminders) {
                    try {
                        const user = await client.users.fetch(reminder.user_id);
                        await user.send(`⏰ Gentle reminder to do your **${reminder.activity}**!`);
                    } catch (err) {
                        console.error(`Could not send reminder DM to user ${reminder.user_id}: ${err.message}`);
                    } finally {
                        database.stmts.deleteReminder.run(reminder.id);
                    }
                }
            } catch (err) {
                console.error("Error in reminder processing interval:", err);
            }
        }, 60 * 1000);
    });

    client.on('interactionCreate', async (interaction) => {
        if (!interaction.isChatInputCommand() || !interaction.guild) return;
        
        try {
            const ephemeralCommands = ['buddy', 'nudge', 'remind', 'admin', 'myscore'];
            let shouldBeEphemeral = ephemeralCommands.includes(interaction.commandName);
            
            if (interaction.commandName === 'protein' && interaction.options.getSubcommand() === 'total') {
                shouldBeEphemeral = true;
            }

            await interaction.deferReply({ ephemeral: shouldBeEphemeral });


            const { commandName } = interaction;
            const claimCategories = Object.keys(POINTS);

            if (claimCategories.includes(commandName)) {
                await handler.handleClaim(interaction, commandName, commandName);
            } else {
                switch (commandName) {
                    case 'exercise': await handler.handleExercise(interaction); break;
                    case 'protein': await handler.handleProtein(interaction); break;
                    case 'walking':
                    case 'jogging':
                    case 'running':
                        await handler.handleClaim(interaction, 'exercise', 'exercise', interaction.options.getNumber('km', true) * DISTANCE_RATES[commandName]);
                        break;
                    case 'junk': await handler.handleJunk(interaction); break;
                    case 'myscore': await handler.handleMyScore(interaction); break;
                    case 'leaderboard': await handler.handleLeaderboard(interaction); break;
                    case 'buddy': await handler.handleBuddy(interaction); break;
                    case 'nudge': await handler.handleNudge(interaction); break;
                    case 'remind': await handler.handleRemind(interaction); break;
                    case 'admin': await handler.handleAdmin(interaction); break;
                }
            }
        } catch (err) {
            console.error(`❌ Error handling command ${interaction.commandName}:`, err);
            if (interaction.deferred || interaction.replied) {
                 await interaction.editReply({ 
                    content: `❌ An error occurred while processing your command.`, 
                    flags: [MessageFlags.Ephemeral] 
                }).catch(console.error);
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