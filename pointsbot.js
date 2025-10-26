// pointsbot.js - Final Comprehensive Version
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
    protein_powder: { name: 'Protein Powder', unit: 'gram', protein_per_unit: 0.80 } // Assumes 80% protein
};

const COOLDOWNS = {
    gym: 12 * 60 * 60 * 1000,
    badminton: 12 * 60 * 60 * 1000,
    cricket: 12 * 60 * 60 * 1000,
    swimming: 12 * 60 * 60 * 1000,
    yoga: 12 * 60 * 60 * 1000,
    exercise: 30 * 60 * 1000, // Shared for distance/reps/plank
    cooking: 60 * 60 * 1000, // 1 hour for chores
    sweeping: 60 * 60 * 1000,
    gardening: 60 * 60 * 1000,
    carwash: 60 * 60 * 1000,
    toiletcleaning: 60 * 60 * 1000,
};

const POINTS = {
    gym: 2,
    badminton: 5,
    cricket: 5,
    swimming: 3,
    yoga: 2, // Yoga points are fixed, minutes logged for info
    cooking: 2,
    sweeping: 2,
    gardening: 2,
    carwash: 2,
    toiletcleaning: 5,
};

const EXERCISE_RATES = {
    // yoga_per_minute: 0.2, // Yoga now uses fixed points
    per_rep: 0.002, // Default rate for reps exercises
};

const DISTANCE_RATES = {
    walking: 0.5,
    jogging: 0.6,
    running: 0.7,
};
const REP_RATES = { // Specific rates override the default per_rep
    squat: 0.02,
    kettlebell: 0.2,
    lunge: 0.2,
    pushup: 0.02
};
const PLANK_RATE_PER_MIN = 1;
const PLANK_MIN_MIN = 0.75; // 45s

const DEDUCTIONS = {
    // General Junk (11)
    chocolate: { points: 2, emoji: '🍫', label: 'Chocolate' },
    fries: { points: 3, emoji: '🍟', label: 'Fries' },
    soda: { points: 2, emoji: '🥤', label: 'Soda / Soft Drink' },
    pizza: { points: 4, emoji: '🍕', label: 'Pizza Slice' },
    burger: { points: 3, emoji: '🍔', label: 'Burger' },
    sweets: { points: 2, emoji: '🍬', label: 'Sweets / Candy' },
    chips: { points: 2, emoji: '🥔', label: 'Chips (Packet)' },
    ice_cream: { points: 3, emoji: '🍦', label: 'Ice Cream' },
    cake: { points: 4, emoji: '🍰', label: 'Cake / Pastry' },
    cookies: { points: 2, emoji: '🍪', label: 'Cookies' },
    // Indian / Tamil Junk (14) - Total 25
    samosa: { points: 3, emoji: '🥟', label: 'Samosa' },
    parotta: { points: 4, emoji: '🫓', label: 'Parotta / Malabar Parotta' },
    vada_pav: { points: 3, emoji: '🍔', label: 'Vada Pav' },
    pani_puri: { points: 2, emoji: '🧆', label: 'Pani Puri / Golgappe' },
    jalebi: { points: 3, emoji: '🍥', label: 'Jalebi' },
    pakora: { points: 2, emoji: '🌶️', label: 'Pakora / Bhaji / Fritter' },
    bonda: { points: 2, emoji: '🥔', label: 'Bonda (Potato/Aloo)' },
    murukku: { points: 2, emoji: '🥨', label: 'Murukku / Chakli' },
    kachori: { points: 3, emoji: '🍘', label: 'Kachori' },
    chaat: { points: 3, emoji: '🥣', label: 'Chaat (Generic)' },
    gulab_jamun: { points: 3, emoji: '🍮', label: 'Gulab Jamun' },
    bhel_puri: { points: 2, emoji: '🥗', label: 'Bhel Puri' },
    dahi_vada: { points: 3, emoji: '🥣', label: 'Dahi Vada / Dahi Bhalla' },
    medu_vada: { points: 3, emoji: '🍩', label: 'Medu Vada (Sambar/Chutney)' },
    masala_dosa: { points: 4, emoji: '🌯', label: 'Masala Dosa' },
    // mysore_pak removed to stay at 25 limit
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
    { id: 'cardio_king', name: '🏃 Cardio King', requirement: (stats) => stats.exercise >= 100, description: 'Earn 100 exercise points' }, // Exercise column includes distance, reps, plank
    { id: 'streak_7', name: '🔥 Week Warrior', requirement: (stats) => stats.current_streak >= 7, description: 'Maintain a 7-day streak' },
    { id: 'century_club', name: '💯 Century Club', requirement: (stats) => stats.total >= 100, description: 'Reach 100 total points' },
];

/* =========================
    DATABASE CLASS
========================= */
class PointsDatabase {
    constructor(dbPath) {
        try { fs.mkdirSync(path.dirname(dbPath), { recursive: true }); } catch (err) { if (err.code !== 'EEXIST') console.error('DB dir error:', err); }
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
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, user_id TEXT NOT NULL,
            item_name TEXT NOT NULL, protein_grams REAL NOT NULL, timestamp INTEGER NOT NULL
          );
        `);
    }
    
    prepareStatements() {
        const stmts = {};
        stmts.upsertUser = this.db.prepare(`INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id) ON CONFLICT(guild_id, user_id) DO NOTHING`);
        // Includes cases for all point categories now (incl chores) in main points table update
        stmts.addPoints = this.db.prepare(`
            UPDATE points SET total = total + @add, 
            gym = CASE WHEN @category = 'gym' THEN gym + @add ELSE gym END, 
            badminton = CASE WHEN @category = 'badminton' THEN badminton + @add ELSE badminton END, 
            cricket = CASE WHEN @category = 'cricket' THEN cricket + @add ELSE cricket END, 
            exercise = CASE WHEN @category IN ('exercise', 'walking', 'jogging', 'running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup') THEN exercise + @add ELSE exercise END, 
            swimming = CASE WHEN @category = 'swimming' THEN swimming + @add ELSE swimming END, 
            yoga = CASE WHEN @category = 'yoga' THEN yoga + @add ELSE yoga END, 
            -- Chores currently don't have separate columns, they just add to 'total' via the log and modifyPoints logic
            updated_at = strftime('%s', 'now') 
            WHERE guild_id = @guild_id AND user_id = @user_id`);
        stmts.getUser = this.db.prepare(`SELECT * FROM points WHERE guild_id = ? AND user_id = ?`);
        stmts.updateStreak = this.db.prepare(`UPDATE points SET current_streak = @current_streak, longest_streak = @longest_streak, last_activity_date = @last_activity_date WHERE guild_id = @guild_id AND user_id = @user_id`);
        stmts.setCooldown = this.db.prepare(`INSERT INTO cooldowns (guild_id, user_id, category, last_ms) VALUES (@guild_id, @user_id, @category, @last_ms) ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms = excluded.last_ms`);
        stmts.getCooldown = this.db.prepare(`SELECT last_ms FROM cooldowns WHERE guild_id = ? AND user_id = ? AND category = ?`);
        stmts.logPoints = this.db.prepare(`INSERT INTO points_log (guild_id, user_id, category, amount, ts, reason, notes) VALUES (?, ?, ?, ?, ?, ?, ?)`),
        // Uses points table for consistency with /myscore
        stmts.lbAllFromPoints = this.db.prepare(`SELECT user_id as userId, total as score FROM points WHERE guild_id=? AND total > 0 ORDER BY total DESC LIMIT 10`);
        // Separate queries for each category column for All-Time category LB
        stmts.lbAllCatFromPoints_gym = this.db.prepare(`SELECT user_id as userId, gym as score FROM points WHERE guild_id=? AND gym > 0 ORDER BY gym DESC LIMIT 10`);
        stmts.lbAllCatFromPoints_badminton = this.db.prepare(`SELECT user_id as userId, badminton as score FROM points WHERE guild_id=? AND badminton > 0 ORDER BY badminton DESC LIMIT 10`);
        stmts.lbAllCatFromPoints_cricket = this.db.prepare(`SELECT user_id as userId, cricket as score FROM points WHERE guild_id=? AND cricket > 0 ORDER BY cricket DESC LIMIT 10`);
        stmts.lbAllCatFromPoints_exercise = this.db.prepare(`SELECT user_id as userId, exercise as score FROM points WHERE guild_id=? AND exercise > 0 ORDER BY exercise DESC LIMIT 10`);
        stmts.lbAllCatFromPoints_swimming = this.db.prepare(`SELECT user_id as userId, swimming as score FROM points WHERE guild_id=? AND swimming > 0 ORDER BY swimming DESC LIMIT 10`);
        stmts.lbAllCatFromPoints_yoga = this.db.prepare(`SELECT user_id as userId, yoga as score FROM points WHERE guild_id=? AND yoga > 0 ORDER BY yoga DESC LIMIT 10`);
        stmts.selfRankAllFromPoints = this.db.prepare(`
            WITH ranks AS ( SELECT user_id, total, RANK() OVER (ORDER BY total DESC) rk FROM points WHERE guild_id=? AND total > 0 ) 
            SELECT rk as rank, total as score FROM ranks WHERE user_id=?`);
        // Periodic queries use points_log
        stmts.lbSince = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount > 0 GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
        stmts.lbSinceByCat = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount > 0 AND category IN (?) GROUP BY user_id HAVING score > 0 ORDER BY score DESC LIMIT 10`);
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

        this.stmts = stmts;
    }
    
    modifyPoints({ guildId, userId, category, amount, reason = null, notes = null }) {
        this.stmts.upsertUser.run({ guild_id: guildId, user_id: userId });
        const modAmount = Number(amount) || 0;
        if (modAmount === 0) return [];
        let targetCategoryForPointsTable = category; // Which column to update in 'points'
        
        // Map exercise sub-types to the 'exercise' column in the points table
        if (['walking', 'jogging', 'running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'].includes(category)) {
            targetCategoryForPointsTable = 'exercise';
        }
        // Handle junk deduction target category
        else if (modAmount < 0 && category === 'junk') { 
            const userPoints = this.stmts.getUser.get(guildId, userId) || {};
            targetCategoryForPointsTable = ['exercise', 'gym', 'badminton', 'cricket', 'swimming', 'yoga'].sort((a, b) => (userPoints[b] || 0) - (userPoints[a] || 0))[0] || 'exercise';
        } 
        // For chores and other fixed point activities, ensure they exist in POINTS to affect the main table columns if defined
        else if (!POINTS[category]) {
             // If category isn't in POINTS (like a chore without a dedicated column), it only affects 'total'
             targetCategoryForPointsTable = 'total'; // Special case just for the UPDATE query logic
        }


        // Update the points table (total + specific column if applicable)
        this.stmts.addPoints.run({ guild_id: guildId, user_id: userId, category: targetCategoryForPointsTable, add: modAmount });
        
        // Log the event with the original category name
        this.stmts.logPoints.run(guildId, userId, category, modAmount, Math.floor(Date.now() / 1000), reason, notes); 
        
        if (modAmount > 0) {
            this.updateStreak(guildId, userId);
            return this.checkAchievements(guildId, userId);
        }
        return [];
    }

    updateStreak(guildId, userId) {
        // ... (remains the same) ...
        const user = this.stmts.getUser.get(guildId, userId); if (!user) return; const today = new Date().toISOString().slice(0,10); if (user.last_activity_date === today) return; const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0,10); const currentStreak = (user.last_activity_date === yesterday) ? (user.current_streak || 0) + 1 : 1; const longestStreak = Math.max(user.longest_streak || 0, currentStreak); this.stmts.updateStreak.run({ guild_id: guildId, user_id: userId, current_streak: currentStreak, longest_streak: longestStreak, last_activity_date: today });
    }

     checkCooldown({ guildId, userId, category }) {
         // ... (remains the same) ...
        let cooldownKey = category; if (['walking', 'jogging', 'running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'].includes(category)) cooldownKey = 'exercise'; if (!COOLDOWNS[cooldownKey]) { console.warn(`Cooldown undef: ${cooldownKey} (orig: ${category})`); return 0; } const row = this.stmts.getCooldown.get(guildId, userId, cooldownKey); const now = Date.now(); const cooldownMs = COOLDOWNS[cooldownKey]; if (row && now - row.last_ms < cooldownMs) return cooldownMs - (now - row.last_ms); return 0;
    }

    commitCooldown({ guildId, userId, category }) {
        // ... (remains the same) ...
         let cooldownKey = category; if (['walking', 'jogging', 'running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'].includes(category)) cooldownKey = 'exercise'; if (!COOLDOWNS[cooldownKey]) { console.warn(`Commit CD undef: ${cooldownKey} (orig: ${category})`); return; } this.stmts.setCooldown.run({ guild_id: guildId, user_id: userId, category: cooldownKey, last_ms: Date.now() });
    }

    checkAchievements(guildId, userId) {
        // ... (remains the same) ...
        const stats = this.stmts.getUser.get(guildId, userId); if (!stats) return []; const unlocked = this.stmts.getUserAchievements.all(guildId, userId).map(r => r.achievement_id); const fresh = []; for (const a of ACHIEVEMENTS) { if (!unlocked.includes(a.id) && a.requirement(stats)) { this.stmts.unlockAchievement.run(guildId, userId, a.id); fresh.push(a); } } return fresh;
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
const formatCooldown = (ms) => { /* ... (remains the same) ... */ 
    if (ms <= 0) return 'Ready!'; const totalSeconds = Math.floor(ms / 1000); const hours = Math.floor(totalSeconds / 3600); const minutes = Math.floor((totalSeconds % 3600) / 60); const seconds = totalSeconds % 60; let str = ''; if (hours > 0) str += `${hours}h `; if (minutes > 0) str += `${minutes}m `; if (hours === 0 && minutes === 0 && seconds > 0) str += `${seconds}s`; else if (hours === 0 && minutes === 0 && seconds <= 0) return 'Ready!'; return str.trim() || 'Ready!';
};
function getPeriodRange(period = 'week') { /* ... (remains the same) ... */ 
    const now = new Date(); let start = new Date(now); let end = new Date(now);
    switch (period) { case 'day': start.setHours(0, 0, 0, 0); end.setHours(23, 59, 59, 999); break; case 'month': start = new Date(now.getFullYear(), now.getMonth(), 1); end = new Date(now.getFullYear(), now.getMonth() + 1, 0); end.setHours(23, 59, 59, 999); break; case 'year': start = new Date(now.getFullYear(), 0, 1); end = new Date(now.getFullYear(), 11, 31); end.setHours(23, 59, 59, 999); break; case 'week': default: const dayOfWeek = now.getDay() || 7; const diffStart = now.getDate() - dayOfWeek + 1; start.setDate(diffStart); start.setHours(0, 0, 0, 0); const diffEnd = diffStart + 6; end.setDate(diffEnd); end.setHours(23, 59, 59, 999); break; }
    return { start: Math.floor(start.getTime() / 1000), end: Math.floor(end.getTime() / 1000) };
}
function getPeriodStart(period = 'day') { const now = new Date(); now.setHours(0, 0, 0, 0); return Math.floor(now.getTime() / 1000); }
function createKeepAliveServer() { http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('Bot is alive.'); }).listen(process.env.PORT || 3000, () => { console.log('✅ Keep-alive started.'); }); }

/* =========================
    COMMAND DEFINITIONS
========================= */
function buildCommands() {
    // Categories with fixed points directly from POINTS const (includes chores)
    const fixedPointCategories = Object.keys(POINTS); 
    
    // Categories specifically for admin choices (includes 'exercise' group)
    const adminCategoryChoices = [
        ...fixedPointCategories.map(c => ({name: c.charAt(0).toUpperCase() + c.slice(1), value: c})), 
        { name: 'Exercise', value: 'exercise' } // Represents the combined group
    ];
    
    // Categories available for leaderboard filtering
    const allLbCategories = [
        'all', 'streak', 'exercise', // 'exercise' represents the group
        ...fixedPointCategories // gym, badminton, ..., chores
    ];

    return [
        // Fitness activities & Chores with fixed points (direct commands)
        ...fixedPointCategories.map(name => new SlashCommandBuilder().setName(name).setDescription(`Log ${name} (+${POINTS[name]} pts)`)),
        
        // Exercise subcommands (includes yoga, distance, reps, plank, specific reps)
        new SlashCommandBuilder().setName('exercise').setDescription('💪 Log a detailed exercise session')
            .addSubcommand(sub => sub.setName('yoga').setDescription(`🧘 Log yoga session (+${POINTS.yoga} pts)`) // Yoga is under exercise but uses POINTS value
                .addNumberOption(o => o.setName('minutes').setDescription('Minutes').setRequired(true).setMinValue(1))) // Keep minutes for notes
            .addSubcommand(sub => sub.setName('reps').setDescription(`💪 Log generic reps (${EXERCISE_RATES.per_rep} pts/rep)`)
                .addNumberOption(o => o.setName('count').setDescription('Total reps').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('dumbbells').setDescription(`🏋️ Log dumbbells (${EXERCISE_RATES.per_rep} pts/rep)`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Sets').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('barbell').setDescription(`🏋️ Log barbell (${EXERCISE_RATES.per_rep} pts/rep)`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Sets').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('pushup').setDescription(`💪 Log pushups (${REP_RATES.pushup} pts/rep)`)
                .addNumberOption(o => o.setName('reps').setDescription('Reps per set').setRequired(true).setMinValue(1))
                .addNumberOption(o => o.setName('sets').setDescription('Sets').setRequired(true).setMinValue(1)))
             .addSubcommand(sub => sub.setName('plank').setDescription(`🧱 Log plank (${PLANK_RATE_PER_MIN} pt/min, min ${PLANK_MIN_MIN}m)`)
                .addNumberOption(o => o.setName('minutes').setDescription('Minutes (e.g., 1.5)').setMinValue(PLANK_MIN_MIN).setRequired(true)))
             .addSubcommand(sub => sub.setName('squat').setDescription(`🦵 Log squats (${REP_RATES.squat} pts/rep)`)
                .addIntegerOption(o => o.setName('reps').setDescription('Total Reps').setMinValue(1).setRequired(true)))
             .addSubcommand(sub => sub.setName('kettlebell').setDescription(`🏋️ Log kettlebell (${REP_RATES.kettlebell} pts/rep)`)
                .addIntegerOption(o => o.setName('reps').setDescription('Total Reps').setMinValue(1).setRequired(true)))
             .addSubcommand(sub => sub.setName('lunge').setDescription(`🦿 Log lunges (${REP_RATES.lunge} pts/rep)`)
                .addIntegerOption(o => o.setName('reps').setDescription('Total Reps').setMinValue(1).setRequired(true))),

        // Protein commands
        new SlashCommandBuilder().setName('protein').setDescription('🥩 Track protein intake')
            .addSubcommand(sub => sub.setName('add_item').setDescription('Add by item count')
                .addStringOption(o => o.setName('item').setDescription('Food').setRequired(true).addChoices(
                    ...Object.entries(PROTEIN_SOURCES).filter(([, val]) => val.unit === 'item').map(([key, val]) => ({ name: val.name, value: key }))
                )).addIntegerOption(o => o.setName('quantity').setDescription('Quantity').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('add_grams').setDescription('Add by weight')
                .addStringOption(o => o.setName('item').setDescription('Food').setRequired(true).addChoices(
                    ...Object.entries(PROTEIN_SOURCES).filter(([, val]) => val.unit === 'gram').map(([key, val]) => ({ name: val.name, value: key }))
                )).addNumberOption(o => o.setName('grams').setDescription('Grams').setRequired(true).setMinValue(1)))
            .addSubcommand(sub => sub.setName('log_direct').setDescription('Log exact amount')
                .addNumberOption(o => o.setName('grams').setDescription('Grams protein').setRequired(true).setMinValue(0.1))
                .addStringOption(o => o.setName('source').setDescription('Source (optional)').setRequired(false)))
            .addSubcommand(sub => sub.setName('total').setDescription("View today's protein")
                .addUserOption(o => o.setName('user').setDescription('View another user\'s (optional)'))),

        // Distance commands (Separate for clarity, use exercise cooldown)
        new SlashCommandBuilder().setName('walking').setDescription(`🚶 Log walking (${DISTANCE_RATES.walking} pts/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('jogging').setDescription(`🏃 Log jogging (${DISTANCE_RATES.jogging} pts/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers').setMinValue(0.1).setRequired(true)),
        new SlashCommandBuilder().setName('running').setDescription(`💨 Log running (${DISTANCE_RATES.running} pts/km)`).addNumberOption(o => o.setName('km').setDescription('Kilometers').setMinValue(0.1).setRequired(true)),
        
        // Utility commands
        new SlashCommandBuilder().setName('myscore').setDescription('🏆 Show your score and rank'),
        new SlashCommandBuilder().setName('leaderboard').setDescription('📊 Show the All-Time leaderboard')
            .addStringOption(o => o.setName('category').setDescription('Filter by category (default: all)')
                .addChoices(...allLbCategories.map(c => ({ name: c.charAt(0).toUpperCase() + c.slice(1), value: c })))
            ),
        new SlashCommandBuilder().setName('leaderboard_period').setDescription('📅 Show leaderboard for a specific period')
            .addStringOption(o => o.setName('period').setDescription('Time period').setRequired(true).addChoices(
                { name:'Today', value:'day' }, { name:'This Week', value:'week' },
                { name:'This Month', value:'month' }, { name:'This Year', value:'year' }
            ))
            .addStringOption(o => o.setName('category').setDescription('Filter by category (default: all)')
                .addChoices(...allLbCategories.map(c => ({ name: c.charAt(0).toUpperCase() + c.slice(1), value: c }))) 
            ),
        new SlashCommandBuilder().setName('junk').setDescription('🍕 Log junk food (deducts points)').addStringOption(o => o.setName('item').setDescription('Junk food item').setRequired(true).addChoices(...Object.entries(DEDUCTIONS).map(([key, { emoji, label }]) => ({ name: `${emoji} ${label}`, value: key })))),
        new SlashCommandBuilder().setName('buddy').setDescription('👯 Set or view your workout buddy').addUserOption(o => o.setName('user').setDescription('Your buddy (leave empty to view)')),
        new SlashCommandBuilder().setName('nudge').setDescription('👉 Nudge a user to work out').addUserOption(o => o.setName('user').setRequired(true).setDescription('User to nudge')).addStringOption(o => o.setName('activity').setRequired(true).setDescription('Activity reminder')),
        new SlashCommandBuilder().setName('remind').setDescription('⏰ Set a personal workout reminder').addStringOption(o => o.setName('activity').setRequired(true).setDescription('Reminder text')).addNumberOption(o => o.setName('hours').setRequired(true).setDescription('Hours from now').setMinValue(1)),
        new SlashCommandBuilder().setName('admin').setDescription('🛠️ Admin commands').setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
            .addSubcommand(sub => sub.setName('award').setDescription('Award points').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User')).addNumberOption(o=>o.setName('amount').setRequired(true).setDescription('Points')).addStringOption(o=>o.setName('category').setRequired(true).setDescription('Category').addChoices(...adminCategoryChoices)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(sub => sub.setName('deduct').setDescription('Deduct points').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User')).addNumberOption(o=>o.setName('amount').setRequired(true).setDescription('Points')).addStringOption(o=>o.setName('category').setRequired(true).setDescription('Category').addChoices(...adminCategoryChoices)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(sub => sub.setName('add_protein').setDescription('Manually add protein').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User')).addNumberOption(o=>o.setName('grams').setRequired(true).setDescription('Grams').setMinValue(0.1)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(sub => sub.setName('deduct_protein').setDescription('Manually deduct protein').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User')).addNumberOption(o=>o.setName('grams').setRequired(true).setDescription('Grams').setMinValue(0.1)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
    ].map(c => c.toJSON());
}

/* =========================
    COMMAND HANDLERS
========================= */
class CommandHandler {
    constructor(db) { this.db = db; }

    // handleClaim now handles all fixed point activities (gym, swim, chores)
    async handleClaim(interaction, category) {
        const { guild, user } = interaction;
        const amount = POINTS[category] || 0; 
        const cooldownKey = category; 
        
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for **${category}**: ${formatCooldown(remaining)}.`, flags: [MessageFlags.Ephemeral] });
        
        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category, amount, reason: `claim:${category}` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.', flags: [MessageFlags.Ephemeral] });
        
        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} claimed **+${formatNumber(amount)}** pts for **${category}**!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL());
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} pts to next rank!` });
        
        const payload = { embeds:[embed], ephemeral:false };
        if (achievements.length) { await interaction.editReply(payload); return interaction.followUp({ embeds: [new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n'))], flags: [MessageFlags.Ephemeral] }); }
        return interaction.editReply(payload);
    }
    
    // Handles distance activities
    async handleDistance(interaction, activity) {
        const { guild, user, options } = interaction;
        const km = options.getNumber('km', true); const amount = km * DISTANCE_RATES[activity]; const cooldownKey = 'exercise'; 
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for exercises: ${formatCooldown(remaining)}.`, flags: [MessageFlags.Ephemeral] });
        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: activity, amount, reason: `distance:${activity}`, notes: `${km}km` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey }); 
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.', flags: [MessageFlags.Ephemeral] });
        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} logged **${formatNumber(km)}km** ${activity} → **+${formatNumber(amount)}** pts!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL());
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} pts to next rank!` });
        const payload = { embeds:[embed], ephemeral: false };
        if (achievements.length) { await interaction.editReply(payload); return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')) ], flags: [MessageFlags.Ephemeral] }); }
        return interaction.editReply(payload);
    }

    // Handles ALL exercise subcommands
    async handleExercise(interaction) {
        const { guild, user, options } = interaction; const subcommand = options.getSubcommand(); let amount = 0; let description = ''; let cooldownCategory = 'exercise'; let logCategory = 'exercise'; let reasonPrefix = 'exercise'; let notes = '';
        if (subcommand === 'yoga') { cooldownCategory = 'yoga'; logCategory = 'yoga'; reasonPrefix = 'claim'; }
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for **${subcommand}**: ${formatCooldown(remaining)}.`, flags: [MessageFlags.Ephemeral] });
        switch (subcommand) {
             case 'yoga': { const m = options.getNumber('minutes', true); amount = POINTS.yoga || 0; description = `${user.toString()} claimed **+${formatNumber(amount)}** pts for **Yoga**!`; notes = `${m} min`; break; }
             case 'plank': { const m = options.getNumber('minutes', true); amount = m * PLANK_RATE_PER_MIN; description = `${user.toString()} planked **${formatNumber(m)} min** → **+${formatNumber(amount)}** pts!`; notes = `${m} min`; reasonPrefix = 'time'; break; }
            case 'reps': { const c = options.getNumber('count', true); amount = c * EXERCISE_RATES.per_rep; description = `${user.toString()} logged **${c} reps** → **+${formatNumber(amount)}** pts!`; notes = `${c} reps`; reasonPrefix = 'reps'; break; }
            case 'dumbbells': case 'barbell': case 'pushup': case 'squat': case 'kettlebell': case 'lunge': { const r = options.getNumber('reps', true); const s = options.getNumber('sets', true); const t = r*s; const rate = REP_RATES[subcommand] ?? EXERCISE_RATES.per_rep; amount = t * rate; description = `${user.toString()} logged ${s}x${r} (${t}) **${subcommand}** → **+${formatNumber(amount)}** pts!`; notes = `${s}x${r} reps`; reasonPrefix = 'reps'; break; }
        }
        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: logCategory, amount, reason: `${reasonPrefix}:${subcommand}`, notes });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.', flags: [MessageFlags.Ephemeral] });
        const { cur, need } = nextRankProgress(userRow.total);
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(description).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL());
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} pts to next rank!` });
        const payload = { embeds:[embed], ephemeral:false };
        if (achievements.length) { await interaction.editReply(payload); return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')) ], flags: [MessageFlags.Ephemeral] }); }
        return interaction.editReply(payload);
    }

    async handleProtein(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const subcommand = options.getSubcommand(); const targetUser = options.getUser('user') || user;
        if (subcommand === 'total') { const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, targetUser.id, since); const totalProtein = result?.total || 0; const embed = new EmbedBuilder().setColor(0x5865F2).setTitle(`🥩 Daily Protein for ${targetUser.displayName}`).setDescription(`Logged **${formatNumber(totalProtein)}g** protein today.`).setThumbnail(targetUser.displayAvatarURL()); return interaction.editReply({ embeds: [embed] }); }
        let proteinGrams = 0; let itemName = '';
        if (subcommand === 'add_item') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const q = options.getInteger('quantity', true); proteinGrams = s.protein_per_unit * q; itemName = `${q} ${s.name}`; } 
        else if (subcommand === 'add_grams') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const g = options.getNumber('grams', true); proteinGrams = s.protein_per_unit * g; itemName = `${g}g of ${s.name}`; } 
        else if (subcommand === 'log_direct') { const g = options.getNumber('grams', true); const n = options.getString('source') || 'direct source'; proteinGrams = g; itemName = n; }
        this.db.stmts.addProteinLog.run(guild.id, user.id, itemName, proteinGrams, Math.floor(Date.now() / 1000));
        const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, user.id, since); const totalProtein = result?.total || 0;
        const embed = new EmbedBuilder().setColor(0x2ECC71).setTitle('✅ Protein Logged!').setDescription(`${user.toString()} added **${formatNumber(proteinGrams)}g** protein from **${itemName}**.`).addFields({ name: 'Daily Total', value: `Today: **${formatNumber(totalProtein)}g** protein.` }).setThumbnail(user.displayAvatarURL());
        return interaction.editReply({ embeds: [embed], ephemeral: false }); 
    }

    async handleJunk(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const item = options.getString('item', true); const deduction = DEDUCTIONS[item]; const msgs = ["Balance is key!", "One step back, two forward!", "Honesty is progress!", "Treats happen!", "Acknowledge and move on!"]; const msg = msgs[Math.floor(Math.random() * msgs.length)];
        this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'junk', amount: -deduction.points, reason: `junk:${item}` }); const userRow = this.db.stmts.getUser.get(guild.id, user.id); const total = userRow ? userRow.total : 0;
        const embed = new EmbedBuilder().setColor(0xED4245).setDescription(`${user.toString()} logged ${deduction.emoji} **${deduction.label}** (-**${formatNumber(deduction.points)}** pts).`).addFields({ name: "Total", value: `🏆 ${formatNumber(total)}` }).setFooter({ text: msg });
        return interaction.editReply({ embeds: [embed], ephemeral: false }); 
    }

    async handleMyScore(interaction) { /* ... (remains the same) ... */ 
        const { guild, user } = interaction; const userRow = this.db.stmts.getUser.get(guild.id, user.id) || { total: 0, current_streak: 0 }; const { pct, cur, need } = nextRankProgress(userRow.total); const ach = this.db.stmts.getUserAchievements.all(guild.id, user.id).map(r => r.achievement_id);
        const embed = new EmbedBuilder().setColor(cur.color).setAuthor({ name: user.displayName, iconURL: user.displayAvatarURL() }).setTitle(`Rank: ${cur.name}`).addFields({ name: 'Points', value: formatNumber(userRow.total), inline: true }, { name: 'Streak', value: `🔥 ${userRow.current_streak || 0}d`, inline: true }, { name: 'Progress', value: progressBar(pct), inline: false }, { name: 'Achievements', value: ach.length > 0 ? ach.map(id => `**${ACHIEVEMENTS.find(a => a.id === id)?.name || id}**`).join(', ') : 'None' });
        if (need > 0) embed.setFooter({ text: `${formatNumber(need)} pts to next rank!` });
        return interaction.editReply({ embeds: [embed] }); 
    }

    async handleLeaderboard(interaction) { /* ... (uses points table for consistency) ... */ 
        const { guild, user, options } = interaction; const cat = options.getString('category') || 'all'; 
        try {
            let rows = []; let subtitle = ''; let selfRank = null; const exerciseCategories = ['exercise','walking','jogging','running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'];
            subtitle = `All Time • ${cat === 'all' ? 'Total Points' : cat === 'streak' ? 'Current Streak' : cat.charAt(0).toUpperCase() + cat.slice(1)}`;
            if (cat === 'streak') { rows = this.db.stmts.topStreaks.all(guild.id); } 
            else if (cat === 'all') { rows = this.db.stmts.lbAllFromPoints.all(guild.id); const my = this.db.stmts.selfRankAllFromPoints.get(guild.id, user.id); if (my) selfRank = { userId: user.id, rank: my.rank, score: my.score }; } 
            else { const catQueryKey = `lbAllCatFromPoints_${cat}`; if (this.db.stmts[catQueryKey]) { rows = this.db.stmts[catQueryKey].all(guild.id); } 
            else { const query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND amount>0 AND category = ? GROUP BY user_id HAVING score>0 ORDER BY score DESC LIMIT 10`; rows = this.db.db.prepare(query).all(guild.id, cat); subtitle += " (from log)"; } }
            if (!rows.length) return interaction.editReply({ content: '📊 No data.' });
            rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
            const userIds = rows.map(r => r.userId); const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || 'Unknown'; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\`${cat === 'streak' ? ' days' : ''}`; });
            const embed = new EmbedBuilder().setTitle(`🏆 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp();
            if (cat === 'all' && selfRank && !rows.some(r => r.userId === user.id)) embed.setFooter({ text: `Your Rank: #${selfRank.rank} (${formatNumber(selfRank.score)} pts)` });
            return interaction.editReply({ embeds: [embed] }); 
        } catch (e) { console.error('LB Error:', e); return interaction.editReply({ content: '❌ Error generating leaderboard.' }); }
    }

    async handleLeaderboardPeriod(interaction) { /* ... (uses log table) ... */ 
        const { guild, user, options } = interaction; const period = options.getString('period', true); const cat = options.getString('category') || 'all';
        try {
            let rows = []; const exerciseCategories = ['exercise','walking','jogging','running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'];
            const { start, end } = getPeriodRange(period); const periodName = { day: 'Today', week: 'This Week', month:'This Month', year:'This Year' }[period]; const startStr = `<t:${start}:d>`; const endStr = `<t:${end}:d>`;
            let subtitle = `${periodName} (${startStr}-${endStr}) • ${cat === 'all' ? 'Total' : cat.charAt(0).toUpperCase() + cat.slice(1)}`;
            if (cat === 'streak') return interaction.editReply({ content: '📊 Streak LB only All-Time.' }); 
            else {
                 let queryCategory = cat; if (cat === 'exercise') queryCategory = exerciseCategories;
                 const placeholders = Array.isArray(queryCategory) ? queryCategory.map(() => '?').join(',') : '?'; let query = ''; let params = [];
                 if (cat === 'all') { query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`; params = [guild.id, start, end]; } // Show net points for period
                 else { query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 AND category IN (${placeholders}) GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`; params = Array.isArray(queryCategory) ? [guild.id, start, end, ...queryCategory] : [guild.id, start, end, queryCategory]; } // Show net points for category
                 rows = this.db.db.prepare(query).all(...params);
            }
            if (!rows.length) return interaction.editReply({ content: `📊 No data for ${periodName}.` });
            rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
            const userIds = rows.map(r => r.userId); const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || 'Unknown'; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\``; });
            const embed = new EmbedBuilder().setTitle(`📅 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp();
            return interaction.editReply({ embeds: [embed] }); 
        } catch (e) { console.error('Period LB Error:', e); return interaction.editReply({ content: '❌ Error generating periodic leaderboard.' }); }
    }

    async handleBuddy(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const targetUser = options.getUser('user'); if (!targetUser) { const b = this.db.stmts.getBuddy.get(guild.id, user.id); return interaction.editReply({ content: b?.buddy_id ? `Buddy: <@${b.buddy_id}>` : 'No buddy set!' }); } if (targetUser.id === user.id) return interaction.editReply({ content: 'Cannot be own buddy!', flags: [MessageFlags.Ephemeral] }); this.db.stmts.setBuddy.run(guild.id, user.id, targetUser.id); return interaction.editReply({ content: `✨ ${user.toString()} set <@${targetUser.id}> as buddy!` }); 
    }
    async handleNudge(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const targetUser = options.getUser('user', true); const activity = options.getString('activity', true); if (targetUser.bot || targetUser.id === user.id) return interaction.editReply({ content: "Cannot nudge bots/self." }); const isAdmin = interaction.member.permissions.has(PermissionFlagsBits.ManageGuild); const buddy = this.db.stmts.getBuddy.get(guild.id, user.id); const isBuddy = buddy?.buddy_id === targetUser.id; if (!isAdmin && !isBuddy) return interaction.editReply({ content: "Can only nudge buddy (or ask admin)." }); try { await targetUser.send(`⏰ <@${user.id}> from **${guild.name}** nudges: **${activity}**!`); return interaction.editReply({ content: `✅ Nudge sent to <@${targetUser.id}>.` }); } catch (err) { return interaction.editReply({ content: `❌ Could not DM user.` }); }
    }
    async handleRemind(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const activity = options.getString('activity', true); const hours = options.getNumber('hours', true); const dueAt = Date.now() + hours * 3600000; this.db.stmts.addReminder.run(guild.id, user.id, activity, dueAt); return interaction.editReply({ content: `⏰ Reminder set for **${activity}** in ${hours}h.` }); 
    }
    async handleAdmin(interaction) { /* ... (remains the same) ... */ 
        const { guild, user, options } = interaction; const sub = options.getSubcommand(); const targetUser = options.getUser('user', true);
        if (sub === 'award' || sub === 'deduct') { const amt = options.getNumber('amount', true); const cat = options.getString('category', true); const rsn = options.getString('reason') || `Admin action`; const finalAmt = sub === 'award' ? amt : -amt; const logCat = sub === 'deduct' ? 'junk' : cat; this.db.modifyPoints({ guildId: guild.id, userId: targetUser.id, category: logCat, amount: finalAmt, reason: `admin:${sub}`, notes: rsn }); const act = sub === 'award' ? 'Awarded' : 'Deducted'; return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(amt))} ${cat} points for <@${targetUser.id}>.` }); }
        if (sub === 'add_protein' || sub === 'deduct_protein') { let g = options.getNumber('grams', true); const rsn = options.getString('reason') || `Admin action`; if (sub === 'deduct_protein') g = -g; this.db.stmts.addProteinLog.run(guild.id, targetUser.id, `Admin: ${rsn}`, g, Math.floor(Date.now() / 1000)); const act = sub === 'add_protein' ? 'Added' : 'Deducted'; return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(g))}g protein for <@${targetUser.id}>.` }); }
    }
} // End CommandHandler

/* =========================
    MAIN BOT INITIALIZATION
========================= */
async function main() {
    createKeepAliveServer();
    if (!CONFIG.token || !CONFIG.appId) { /* ... error handling ... */ console.error('Missing env vars'); process.exit(1); }

    const database = new PointsDatabase(CONFIG.dbFile);
    const handler = new CommandHandler(database);

    const rest = new REST({ version: '10' }).setToken(CONFIG.token);
    try { /* ... command registration ... */ console.log('Registering...'); const route = CONFIG.devGuildId ? Routes.applicationGuildCommands(CONFIG.appId, CONFIG.devGuildId) : Routes.applicationCommands(CONFIG.appId); await rest.put(route, { body: buildCommands() }); console.log('Registered.'); }
    catch (err) { console.error('Cmd reg failed:', err); process.exit(1); }

    const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers] });

    client.once('clientReady', (c) => { /* ... ready + reminder interval ... */ 
        console.log(`Logged in: ${c.user.tag}`); console.log(`Servers: ${c.guilds.cache.size}`);
        setInterval(async () => { /* ... reminder logic ... */ try { const now = Date.now(); const due = database.stmts.getDueReminders.all(now); for (const r of due) { try { const u = await client.users.fetch(r.user_id); await u.send(`⏰ Reminder: **${r.activity}**!`); } catch (e) { console.error(`DM fail ${r.user_id}: ${e.message}`); } finally { database.stmts.deleteReminder.run(r.id); } } } catch (e) { console.error("Reminder Error:", e); } }, 60000);
    });

    client.on('interactionCreate', async (interaction) => {
        if (!interaction.isChatInputCommand() || !interaction.guild) return;
        
        try {
            const ephemeralCommands = ['buddy', 'nudge', 'remind', 'admin', 'myscore'];
            let shouldBeEphemeral = ephemeralCommands.includes(interaction.commandName);
            if (interaction.commandName === 'buddy' && !interaction.options.getUser('user')) shouldBeEphemeral = true;
            if (interaction.commandName === 'protein' && interaction.options.getSubcommand() === 'total') shouldBeEphemeral = true;
            if (interaction.commandName.startsWith('leaderboard')) shouldBeEphemeral = false; 

            await interaction.deferReply({ ephemeral: shouldBeEphemeral });

            const { commandName } = interaction;
            const fixedPointCategories = Object.keys(POINTS);

            // Handle fixed point commands (gym, swim, chores...)
            if (fixedPointCategories.includes(commandName)) {
                 await handler.handleClaim(interaction, commandName);
            }
            // Handle exercise command with subcommands
            else if (commandName === 'exercise') {
                 await handler.handleExercise(interaction);
            }
             // Handle distance commands
             else if (['walking', 'jogging', 'running'].includes(commandName)) {
                 await handler.handleDistance(interaction, commandName); 
             }
            // Handle protein commands
            else if (commandName === 'protein') {
                 await handler.handleProtein(interaction);
            }
            // Handle other commands
            else {
                switch (commandName) {
                    case 'junk': await handler.handleJunk(interaction); break;
                    case 'myscore': await handler.handleMyScore(interaction); break;
                    case 'leaderboard': await handler.handleLeaderboard(interaction); break;
                    case 'leaderboard_period': await handler.handleLeaderboardPeriod(interaction); break; 
                    case 'buddy': await handler.handleBuddy(interaction); break;
                    case 'nudge': await handler.handleNudge(interaction); break;
                    case 'remind': await handler.handleRemind(interaction); break;
                    case 'admin': await handler.handleAdmin(interaction); break;
                    default:
                         console.warn(`Unhandled: ${commandName}`);
                         await interaction.editReply({ content: "Unknown command.", flags: [MessageFlags.Ephemeral] });
                }
            }
        } catch (err) {
            console.error(`Cmd Error ${interaction.commandName}:`, err);
            const errorReply = { content: `❌ Error processing command.`, flags: [MessageFlags.Ephemeral] };
            if (interaction.deferred || interaction.replied) await interaction.editReply(errorReply).catch(console.error);
            else await interaction.reply(errorReply).catch(console.error);
        }
    });

    process.on('SIGINT', () => { /* ... shutdown ... */ console.log('SIGINT...'); database.close(); client.destroy(); process.exit(0); });
    process.on('SIGTERM', () => { /* ... shutdown ... */ console.log('SIGTERM...'); database.close(); client.destroy(); process.exit(0); });

    await client.login(CONFIG.token);
}

main().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});