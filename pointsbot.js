// pointsbot.js - Final Version (with User's modifyPoints Patch & Clear Data)
import 'dotenv/config';
import http from 'node:http';
import crypto from 'node:crypto'; // Added for event_key hash
import {
    Client, GatewayIntentBits, REST, Routes,
    SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits, MessageFlags
} from 'discord.js';
import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BOT_PROCESS_ID = process.pid;

// --- NEW: Readiness Flag ---
let isBotReady = false;
// -------------------------

/* =========================
    CONFIG & CONSTANTS
========================= */
const CONFIG = {
    appId: (process.env.APPLICATION_ID || '').trim(),
    token: (process.env.DISCORD_TOKEN || '').trim(),
    devGuildId: (process.env.DEV_GUILD_ID || '').trim(),
    dbFile: (process.env.DB_PATH || path.join(__dirname, 'data', 'points.db')).trim(),
};

// --- (Constants like PROTEIN_SOURCES, COOLDOWNS, POINTS, etc. remain unchanged) ---
const PROTEIN_SOURCES = {
    chicken_breast: { name: 'Chicken Breast (Cooked)', unit: 'gram', protein_per_unit: 0.31 },
    chicken_thigh:  { name: 'Chicken Thigh (Cooked)', unit: 'gram', protein_per_unit: 0.26 },
    ground_beef:    { name: 'Ground Beef 85/15 (Cooked)', unit: 'gram', protein_per_unit: 0.26 },
    steak:          { name: 'Steak (Sirloin, Cooked)', unit: 'gram', protein_per_unit: 0.29 },
    pork_chop:      { name: 'Pork Chop (Cooked)', unit: 'gram', protein_per_unit: 0.27 },
    mutton:         { name: 'Mutton (Cooked)', unit: 'gram', protein_per_unit: 0.27 },
    salmon: { name: 'Salmon (Cooked)', unit: 'gram', protein_per_unit: 0.25 },
    tuna:   { name: 'Tuna (Canned in water)', unit: 'gram', protein_per_unit: 0.23 },
    shrimp: { name: 'Shrimp (Cooked)', unit: 'gram', protein_per_unit: 0.24 },
    cod:    { name: 'Cod (Cooked)', unit: 'gram', protein_per_unit: 0.26 },
    egg:            { name: 'Large Egg', unit: 'item', protein_per_unit: 6 },
    egg_white:      { name: 'Large Egg White', unit: 'item', protein_per_unit: 3.6 },
    greek_yogurt:   { name: 'Greek Yogurt', unit: 'gram', protein_per_unit: 0.10 },
    cottage_cheese: { name: 'Cottage Cheese', unit: 'gram', protein_per_unit: 0.11 },
    milk:           { name: 'Milk (Dairy)', unit: 'gram', protein_per_unit: 0.034 },
    tofu:       { name: 'Tofu (Firm)', unit: 'gram', protein_per_unit: 0.08 },
    edamame:    { name: 'Edamame (Shelled)', unit: 'gram', protein_per_unit: 0.11 },
    lentils:    { name: 'Lentils (Cooked)', unit: 'gram', protein_per_unit: 0.09 },
    dahl:       { name: 'Dahl (Cooked Lentils)', unit: 'gram', protein_per_unit: 0.09 },
    chickpeas:  { name: 'Chickpeas (Cooked)', unit: 'gram', protein_per_unit: 0.09 },
    black_beans: { name: 'Black Beans (Cooked)', unit: 'gram', protein_per_unit: 0.08 },
    quinoa:     { name: 'Quinoa (Cooked)', unit: 'gram', protein_per_unit: 0.04 },
    almonds:    { name: 'Almonds', unit: 'gram', protein_per_unit: 0.21 },
    peanuts:    { name: 'Peanuts', unit: 'gram', protein_per_unit: 0.26 },
    protein_powder: { name: 'Protein Powder', unit: 'gram', protein_per_unit: 0.80 }
};

const COOLDOWNS = {
    gym: 12 * 60 * 60 * 1000,
    badminton: 12 * 60 * 60 * 1000,
    cricket: 12 * 60 * 60 * 1000,
    swimming: 12 * 60 * 60 * 1000,
    yoga: 12 * 60 * 60 * 1000,
    exercise: 30 * 60 * 1000,
    cooking: 60 * 60 * 1000,
    sweeping: 60 * 60 * 1000,
    gardening: 60 * 60 * 1000,
    carwash: 60 * 60 * 1000,
    toiletcleaning: 60 * 60 * 1000,
    dishwashing: 60 * 60 * 1000,
};

const POINTS = {
    gym: 2,
    badminton: 5,
    cricket: 5,
    swimming: 3,
    yoga: 2,
    cooking: 2,
    sweeping: 2,
    gardening: 2,
    carwash: 2,
    toiletcleaning: 5,
    dishwashing: 2,
};

const EXERCISE_RATES = { per_rep: 0.002 };
const DISTANCE_RATES = { walking: 0.5, jogging: 0.6, running: 0.7 };
const REP_RATES = { squat: 0.02, kettlebell: 0.2, lunge: 0.2, pushup: 0.02 };
const PLANK_RATE_PER_MIN = 1;
const PLANK_MIN_MIN = 0.75;

// 25 items
const DEDUCTIONS = {
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

const EXERCISE_CATEGORIES = ['exercise', 'walking', 'jogging', 'running', 'plank', 'squat', 'kettlebell', 'lunge', 'pushup'];
const CHORE_CATEGORIES = ['cooking','sweeping','toiletcleaning','gardening','carwash','dishwashing'];
// All columns in the 'points' table that hold point values
const ALL_POINT_COLUMNS = ['gym', 'badminton', 'cricket', 'exercise', 'swimming', 'yoga', ...CHORE_CATEGORIES];

/* =========================
    DATABASE CLASS
========================= */
class PointsDatabase {
    constructor(dbPath) {
        try { fs.mkdirSync(path.dirname(dbPath), { recursive: true }); } catch (err) { if (err.code !== 'EEXIST') console.error('DB dir error:', err); }
        this.db = new Database(dbPath);
        this.db.pragma('journal_mode = WAL');
        this.db.pragma('foreign_keys = ON');

        // **PATCH: Auto-migrate chore columns**
        try {
            console.log("🔄 [DB] Migrating database schema (if needed)...");
            const cols = CHORE_CATEGORIES;
            for (const c of cols) {
                try { this.db.exec(`ALTER TABLE points ADD COLUMN ${c} REAL NOT NULL DEFAULT 0;`); }
                catch (e) { if (!e.message.includes("duplicate column")) console.error(`[DB Migration Error] Alter points for ${c}:`, e); }
            }
            // **PATCH: Add event_key to points_log for idempotency**
            try {
                // Step 1: Add the column, allowing NULLs (which is what failed before)
                this.db.exec(`ALTER TABLE points_log ADD COLUMN event_key TEXT;`);
                console.log("[DB Migration] Added event_key column (or ignored if exists).");
            }
            catch (e) {
                // Ignore "duplicate column" error if it already exists
                if (!e.message.includes("duplicate column")) {
                    console.error("[DB Migration Error] Alter points_log adding event_key column:", e);
                } else {
                     console.log("[DB Migration] event_key column already exists.");
                }
            }

            try {
                // Step 2: Create a UNIQUE index on the column.
                this.db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pointslog_eventkey ON points_log (event_key) WHERE event_key IS NOT NULL;`);
                 console.log("[DB Migration] Ensured unique index on event_key exists.");
            } catch(e) {
                console.error("[DB Migration Error] Creating event_key index:", e);
            }
            // **PATCH: Add indexes**
            try { this.db.exec(`CREATE INDEX IF NOT EXISTS idx_pointslog_user ON points_log (guild_id, user_id);`); } catch(e) { console.error("[DB Migration Error] Creating user index:", e);}
            try { this.db.exec(`CREATE INDEX IF NOT EXISTS idx_pointslog_category ON points_log (category);`); } catch(e) { console.error("[DB Migration Error] Creating category index:", e);}

            console.log("✅ [DB] Schema migration checks complete.");
        } catch(e) { console.error("❌ [DB] Outer migration block error:", e); }

        this.initSchema();
        this.prepareStatements();
        console.log("✅ [DB] Database class initialized.");
    }

    // --- (initSchema remains unchanged) ---
    initSchema() {
        this.db.exec(`
          CREATE TABLE IF NOT EXISTS points (
            guild_id TEXT NOT NULL, user_id TEXT NOT NULL, total REAL NOT NULL DEFAULT 0,
            gym REAL NOT NULL DEFAULT 0, badminton REAL NOT NULL DEFAULT 0, cricket REAL NOT NULL DEFAULT 0,
            exercise REAL NOT NULL DEFAULT 0, swimming REAL NOT NULL DEFAULT 0, yoga REAL NOT NULL DEFAULT 0,
            cooking REAL NOT NULL DEFAULT 0, sweeping REAL NOT NULL DEFAULT 0, toiletcleaning REAL NOT NULL DEFAULT 0,
            gardening REAL NOT NULL DEFAULT 0, carwash REAL NOT NULL DEFAULT 0, dishwashing REAL NOT NULL DEFAULT 0,
            current_streak INTEGER DEFAULT 0, longest_streak INTEGER DEFAULT 0, last_activity_date TEXT,
            created_at INTEGER DEFAULT (strftime('%s', 'now')), updated_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (guild_id, user_id)
          );
          CREATE TABLE IF NOT EXISTS cooldowns ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, category TEXT NOT NULL, last_ms INTEGER NOT NULL, PRIMARY KEY (guild_id, user_id, category) );

          -- **PATCH: Updated points_log schema**
          CREATE TABLE IF NOT EXISTS points_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id TEXT NOT NULL, user_id TEXT NOT NULL, category TEXT NOT NULL,
            amount REAL NOT NULL, ts INTEGER NOT NULL, reason TEXT, notes TEXT,
            event_key TEXT -- Prevents duplicate entries
            -- Removed FOREIGN KEY constraint as it can cause issues with clearing users
          );

          CREATE TABLE IF NOT EXISTS buddies ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, buddy_id TEXT, created_at INTEGER DEFAULT (strftime('%s', 'now')), PRIMARY KEY (guild_id, user_id) );
          CREATE TABLE IF NOT EXISTS achievements ( guild_id TEXT NOT NULL, user_id TEXT NOT NULL, achievement_id TEXT NOT NULL, unlocked_at INTEGER DEFAULT (strftime('%s', 'now')), PRIMARY KEY (guild_id, user_id, achievement_id) );
          CREATE TABLE IF NOT EXISTS reminders ( id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT, user_id TEXT, activity TEXT, due_at INTEGER );
          CREATE INDEX IF NOT EXISTS idx_points_log_guild_ts ON points_log(guild_id, ts);
          CREATE INDEX IF NOT EXISTS idx_points_total ON points(guild_id, total DESC);
          CREATE UNIQUE INDEX IF NOT EXISTS idx_pointslog_eventkey ON points_log (event_key) WHERE event_key IS NOT NULL;
          CREATE TABLE IF NOT EXISTS protein_log ( id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id TEXT NOT NULL, user_id TEXT NOT NULL, item_name TEXT NOT NULL, protein_grams REAL NOT NULL, timestamp INTEGER NOT NULL );
        `);
        console.log("[DB] initSchema executed.");
    }
    // --- (prepareStatements remains unchanged) ---
    prepareStatements() {
        const S = this.stmts = {};
        S.upsertUser = this.db.prepare(`INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id) ON CONFLICT(guild_id, user_id) DO NOTHING`);
        S.getUser = this.db.prepare(`SELECT * FROM points WHERE guild_id = ? AND user_id = ?`);
        S.updateStreak = this.db.prepare(`UPDATE points SET current_streak = @current_streak, longest_streak = @longest_streak, last_activity_date = @last_activity_date WHERE guild_id = @guild_id AND user_id = @user_id`);
        S.setCooldown = this.db.prepare(`INSERT INTO cooldowns (guild_id, user_id, category, last_ms) VALUES (@guild_id, @user_id, @category, @last_ms) ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms = excluded.last_ms`);
        S.getCooldown = this.db.prepare(`SELECT last_ms FROM cooldowns WHERE guild_id = ? AND user_id = ? AND category = ?`);

        // **PATCH: Updated logPoints statement for event_key**
        S.logPoints = this.db.prepare(`
          INSERT INTO points_log (guild_id, user_id, category, amount, ts, reason, notes, event_key)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(event_key) WHERE event_key IS NOT NULL DO NOTHING
        `);

        S.lbAllFromPoints = this.db.prepare(`SELECT user_id as userId, total as score FROM points WHERE guild_id=? AND total > 0 ORDER BY total DESC LIMIT 10`);
        S.lbAllCatFromPoints_gym = this.db.prepare(`SELECT user_id as userId, gym as score FROM points WHERE guild_id=? AND gym > 0 ORDER BY gym DESC LIMIT 10`);
        S.lbAllCatFromPoints_badminton = this.db.prepare(`SELECT user_id as userId, badminton as score FROM points WHERE guild_id=? AND badminton > 0 ORDER BY badminton DESC LIMIT 10`);
        S.lbAllCatFromPoints_cricket = this.db.prepare(`SELECT user_id as userId, cricket as score FROM points WHERE guild_id=? AND cricket > 0 ORDER BY cricket DESC LIMIT 10`);
        S.lbAllCatFromPoints_exercise = this.db.prepare(`SELECT user_id as userId, exercise as score FROM points WHERE guild_id=? AND exercise > 0 ORDER BY exercise DESC LIMIT 10`);
        S.lbAllCatFromPoints_swimming = this.db.prepare(`SELECT user_id as userId, swimming as score FROM points WHERE guild_id=? AND swimming > 0 ORDER BY swimming DESC LIMIT 10`);
        S.lbAllCatFromPoints_yoga = this.db.prepare(`SELECT user_id as userId, yoga as score FROM points WHERE guild_id=? AND yoga > 0 ORDER BY yoga DESC LIMIT 10`);
        CHORE_CATEGORIES.forEach(chore => {
            S[`lbAllCatFromPoints_${chore}`] = this.db.prepare(`SELECT user_id as userId, ${chore} as score FROM points WHERE guild_id=? AND ${chore} > 0 ORDER BY ${chore} DESC LIMIT 10`);
        });
        S.selfRankAllFromPoints = this.db.prepare(`WITH ranks AS ( SELECT user_id, total, RANK() OVER (ORDER BY total DESC) rk FROM points WHERE guild_id=? AND total > 0 ) SELECT rk as rank, total as score FROM ranks WHERE user_id=?`);
        S.lbSince = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`);
        S.lbSinceByCat = this.db.prepare(`SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 AND category IN (?) GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`);
        S.getTopStreaks = this.db.prepare(`SELECT user_id as userId, current_streak as score FROM points WHERE guild_id = ? AND current_streak > 0 ORDER BY current_streak DESC LIMIT 10`);
        S.getBuddy = this.db.prepare(`SELECT buddy_id FROM buddies WHERE guild_id = ? AND user_id = ?`);
        S.setBuddy = this.db.prepare(`INSERT INTO buddies (guild_id, user_id, buddy_id) VALUES (?, ?, ?) ON CONFLICT(guild_id, user_id) DO UPDATE SET buddy_id = excluded.buddy_id`);
        S.unlockAchievement = this.db.prepare(`INSERT OR IGNORE INTO achievements (guild_id, user_id, achievement_id) VALUES (?, ?, ?)`),
        S.getUserAchievements = this.db.prepare(`SELECT achievement_id FROM achievements WHERE guild_id = ? AND user_id = ?`);
        S.addReminder = this.db.prepare(`INSERT INTO reminders (guild_id, user_id, activity, due_at) VALUES (?, ?, ?, ?)`);
        S.getDueReminders = this.db.prepare(`SELECT id, guild_id, user_id, activity FROM reminders WHERE due_at <= ?`);
        S.deleteReminder = this.db.prepare(`DELETE FROM reminders WHERE id = ?`);
        S.addProteinLog = this.db.prepare(`INSERT INTO protein_log (guild_id, user_id, item_name, protein_grams, timestamp) VALUES (?, ?, ?, ?, ?)`);
        S.getDailyProtein = this.db.prepare(`SELECT SUM(protein_grams) AS total FROM protein_log WHERE guild_id = ? AND user_id = ? AND timestamp >= ?`);

        S.clearUserPoints = this.db.prepare(`DELETE FROM points WHERE guild_id = ? AND user_id = ?`);
        S.clearUserLog = this.db.prepare(`DELETE FROM points_log WHERE guild_id = ? AND user_id = ?`);
        S.clearUserAchievements = this.db.prepare(`DELETE FROM achievements WHERE guild_id = ? AND user_id = ?`);
        S.clearUserCooldowns = this.db.prepare(`DELETE FROM cooldowns WHERE guild_id = ? AND user_id = ?`);
        S.clearUserProtein = this.db.prepare(`DELETE FROM protein_log WHERE guild_id = ? AND user_id = ?`);
        S.clearUserBuddy = this.db.prepare(`DELETE FROM buddies WHERE guild_id = ? AND user_id = ?`); // Also clear buddy

        this.stmts = S;
         console.log("[DB] Statements prepared.");
    }

    // --- (modifyPoints remains unchanged) ---
    modifyPoints({ guildId, userId, category, amount, reason = null, notes = null }) {
      this.stmts.upsertUser.run({ guild_id: guildId, user_id: userId });
      const modAmount = Number(amount) || 0;
     console.log(`[modifyPoints] Guild: ${guildId}, User: ${userId}, Cat: ${category}, Amount: ${amount}, ModAmount: ${modAmount}, Reason: ${reason}`);
      if (modAmount === 0) return [];

      // 🔹 Choose correct column or fallback
      const safeCols = [
        'gym', 'badminton', 'cricket', 'exercise', 'swimming', 'yoga',
        'cooking', 'sweeping', 'toiletcleaning', 'gardening', 'carwash', 'dishwashing'
      ];

      let logCategory = category; // The category for the log
      let targetCol = category; // The column in the 'points' table

      if (EXERCISE_CATEGORIES.includes(category)) {
          targetCol = 'exercise';
      } else if (category === 'junk') {
          // Junk deducts from the highest score category
          const userPoints = this.stmts.getUser.get(guildId, userId) || {};
          targetCol = ['exercise', 'gym', 'badminton', 'cricket', 'swimming', 'yoga'].sort((a, b) => (userPoints[b] || 0) - (userPoints[a] || 0))[0] || 'exercise';
      } else if (!safeCols.includes(category)) {
          // This should not happen if admin commands use choices, but as a safeguard:
          console.warn(`Unknown category ${category} in modifyPoints, applying to 'total' only via log`);
          targetCol = null; // Will not update a specific column
      }

      // 🔹 Apply increment directly to that column if it's a safe one
      if (targetCol && safeCols.includes(targetCol)) {
          const stmt = this.db.prepare(`
            UPDATE points
            SET ${targetCol} = ${targetCol} + @amt,
                updated_at = strftime('%s','now')
            WHERE guild_id = @gid AND user_id = @uid
          `);
          stmt.run({ amt: modAmount, gid: guildId, uid: userId });
      }

      // 🔹 Recalculate total to ensure exact sum across all columns
      const recalc = this.db.prepare(`
        UPDATE points
        SET total = gym + badminton + cricket + exercise + swimming + yoga +
                    cooking + sweeping + toiletcleaning + gardening + carwash + dishwashing
        WHERE guild_id = ? AND user_id = ?
      `);
      recalc.run(guildId, userId);

      // 🔹 Log transaction (with event_key)
      const keyData = `${guildId}:${userId}:${category}:${amount}:${reason || ''}:${notes || ''}:${Date.now()}`; // Added timestamp to hash for extra uniqueness if needed
      const eventKey = crypto.createHash('sha256').update(keyData).digest('hex');

      const info = this.stmts.logPoints.run(
        guildId,
        userId,
        logCategory, // Log the original, specific category
        modAmount,
        Math.floor(Date.now() / 1000),
        reason,
        notes,
        eventKey // Add the unique key
      );

       // Optional: Log if a duplicate was prevented
      // if (info.changes === 0) {
      //   console.log(`[DB] Duplicate event prevented for key: ${eventKey.substring(0, 8)}...`);
      // }


      // 🔹 Maintain streaks and achievements
      if (modAmount > 0) {
        this.updateStreak(guildId, userId);
        return this.checkAchievements(guildId, userId);
      }
      return [];
    }
    // --- (updateStreak, checkCooldown, commitCooldown, checkAchievements, close remain unchanged) ---
     updateStreak(guildId, userId) { const user = this.stmts.getUser.get(guildId, userId); if (!user) return; const today = new Date().toISOString().slice(0,10); if (user.last_activity_date === today) return; const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0,10); const currentStreak = (user.last_activity_date === yesterday) ? (user.current_streak || 0) + 1 : 1; const longestStreak = Math.max(user.longest_streak || 0, currentStreak); this.stmts.updateStreak.run({ guild_id: guildId, user_id: userId, current_streak: currentStreak, longest_streak: longestStreak, last_activity_date: today }); }
    checkCooldown({ guildId, userId, category }) { let k = category; if (EXERCISE_CATEGORIES.includes(k)) k = 'exercise'; if (!COOLDOWNS[k]) { console.warn(`Cooldown undef: ${k} (orig: ${category})`); return 0; } const r = this.stmts.getCooldown.get(guildId, userId, k); const n = Date.now(); const c = COOLDOWNS[k]; if (r && n - r.last_ms < c) return c - (n - r.last_ms); return 0; }
    commitCooldown({ guildId, userId, category }) { let k = category; if (EXERCISE_CATEGORIES.includes(k)) k = 'exercise'; if (!COOLDOWNS[k]) { console.warn(`Commit CD undef: ${k} (orig: ${category})`); return; } this.stmts.setCooldown.run({ guild_id: guildId, user_id: userId, category: k, last_ms: Date.now() }); }
    checkAchievements(guildId, userId) { const s = this.stmts.getUser.get(guildId, userId); if (!s) return []; const u = this.stmts.getUserAchievements.all(guildId, userId).map(r => r.achievement_id); const f = []; for (const a of ACHIEVEMENTS) { if (!u.includes(a.id) && a.requirement(s)) { this.stmts.unlockAchievement.run(guildId, userId, a.id); f.push(a); } } return f; }
    close() { this.db.close(); console.log("[DB] Database connection closed.") }
}

// --- (reconcileTotals remains unchanged) ---
function reconcileTotals(db) {
  try {
    console.log("🔄 [Reconcile] Starting reconciliation...");

    const exerciseCatPlaceholders = EXERCISE_CATEGORIES.map(() => '?').join(',');

    const logTotals = db.prepare(`
      SELECT
        guild_id,
        user_id,
        SUM(amount) as total,
        SUM(CASE WHEN category = 'gym' THEN amount ELSE 0 END) as gym,
        SUM(CASE WHEN category = 'badminton' THEN amount ELSE 0 END) as badminton,
        SUM(CASE WHEN category = 'cricket' THEN amount ELSE 0 END) as cricket,
        SUM(CASE WHEN category IN (${exerciseCatPlaceholders}) THEN amount ELSE 0 END) as exercise,
        SUM(CASE WHEN category = 'swimming' THEN amount ELSE 0 END) as swimming,
        SUM(CASE WHEN category = 'yoga' THEN amount ELSE 0 END) as yoga,
        SUM(CASE WHEN category = 'cooking' THEN amount ELSE 0 END) as cooking,
        SUM(CASE WHEN category = 'sweeping' THEN amount ELSE 0 END) as sweeping,
        SUM(CASE WHEN category = 'toiletcleaning' THEN amount ELSE 0 END) as toiletcleaning,
        SUM(CASE WHEN category = 'gardening' THEN amount ELSE 0 END) as gardening,
        SUM(CASE WHEN category = 'carwash' THEN amount ELSE 0 END) as carwash,
        SUM(CASE WHEN category = 'dishwashing' THEN amount ELSE 0 END) as dishwashing
      FROM points_log
      GROUP BY guild_id, user_id
    `).all(...EXERCISE_CATEGORIES);
    console.log(`[Reconcile] Fetched ${logTotals.length} user totals from points_log.`);

    const resetGuild = db.prepare(`
      UPDATE points SET
      total = 0, gym = 0, badminton = 0, cricket = 0,
      exercise = 0, swimming = 0, yoga = 0,
      cooking = 0, sweeping = 0, toiletcleaning = 0,
      gardening = 0, carwash = 0, dishwashing = 0
      WHERE guild_id = ?
    `);

    const upsert = db.prepare(`
      INSERT INTO points (guild_id, user_id, total, gym, badminton, cricket, exercise, swimming, yoga, cooking, sweeping, toiletcleaning, gardening, carwash, dishwashing)
      VALUES (@guild_id, @user_id, @total, @gym, @badminton, @cricket, @exercise, @swimming, @yoga, @cooking, @sweeping, @toiletcleaning, @gardening, @carwash, @dishwashing)
      ON CONFLICT(guild_id, user_id)
      DO UPDATE SET
        total = excluded.total, gym = excluded.gym, badminton = excluded.badminton,
        cricket = excluded.cricket, exercise = excluded.exercise,
        swimming = excluded.swimming, yoga = excluded.yoga,
        cooking = excluded.cooking, sweeping = excluded.sweeping, toiletcleaning = excluded.toiletcleaning,
        gardening = excluded.gardening, carwash = excluded.carwash, dishwashing = excluded.dishwashing,
        updated_at = strftime('%s','now')
    `);

    const guilds = db.prepare(`SELECT DISTINCT guild_id FROM points_log`).all();
     console.log(`[Reconcile] Found ${guilds.length} distinct guilds in points_log.`);

    const tx = db.transaction((guilds, rows) => {
      console.log(`[Reconcile] Starting transaction...`);
      let resetCount = 0;
      for (const g of guilds) {
        resetGuild.run(g.guild_id);
        resetCount++;
      }
      console.log(`[Reconcile] Reset points table for ${resetCount} guilds.`);
      let upsertCount = 0;
      for (const row of rows) {
        db.prepare(`INSERT INTO points (guild_id, user_id) VALUES (?, ?) ON CONFLICT DO NOTHING`).run(row.guild_id, row.user_id);
        upsert.run(row);
        upsertCount++;
      }
       console.log(`[Reconcile] Upserted ${upsertCount} user rows into points table.`);
       console.log(`[Reconcile] Transaction finished.`);
    });

    tx(guilds, logTotals);

    console.log(`✅ [Reconcile] Reconciliation complete.`);
  } catch (err) {
    console.error("❌ [Reconcile] Reconciliation error:", err);
  }
}


/* =========================
    UTILITIES
========================= */
// --- (Utilities remain unchanged) ---
const formatNumber = (n) => (Math.round(n * 1000) / 1000).toLocaleString(undefined, { maximumFractionDigits: 3 });
const progressBar = (pct) => `${'█'.repeat(Math.floor(pct / 10))}${'░'.repeat(10 - Math.floor(pct / 10))} ${pct}%`;
const getUserRank = (total) => RANKS.reduce((acc, rank) => total >= rank.min ? rank : acc, RANKS[0]);
function nextRankProgress(total) { const cur = getUserRank(total); if (cur.next === null) return { pct: 100, cur, need: 0 }; const span = cur.next - cur.min; const done = total - cur.min; return { pct: Math.max(0, Math.min(100, Math.floor((done / span) * 100))), cur, need: cur.next - total }; }
const formatCooldown = (ms) => { if (ms <= 0) return 'Ready!'; const s = Math.floor(ms / 1000); const h = Math.floor(s / 3600); const m = Math.floor((s % 3600) / 60); const sec = s % 60; let str = ''; if (h > 0) str += `${h}h `; if (m > 0) str += `${m}m `; if (h === 0 && m === 0 && sec > 0) str += `${sec}s`; else if (h === 0 && m === 0 && sec <= 0) return 'Ready!'; return str.trim() || 'Ready!'; };
function getPeriodRange(period = 'week') { const n = new Date(); let s = new Date(n); let e = new Date(n); switch(period){ case 'day': s.setHours(0,0,0,0); e.setHours(23,59,59,999); break; case 'month': s = new Date(n.getFullYear(), n.getMonth(), 1); e = new Date(n.getFullYear(), n.getMonth()+1, 0, 23, 59, 59, 999); break; case 'year': s = new Date(n.getFullYear(), 0, 1); e = new Date(n.getFullYear(), 11, 31, 23, 59, 59, 999); break; case 'week': default: const d=n.getDay()||7; s.setDate(n.getDate()-d+1); s.setHours(0,0,0,0); e.setDate(s.getDate()+6); e.setHours(23,59,59,999); break; } return {start: Math.floor(s.getTime()/1000), end: Math.floor(e.getTime()/1000)}; }
function getPeriodStart(period = 'day') { const n=new Date(); n.setHours(0,0,0,0); return Math.floor(n.getTime()/1000); }
function createKeepAliveServer() { http.createServer((r,res)=>{res.writeHead(200,{'Content-Type':'text/plain'});res.end('OK');}).listen(process.env.PORT||3000,()=>console.log('✅ Keep-alive server running.'));}


/* =========================
    COMMAND DEFINITIONS
========================= */
// --- (buildCommands remains unchanged, includes show_table subcommand) ---
function buildCommands() {
    const fixedPointCategories = Object.keys(POINTS);
    const adminCategoryChoices = [...new Set([ ...fixedPointCategories, 'exercise' ])].map(c => ({name: c.charAt(0).toUpperCase() + c.slice(1), value: c}));
    const allLbCategories = ['all', 'streak', 'exercise', ...fixedPointCategories ];

    // NEW: Table choices for the new admin command
    const tableChoices = [
        { name: 'Points (Summary)', value: 'points' },
        { name: 'Points Log (History)', value: 'points_log' },
        { name: 'Cooldowns', value: 'cooldowns' },
        { name: 'Buddies', value: 'buddies' },
        { name: 'Achievements', value: 'achievements' },
        { name: 'Protein Log', value: 'protein_log' }
    ];

    return [
        ...fixedPointCategories.map(name => new SlashCommandBuilder().setName(name).setDescription(`Log ${name} (+${POINTS[name]} pts)`)),

        new SlashCommandBuilder().setName('exercise').setDescription('💪 Log detailed exercise')
            .addSubcommand(s=>s.setName('yoga').setDescription(`🧘 Yoga (+${POINTS.yoga} pts)`).addNumberOption(o=>o.setName('minutes').setRequired(true).setMinValue(1).setDescription('Minutes performed (for notes)')))
            .addSubcommand(s=>s.setName('reps').setDescription(`💪 Generic reps (${EXERCISE_RATES.per_rep} pts/rep)`).addNumberOption(o=>o.setName('count').setRequired(true).setMinValue(1).setDescription('Total reps')))
            .addSubcommand(s=>s.setName('dumbbells').setDescription(`🏋️ Dumbbells (${EXERCISE_RATES.per_rep} pts/rep)`).addNumberOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Reps/set')).addNumberOption(o=>o.setName('sets').setRequired(true).setMinValue(1).setDescription('Sets')))
            .addSubcommand(s=>s.setName('barbell').setDescription(`🏋️ Barbell (${EXERCISE_RATES.per_rep} pts/rep)`).addNumberOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Reps/set')).addNumberOption(o=>o.setName('sets').setRequired(true).setMinValue(1).setDescription('Sets')))
            .addSubcommand(s=>s.setName('pushup').setDescription(`💪 Pushups (${REP_RATES.pushup} pts/rep)`).addNumberOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Reps/set')).addNumberOption(o=>o.setName('sets').setRequired(true).setMinValue(1).setDescription('Sets')))
            .addSubcommand(s=>s.setName('plank').setDescription(`🧱 Plank (${PLANK_RATE_PER_MIN} pt/min)`).addNumberOption(o=>o.setName('minutes').setRequired(true).setMinValue(PLANK_MIN_MIN).setDescription(`Minutes (min ${PLANK_MIN_MIN})`)))
            .addSubcommand(s=>s.setName('squat').setDescription(`🦵 Squats (${REP_RATES.squat} pts/rep)`).addIntegerOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Total Reps')))
            .addSubcommand(s=>s.setName('kettlebell').setDescription(`🏋️ Kettlebell (${REP_RATES.kettlebell} pts/rep)`).addIntegerOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Total Reps')))
            .addSubcommand(s=>s.setName('lunge').setDescription(`🦿 Lunges (${REP_RATES.lunge} pts/rep)`).addIntegerOption(o=>o.setName('reps').setRequired(true).setMinValue(1).setDescription('Total Reps'))),

        new SlashCommandBuilder().setName('protein').setDescription('🥩 Track protein')
            .addSubcommand(s=>s.setName('add_item').setDescription('Add by item').addStringOption(o=>o.setName('item').setRequired(true).setDescription('Food').addChoices(...Object.entries(PROTEIN_SOURCES).filter(([,v])=>v.unit==='item').map(([k,v])=>({name:v.name, value:k})))).addIntegerOption(o=>o.setName('quantity').setRequired(true).setMinValue(1).setDescription('Qty')))
            .addSubcommand(s=>s.setName('add_grams').setDescription('Add by weight').addStringOption(o=>o.setName('item').setRequired(true).setDescription('Food').addChoices(...Object.entries(PROTEIN_SOURCES).filter(([,v])=>v.unit==='gram').map(([k,v])=>({name:v.name, value:k})))).addNumberOption(o=>o.setName('grams').setRequired(true).setMinValue(1).setDescription('Grams')))
            .addSubcommand(s=>s.setName('log_direct').setDescription('Log exact amount').addNumberOption(o=>o.setName('grams').setRequired(true).setMinValue(0.1).setDescription('Grams protein')).addStringOption(o=>o.setName('source').setDescription('Source (optional)')))
            .addSubcommand(s=>s.setName('total').setDescription("View today's protein").addUserOption(o=>o.setName('user').setDescription('View another user (optional)'))),

        new SlashCommandBuilder().setName('walking').setDescription(`🚶 Log walking (${DISTANCE_RATES.walking} pts/km)`).addNumberOption(o=>o.setName('km').setRequired(true).setMinValue(0.1).setDescription('Km')),
        new SlashCommandBuilder().setName('jogging').setDescription(`🏃 Log jogging (${DISTANCE_RATES.jogging} pts/km)`).addNumberOption(o=>o.setName('km').setRequired(true).setMinValue(0.1).setDescription('Km')),
        new SlashCommandBuilder().setName('running').setDescription(`💨 Log running (${DISTANCE_RATES.running} pts/km)`).addNumberOption(o=>o.setName('km').setRequired(true).setMinValue(0.1).setDescription('Km')),

        new SlashCommandBuilder().setName('myscore').setDescription('🏆 Show score & rank')
            .addUserOption(o => o.setName('user').setDescription('The user to view (defaults to yourself)')),

        new SlashCommandBuilder().setName('leaderboard').setDescription('📊 Show All-Time leaderboard').addStringOption(o=>o.setName('category').setDescription('Filter category (default: all)').addChoices(...allLbCategories.map(c=>({name:c[0].toUpperCase()+c.slice(1), value:c})))),
        new SlashCommandBuilder().setName('leaderboard_period').setDescription('📅 Show periodic leaderboard').addStringOption(o=>o.setName('period').setRequired(true).setDescription('Period').addChoices({name:'Today',value:'day'},{name:'This Week',value:'week'},{name:'Month',value:'month'},{name:'Year',value:'year'})).addStringOption(o=>o.setName('category').setDescription('Filter category (default: all)').addChoices(...allLbCategories.map(c=>({name:c[0].toUpperCase()+c.slice(1), value:c})))),
        new SlashCommandBuilder().setName('junk').setDescription('🍕 Log junk food').addStringOption(o=>o.setName('item').setRequired(true).setDescription('Item').addChoices(...Object.entries(DEDUCTIONS).map(([k,{emoji,label}])=>({name:`${emoji} ${label}`,value:k})))),
        new SlashCommandBuilder().setName('buddy').setDescription('👯 Set/view buddy').addUserOption(o=>o.setName('user').setDescription('User to set as buddy (leave blank to view)')),
        new SlashCommandBuilder().setName('nudge').setDescription('👉 Nudge user').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User to nudge')).addStringOption(o=>o.setName('activity').setRequired(true).setDescription('Activity')),
        new SlashCommandBuilder().setName('remind').setDescription('⏰ Set reminder').addStringOption(o=>o.setName('activity').setRequired(true).setDescription('Reminder')).addNumberOption(o=>o.setName('hours').setRequired(true).setMinValue(1).setDescription('Hours')),

        new SlashCommandBuilder().setName('admin').setDescription('🛠️ Admin').setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
            .addSubcommand(s=>s.setName('award').setDescription('Award points').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User to award')).addNumberOption(o=>o.setName('amount').setRequired(true).setDescription('Pts')).addStringOption(o=>o.setName('category').setRequired(true).setDescription('Category').addChoices(...adminCategoryChoices)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(s=>s.setName('deduct').setDescription('Deduct points').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User to deduct from')).addNumberOption(o=>o.setName('amount').setRequired(true).setDescription('Pts')).addStringOption(o=>o.setName('category').setRequired(true).setDescription('Category').addChoices(...adminCategoryChoices)).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(s=>s.setName('add_protein').setDescription('Add protein').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User to add to')).addNumberOption(o=>o.setName('grams').setRequired(true).setMinValue(0.1).setDescription('Grams')).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(s=>s.setName('deduct_protein').setDescription('Deduct protein').addUserOption(o=>o.setName('user').setRequired(true).setDescription('User to deduct from')).addNumberOption(o=>o.setName('grams').setRequired(true).setMinValue(0.1).setDescription('Grams')).addStringOption(o=>o.setName('reason').setDescription('Reason')))
            .addSubcommand(s=>s.setName('clear_user_data').setDescription('🔥 Wipe ALL data for a user (IRREVERSIBLE)')
                .addUserOption(o=>o.setName('user').setRequired(true).setDescription('The user to wipe'))
                .addStringOption(o=>o.setName('confirm').setRequired(true).setDescription('Type the word CONFIRM to approve this action'))
            )
            // NEW: Subcommand to show tables
            .addSubcommand(s=>s.setName('show_table').setDescription('🔒 Dumps the content of a database table (Top 30 rows)')
                .addStringOption(o=>o.setName('table_name').setRequired(true).setDescription('The table to show').addChoices(...tableChoices))
            ),

        new SlashCommandBuilder()
            .setName('recalculate')
            .setDescription('🧮 Admin: Recalculate all totals from the log')
            .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild),

        new SlashCommandBuilder()
            .setName('db_download')
            .setDescription('🔒 Admin: Download a copy of the database file.')
            .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)

    ].map(c => c.toJSON());
}


/* =========================
    COMMAND HANDLERS
========================= */
// --- (CommandHandler remains unchanged, includes handleAdmin changes for show_table) ---
class CommandHandler {
    constructor(db) { this.db = db; }

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
        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;

        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} claimed **+${formatNumber(amount)}** pts for **${category}**!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });

        const payload = { embeds:[embed], ephemeral:false };
        if (achievements.length) {
            await interaction.editReply(payload);
            return interaction.followUp({ embeds: [new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` })], flags: [MessageFlags.Ephemeral] });
        }
        return interaction.editReply(payload);
    }

    async handleDistance(interaction, activity) {
        const { guild, user, options } = interaction;
        const km = options.getNumber('km', true);
        const amount = km * DISTANCE_RATES[activity];
        const cooldownKey = 'exercise';
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for exercises: ${formatCooldown(remaining)}.`, flags: [MessageFlags.Ephemeral] });

        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: activity, amount, reason: `distance:${activity}`, notes: `${km}km` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.', flags: [MessageFlags.Ephemeral] });
        const { cur, need } = nextRankProgress(userRow.total);

        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;

        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} logged **${formatNumber(km)}km** ${activity} → **+${formatNumber(amount)}** pts!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });

        const payload = { embeds:[embed], ephemeral: false };
         if (achievements.length) {
             await interaction.editReply(payload);
             return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }) ], flags: [MessageFlags.Ephemeral] });
         }
        return interaction.editReply(payload);
    }

    async handleExercise(interaction) {
        const { guild, user, options } = interaction;
        const subcommand = options.getSubcommand();
        let amount = 0;
        let description = '';
        let cooldownCategory = 'exercise';
        let logCategory = 'exercise';
        let reasonPrefix = 'exercise';
        let notes = '';

        if (subcommand === 'yoga') {
            cooldownCategory = 'yoga';
            logCategory = 'yoga';
            reasonPrefix = 'claim';
        } else if (subcommand === 'plank') {
            logCategory = 'plank';
            reasonPrefix = 'time';
        } else if (REP_RATES[subcommand]) {
            logCategory = subcommand;
            reasonPrefix = 'reps';
        }

        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for **${subcommand}**: ${formatCooldown(remaining)}.`, flags: [MessageFlags.Ephemeral] });

        switch (subcommand) {
             case 'yoga': {
                 const minutes = options.getNumber('minutes', true);
                 amount = POINTS.yoga || 0;
                 description = `${user.toString()} claimed **+${formatNumber(amount)}** pts for **Yoga**!`;
                 notes = `${minutes} min`;
                 break;
             }
             case 'plank': {
                 const minutes = options.getNumber('minutes', true);
                 amount = minutes * PLANK_RATE_PER_MIN;
                 description = `${user.toString()} held a **plank** for **${formatNumber(minutes)} min** → **+${formatNumber(amount)}** pts!`;
                 notes = `${minutes} min`;
                 break;
             }
             case 'reps': {
                 const count = options.getNumber('count', true);
                 amount = count * EXERCISE_RATES.per_rep;
                 description = `${user.toString()} logged **${count} total reps** → **+${formatNumber(amount)}** pts!`;
                 notes = `${count} reps`;
                 break;
             }
             case 'dumbbells': case 'barbell': case 'pushup':
             case 'squat': case 'kettlebell': case 'lunge': {
                 const repsInput = options.getInteger('reps', false); // Allow null for squat, kettlebell, lunge
                 const setsInput = options.getInteger('sets', false); // Allow null for squat, kettlebell, lunge
                 let totalReps;
                 if (['squat', 'kettlebell', 'lunge'].includes(subcommand)) {
                    totalReps = repsInput || options.getInteger('reps', true); // Use the single 'reps' option if others are null
                    notes = `${totalReps} reps`;
                 } else {
                    const reps = repsInput ?? 1; // Default to 1 if not provided (though required by definition)
                    const sets = setsInput ?? 1; // Default to 1 if not provided (though required by definition)
                    totalReps = reps * sets;
                    notes = `${sets}x${reps} reps`;
                 }

                 const rate = REP_RATES[subcommand] ?? EXERCISE_RATES.per_rep;
                 amount = totalReps * rate;
                 description = `${user.toString()} logged ${notes} **${subcommand}** → **+${formatNumber(amount)}** pts!`;
                 break;
             }
        }

        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: logCategory, amount, reason: `${reasonPrefix}:${subcommand}`, notes });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.', flags: [MessageFlags.Ephemeral] });
        const { cur, need } = nextRankProgress(userRow.total);

        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;

        const embed = new EmbedBuilder().setColor(cur.color).setDescription(description).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });

        const payload = { embeds:[embed], ephemeral:false };
        if (achievements.length) {
             await interaction.editReply(payload);
             return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }) ], flags: [MessageFlags.Ephemeral] });
        }
        return interaction.editReply(payload);
    }

    async handleProtein(interaction) {
        const { guild, user, options } = interaction; const subcommand = options.getSubcommand(); const targetUser = options.getUser('user') || user;
        if (subcommand === 'total') { const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, targetUser.id, since); const totalProtein = result?.total || 0; const embed = new EmbedBuilder().setColor(0x5865F2).setTitle(`🥩 Daily Protein for ${targetUser.displayName}`).setDescription(`Logged **${formatNumber(totalProtein)}g** protein today.`).setThumbnail(targetUser.displayAvatarURL()).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }); return interaction.editReply({ embeds: [embed] }); }
        let proteinGrams = 0; let itemName = '';
        if (subcommand === 'add_item') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const q = options.getInteger('quantity', true); proteinGrams = s.protein_per_unit * q; itemName = `${q} ${s.name}`; }
        else if (subcommand === 'add_grams') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const g = options.getNumber('grams', true); proteinGrams = s.protein_per_unit * g; itemName = `${g}g of ${s.name}`; }
        else if (subcommand === 'log_direct') { const g = options.getNumber('grams', true); const n = options.getString('source') || 'direct source'; proteinGrams = g; itemName = n; }
        this.db.stmts.addProteinLog.run(guild.id, user.id, itemName, proteinGrams, Math.floor(Date.now() / 1000));
        const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, user.id, since); const totalProtein = result?.total || 0;
        const embed = new EmbedBuilder().setColor(0x2ECC71).setTitle('✅ Protein Logged!').setDescription(`${user.toString()} added **${formatNumber(proteinGrams)}g** protein from **${itemName}**.`).addFields({ name: 'Daily Total', value: `Today: **${formatNumber(totalProtein)}g** protein.` }).setThumbnail(user.displayAvatarURL()).setFooter({ text: `PID: ${BOT_PROCESS_ID}` });
        return interaction.editReply({ embeds: [embed], ephemeral: false });
    }

    async handleJunk(interaction) {
        const { guild, user, options } = interaction; const item = options.getString('item', true); const deduction = DEDUCTIONS[item]; const msgs = ["Balance is key!", "One step back, two forward!", "Honesty is progress!", "Treats happen!", "Acknowledge and move on!"]; const msg = msgs[Math.floor(Math.random() * msgs.length)];
        this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'junk', amount: -deduction.points, reason: `junk:${item}` }); const userRow = this.db.stmts.getUser.get(guild.id, user.id); const total = userRow ? userRow.total : 0;
        const embed = new EmbedBuilder().setColor(0xED4245).setDescription(`${user.toString()} logged ${deduction.emoji} **${deduction.label}** (-**${formatNumber(deduction.points)}** pts).`).addFields({ name: "Total", value: `🏆 ${formatNumber(total)}` }).setFooter({ text: `${msg} | PID: ${BOT_PROCESS_ID}` });
        return interaction.editReply({ embeds: [embed], ephemeral: false });
    }

    async handleMyScore(interaction) {
        const { guild, options } = interaction;
        const targetUser = options.getUser('user') || interaction.user;
        const userRow = this.db.stmts.getUser.get(guild.id, targetUser.id) || { total: 0, current_streak: 0 };
        const { pct, cur, need } = nextRankProgress(userRow.total);
        const ach = this.db.stmts.getUserAchievements.all(guild.id, targetUser.id).map(r => r.achievement_id);
        const embed = new EmbedBuilder().setColor(cur.color).setAuthor({ name: targetUser.displayName, iconURL: targetUser.displayAvatarURL() }).setTitle(`Rank: ${cur.name}`).addFields({ name: 'Points', value: formatNumber(userRow.total), inline: true }, { name: 'Streak', value: `🔥 ${userRow.current_streak || 0}d`, inline: true }, { name: 'Progress', value: progressBar(pct), inline: false }, { name: 'Achievements', value: ach.length > 0 ? ach.map(id => `**${ACHIEVEMENTS.find(a => a.id === id)?.name || id}**`).join(', ') : 'None' });
        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;
        embed.setFooter({ text: footerText });
        return interaction.editReply({ embeds: [embed] });
    }

    // Handles All-Time leaderboard (/leaderboard) - Uses points table
    async handleLeaderboard(interaction) {
        const { guild, user, options } = interaction;
        const cat = options.getString('category') || 'all';
        try {
            let rows = []; let subtitle = ''; let selfRank = null;
            subtitle = `All Time • ${cat === 'all' ? 'Total Points' : cat === 'streak' ? 'Current Streak' : cat.charAt(0).toUpperCase() + cat.slice(1)}`;

            if (cat === 'streak') {
                rows = this.db.stmts.getTopStreaks.all(guild.id);
            } else if (cat === 'all') {
                rows = this.db.stmts.lbAllFromPoints.all(guild.id);
                const my = this.db.stmts.selfRankAllFromPoints.get(guild.id, user.id);
                if (my) selfRank = { userId: user.id, rank: my.rank, score: my.score };
            } else {
                 const catQueryKey = `lbAllCatFromPoints_${cat}`;
                 if (this.db.stmts[catQueryKey]) {
                     rows = this.db.stmts[catQueryKey].all(guild.id);
                 } else {
                     let queryCategory = cat; if (cat === 'exercise') queryCategory = EXERCISE_CATEGORIES;
                     const placeholders = Array.isArray(queryCategory) ? queryCategory.map(() => '?').join(',') : '?';
                     const query = `
                         SELECT user_id as userId, SUM(amount) AS score FROM points_log
                         WHERE guild_id=? AND amount>0 AND category IN (${placeholders})
                         GROUP BY user_id HAVING score>0 ORDER BY score DESC LIMIT 10`;
                     const params = Array.isArray(queryCategory) ? [guild.id, ...queryCategory] : [guild.id, queryCategory];
                     rows = this.db.db.prepare(query).all(...params);
                     if (!EXERCISE_CATEGORIES.includes(cat)) subtitle += " (from log)";
                 }
            }

            if (!rows.length) return interaction.editReply({ content: '📊 No data.' });
            rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
            const userIds = rows.map(r => r.userId);
            const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || 'Unknown'; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\`${cat === 'streak' ? ' days' : ''}`; });

            let footerText = `PID: ${BOT_PROCESS_ID}`;
            if (cat === 'all' && selfRank && !rows.some(r => r.userId === user.id)) footerText = `Your Rank: #${selfRank.rank} (${formatNumber(selfRank.score)} pts) | ${footerText}`;

            const embed = new EmbedBuilder().setTitle(`🏆 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp().setFooter({ text: footerText });
            return interaction.editReply({ embeds: [embed] });
        } catch (e) { console.error('LB Error:', e); return interaction.editReply({ content: '❌ Error generating leaderboard.' }); }
    }

    // Handles Periodic leaderboard (/leaderboard_period) - Uses log table
    async handleLeaderboardPeriod(interaction) {
        const { guild, user, options } = interaction; const period = options.getString('period', true); const cat = options.getString('category') || 'all';
        try {
            let rows = [];
            const { start, end } = getPeriodRange(period); const periodName = { day: 'Today', week: 'This Week', month:'This Month', year:'This Year' }[period]; const startStr = `<t:${start}:d>`; const endStr = `<t:${end}:d>`;
            let subtitle = `${periodName} (${startStr}-${endStr}) • ${cat === 'all' ? 'Total Net Points' : cat.charAt(0).toUpperCase() + cat.slice(1)}`;
            if (cat === 'streak') return interaction.editReply({ content: '📊 Streak LB only All-Time.' });
            else {
                 let queryCategory = cat; if (cat === 'exercise') queryCategory = EXERCISE_CATEGORIES;
                 const placeholders = Array.isArray(queryCategory) ? queryCategory.map(() => '?').join(',') : '?'; let query = ''; let params = [];
                 if (cat === 'all') { query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`; params = [guild.id, start, end]; }
                 else { query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts >= ? AND ts < ? AND amount <> 0 AND category IN (${placeholders}) GROUP BY user_id HAVING SUM(amount) <> 0 ORDER BY score DESC LIMIT 10`; params = Array.isArray(queryCategory) ? [guild.id, start, end, ...queryCategory] : [guild.id, start, end, queryCategory]; }
                 rows = this.db.db.prepare(query).all(...params);
            }
            if (!rows.length) return interaction.editReply({ content: `📊 No data for ${periodName}.` });
            rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
            const userIds = rows.map(r => r.userId); const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || 'Unknown'; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\``; });
            const embed = new EmbedBuilder().setTitle(`📅 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp().setFooter({ text: `PID: ${BOT_PROCESS_ID}` });
            return interaction.editReply({ embeds: [embed] });
        } catch (e) { console.error('Period LB Error:', e); return interaction.editReply({ content: '❌ Error generating periodic leaderboard.' }); }
    }

    async handleBuddy(interaction) {
        const { guild, user, options } = interaction; const targetUser = options.getUser('user'); if (!targetUser) { const b = this.db.stmts.getBuddy.get(guild.id, user.id); return interaction.editReply({ content: b?.buddy_id ? `Buddy: <@${b.buddy_id}>` : 'No buddy set!' }); } if (targetUser.id === user.id) return interaction.editReply({ content: 'Cannot be own buddy!', flags: [MessageFlags.Ephemeral] }); this.db.stmts.setBuddy.run(guild.id, user.id, targetUser.id); return interaction.editReply({ content: `✨ ${user.toString()} set <@${targetUser.id}> as buddy!` });
    }
    async handleNudge(interaction) {
        const { guild, user, options } = interaction; const targetUser = options.getUser('user', true); const activity = options.getString('activity', true); if (targetUser.bot || targetUser.id === user.id) return interaction.editReply({ content: "Cannot nudge bots/self." }); const isAdmin = interaction.member.permissions.has(PermissionFlagsBits.ManageGuild); const buddy = this.db.stmts.getBuddy.get(guild.id, user.id); const isBuddy = buddy?.buddy_id === targetUser.id; if (!isAdmin && !isBuddy) return interaction.editReply({ content: "Can only nudge buddy (or ask admin)." }); try { await targetUser.send(`⏰ <@${user.id}> from **${guild.name}** nudges: **${activity}**!`); return interaction.editReply({ content: `✅ Nudge sent to <@${targetUser.id}>.` }); } catch (err) { return interaction.editReply({ content: `❌ Could not DM user.` }); }
    }
    async handleRemind(interaction) {
        const { guild, user, options } = interaction; const activity = options.getString('activity', true); const hours = options.getNumber('hours', true); const dueAt = Date.now() + hours * 3600000; this.db.stmts.addReminder.run(guild.id, user.id, activity, dueAt); return interaction.editReply({ content: `⏰ Reminder set for **${activity}** in ${hours}h.` });
    }

    async handleAdmin(interaction) {
        const { guild, user, options } = interaction; const sub = options.getSubcommand();
        const targetUser = options.getUser('user'); // 'user' is not required for 'show_table'

        if (sub === 'clear_user_data') {
            if (!targetUser) return interaction.editReply({ content: 'You must specify a user to clear.', flags: [MessageFlags.Ephemeral] });
            const confirm = options.getString('confirm', true);
            if (confirm !== 'CONFIRM') {
                return interaction.editReply({ content: '❌ Action cancelled. You must type `CONFIRM` to proceed.', flags: [MessageFlags.Ephemeral] });
            }
            try {
                // This transaction correctly clears ALL data for the user from
                // the "current" table (points) and "historic" table (points_log)
                this.db.db.transaction(() => {
                    this.db.stmts.clearUserPoints.run(guild.id, targetUser.id);
                    this.db.stmts.clearUserLog.run(guild.id, targetUser.id);
                    this.db.stmts.clearUserAchievements.run(guild.id, targetUser.id);
                    this.db.stmts.clearUserCooldowns.run(guild.id, targetUser.id);
                    this.db.stmts.clearUserProtein.run(guild.id, targetUser.id);
                    this.db.stmts.clearUserBuddy.run(guild.id, targetUser.id);
                })();
                return interaction.editReply({ content: `✅ All data for <@${targetUser.id}> has been permanently deleted.`, flags: [MessageFlags.Ephemeral] });
            } catch (err) {
                console.error("Error clearing user data:", err);
                return interaction.editReply({ content: `❌ An error occurred while trying to clear data.`, flags: [MessageFlags.Ephemeral] });
            }
        }

        // NEW: Handle 'show_table'
        if (sub === 'show_table') {
            const tableName = options.getString('table_name', true);
            // Basic sanitization to prevent injection
            const allowedTables = ['points', 'points_log', 'cooldowns', 'buddies', 'achievements', 'protein_log'];
            if (!allowedTables.includes(tableName)) {
                return interaction.editReply({ content: '❌ Invalid table name.', flags: [MessageFlags.Ephemeral] });
            }
            try {
                // Determine sort order based on table
                let orderBy = '';
                if (tableName === 'points_log' || tableName === 'protein_log' || tableName === 'reminders') {
                    orderBy = 'ORDER BY id DESC'; // Show newest first for log-like tables
                } else if (tableName === 'points') {
                     orderBy = 'ORDER BY total DESC'; // Show highest points first
                }
                // Default is no specific order (or primary key order) for others like cooldowns, buddies, achievements

                const rows = this.db.db.prepare(`SELECT * FROM ${tableName} ${orderBy} LIMIT 30`).all();

                if (rows.length === 0) {
                    return interaction.editReply({ content: `✅ Table \`${tableName}\` is empty.`, flags: [MessageFlags.Ephemeral] });
                }
                const data = JSON.stringify(rows, null, 2);
                const attachment = new AttachmentBuilder(Buffer.from(data), { name: `${tableName}_dump.json` });
                return interaction.editReply({
                    content: `✅ Here are the top/last 30 rows from the \`${tableName}\` table:`,
                    files: [attachment],
                    flags: [MessageFlags.Ephemeral]
                });
            } catch (err) {
                 console.error(`Error showing table ${tableName}:`, err);
                return interaction.editReply({ content: `❌ Error fetching table data. Check logs.`, flags: [MessageFlags.Ephemeral] });
            }
        }

        // All other commands require a targetUser
        if (!targetUser) return interaction.editReply({ content: `You must specify a user for this command.`, flags: [MessageFlags.Ephemeral] });

        if (sub === 'award' || sub === 'deduct') {
            const amt = options.getNumber('amount', true);
            const cat = options.getString('category', true);
            const rsn = options.getString('reason') || `Admin action`;
            const finalAmt = sub === 'award' ? amt : -amt; // Corrected logic

            this.db.modifyPoints({ guildId: guild.id, userId: targetUser.id, category: cat, amount: finalAmt, reason: `admin:${sub}`, notes: rsn });

            const act = sub === 'award' ? 'Awarded' : 'Deducted';
            return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(amt))} ${cat} points for <@${targetUser.id}>.` });
        }
        if (sub === 'add_protein' || sub === 'deduct_protein') {
            let g = options.getNumber('grams', true);
            const rsn = options.getString('reason') || `Admin action`;
            if (sub === 'deduct_protein') g = -g;
            this.db.stmts.addProteinLog.run(guild.id, targetUser.id, `Admin: ${rsn}`, g, Math.floor(Date.now() / 1000));
            const act = sub === 'add_protein' ? 'Added' : 'Deducted';
            return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(g))}g protein for <@${targetUser.id}>.` });
        }
    }


    async handleDbDownload(interaction) {
        const dbPath = CONFIG.dbFile;
        try {
            if (!fs.existsSync(dbPath)) {
                return interaction.editReply({ content: '❌ Database file not found.', flags: [MessageFlags.Ephemeral] });
            }
            const attachment = new AttachmentBuilder(dbPath, { name: 'points.db' });
            await interaction.editReply({
                content: '✅ Here is a backup of the database file.',
                files: [attachment],
                flags: [MessageFlags.Ephemeral]
            });
        } catch (err) {
            console.error("Error sending DB file:", err);
            await interaction.editReply({ content: '❌ Could not send the database file.', flags: [MessageFlags.Ephemeral] });
        }
    }
} // End CommandHandler


/* =========================
    MAIN BOT INITIALIZATION
========================= */
async function main() {
    console.log("[Startup] Starting main function...");
    createKeepAliveServer();
    if (!CONFIG.token || !CONFIG.appId) { console.error('[Startup Error] Missing DISCORD_TOKEN or APPLICATION_ID env vars!'); process.exit(1); }

    console.log("[Startup] Initializing database...");
    const database = new PointsDatabase(CONFIG.dbFile);

    console.log("[Startup] Starting initial data reconciliation...");
    reconcileTotals(database.db);
    console.log("[Startup] Finished reconcileTotals function call.");

    try {
        console.log("[Startup] Attempting WAL checkpoint...");
        const checkpointResult = database.db.pragma('wal_checkpoint(FULL)');
        // Result format: [ { busy: 0, log: 1, checkpointed: 10 } ] (numbers vary)
        console.log("[Startup] WAL Checkpoint Result:", checkpointResult);
        if (checkpointResult && checkpointResult[0] && checkpointResult[0].checkpointed > -1) {
             console.log(`✅ [Startup] Database checkpoint successful (${checkpointResult[0].checkpointed} pages checkpointed).`);
        } else {
             console.warn("⚠️ [Startup] Database checkpoint command executed but result format unexpected or indicates no pages checkpointed. CheckpointResult:", checkpointResult);
        }
    } catch (e) {
        console.error("❌ [Startup Error] Database checkpoint failed:", e);
        // Decide if you want to exit or continue if checkpoint fails
        // process.exit(1);
    }

    console.log("[Startup] Initializing CommandHandler...");
    const handler = new CommandHandler(database);

    console.log("[Startup] Initializing REST client and registering commands...");
    const rest = new REST({ version: '10' }).setToken(CONFIG.token);
    try {
        const route = CONFIG.devGuildId ? Routes.applicationGuildCommands(CONFIG.appId, CONFIG.devGuildId) : Routes.applicationCommands(CONFIG.appId);
        await rest.put(route, { body: buildCommands() });
        console.log('✅ [Startup] Registered application commands.');
    }
    catch (err) {
        console.error('❌ [Startup Error] Command registration failed:', err);
        if (err.rawError) console.error('Validation Errors:', JSON.stringify(err.rawError, null, 2));
        else if (err.errors) console.error('Validation Errors:', JSON.stringify(err.errors, null, 2));
        process.exit(1);
    }

    console.log("[Startup] Initializing Discord Client...");
    const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers] });

    client.once('clientReady', (c) => {
        console.log(`✅ [Discord] Client is Ready! Logged in as ${c.user.tag}. PID: ${BOT_PROCESS_ID}`);
        console.log("[Startup] Setting isBotReady = true");
        isBotReady = true; // --- Set the flag HERE ---

        // Reminder interval setup
        setInterval(async () => {
             if (!isBotReady) return; // Don't run reminders until fully ready
             try {
                 const now = Date.now();
                 const due = database.stmts.getDueReminders.all(now);
                 for (const r of due) {
                     try {
                         const u = await client.users.fetch(r.user_id);
                         await u.send(`⏰ Reminder: **${r.activity}**!`);
                          console.log(`Sent reminder ID ${r.id} to user ${r.user_id}`);
                     } catch (e) {
                         console.error(`[Reminder Error] DM fail for reminder ${r.id} to user ${r.user_id}: ${e.message}`);
                         // Optionally check for specific errors like "Cannot send messages to this user"
                     } finally {
                         database.stmts.deleteReminder.run(r.id);
                     }
                 }
             } catch (e) {
                 console.error("❌ [Reminder Error] Error checking reminders:", e);
             }
         }, 60000); // Check every 60 seconds
    });

    client.on('interactionCreate', async (interaction) => {
        // --- NEW: Check Readiness Flag ---
        if (!isBotReady) {
            console.log(`[Interaction] Received command /${interaction.commandName} while bot not ready. Replying with wait message.`);
             try {
                // Check if already replied/deferred before sending ephemeral message
                if (!interaction.replied && !interaction.deferred) {
                     await interaction.reply({ content: "⏳ Bot is still starting up, please wait a moment and try again.", flags: MessageFlags.Ephemeral });
                }
            } catch (e) {
                 console.error("[Interaction Error] Could not send 'not ready' reply:", e);
            }
            return;
        }
        // ----------------------------------


        if (!interaction.isChatInputCommand() || !interaction.guild) return;
        console.log(`[Interaction] Received command: /${interaction.commandName} from ${interaction.user.tag} (ID: ${interaction.user.id}) in guild ${interaction.guild.name} (ID: ${interaction.guild.id})`);


        try {
            // Ephemeral logic remains the same
            const ephemeralCommands = ['buddy', 'nudge', 'remind', 'admin', 'myscore'];
            let shouldBeEphemeral = ephemeralCommands.includes(interaction.commandName);
            // Specific overrides
            if (interaction.commandName === 'admin' && interaction.options.getSubcommand() === 'show_table') shouldBeEphemeral = true;
            if (interaction.commandName === 'buddy' && !interaction.options.getUser('user')) shouldBeEphemeral = true;
            if (interaction.commandName === 'protein' && interaction.options.getSubcommand() === 'total') shouldBeEphemeral = true;
            if (interaction.commandName === 'myscore' && interaction.options.getUser('user')) shouldBeEphemeral = false;
            if (interaction.commandName.startsWith('leaderboard')) shouldBeEphemeral = false;
            if (interaction.commandName === 'recalculate') shouldBeEphemeral = true;
            if (interaction.commandName === 'db_download') shouldBeEphemeral = true;


            await interaction.deferReply({ flags: shouldBeEphemeral ? MessageFlags.Ephemeral : undefined });

            const { commandName } = interaction;
            const fixedPointCategories = Object.keys(POINTS);

            // Route commands to handlers
            if (fixedPointCategories.includes(commandName)) { await handler.handleClaim(interaction, commandName); }
            else if (commandName === 'exercise') { await handler.handleExercise(interaction); }
            else if (['walking', 'jogging', 'running'].includes(commandName)) { await handler.handleDistance(interaction, commandName); }
            else if (commandName === 'protein') { await handler.handleProtein(interaction); }
            else { // Other commands
                switch (commandName) {
                    case 'junk': await handler.handleJunk(interaction); break;
                    case 'myscore': await handler.handleMyScore(interaction); break;
                    case 'leaderboard': await handler.handleLeaderboard(interaction); break;
                    case 'leaderboard_period': await handler.handleLeaderboardPeriod(interaction); break;
                    case 'buddy': await handler.handleBuddy(interaction); break;
                    case 'nudge': await handler.handleNudge(interaction); break;
                    case 'remind': await handler.handleRemind(interaction); break;
                    case 'admin': await handler.handleAdmin(interaction); break;
                    case 'recalculate':
                        console.log("[Command] Admin triggered /recalculate");
                        await interaction.editReply({ content: '🔄 Recalculating totals...'});
                        reconcileTotals(database.db);
                        // Add checkpoint here too for good measure after manual recalc
                        database.db.pragma('wal_checkpoint(FULL)');
                         console.log("[Command] Recalculation and checkpoint complete.");
                        await interaction.editReply({ content: `✅ Totals recalculated! | PID: ${BOT_PROCESS_ID}` });
                        break;
                    case 'db_download':
                        console.log("[Command] Admin triggered /db_download");
                        await handler.handleDbDownload(interaction);
                        break;
                    default:
                        console.warn(`[Command Warn] Unhandled command: ${commandName}`);
                        await interaction.editReply({ content: "Unknown command.", flags: [MessageFlags.Ephemeral] });
                }
            }
             console.log(`[Interaction] Successfully processed command: /${interaction.commandName} for ${interaction.user.tag}`);
        } catch (err) {
            console.error(`❌ [Interaction Error] Cmd Error for /${interaction.commandName} by ${interaction.user.tag}:`, err);
            const errorReply = { content: `❌ Error processing command. Please check the bot logs or contact the administrator.`, flags: [MessageFlags.Ephemeral] };
            try {
                if (interaction.replied || interaction.deferred) {
                    await interaction.editReply(errorReply);
                } else {
                    // If deferReply failed, try a fresh reply
                    await interaction.reply(errorReply);
                }
            }
            catch (e) { console.error("❌ [Interaction Error] Error sending error reply:", e); }
        }
    });

    // Graceful Shutdown Logic
    const shutdown = (signal) => {
        console.log(`[Shutdown] Received ${signal}. Shutting down gracefully...`);
        isBotReady = false; // Stop accepting new commands
        if (database) {
            database.close(); // Close DB connection
        }
        if (client) {
            client.destroy(); // Log out Discord client
        }
        console.log("[Shutdown] Exiting process.");
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    console.log("[Startup] Attempting client login...");
    await client.login(CONFIG.token);
    // Note: The 'clientReady' event fires asynchronously after login succeeds.
    console.log("[Startup] client.login() promise resolved. Waiting for 'clientReady' event...");
}

main().catch(err => {
    console.error('❌ [FATAL ERROR] Uncaught error in main function:', err);
    process.exit(1);
});