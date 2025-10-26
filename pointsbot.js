// pointsbot.js — Full Production Build (Text-based Leaderboard)
// -------------------------------------------------------------
// Features:
// - Walking/Jogging/Running (km → points)
// - Plank (min → points), Reps (squat/kettlebell/lunge/pushup)
// - House chores (1h cooldown for cooking/sweeping/toiletcleaning, others as set)
// - Greens (sprouts/salad/smoothie)
// - Junk food deduction
// - Buddy system (guild-safe composite PK)
// - Ranks, achievements, streaks
// - Consistent, fast text-based leaderboard (from points_log)
// - SQLite (WAL), Keep-alive HTTP server (Railway-friendly)

import 'dotenv/config';
import http from 'node:http';
import {
  Client, GatewayIntentBits, REST, Routes,
  SlashCommandBuilder, EmbedBuilder, PermissionFlagsBits, MessageFlags
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

const ONE_HOUR = 60 * 60 * 1000;

const COOLDOWNS = {
  // base activities
  gym: 12 * 60 * 60 * 1000,
  badminton: 12 * 60 * 60 * 1000,
  cricket: 12 * 60 * 60 * 1000,

  // shared for all "quick exercise" based commands (distance/reps/plank)
  exercise: 6 * 60 * 60 * 1000,

  // extras
  swimming: 12 * 60 * 60 * 1000,
  yoga: 12 * 60 * 60 * 1000,

  // chores (per command, as requested)
  cooking: ONE_HOUR,
  sweeping: ONE_HOUR,
  toiletcleaning: ONE_HOUR,
  gardening: 12 * 60 * 60 * 1000,
  carwash: 12 * 60 * 60 * 1000,
};

const POINTS = {
  // fixed points
  gym: 2,
  badminton: 5,
  cricket: 5,
  swimming: 3,
  yoga: 2,

  // chores fixed
  cooking: 2,
  sweeping: 2,
  gardening: 2,
  carwash: 2,
  toiletcleaning: 5,

  // greens fixed
  sprouts: 2,
  salad: 2,
  smoothie: 2,
};

// variable rates
const KM_RATES = { walking: 0.5, jogging: 0.6, running: 0.7 };
const REP_RATES = { squat: 0.02, kettlebell: 0.2, lunge: 0.2, pushup: 0.02 };
const PLANK_RATE_PER_MIN = 1;   // 1 point per minute
const PLANK_MIN_MINUTES  = 0.75; // 45 seconds

// deductions
const DEDUCTIONS = {
  chocolate: { points: 2, emoji: '🍫', label: 'Chocolate' },
  fries:     { points: 3, emoji: '🍟', label: 'Fries' },
  soda:      { points: 2, emoji: '🥤', label: 'Soda' },
  pizza:     { points: 4, emoji: '🍕', label: 'Pizza' },
  burger:    { points: 3, emoji: '🍔', label: 'Burger' },
  sweets:    { points: 2, emoji: '🍬', label: 'Sweets' },
};

const RANKS = [
  { min:   0, name: '🆕 Rookie',   color: 0x95a5a6, next: 20  },
  { min:  20, name: '🌟 Beginner', color: 0x3498db, next: 50  },
  { min:  50, name: '💪 Athlete',  color: 0x9b59b6, next: 100 },
  { min: 100, name: '🥉 Pro',      color: 0xf39c12, next: 200 },
  { min: 200, name: '🥈 Expert',   color: 0xe67e22, next: 350 },
  { min: 350, name: '🥇 Champion', color: 0xf1c40f, next: 500 },
  { min: 500, name: '🏆 Legend',   color: 0xe74c3c, next: 1000},
  { min:1000, name: '👑 Godlike',  color: 0x8e44ad, next: null},
];

const ACHIEVEMENTS = [
  { id: 'first_points', name: '🎯 First Steps', requirement: (s) => s.total >= 1,   description: 'Earn your first point' },
  { id: 'gym_rat',      name: '💪 Gym Rat',     requirement: (s) => s.gym >= 50,    description: 'Earn 50 gym points' },
  { id: 'cardio_king',  name: '🏃 Cardio King', requirement: (s) => s.exercise >= 100, description: 'Earn 100 exercise points' },
  { id: 'streak_7',     name: '🔥 Week Warrior', requirement: (s) => s.current_streak >= 7, description: 'Maintain a 7-day streak' },
  { id: 'century_club', name: '💯 Century Club', requirement: (s) => s.total >= 100, description: 'Reach 100 total points' },
];

/* =========================
   DATABASE
========================= */
class PointsDatabase {
  constructor(dbPath) {
    try { fs.mkdirSync(path.dirname(dbPath), { recursive: true }); } catch {}
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('foreign_keys = ON');
    this.initSchema();
    this.prepareStatements();
  }

  initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS points (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL,
        total REAL NOT NULL DEFAULT 0,
        gym REAL NOT NULL DEFAULT 0,
        badminton REAL NOT NULL DEFAULT 0,
        cricket REAL NOT NULL DEFAULT 0,
        exercise REAL NOT NULL DEFAULT 0,
        swimming REAL NOT NULL DEFAULT 0,
        yoga REAL NOT NULL DEFAULT 0,
        current_streak INTEGER DEFAULT 0,
        longest_streak INTEGER DEFAULT 0,
        last_activity_date TEXT,
        created_at INTEGER DEFAULT (strftime('%s','now')),
        updated_at INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (guild_id, user_id)
      );
      CREATE TABLE IF NOT EXISTS cooldowns (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL, category TEXT NOT NULL, last_ms INTEGER NOT NULL,
        PRIMARY KEY (guild_id, user_id, category)
      );
      CREATE TABLE IF NOT EXISTS points_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL,
        category TEXT NOT NULL, amount REAL NOT NULL,
        ts INTEGER NOT NULL, reason TEXT, notes TEXT
      );
      CREATE TABLE IF NOT EXISTS buddies (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL, buddy_id TEXT,
        created_at INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (guild_id, user_id)
      );
      CREATE TABLE IF NOT EXISTS achievements (
        guild_id TEXT NOT NULL, user_id TEXT NOT NULL, achievement_id TEXT NOT NULL,
        unlocked_at INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY (guild_id, user_id, achievement_id)
      );
      CREATE INDEX IF NOT EXISTS idx_points_log_guild_ts ON points_log(guild_id, ts);
      CREATE INDEX IF NOT EXISTS idx_points_total ON points(guild_id, total DESC);
    `);
  }

  prepareStatements() {
    const S = this.stmts = {};

    S.upsertUser = this.db.prepare(`
      INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id)
      ON CONFLICT(guild_id, user_id) DO NOTHING
    `);

    S.addPoints = this.db.prepare(`
      UPDATE points
      SET total = total + @add,
          gym        = CASE WHEN @credit='gym' THEN gym + @add ELSE gym END,
          badminton  = CASE WHEN @credit='badminton' THEN badminton + @add ELSE badminton END,
          cricket    = CASE WHEN @credit='cricket' THEN cricket + @add ELSE cricket END,
          exercise   = CASE WHEN @credit='exercise' THEN exercise + @add ELSE exercise END,
          swimming   = CASE WHEN @credit='swimming' THEN swimming + @add ELSE swimming END,
          yoga       = CASE WHEN @credit='yoga' THEN yoga + @add ELSE yoga END,
          updated_at = strftime('%s','now')
      WHERE guild_id=@guild_id AND user_id=@user_id
    `);

    S.getUser = this.db.prepare(`SELECT * FROM points WHERE guild_id=? AND user_id=?`);

    S.updateStreak = this.db.prepare(`
      UPDATE points
      SET current_streak=@current_streak, longest_streak=@longest_streak, last_activity_date=@last_activity_date
      WHERE guild_id=@guild_id AND user_id=@user_id
    `);

    S.setCooldown = this.db.prepare(`
      INSERT INTO cooldowns (guild_id, user_id, category, last_ms)
      VALUES (@guild_id, @user_id, @category, @last_ms)
      ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms=excluded.last_ms
    `);
    S.getCooldown = this.db.prepare(`SELECT last_ms FROM cooldowns WHERE guild_id=? AND user_id=? AND category=?`);

    S.logPoints = this.db.prepare(`
      INSERT INTO points_log (guild_id,user_id,category,amount,ts,reason,notes)
      VALUES (?,?,?,?,?,?,?)
    `);

    // Leaderboards (ALWAYS from points_log for consistency)
    S.lbAll = this.db.prepare(`
      SELECT user_id as userId, SUM(amount) AS score
      FROM points_log
      WHERE guild_id=? AND amount>0
      GROUP BY user_id
      HAVING score>0
      ORDER BY score DESC
      LIMIT 10
    `);
    S.lbAllByCat = this.db.prepare(`
      SELECT user_id as userId, SUM(amount) AS score
      FROM points_log
      WHERE guild_id=? AND amount>0 AND category=?
      GROUP BY user_id
      HAVING score>0
      ORDER BY score DESC
      LIMIT 10
    `);
    S.lbSince = this.db.prepare(`
      SELECT user_id as userId, SUM(amount) AS score
      FROM points_log
      WHERE guild_id=? AND ts>=? AND amount>0
      GROUP BY user_id
      HAVING score>0
      ORDER BY score DESC
      LIMIT 10
    `);
    S.lbSinceByCat = this.db.prepare(`
      SELECT user_id as userId, SUM(amount) AS score
      FROM points_log
      WHERE guild_id=? AND ts>=? AND amount>0 AND category=?
      GROUP BY user_id
      HAVING score>0
      ORDER BY score DESC
      LIMIT 10
    `);

    S.topStreaks = this.db.prepare(`
      SELECT user_id as userId, current_streak as score
      FROM points
      WHERE guild_id=? AND current_streak>0
      ORDER BY current_streak DESC
      LIMIT 10
    `);

    S.selfRankAll = this.db.prepare(`
      WITH sums AS (
        SELECT user_id, SUM(amount) AS s
        FROM points_log
        WHERE guild_id=? AND amount>0
        GROUP BY user_id
      ),
      ranks AS (
        SELECT user_id, s, RANK() OVER (ORDER BY s DESC) rk FROM sums
      )
      SELECT rk as rank, s as score FROM ranks WHERE user_id=?
    `);

    S.getBuddy = this.db.prepare(`SELECT buddy_id FROM buddies WHERE guild_id=? AND user_id=?`);
    S.setBuddy = this.db.prepare(`
      INSERT INTO buddies (guild_id,user_id,buddy_id) VALUES (?,?,?)
      ON CONFLICT(guild_id,user_id) DO UPDATE SET buddy_id=excluded.buddy_id
    `);

    S.userAchievements = this.db.prepare(`SELECT achievement_id FROM achievements WHERE guild_id=? AND user_id=?`);
    S.unlockAchievement = this.db.prepare(`
      INSERT OR IGNORE INTO achievements (guild_id,user_id,achievement_id)
      VALUES (?,?,?)
    `);
  }

  // Internal: map a "logged category" to the points column to credit
  creditColumnFor(category) {
    if (['gym','badminton','cricket','swimming','yoga'].includes(category)) return category;
    // variable + micro activities credit into "exercise"
    return 'exercise';
  }

  modifyPoints({ guildId, userId, category, amount, reason=null, notes=null }) {
    this.stmts.upsertUser.run({ guild_id: guildId, user_id: userId });
    const add = Number(amount) || 0;
    if (add === 0) return [];

    // For negative "junk", we log category='junk' but we still credit column as exercise for aggregate distribution
    const credit = add > 0 ? this.creditColumnFor(category) : this.creditColumnFor('exercise');

    this.stmts.addPoints.run({
      guild_id: guildId, user_id: userId, credit, add
    });
    this.stmts.logPoints.run(
      guildId, userId, category, add,
      Math.floor(Date.now() / 1000), reason, notes
    );

    if (add > 0) {
      this.updateStreak(guildId, userId);
      return this.checkAchievements(guildId, userId);
    }
    return [];
  }

  updateStreak(guildId, userId) {
    const u = this.stmts.getUser.get(guildId, userId);
    if (!u) return;
    const today = new Date().toISOString().slice(0,10);
    if (u.last_activity_date === today) return;
    const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0,10);
    const current = (u.last_activity_date === yesterday) ? (u.current_streak||0)+1 : 1;
    const longest = Math.max(u.longest_streak||0, current);
    this.stmts.updateStreak.run({
      guild_id: guildId, user_id: userId,
      current_streak: current, longest_streak: longest, last_activity_date: today
    });
  }

  checkCooldown({ guildId, userId, category }) {
    const row = this.stmts.getCooldown.get(guildId, userId, category);
    const now = Date.now();
    const cd = COOLDOWNS[category] ?? 6 * 60 * 60 * 1000;
    if (row && now - row.last_ms < cd) return cd - (now - row.last_ms);
    return 0;
  }

  commitCooldown({ guildId, userId, category }) {
    this.stmts.setCooldown.run({ guild_id: guildId, user_id: userId, category, last_ms: Date.now() });
  }

  checkAchievements(guildId, userId) {
    const stats = this.stmts.getUser.get(guildId, userId);
    if (!stats) return [];
    const unlocked = this.stmts.userAchievements.all(guildId, userId).map(r => r.achievement_id);
    const fresh = [];
    for (const a of ACHIEVEMENTS) {
      if (!unlocked.includes(a.id) && a.requirement(stats)) {
        this.stmts.unlockAchievement.run(guildId, userId, a.id);
        fresh.push(a);
      }
    }
    return fresh;
  }

  close() { this.db.close(); }
}

/* =========================
   UTILITIES
========================= */
const fmt = (n) => (Math.round(n*10)/10).toLocaleString(undefined, { maximumFractionDigits: 1 });
const progressBar = (pct) => `${'█'.repeat(Math.floor(pct/10))}${'░'.repeat(10-Math.floor(pct/10))} ${pct}%`;
const rankFor = (total) => RANKS.reduce((acc, r) => total >= r.min ? r : acc, RANKS[0]);
function nextRank(total) {
  const cur = rankFor(total);
  if (cur.next === null) return { pct: 100, cur, need: 0 };
  const span = cur.next - cur.min, done = total - cur.min;
  return { pct: Math.max(0, Math.min(100, Math.floor((done/span)*100))), cur, need: cur.next - total };
}
const fmtCooldown = (ms) => {
  const h = Math.floor(ms/3600000), m = Math.floor((ms%3600000)/60000);
  return `${h}h ${m}m`;
};
function periodStart(period='week') {
  const now = new Date();
  switch (period) {
    case 'day':   return Math.floor(new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime()/1000);
    case 'month': return Math.floor(new Date(now.getFullYear(), now.getMonth(), 1).getTime()/1000);
    case 'year':  return Math.floor(new Date(now.getFullYear(), 0, 1).getTime()/1000);
    case 'week':
    default: {
      const day = now.getDay() || 7; // make Sunday => 7
      const d = new Date(now);
      d.setDate(now.getDate() - (day - 1)); // back to Monday
      d.setHours(0,0,0,0);
      return Math.floor(d.getTime()/1000);
    }
  }
}
const medal = (n) => (n===1?'🥇':n===2?'🥈':n===3?'🥉':`**${n}.**`);
const encourage = () => {
  const bank = [
    "🔥 Fantastic effort!", "💪 Crushing it!", "🚀 Momentum rising!",
    "🌟 Keep stacking wins!", "🏆 Champion energy!", "🎉 Well done!"
  ];
  return bank[Math.floor(Math.random()*bank.length)];
};

/* =========================
   KEEP-ALIVE
========================= */
function keepAlive() {
  http.createServer((_, res) => {
    res.writeHead(200, {'Content-Type':'text/plain'});
    res.end('OK');
  }).listen(process.env.PORT || 3000, () => console.log('✅ Keep-alive server started.'));
}

/* =========================
   COMMANDS
========================= */
function buildCommands() {
  // Fixed points (incl. chores & greens)
  const fixed = Object.keys(POINTS).map(k =>
    new SlashCommandBuilder().setName(k).setDescription(`+${POINTS[k]} points for ${k}`)
  );

  // Distance-based
  const distance = ['walking','jogging','running'].map(k =>
    new SlashCommandBuilder()
      .setName(k)
      .setDescription(`${k === 'walking' ? '🚶' : (k==='jogging' ? '🏃' : '💨')} Log ${k} by distance (${KM_RATES[k]} points/km)`)
      .addNumberOption(o => o.setName('km').setDescription('Kilometers (e.g., 2.5)').setMinValue(0.1).setRequired(true))
  );

  // Plank
  const plank = new SlashCommandBuilder()
    .setName('plank')
    .setDescription(`🧱 Log plank time (${PLANK_RATE_PER_MIN} point/min, min ${PLANK_MIN_MINUTES}m)`)
    .addNumberOption(o => o.setName('minutes').setDescription('Minutes (e.g., 1.5)').setMinValue(PLANK_MIN_MINUTES).setRequired(true));

  // Reps
  const reps = Object.keys(REP_RATES).map(k =>
    new SlashCommandBuilder()
      .setName(k)
      .setDescription(`💪 Log ${k} (${REP_RATES[k]} points per rep)`)
      .addIntegerOption(o => o.setName('reps').setDescription('Repetitions').setMinValue(1).setRequired(true))
  );

  // My score
  const myscore = new SlashCommandBuilder().setName('myscore').setDescription('🏆 Show your score, rank, and progress');

  // Leaderboard
  const allCats = [
    'all','streak',
    ...new Set([
      ...Object.keys(POINTS),
      ...Object.keys(KM_RATES),
      ...Object.keys(REP_RATES),
      'plank',
    ].filter(c => c !== 'exercise'))
  ];
  const leaderboard = new SlashCommandBuilder()
    .setName('leaderboard')
    .setDescription('📊 Show the server leaderboard')
    .addStringOption(o => o.setName('period').setDescription('Time period').setRequired(true).addChoices(
      { name:'Today', value:'day' },
      { name:'This Week', value:'week' },
      { name:'This Month', value:'month' },
      { name:'This Year', value:'year' },
      { name:'All Time', value:'all' }
    ))
    .addStringOption(o => {
      o.setName('category').setDescription('Category to rank (default: all)');
      allCats.forEach(c => o.addChoices({ name:c, value:c }));
      return o;
    });

  // Junk
  const junk = new SlashCommandBuilder()
    .setName('junk')
    .setDescription('🍕 Log junk food to deduct points')
    .addStringOption(o => {
      o.setName('item').setDescription('Item').setRequired(true);
      Object.entries(DEDUCTIONS).forEach(([k,{emoji,label}]) => o.addChoices({ name:`${emoji} ${label}`, value:k }));
      return o;
    });

  // Buddy
  const buddy = new SlashCommandBuilder()
    .setName('buddy')
    .setDescription('👯 Set or view your workout buddy')
    .addUserOption(o => o.setName('user').setDescription('Your buddy (leave empty to view)'));

  // Admin
  const adminCats = allCats.filter(c => c!=='all' && c!=='streak');
  const admin = new SlashCommandBuilder()
    .setName('admin')
    .setDescription('🛠️ Admin commands')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addSubcommand(sub => sub
      .setName('award').setDescription('Award points')
      .addUserOption(o => o.setName('user').setRequired(true).setDescription('User'))
      .addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points'))
      .addStringOption(o => {
        o.setName('category').setRequired(true).setDescription('Category');
        adminCats.forEach(c => o.addChoices({ name:c, value:c }));
        return o;
      })
      .addStringOption(o => o.setName('reason').setDescription('Reason')))
    .addSubcommand(sub => sub
      .setName('deduct').setDescription('Deduct points')
      .addUserOption(o => o.setName('user').setRequired(true).setDescription('User'))
      .addNumberOption(o => o.setName('amount').setRequired(true).setDescription('Points'))
      .addStringOption(o => {
        o.setName('category').setRequired(true).setDescription('Category');
        adminCats.forEach(c => o.addChoices({ name:c, value:c }));
        return o;
      })
      .addStringOption(o => o.setName('reason').setDescription('Reason')));

  return [
    ...fixed, ...distance, plank, ...reps, myscore, leaderboard, junk, buddy, admin
  ].map(c => c.toJSON());
}

/* =========================
   COMMAND HANDLERS
========================= */
class CommandHandler {
  constructor(db) { this.db = db; }

  // Fixed-point + chores + greens
  async handleFixed(interaction, category) {
    const { guild, user } = interaction;
    const cooldownKey = category;
    const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
    if (remaining > 0) {
      return interaction.reply({ content:`⏳ Cooldown active for **${category}**. Try again in **${fmtCooldown(remaining)}**.`, ephemeral:true });
    }

    const amt = POINTS[category] || 0;
    const achievements = this.db.modifyPoints({ guildId:guild.id, userId:user.id, category, amount:amt, reason:`claim:${category}` });
    this.db.commitCooldown({ guildId:guild.id, userId:user.id, category: cooldownKey });

    const row = this.db.stmts.getUser.get(guild.id, user.id);
    const { cur, need } = nextRank(row.total);
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setDescription(`${user.toString()} **+${fmt(amt)}** for **${category}**! ${encourage()}`)
      .addFields(
        { name:'New Total', value:`🏆 ${fmt(row.total)}`, inline:true },
        { name:'Rank', value:cur.name, inline:true }
      )
      .setThumbnail(user.displayAvatarURL());
    if (need > 0) embed.setFooter({ text:`${fmt(need)} points to the next rank!` });

    await interaction.reply({ embeds:[embed] });

    if (achievements.length) {
      await interaction.followUp({
        embeds: [new EmbedBuilder()
          .setColor(0xFFD700)
          .setTitle('🏆 Achievement Unlocked!')
          .setDescription(achievements.map(a => `**${a.name}** — ${a.description}`).join('\n'))],
        ephemeral: true
      });
    }
  }

  // Distance
  async handleDistance(interaction, which) {
    const { guild, user, options } = interaction;
    const km = options.getNumber('km', true);
    const rate = KM_RATES[which];
    const points = km * rate;

    const cooldownKey = 'exercise'; // shared
    const remaining = this.db.checkCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });
    if (remaining > 0) return interaction.reply({ content:`⏳ Cooldown active for distance exercises. Try again in **${fmtCooldown(remaining)}**.`, ephemeral:true });

    const achievements = this.db.modifyPoints({
      guildId:guild.id, userId:user.id, category:which, amount:points,
      reason:`distance:${which}`, notes:`${km}km`
    });
    this.db.commitCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });

    const row = this.db.stmts.getUser.get(guild.id, user.id);
    const { cur, need } = nextRank(row.total);
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setDescription(`${user.toString()} logged **${fmt(km)} km** ${which} → **+${fmt(points)}**! ${encourage()}`)
      .addFields(
        { name:'New Total', value:`🏆 ${fmt(row.total)}`, inline:true },
        { name:'Rank', value:cur.name, inline:true }
      )
      .setThumbnail(user.displayAvatarURL());
    if (need > 0) embed.setFooter({ text:`${fmt(need)} points to the next rank!` });

    await interaction.reply({ embeds:[embed] });

    if (achievements.length) {
      await interaction.followUp({
        embeds: [new EmbedBuilder()
          .setColor(0xFFD700)
          .setTitle('🏆 Achievement Unlocked!')
          .setDescription(achievements.map(a => `**${a.name}** — ${a.description}`).join('\n'))],
        ephemeral: true
      });
    }
  }

  // Plank
  async handlePlank(interaction) {
    const { guild, user, options } = interaction;
    const minutes = options.getNumber('minutes', true);
    const points = minutes * PLANK_RATE_PER_MIN;

    const cooldownKey = 'exercise';
    const remaining = this.db.checkCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });
    if (remaining > 0) return interaction.reply({ content:`⏳ Cooldown active for exercises. Try again in **${fmtCooldown(remaining)}**.`, ephemeral:true });

    const achievements = this.db.modifyPoints({ guildId:guild.id, userId:user.id, category:'plank', amount:points, reason:'time:plank', notes:`${minutes} min` });
    this.db.commitCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });

    const row = this.db.stmts.getUser.get(guild.id, user.id);
    const { cur, need } = nextRank(row.total);
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setDescription(`${user.toString()} held a **plank** for **${fmt(minutes)} min** → **+${fmt(points)}**! ${encourage()}`)
      .addFields(
        { name:'New Total', value:`🏆 ${fmt(row.total)}`, inline:true },
        { name:'Rank', value:cur.name, inline:true }
      )
      .setThumbnail(user.displayAvatarURL());
    if (need > 0) embed.setFooter({ text:`${fmt(need)} points to the next rank!` });

    await interaction.reply({ embeds:[embed] });

    if (achievements.length) {
      await interaction.followUp({
        embeds: [new EmbedBuilder()
          .setColor(0xFFD700)
          .setTitle('🏆 Achievement Unlocked!')
          .setDescription(achievements.map(a => `**${a.name}** — ${a.description}`).join('\n'))],
        ephemeral: true
      });
    }
  }

  // Reps
  async handleReps(interaction, which) {
    const { guild, user, options } = interaction;
    const reps = options.getInteger('reps', true);
    const rate = REP_RATES[which];
    const points = reps * rate;

    const cooldownKey = 'exercise';
    const remaining = this.db.checkCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });
    if (remaining > 0) return interaction.reply({ content:`⏳ Cooldown active for exercises. Try again in **${fmtCooldown(remaining)}**.`, ephemeral:true });

    const achievements = this.db.modifyPoints({ guildId:guild.id, userId:user.id, category:which, amount:points, reason:`reps:${which}`, notes:`${reps} reps` });
    this.db.commitCooldown({ guildId:guild.id, userId:user.id, category:cooldownKey });

    const row = this.db.stmts.getUser.get(guild.id, user.id);
    const { cur, need } = nextRank(row.total);
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setDescription(`${user.toString()} did **${reps} ${which}** → **+${fmt(points)}**! ${encourage()}`)
      .addFields(
        { name:'New Total', value:`🏆 ${fmt(row.total)}`, inline:true },
        { name:'Rank', value:cur.name, inline:true }
      )
      .setThumbnail(user.displayAvatarURL());
    if (need > 0) embed.setFooter({ text:`${fmt(need)} points to the next rank!` });

    await interaction.reply({ embeds:[embed] });

    if (achievements.length) {
      await interaction.followUp({
        embeds: [new EmbedBuilder()
          .setColor(0xFFD700)
          .setTitle('🏆 Achievement Unlocked!')
          .setDescription(achievements.map(a => `**${a.name}** — ${a.description}`).join('\n'))],
        ephemeral: true
      });
    }
  }

  async handleJunk(interaction) {
    const { guild, user, options } = interaction;
    const key = options.getString('item', true);
    const d = DEDUCTIONS[key];

    // Log with the exact junk key; negative points apply
    this.db.modifyPoints({ guildId:guild.id, userId:user.id, category:'junk', amount:-d.points, reason:`junk:${key}` });

    const row = this.db.stmts.getUser.get(guild.id, user.id);
    const embed = new EmbedBuilder()
      .setColor(0xED4245)
      .setDescription(`${user.toString()} logged ${d.emoji} **${d.label}**, deducting **-${fmt(d.points)}**. Back on track!`)
      .addFields({ name:'New Total', value:`🏆 ${fmt(row.total)}`, inline:true })
      .setThumbnail(user.displayAvatarURL());
    return interaction.reply({ embeds:[embed] });
  }

  async handleMyScore(interaction) {
    const { guild, user } = interaction;
    const row = this.db.stmts.getUser.get(guild.id, user.id) || { total:0, current_streak:0 };
    const { pct, cur, need } = nextRank(row.total);
    const achievements = this.db.stmts.userAchievements.all(guild.id, user.id).map(r => r.achievement_id);
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setAuthor({ name: user.displayName, iconURL: user.displayAvatarURL() })
      .setTitle(`Rank: ${cur.name}`)
      .addFields(
        { name:'Total Points', value: fmt(row.total), inline:true },
        { name:'Current Streak', value: `🔥 ${row.current_streak||0} days`, inline:true },
        { name:'Progress', value: progressBar(pct), inline:false },
        { name:'Achievements', value: achievements.length ? achievements.map(id => `**${ACHIEVEMENTS.find(a=>a.id===id)?.name||id}**`).join(', ') : 'None yet!' }
      );
    if (need > 0) embed.setFooter({ text:`${fmt(need)} points to the next rank!` });
    return interaction.reply({ embeds:[embed], ephemeral:true });
  }

  async handleLeaderboard(interaction) {
    // Defer to avoid "Unknown interaction" if data fetch is slow
    await interaction.deferReply();

    const { guild, user, options } = interaction;
    const period = options.getString('period', true);
    const cat = options.getString('category') || 'all';

    try {
      let rows = [];
      let subtitle = '';
      let selfRank = null;

      if (cat === 'streak') {
        rows = this.db.stmts.topStreaks.all(guild.id);
        subtitle = 'Top Current Streaks';
      } else {
        const since = period === 'all' ? null : periodStart(period);
        subtitle = `${period === 'all' ? 'All Time' : {
          day: 'Today', week: 'This Week', month:'This Month', year:'This Year'
        }[period]} • ${cat==='all' ? 'Total' : cat}`;

        if (since == null) { // All time
          rows = (cat === 'all')
            ? this.db.stmts.lbAll.all(guild.id)
            : this.db.stmts.lbAllByCat.all(guild.id, cat);
          if (cat === 'all') {
            const my = this.db.stmts.selfRankAll.get(guild.id, user.id);
            if (my) selfRank = { userId: user.id, rank: my.rank, score: my.score };
          }
        } else { // Periodic
          rows = (cat === 'all')
            ? this.db.stmts.lbSince.all(guild.id, since)
            : this.db.stmts.lbSinceByCat.all(guild.id, since, cat);
        }
      }

      if (!rows.length) {
        return interaction.editReply({ content: '📊 No data yet for this leaderboard. Keep logging!' });
      }

      rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
      const userIds = rows.map(r => r.userId);
      const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());

      const lines = rows.map(r => {
        const member = members.get(r.userId);
        const name = member?.displayName || 'Unknown User';
        const score = fmt(r.score);
        const suffix = (cat === 'streak') ? ' days' : '';
        return `${medal(r.rank)} ${name} — \`${score}${suffix}\``;
      });

      const embed = new EmbedBuilder()
        .setTitle(`🏆 Leaderboard: ${subtitle}`)
        .setColor(0x3498db)
        .setDescription(lines.join('\n'))
        .setTimestamp();

      if (selfRank && !rows.some(r => r.userId === user.id)) {
        embed.setFooter({ text: `Your All-Time Rank: #${selfRank.rank} with ${fmt(selfRank.score)} points` });
      }

      return interaction.editReply({ embeds: [embed] });
    } catch (e) {
      console.error('Leaderboard error:', e);
      return interaction.editReply({ content: '❌ Sorry, there was an error generating the leaderboard.' });
    }
  }

  async handleBuddy(interaction) {
    const { guild, user, options } = interaction;
    const target = options.getUser('user');
    if (!target) {
      const b = this.db.stmts.getBuddy.get(guild.id, user.id);
      return interaction.reply({
        content: b?.buddy_id ? `👯 Your buddy is <@${b.buddy_id}>.` : 'You haven’t set a buddy yet. Use \`/buddy user:@name\`.',
        ephemeral: true
      });
    }
    if (target.id === user.id) return interaction.reply({ content:'Pick someone else 🤝', ephemeral:true });
    this.db.stmts.setBuddy.run(guild.id, user.id, target.id);
    return interaction.reply({ content:`👯 Buddy set! You & <@${target.id}> got this.` });
  }

  async handleAdmin(interaction) {
    await interaction.deferReply({ ephemeral: true });
    const { guild, user, options } = interaction;
    const sub = options.getSubcommand();
    const target = options.getUser('user', true);
    const amount = options.getNumber('amount', true);
    const category = options.getString('category', true);
    const reason = options.getString('reason') || `Admin by ${user.tag}`;

    if (sub === 'award') {
      this.db.modifyPoints({ guildId:guild.id, userId:target.id, category, amount, reason:'admin:award', notes:reason });
      return interaction.editReply({ content:`✅ Awarded **${fmt(amount)}** ${category} points to <@${target.id}>.` });
    } else {
      this.db.modifyPoints({ guildId:guild.id, userId:target.id, category, amount:-amount, reason:'admin:deduct', notes:reason });
      return interaction.editReply({ content:`✅ Deducted **${fmt(amount)}** ${category} points from <@${target.id}>.` });
    }
  }
}

/* =========================
   MAIN
========================= */
async function main() {
  keepAlive();

  if (!CONFIG.token || !CONFIG.appId) {
    console.error('❌ Missing DISCORD_TOKEN or APPLICATION_ID');
    process.exit(1);
  }

  const db = new PointsDatabase(CONFIG.dbFile);
  const handler = new CommandHandler(db);

  // Register slash commands
  const rest = new REST({ version: '10' }).setToken(CONFIG.token);
  try {
    console.log('🔄 Registering slash commands…');
    const route = CONFIG.devGuildId
      ? Routes.applicationGuildCommands(CONFIG.appId, CONFIG.devGuildId)
      : Routes.applicationCommands(CONFIG.appId);
    await rest.put(route, { body: buildCommands() });
    console.log('✅ Commands registered');
  } catch (e) {
    console.error('❌ Command registration failed:', e);
    process.exit(1);
  }

  const client = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers]
  });

  client.once('ready', (c) => {
    console.log(`🤖 Logged in as ${c.user.tag}`);
    console.log(`📊 Serving ${c.guilds.cache.size} server(s)`);
  });

  client.on('interactionCreate', async (i) => {
    if (!i.isChatInputCommand() || !i.guild) return;
    const name = i.commandName;

    try {
      // Fixed (POINTS) includes chores & greens
      if (POINTS[name] !== undefined) return handler.handleFixed(i, name);

      // Distance
      if (['walking','jogging','running'].includes(name)) return handler.handleDistance(i, name);

      // Plank/Reps
      if (name === 'plank') return handler.handlePlank(i);
      if (Object.keys(REP_RATES).includes(name)) return handler.handleReps(i, name);

      // Utility
      if (name === 'junk') return handler.handleJunk(i);
      if (name === 'myscore') return handler.handleMyScore(i);
      if (name === 'leaderboard') return handler.handleLeaderboard(i);
      if (name === 'buddy') return handler.handleBuddy(i);
      if (name === 'admin') return handler.handleAdmin(i);

    } catch (err) {
      console.error(`❌ Error handling command ${name}:`, err);
      const response = { content:'❌ Oops! Something went wrong processing your command.', flags: [MessageFlags.Ephemeral] };
      try {
        if (i.replied || i.deferred) {
          await i.followUp(response);
        } else {
          await i.reply(response);
        }
      } catch (e) {
        console.error('Failed to send error reply:', e);
      }
    }
  });

  process.on('SIGINT', () => { console.log('🛑 SIGINT'); db.close(); client.destroy(); process.exit(0); });
  process.on('SIGTERM', () => { console.log('🛑 SIGTERM'); db.close(); client.destroy(); process.exit(0); });

  await client.login(CONFIG.token);
}

main().catch(e => {
  console.error('❌ Fatal:', e);
  process.exit(1);
});
