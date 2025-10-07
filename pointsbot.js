import "dotenv/config";
import {
  Client, GatewayIntentBits, Partials, REST, Routes,
  SlashCommandBuilder, PermissionFlagsBits, ChannelType, EmbedBuilder, AttachmentBuilder
} from "discord.js";
import Database from "better-sqlite3";
import fs from "node:fs";
import path from "node:path";
import { createCanvas, loadImage } from "@napi-rs/canvas";

/* =========================
   CONFIG & CONSTANTS
========================= */
const APP_ID       = (process.env.APPLICATION_ID  || "").trim();
const TOKEN        = (process.env.DISCORD_TOKEN   || "").trim();
const DEV_GUILD_ID = (process.env.DEV_GUILD_ID    || "").trim();
const DB_FILE      = (process.env.DB_PATH         || "points.db").trim();

const COOLDOWNS = { // ms
  gym:        12 * 60 * 60 * 1000,
  badminton:  12 * 60 * 60 * 1000,
  cricket:    12 * 60 * 60 * 1000,
  exercise:    6 * 60 * 60 * 1000
};
const POINTS = { gym: 2, badminton: 5, cricket: 5, exercise: 1 };

const SUCCESS_MESSAGES = [
  "🔥 On fire! Keep crushing it!",
  "💪 Beast mode activated!",
  "⚡ Lightning fast progress!",
  "🚀 To the moon and back!",
  "👑 Absolute legend status!",
  "🌟 Star performer right here!",
  "💎 Diamond dedication!",
  "🎯 Bullseye! Perfect form!",
  "🦾 Unstoppable force!",
  "🏆 Champion mindset!"
];

const RANKS = [
  { min: 0,    name: "🆕 Rookie",   color: 0x95a5a6, next: 20  },
  { min: 20,   name: "🌟 Beginner", color: 0x3498db, next: 50  },
  { min: 50,   name: "💪 Athlete",  color: 0x9b59b6, next: 100 },
  { min: 100,  name: "🥉 Pro",      color: 0xf39c12, next: 200 },
  { min: 200,  name: "🥈 Expert",   color: 0xe67e22, next: 350 },
  { min: 350,  name: "🥇 Champion", color: 0xf1c40f, next: 500 },
  { min: 500,  name: "🏆 Legend",   color: 0xe74c3c, next: 1000},
  { min: 1000, name: "👑 Godlike",  color: 0x8e44ad, next: null}
];

const WEEKLY_CHALLENGES = [
  { id: "gym_warrior", name: "💪 Gym Warrior",   target: "gym",          goal: 15, reward: 25, emoji: "💪", rewardCat: "gym" },
  { id: "cardio_king", name: "🏃 Cardio King",   target: "exercise",     goal: 25, reward: 20, emoji: "🏃", rewardCat: "exercise" },
  { id: "sport_star",  name: "🏸 Sport Star",    target: "total_sports", goal: 30, reward: 30, emoji: "🏸", rewardCat: "exercise" },
  { id: "all_rounder", name: "🌟 All-Rounder",   target: "total",        goal: 60, reward: 35, emoji: "🌟", rewardCat: "exercise" }
];

const MEDAL = (pos) => (pos===1 ? "🥇" : pos===2 ? "🥈" : pos===3 ? "🥉" : "🏃");

/* =========================
   DB INIT & SCHEMA
========================= */
try { fs.mkdirSync(path.dirname(DB_FILE), { recursive: true }); } catch {}
const db = new Database(DB_FILE);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

db.prepare(`
  CREATE TABLE IF NOT EXISTS points (
    guild_id TEXT NOT NULL,
    user_id  TEXT NOT NULL,
    total    INTEGER NOT NULL DEFAULT 0,
    gym      INTEGER NOT NULL DEFAULT 0,
    badminton INTEGER NOT NULL DEFAULT 0,
    cricket  INTEGER NOT NULL DEFAULT 0,
    exercise INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (guild_id, user_id)
  )
`).run();

db.prepare(`
  CREATE TABLE IF NOT EXISTS cooldowns (
    guild_id TEXT NOT NULL,
    user_id  TEXT NOT NULL,
    category TEXT NOT NULL,
    last_ms  INTEGER NOT NULL,
    PRIMARY KEY (guild_id, user_id, category)
  )
`).run();

db.prepare(`
  CREATE TABLE IF NOT EXISTS guild_config (
    guild_id TEXT PRIMARY KEY,
    checkins_channel_id TEXT,
    audit_channel_id TEXT,
    gym_cooldown_ms INTEGER,
    badminton_cooldown_ms INTEGER,
    cricket_cooldown_ms INTEGER,
    exercise_cooldown_ms INTEGER
  )
`).run();

/* Streaks & Achievements */
db.prepare(`
  CREATE TABLE IF NOT EXISTS streaks (
    guild_id TEXT,
    user_id TEXT,
    category TEXT,
    current_streak INTEGER DEFAULT 0,
    longest_streak INTEGER DEFAULT 0,
    last_activity_date TEXT,
    PRIMARY KEY (guild_id, user_id, category)
  )
`).run();

db.prepare(`
  CREATE TABLE IF NOT EXISTS achievements (
    guild_id TEXT,
    user_id TEXT,
    achievement_id TEXT,
    unlocked_at INTEGER,
    PRIMARY KEY (guild_id, user_id, achievement_id)
  )
`).run();

/* Historical log for period leaderboards & weekly challenges */
db.prepare(`
  CREATE TABLE IF NOT EXISTS points_log (
    guild_id TEXT, user_id TEXT, category TEXT, amount INTEGER, ts INTEGER
  )
`).run();

/* Weekly challenge claim dedupe */
db.prepare(`
  CREATE TABLE IF NOT EXISTS challenge_claims (
    guild_id TEXT, user_id TEXT, challenge_id TEXT, week_start TEXT, claimed_at INTEGER,
    PRIMARY KEY (guild_id, user_id, challenge_id, week_start)
  )
`).run();

/* Reminders & Buddy (per-guild composite PK) */
db.prepare(`
  CREATE TABLE IF NOT EXISTS reminders (
    guild_id TEXT, user_id TEXT, activity TEXT, due_at INTEGER, every_hours INTEGER
  )
`).run();

db.prepare(`
  CREATE TABLE IF NOT EXISTS buddies (
    guild_id TEXT,
    user_id  TEXT,
    buddy_id TEXT,
    PRIMARY KEY (guild_id, user_id)
  )
`).run();

/* SQUADS */
db.prepare(`
  CREATE TABLE IF NOT EXISTS squads (
    squad_id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at INTEGER NOT NULL
  )
`).run();

db.prepare(`
  CREATE TABLE IF NOT EXISTS squad_members (
    guild_id TEXT NOT NULL,
    squad_id INTEGER NOT NULL,
    user_id TEXT NOT NULL,
    joined_at INTEGER NOT NULL,
    PRIMARY KEY (guild_id, user_id),
    FOREIGN KEY (squad_id) REFERENCES squads(squad_id) ON DELETE CASCADE
  )
`).run();

/* Prepared statements */
const upsertUser = db.prepare(`
  INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id)
  ON CONFLICT(guild_id, user_id) DO NOTHING
`);
const addPointsStmt = db.prepare(`
  UPDATE points
  SET total = total + @add,
      gym       = CASE WHEN @category = "gym" THEN gym + @add ELSE gym END,
      badminton = CASE WHEN @category = "badminton" THEN badminton + @add ELSE badminton END,
      cricket   = CASE WHEN @category = "cricket" THEN cricket + @add ELSE cricket END,
      exercise  = CASE WHEN @category = "exercise" THEN exercise + @add ELSE exercise END
  WHERE guild_id = @guild_id AND user_id = @user_id
`);
const insertLogStmt = db.prepare(`INSERT INTO points_log (guild_id,user_id,category,amount,ts) VALUES (?,?,?,?,?)`);
const getUserStmt      = db.prepare(`SELECT * FROM points WHERE guild_id=? AND user_id=?`);
const setCooldownStmt  = db.prepare(`
  INSERT INTO cooldowns (guild_id, user_id, category, last_ms)
  VALUES (@guild_id, @user_id, @category, @last_ms)
  ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms=excluded.last_ms
`);
const getCooldownStmt  = db.prepare(`SELECT last_ms FROM cooldowns WHERE guild_id=? AND user_id=? AND category=?`);
const upsertConfig     = db.prepare(`
  INSERT INTO guild_config (guild_id, checkins_channel_id, audit_channel_id,
    gym_cooldown_ms, badminton_cooldown_ms, cricket_cooldown_ms, exercise_cooldown_ms)
  VALUES (@guild_id, @checkins_channel_id, @audit_channel_id, @gym, @badminton, @cricket, @exercise)
  ON CONFLICT(guild_id) DO UPDATE SET
    checkins_channel_id=COALESCE(excluded.checkins_channel_id, guild_config.checkins_channel_id),
    audit_channel_id=COALESCE(excluded.audit_channel_id, guild_config.audit_channel_id),
    gym_cooldown_ms=COALESCE(excluded.gym_cooldown_ms, guild_config.gym_cooldown_ms),
    badminton_cooldown_ms=COALESCE(excluded.badminton_cooldown_ms, guild_config.badminton_cooldown_ms),
    cricket_cooldown_ms=COALESCE(excluded.cricket_cooldown_ms, guild_config.cricket_cooldown_ms),
    exercise_cooldown_ms=COALESCE(excluded.exercise_cooldown_ms, guild_config.exercise_cooldown_ms)
`);
const readConfig       = db.prepare(`SELECT * FROM guild_config WHERE guild_id=?`);

const getStreak        = db.prepare(`SELECT * FROM streaks WHERE guild_id=? AND user_id=? AND category=?`);
const upsertStreak     = db.prepare(`
  INSERT INTO streaks (guild_id,user_id,category,current_streak,longest_streak,last_activity_date)
  VALUES (@guild_id,@user_id,@category,@current,@longest,@date)
  ON CONFLICT(guild_id,user_id,category) DO UPDATE SET
    current_streak=excluded.current_streak,
    longest_streak=excluded.longest_streak,
    last_activity_date=excluded.last_activity_date
`);
const hasAchievement   = db.prepare(`SELECT 1 FROM achievements WHERE guild_id=? AND user_id=? AND achievement_id=?`);
const addAchievement   = db.prepare(`INSERT INTO achievements (guild_id,user_id,achievement_id,unlocked_at) VALUES (?,?,?,?)`);

const topAllTimeStmt   = db.prepare(`SELECT user_id,total FROM points WHERE guild_id=? ORDER BY total DESC LIMIT 1`);
const guildTotalsStmt  = db.prepare(`SELECT SUM(total) as t, SUM(gym) as g, SUM(badminton) as b, SUM(cricket) as c, SUM(exercise) as e FROM points WHERE guild_id=?`);

/* Squad helpers */
const createSquadStmt  = db.prepare(`INSERT INTO squads (guild_id, owner_id, name, created_at) VALUES (?,?,?,?)`);
const getSquadByName   = db.prepare(`SELECT * FROM squads WHERE guild_id=? AND LOWER(name)=LOWER(?)`);
const getSquadById     = db.prepare(`SELECT * FROM squads WHERE guild_id=? AND squad_id=?`);
const getUserSquadRow  = db.prepare(`SELECT s.* FROM squad_members m JOIN squads s ON s.squad_id=m.squad_id AND s.guild_id=m.guild_id WHERE m.guild_id=? AND m.user_id=?`);
const addMemberStmt    = db.prepare(`INSERT INTO squad_members (guild_id,squad_id,user_id,joined_at) VALUES (?,?,?,?)`);
const removeMemberStmt = db.prepare(`DELETE FROM squad_members WHERE guild_id=? AND user_id=?`);
const renameSquadStmt  = db.prepare(`UPDATE squads SET name=? WHERE guild_id=? AND squad_id=?`);
const deleteSquadStmt  = db.prepare(`DELETE FROM squads WHERE guild_id=? AND squad_id=?`);
const listSquadMembers = db.prepare(`SELECT user_id FROM squad_members WHERE guild_id=? AND squad_id=?`);

/* =========================
   UTILITIES
========================= */
const toYMD = (d) => d.toISOString().slice(0,10);
function isoWeekStart(date = new Date()) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7; // 1..7 (Mon..Sun)
  if (day !== 1) d.setUTCDate(d.getUTCDate() - (day - 1));
  return toYMD(d);
}
function monthStart(date = new Date()) {
  return toYMD(new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)));
}
function isYesterday(isoPrev, now) {
  if (!isoPrev) return false;
  const prev = new Date(isoPrev + "T00:00:00Z");
  const y = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()-1));
  return prev.getUTCFullYear()===y.getUTCFullYear() && prev.getUTCMonth()===y.getUTCMonth() && prev.getUTCDate()===y.getUTCDate();
}
const bar = (pct) => `${"█".repeat(Math.floor(pct/10))}${"░".repeat(10-Math.floor(pct/10))} ${pct}%`;
function getUserRank(total) {
  let last = RANKS[0];
  for (const r of RANKS) { if (total >= r.min) last = r; }
  return last;
}
function nextRankProgress(total) {
  const cur = getUserRank(total);
  if (cur.next == null) return { pct: 100, cur, need: 0 };
  const span = cur.next - cur.min;
  const done = total - cur.min;
  const pct = Math.max(0, Math.min(100, Math.floor((done/span)*100)));
  return { pct, cur, need: cur.next - total };
}
function getRandomSuccess() { return SUCCESS_MESSAGES[Math.floor(Math.random()*SUCCESS_MESSAGES.length)]; }

function sumSince(guildId, sinceTs, category /* "all" for total */) {
  if (category === "all") {
    return db.prepare(`SELECT user_id, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts>=? GROUP BY user_id ORDER BY score DESC LIMIT 10`)
             .all(guildId, sinceTs);
  }
  return db.prepare(`SELECT user_id, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts>=? AND category=? GROUP BY user_id ORDER BY score DESC LIMIT 10`)
           .all(guildId, sinceTs, category);
}
function sumForUserSince(guildId, userId, sinceTs, category /* "total" or cat */) {
  if (category === "total") {
    return db.prepare(`SELECT COALESCE(SUM(amount),0) AS s FROM points_log WHERE guild_id=? AND user_id=? AND ts>=?`)
             .get(guildId, userId, sinceTs).s || 0;
  }
  return db.prepare(`SELECT COALESCE(SUM(amount),0) AS s FROM points_log WHERE guild_id=? AND user_id=? AND ts>=? AND category=?`)
           .get(guildId, userId, sinceTs, category).s || 0;
}
function sumForUsersSince(guildId, userIds, sinceTs, category /* "all" or cat */) {
  if (!userIds.length) return [];
  const placeholders = userIds.map(()=>"?").join(",");
  if (category === "all") {
    return db.prepare(`SELECT user_id, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts>=? AND user_id IN (${placeholders}) GROUP BY user_id`)
             .all(guildId, sinceTs, ...userIds);
  }
  return db.prepare(`SELECT user_id, SUM(amount) AS score FROM points_log WHERE guild_id=? AND ts>=? AND category=? AND user_id IN (${placeholders}) GROUP BY user_id`)
           .all(guildId, sinceTs, category, ...userIds);
}

/* =========================
   CORE AWARD PIPELINE
========================= */
function ensureUserRow(guildId, userId) {
  upsertUser.run({ guild_id: guildId, user_id: userId });
}
function addPoints({ guildId, userId, category, amount }) {
  ensureUserRow(guildId, userId);
  addPointsStmt.run({ guild_id: guildId, user_id: userId, category, add: amount });
  insertLogStmt.run(guildId, userId, category, amount, Date.now());

  // Update streaks & achievements
  const now = new Date();
  const today = toYMD(now);
  const st = getStreak.get(guildId, userId, category);
  let current = 1, longest = 1;
  if (st) {
    if (st.last_activity_date === today) {
      current = st.current_streak; longest = st.longest_streak;
    } else if (isYesterday(st.last_activity_date, now)) {
      current = st.current_streak + 1; longest = Math.max(st.longest_streak, current);
    } else {
      current = 1; longest = Math.max(st.longest_streak || 1, 1);
    }
  }
  upsertStreak.run({ guild_id: guildId, user_id: userId, category, current, longest, date: today });

  // Achievements (sample set)
  if (current >= 3 && !hasAchievement.get(guildId, userId, "fire_starter")) {
    addAchievement.run(guildId, userId, "fire_starter", Date.now());
  }
  const row = getUserStmt.get(guildId, userId);
  if ((row?.gym || 0) >= 50 && !hasAchievement.get(guildId, userId, "gym_warrior")) {
    addAchievement.run(guildId, userId, "gym_warrior", Date.now());
  }
  const sportsPoints = (row?.badminton||0)+(row?.cricket||0);
  if (sportsPoints >= 75 && !hasAchievement.get(guildId, userId, "sports_star")) {
    addAchievement.run(guildId, userId, "sports_star", Date.now());
  }
  if ((row?.total || 0) >= 100 && !hasAchievement.get(guildId, userId, "century_club")) {
    addAchievement.run(guildId, userId, "century_club", Date.now());
  }
  const top = topAllTimeStmt.get(guildId);
  if (top && top.user_id === userId && !hasAchievement.get(guildId, userId, "champion")) {
    addAchievement.run(guildId, userId, "champion", Date.now());
  }
}
function deductPoints({ guildId, userId, category, amount }) {
  ensureUserRow(guildId, userId);
  const row = getUserStmt.get(guildId, userId);
  if (!row) return { deducted: 0, row: null };

  const current = Math.max(0, row[category] || 0);
  const deducted = Math.min(amount, current);
  if (deducted <= 0) return { deducted: 0, row };

  const next = {
    gym: category === "gym" ? Math.max(0, row.gym - deducted) : Math.max(0, row.gym),
    badminton: category === "badminton" ? Math.max(0, row.badminton - deducted) : Math.max(0, row.badminton),
    cricket: category === "cricket" ? Math.max(0, row.cricket - deducted) : Math.max(0, row.cricket),
    exercise: category === "exercise" ? Math.max(0, row.exercise - deducted) : Math.max(0, row.exercise)
  };
  next.total = Math.max(0, next.gym + next.badminton + next.cricket + next.exercise);

  db.prepare(`
    UPDATE points
    SET total=@total, gym=@gym, badminton=@badminton, cricket=@cricket, exercise=@exercise
    WHERE guild_id=@guild_id AND user_id=@user_id
  `).run({ ...next, guild_id: guildId, user_id: userId });

  insertLogStmt.run(guildId, userId, category, -deducted, Date.now());
  return { deducted, row: getUserStmt.get(guildId, userId) };
}
function clearUserPoints(guildId, userId) {
  ensureUserRow(guildId, userId);
  const row = getUserStmt.get(guildId, userId);
  if (!row) return { cleared: 0, row: null };

  const now = Date.now();
  const cats = ["gym", "badminton", "cricket", "exercise"];
  for (const cat of cats) {
    const val = row[cat] || 0;
    if (val > 0) insertLogStmt.run(guildId, userId, cat, -val, now);
  }

  db.prepare(`
    UPDATE points
    SET total=0, gym=0, badminton=0, cricket=0, exercise=0
    WHERE guild_id=? AND user_id=?
  `).run(guildId, userId);

  return { cleared: row.total || 0, row: getUserStmt.get(guildId, userId) };
}

function getEffectiveCooldownMs(guildId, category) {
  const cfg = readConfig.get(guildId);
  if (!cfg) return COOLDOWNS[category];
  const map = {
    gym: cfg.gym_cooldown_ms ?? COOLDOWNS.gym,
    badminton: cfg.badminton_cooldown_ms ?? COOLDOWNS.badminton,
    cricket: cfg.cricket_cooldown_ms ?? COOLDOWNS.cricket,
    exercise: cfg.exercise_cooldown_ms ?? COOLDOWNS.exercise
  };
  return map[category];
}
function checkCooldown({ guildId, userId, category }) {
  const row = getCooldownStmt.get(guildId, userId, category);
  const now = Date.now();
  const cd = getEffectiveCooldownMs(guildId, category);
  if (row && now - row.last_ms < cd) return cd - (now - row.last_ms);
  return 0;
}
function commitCooldown({ guildId, userId, category }) {
  setCooldownStmt.run({ guild_id: guildId, user_id: userId, category, last_ms: Date.now() });
}

/* =========================
   AUDIT
========================= */
async function auditLog(guild, description) {
  const cfg = readConfig.get(guild.id);
  if (!cfg?.audit_channel_id) return;
  const ch = guild.channels.cache.get(cfg.audit_channel_id) || await guild.channels.fetch(cfg.audit_channel_id).catch(() => null);
  if (!ch) return;
  const embed = new EmbedBuilder().setColor(0x44d17a).setDescription(description).setTimestamp(new Date());
  ch.send({ embeds: [embed] }).catch(() => {});
}

/* =========================
   DISCORD CLIENT
========================= */
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel]
});

/* =========================
   PRETTY LEADERBOARD RENDER (Canvas)
========================= */
function drawRoundedRect(ctx, x, y, w, h, r) {
  const rr = Math.min(r, h/2, w/2);
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.arcTo(x + w, y, x + w, y + h, rr);
  ctx.arcTo(x + w, y + h, x, y + h, rr);
  ctx.arcTo(x, y + h, x, y, rr);
  ctx.arcTo(x, y, x + w, y, rr);
  ctx.closePath();
}
function ellipsize(ctx, text, maxWidth) {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let s = text;
  while (s.length && ctx.measureText(s + "…").width > maxWidth) s = s.slice(0, -1);
  return s + "…";
}
async function circleImage(ctx, url, x, y, size) {
  const img = await loadImage(url);
  ctx.save();
  ctx.beginPath();
  ctx.arc(x + size/2, y + size/2, size/2, 0, Math.PI*2);
  ctx.closePath();
  ctx.clip();
  ctx.drawImage(img, x, y, size, size);
  ctx.restore();
}
async function renderLeaderboardCard({ title, rows, client, guild, cat, period }) {
  const TOP_H = 120;
  const ROW_H_TOP = 96;
  const ROW_H = 74;
  const maxRows = rows.length;
  const H = TOP_H + (maxRows > 0 ? ROW_H_TOP : 0) + Math.max(0, (maxRows - 1)) * ROW_H + 36;
  const W = 980;

  const canvas = createCanvas(W, H);
  const ctx = canvas.getContext("2d");

  // background gradient & subtle grid
  const bg = ctx.createLinearGradient(0, 0, 0, H);
  bg.addColorStop(0, "#0f172a");
  bg.addColorStop(1, "#111827");
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, W, H);
  ctx.globalAlpha = 0.15;
  ctx.strokeStyle = "#334155";
  for (let y = 0; y < H; y += 32) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke(); }
  ctx.globalAlpha = 1;

  // header card
  const headerPad = 20;
  drawRoundedRect(ctx, headerPad, headerPad, W - headerPad*2, TOP_H - headerPad, 16);
  const headGrad = ctx.createLinearGradient(headerPad, headerPad, headerPad, TOP_H);
  headGrad.addColorStop(0, "#111827");
  headGrad.addColorStop(1, "#0b1220");
  ctx.fillStyle = headGrad;
  ctx.fill();

  // server icon
  if (guild.iconURL) {
    const iconURL = guild.iconURL({ extension: "png", size: 128 });
    try { await circleImage(ctx, iconURL, headerPad + 18, headerPad + 14, 72); } catch {}
  }

  // category chip
  const CAT_EMOJI = { all: "🏆", gym: "🏋️", badminton: "🏸", cricket: "🏏", exercise: "🏃" };
  const catText = `${CAT_EMOJI[cat] || "🏆"} ${(cat === "all" ? "Total" : cat)} • ${period}`;
  ctx.font = "600 16px sans-serif";
  const chipW = ctx.measureText(catText).width + 24;
  const chipX = W - headerPad - chipW - 8;
  const chipY = headerPad + 18;
  drawRoundedRect(ctx, chipX, chipY, chipW, 28, 14);
  ctx.fillStyle = "#1f2937";
  ctx.fill();
  ctx.fillStyle = "#93c5fd";
  ctx.fillText(catText, chipX + 12, chipY + 19);

  // title + subtitle
  ctx.fillStyle = "#e5e7eb";
  ctx.font = "700 28px sans-serif";
  ctx.fillText(title, headerPad + 110, headerPad + 44);
  ctx.fillStyle = "#94a3b8";
  ctx.font = "500 16px sans-serif";
  ctx.fillText(`${guild.name} • Top ${rows.length}`, headerPad + 110, headerPad + 70);

  // rows
  const medalCols = {
    1: ["#f59e0b", "#fbbf24"],
    2: ["#9ca3af", "#e5e7eb"],
    3: ["#b45309", "#f59e0b"]
  };

  let y = TOP_H + 8;
  for (let idx = 0; idx < rows.length; idx++) {
    const r = rows[idx];
    const isTop = idx === 0;
    const rowH = isTop ? ROW_H_TOP : ROW_H;
    const cardX = 20, cardW = W - 40;

    // card bg
    drawRoundedRect(ctx, cardX, y, cardW, rowH, 14);
    if (idx < 3) {
      const g = ctx.createLinearGradient(cardX, y, cardX + cardW, y + rowH);
      const [c1, c2] = medalCols[idx + 1];
      g.addColorStop(0, `${c1}22`); g.addColorStop(1, `${c2}18`);
      ctx.fillStyle = g;
    } else {
      ctx.fillStyle = idx % 2 ? "#0d1220" : "#0b111c";
    }
    ctx.fill();

    // left rank ribbon
    const ribbonW = 64;
    drawRoundedRect(ctx, cardX, y, ribbonW, rowH, 14);
    const ribGrad = ctx.createLinearGradient(cardX, y, cardX, y + rowH);
    ribGrad.addColorStop(0, "#1f2937");
    ribGrad.addColorStop(1, "#0b1220");
    ctx.fillStyle = ribGrad;
    ctx.fill();

    // rank text
    ctx.fillStyle = idx < 3 ? medalCols[idx+1][0] : "#cbd5e1";
    ctx.font = isTop ? "800 28px sans-serif" : "800 22px sans-serif";
    const rankStr = `#${r.rank}`;
    const rW = ctx.measureText(rankStr).width;
    ctx.fillText(rankStr, cardX + ribbonW/2 - rW/2, y + (isTop ? 58 : 46));

    // avatar + name
    try {
      const user = await client.users.fetch(r.userId);
      const url = user.displayAvatarURL({ extension: "png", size: 256 });
      const aSize = isTop ? 72 : 54;
      const aX = cardX + ribbonW + 18;
      const aY = y + (rowH/2 - aSize/2);
      await circleImage(ctx, url, aX, aY, aSize);

      ctx.fillStyle = "#e5e7eb";
      ctx.font = isTop ? "700 24px sans-serif" : "700 20px sans-serif";
      const name = user.globalName || user.username;
      const nameX = aX + aSize + 18;
      const nameMax = W - nameX - 160;
      ctx.fillText(ellipsize(ctx, name, nameMax), nameX, y + (isTop ? 44 : 34));

      ctx.fillStyle = "#94a3b8";
      ctx.font = "500 14px sans-serif";
      const handle = `@${user.username}`;
      ctx.fillText(ellipsize(ctx, handle, nameMax), nameX, y + (isTop ? 68 : 54));
    } catch {
      ctx.fillStyle = "#e5e7eb";
      ctx.font = isTop ? "700 24px sans-serif" : "700 20px sans-serif";
      ctx.fillText("Unknown User", cardX + ribbonW + 110, y + (isTop ? 44 : 34));
    }

    // score pill (right)
    const score = String(r.score);
    ctx.font = isTop ? "800 26px monospace" : "800 20px monospace";
    const sw = ctx.measureText(score).width + 28;
    const pillX = W - 24 - sw, pillY = y + (rowH/2 - 20);
    drawRoundedRect(ctx, pillX, pillY, sw, 40, 12);
    const pillGrad = ctx.createLinearGradient(pillX, pillY, pillX, pillY + 40);
    pillGrad.addColorStop(0, "#10b981");
    pillGrad.addColorStop(1, "#059669");
    ctx.fillStyle = pillGrad; ctx.fill();
    ctx.fillStyle = "#052e21";
    ctx.fillText(score, pillX + (sw - ctx.measureText(score).width)/2, pillY + 27);

    y += rowH + 10;
  }

  // watermark
  ctx.fillStyle = "#475569";
  ctx.font = "500 12px sans-serif";
  ctx.fillText("Fitness Bot • leaderboard", 22, H - 12);

  return new AttachmentBuilder(canvas.toBuffer("image/png"), { name: "leaderboard.png" });
}

/* =========================
   COMMANDS
========================= */
const leaderboardCmd = new SlashCommandBuilder()
  .setName("leaderboard")
  .setDescription("Show rankings")
  .addStringOption(o =>
    o.setName("category").setDescription("Category to rank").addChoices(
      { name: "All (total)", value: "all" },
      { name: "Gym", value: "gym" },
      { name: "Badminton", value: "badminton" },
      { name: "Cricket", value: "cricket" },
      { name: "Exercise", value: "exercise" }
    )
  )
  .addStringOption(o =>
    o.setName("period").setDescription("Time period").addChoices(
      { name: "All Time", value: "all" },
      { name: "This Week", value: "week" },
      { name: "This Month", value: "month" }
    )
  );

const nudgeCmd = new SlashCommandBuilder()
  .setName("nudge")
  .setDescription("Give someone a gentle fitness nudge")
  .addUserOption(o => o.setName("user").setDescription("Who to nudge").setRequired(true))
  .addStringOption(o => o.setName("activity").setDescription("What to remind them about").setRequired(true))
  .addStringOption(o =>
    o.setName("where")
     .setDescription("Send in DM or here")
     .addChoices(
       { name: "DM", value: "dm" },
       { name: "Here", value: "here" }
     )
  );

const deductCmd = new SlashCommandBuilder()
  .setName("deduct")
  .setDescription("Deduct points from a user (admin)")
  .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
  .addUserOption(o => o.setName("user").setDescription("Member").setRequired(true))
  .addIntegerOption(o => o.setName("amount").setDescription("Points to deduct").setMinValue(1).setRequired(true))
  .addStringOption(o =>
    o.setName("category").setDescription("Category").setRequired(true).addChoices(
      { name: "gym", value: "gym" },
      { name: "badminton", value: "badminton" },
      { name: "cricket", value: "cricket" },
      { name: "exercise", value: "exercise" }
    ))
  .addStringOption(o => o.setName("reason").setDescription("Why"));

const clearPointsCmd = new SlashCommandBuilder()
  .setName("clearpoints")
  .setDescription("Reset all points for a user (admin)")
  .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
  .addUserOption(o => o.setName("user").setDescription("Member").setRequired(true))
  .addStringOption(o => o.setName("reason").setDescription("Why"));

const squadCmd = new SlashCommandBuilder()
  .setName("squad")
  .setDescription("Squad management & stats")
  .addSubcommand(sc => sc.setName("create")
    .setDescription("Create a new squad")
    .addStringOption(o => o.setName("name").setDescription("Squad name").setRequired(true)))
  .addSubcommand(sc => sc.setName("join")
    .setDescription("Join an existing squad by name")
    .addStringOption(o => o.setName("name").setDescription("Exact squad name").setRequired(true)))
  .addSubcommand(sc => sc.setName("leave").setDescription("Leave your current squad"))
  .addSubcommand(sc => sc.setName("rename")
    .setDescription("Rename your squad (owner only)")
    .addStringOption(o => o.setName("name").setDescription("New name").setRequired(true)))
  .addSubcommand(sc => sc.setName("disband")
    .setDescription("Disband your squad (owner only)"))
  .addSubcommand(sc => sc.setName("info")
    .setDescription("View your squad or a specific squad")
    .addStringOption(o => o.setName("name").setDescription("Squad name")))
  .addSubcommand(sc => sc.setName("leaderboard")
    .setDescription("Squad leaderboard")
    .addStringOption(o => o.setName("category").setDescription("Category").addChoices(
      { name: "All (total)", value: "all" },
      { name: "Gym", value: "gym" },
      { name: "Badminton", value: "badminton" },
      { name: "Cricket", value: "cricket" },
      { name: "Exercise", value: "exercise" }
    ))
    .addStringOption(o => o.setName("period").setDescription("Time period").addChoices(
      { name: "All Time", value: "all" },
      { name: "This Week", value: "week" },
      { name: "This Month", value: "month" }
    ))
  );

const commands = [
  new SlashCommandBuilder().setName("gym").setDescription("💪 Claim +2 for Gym (12h cooldown)"),
  new SlashCommandBuilder().setName("badminton").setDescription("🏸 Claim +5 for Badminton (12h cooldown)"),
  new SlashCommandBuilder().setName("cricket").setDescription("🏏 Claim +5 for Cricket (12h cooldown)"),
  new SlashCommandBuilder()
    .setName("exercise")
    .setDescription("🏃 Claim +1 for an exercise (6h cooldown)")
    .addStringOption(opt =>
      opt.setName("type").setDescription("Quick pick (optional)").addChoices(
        { name: "pushup", value: "pushup" },
        { name: "dumbells", value: "dumbells" },
        { name: "yoga", value: "yoga" },
        { name: "walking", value: "walking" },
        { name: "jogging", value: "jogging" },
        { name: "burpees", value: "burpees" },
        { name: "planks", value: "planks" }
      )
    )
    .addStringOption(opt => opt.setName("custom").setDescription("Or type your own").setMaxLength(50)),
  new SlashCommandBuilder().setName("myscore").setDescription("🏆 Show your score, rank, and streaks"),
  new SlashCommandBuilder().setName("profile").setDescription("🏆 Show your score, rank, and streaks"),
  leaderboardCmd,
  nudgeCmd,
  new SlashCommandBuilder().setName("challenge").setDescription("🎯 View weekly challenges and claim rewards"),
  new SlashCommandBuilder().setName("guildstats").setDescription("📊 View server-wide fitness stats"),
  new SlashCommandBuilder()
    .setName("remind").setDescription("⏰ Set a reminder")
    .addStringOption(o => o.setName("activity").setDescription("Activity").setRequired(true))
    .addIntegerOption(o => o.setName("hours").setDescription("Remind me in X hours").setMinValue(1).setRequired(true)),
  new SlashCommandBuilder()
    .setName("buddy").setDescription("👯 Set or view your workout buddy")
    .addUserOption(o => o.setName("user").setDescription("Your buddy (leave empty to view)")),
  new SlashCommandBuilder()
    .setName("award").setDescription("🎁 Award points to a user (admin)")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addUserOption(o => o.setName("user").setDescription("Member").setRequired(true))
    .addIntegerOption(o => o.setName("amount").setDescription("Points").setMinValue(1).setRequired(true))
    .addStringOption(o => o.setName("category").setDescription("Category").setRequired(true).addChoices(
      { name: "gym", value: "gym" }, { name: "badminton", value: "badminton" },
      { name: "cricket", value: "cricket" }, { name: "exercise", value: "exercise" }
    ))
    .addStringOption(o => o.setName("reason").setDescription("Why")),
  deductCmd,
  clearPointsCmd,
  new SlashCommandBuilder()
    .setName("config").setDescription("⚙️ Configure bot")
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addSubcommand(sc =>
      sc.setName("setcheckins").setDescription("Set the check-ins channel")
        .addChannelOption(o => o.setName("channel").setDescription("Text channel").addChannelTypes(ChannelType.GuildText).setRequired(true)))
    .addSubcommand(sc =>
      sc.setName("setaudit").setDescription("Set the audit/log channel")
        .addChannelOption(o => o.setName("channel").setDescription("Text channel").addChannelTypes(ChannelType.GuildText).setRequired(true)))
    .addSubcommand(sc =>
      sc.setName("setcooldowns").setDescription("Override cooldowns (hours)")
        .addIntegerOption(o => o.setName("gym").setDescription("Gym"))
        .addIntegerOption(o => o.setName("badminton").setDescription("Badminton"))
        .addIntegerOption(o => o.setName("cricket").setDescription("Cricket"))
        .addIntegerOption(o => o.setName("exercise").setDescription("Exercise"))),
  squadCmd
].map(c => c.toJSON());

/* =========================
   REGISTER COMMANDS
========================= */
async function registerCommands() {
  const rest = new REST({ version: "10" }).setToken(TOKEN);
  try {
    if (DEV_GUILD_ID) {
      await rest.put(Routes.applicationGuildCommands(APP_ID, DEV_GUILD_ID), { body: commands });
      console.log("✅ Registered GUILD commands (dev).");
    } else {
      await rest.put(Routes.applicationCommands(APP_ID), { body: commands });
      console.log("✅ Registered GLOBAL commands.");
    }
  } catch (e) {
    console.error("Command registration failed:", e);
  }
}

/* =========================
   HANDLERS
========================= */
client.on("ready", () => console.log(`🤖 Logged in as ${client.user.tag}`));

client.on("interactionCreate", async (i) => {
  if (!i.isChatInputCommand()) return;
  const { commandName, guild, user, options } = i;
  if (!guild) return i.reply({ content: "Guild-only.", ephemeral: true });

  const execClaim = async (category, explicitAmount) => {
    const amount = explicitAmount ?? POINTS[category];
    const remaining = checkCooldown({ guildId: guild.id, userId: user.id, category });
    if (!explicitAmount && remaining > 0) {
      return i.reply({ content: `⏳ Cooldown active for **${category}**. Try again in **${formatMs(remaining)}**.`, ephemeral: true });
    }
    addPoints({ guildId: guild.id, userId: user.id, category, amount });
    if (!explicitAmount) commitCooldown({ guildId: guild.id, userId: user.id, category });
    const row = getUserStmt.get(guild.id, user.id);
    const msg = getRandomSuccess();
    await i.reply({ content: `✅ **+${amount}** ${category} points! ${msg}  Total: **${row.total}**` });
    auditLog(guild, `🏅 <@${user.id}> **+${amount}** in **${category}** • Total: **${row.total}**`);
  };

  if (commandName === "gym")       return execClaim("gym");
  if (commandName === "badminton") return execClaim("badminton");
  if (commandName === "cricket")   return execClaim("cricket");
  if (commandName === "exercise")  return execClaim("exercise");

  if (commandName === "myscore" || commandName === "profile") {
    ensureUserRow(guild.id, user.id);
    const r = getUserStmt.get(guild.id, user.id);
    const { pct, cur, need } = nextRankProgress(r.total);
    const st = (cat) => getStreak.get(guild.id, user.id, cat) || { current_streak:0, longest_streak:0 };
    const embed = new EmbedBuilder()
      .setColor(cur.color)
      .setTitle(`🏆 ${i.user.username}'s Profile`)
      .setThumbnail(i.user.displayAvatarURL())
      .addFields(
        { name: "Total", value: String(r.total), inline: true },
        { name: "Rank",  value: cur.name, inline: true },
        { name: "Progress", value: bar(pct), inline: false },
        { name: "Gym", value: `${r.gym}  (🔥${st("gym").current_streak} • best ${st("gym").longest_streak})`, inline: true },
        { name: "Exercise", value: `${r.exercise}  (🔥${st("exercise").current_streak} • best ${st("exercise").longest_streak})`, inline: true },
        { name: "Badminton", value: `${r.badminton}  (🔥${st("badminton").current_streak} • best ${st("badminton").longest_streak})`, inline: true },
        { name: "Cricket", value: `${r.cricket}  (🔥${st("cricket").current_streak} • best ${st("cricket").longest_streak})`, inline: true }
      );
    if (need > 0) embed.addFields({ name: "Next Rank", value: `${need} points to ${RANKS.find(x=>x.min===cur.next)?.name}`, inline: false });
    return i.reply({ embeds: [embed] });
  }

  if (commandName === "leaderboard") {
    const cat = options.getString("category") ?? "all";
    const period = options.getString("period") ?? "all";
    let listRows;

    if (period === "all") {
      const col = (cat === "all") ? "total" : cat;
      listRows = db.prepare(`SELECT user_id, ${col} as score FROM points WHERE guild_id=? ORDER BY ${col} DESC LIMIT 10`).all(guild.id);
    } else {
      const since = period === "week" ? Date.parse(isoWeekStart(new Date())) : Date.parse(monthStart(new Date()));
      listRows = sumSince(guild.id, since, cat === "all" ? "all" : cat);
    }

    const mapped = (listRows || []).map((r, idx) => ({ rank: idx+1, userId: r.user_id, score: r.score || 0 }));

    // pretty image leaderboard (with fallback)
    try {
      const file = await renderLeaderboardCard({
        title: "🏅 Leaderboard",
        rows: mapped,
        client,
        guild,
        cat,
        period
      });
      return i.reply({ files: [file] });
    } catch (err) {
      console.error("Leaderboard image render failed:", err);
      const lines = mapped.length
        ? mapped.map(r => `${MEDAL(r.rank)} <@${r.userId}> — **${r.score}**`).join("\n")
        : "_No data yet._";
      const titleCat = (cat === "all") ? "Total" : cat;
      const embed = new EmbedBuilder().setColor(0xffc857).setTitle(`🏅 Leaderboard — ${titleCat} (${period})`).setDescription(lines);
      return i.reply({ embeds: [embed] });
    }
  }

  if (commandName === "nudge") {
    const target = options.getUser("user", true);
    const activity = options.getString("activity", true);
    const where = options.getString("where") ?? "here";

    // Optional guard: allow admins or buddies
    const isAdmin = i.member.permissions.has(PermissionFlagsBits.ManageGuild);
    const b1 = db.prepare("SELECT 1 FROM buddies WHERE guild_id=? AND user_id=? AND buddy_id=?").get(guild.id, user.id, target.id);
    const b2 = db.prepare("SELECT 1 FROM buddies WHERE guild_id=? AND user_id=? AND buddy_id=?").get(guild.id, target.id, user.id);
    if (!isAdmin && !b1 && !b2) {
      return i.reply({ content: "You can nudge your **buddy** or ask an admin. Set a buddy with `/buddy user:@someone`.", ephemeral: true });
    }

    if (target.bot) return i.reply({ content: "Can’t nudge a bot.", ephemeral: true });
    if (target.id === user.id) return i.reply({ content: "Use /remind to nudge yourself 😊", ephemeral: true });

    const checkins = readConfig.get(guild.id)?.checkins_channel_id;
    const msg = `⏰ Nudge from <@${user.id}>: Don’t forget to log **${activity}** in **${guild.name}**!\nTry \`/exercise\` or type \`Exercise + ${activity}\` in #check-in.`;

    try {
      if (where === "dm") {
        const member = await guild.members.fetch(target.id);
        await member.send(msg);
        return i.reply({ content: `✅ Sent a DM nudge to <@${target.id}>.`, ephemeral: true });
      } else {
        const ch =
          (checkins && (guild.channels.cache.get(checkins) || await guild.channels.fetch(checkins).catch(()=>null))) ||
          i.channel;
        await ch.send({ content: `<@${target.id}> ${msg}` });
        return i.reply({ content: `✅ Posted a nudge in ${ch}.`, ephemeral: true });
      }
    } catch {
      return i.reply({ content: "Couldn’t deliver the nudge (they may have DMs off or I lack channel perms).", ephemeral: true });
    }
  }

  if (commandName === "challenge") {
    const week = isoWeekStart(new Date());
    const since = Date.parse(week);
    const parts = [];
    for (const ch of WEEKLY_CHALLENGES) {
      let progress = 0;
      if (ch.target === "total") {
        progress = sumForUserSince(guild.id, user.id, since, "total");
      } else if (ch.target === "total_sports") {
        const b = sumForUserSince(guild.id, user.id, since, "badminton");
        const c = sumForUserSince(guild.id, user.id, since, "cricket");
        progress = b + c;
      } else {
        progress = sumForUserSince(guild.id, user.id, since, ch.target);
      }
      const done = Math.min(progress, ch.goal);
      const pct = Math.min(100, Math.floor((done/ch.goal)*100));
      parts.push(`${ch.emoji} **${ch.name}** — ${done}/${ch.goal}  ${bar(pct)}`);

      const claimed = db.prepare(`SELECT 1 FROM challenge_claims WHERE guild_id=? AND user_id=? AND challenge_id=? AND week_start=?`)
                        .get(guild.id, user.id, ch.id, week);
      if (!claimed && progress >= ch.goal) {
        addPoints({ guildId: guild.id, userId: user.id, category: ch.rewardCat, amount: ch.reward });
        db.prepare(`INSERT INTO challenge_claims (guild_id,user_id,challenge_id,week_start,claimed_at) VALUES (?,?,?,?,?)`)
          .run(guild.id, user.id, ch.id, week, Date.now());
        parts.push(`➡️ Reward claimed: **+${ch.reward}** points!`);
      }
    }
    const embed = new EmbedBuilder().setColor(0x00b894).setTitle("📆 Weekly Challenges").setDescription(parts.join("\n"));
    return i.reply({ embeds: [embed] });
  }

  if (commandName === "guildstats") {
    const t = guildTotalsStmt.get(guild.id) || { t:0,g:0,b:0,c:0,e:0 };
    const embed = new EmbedBuilder()
      .setColor(0x00cec9).setTitle(`📊 ${guild.name} — Server Stats`)
      .addFields(
        { name: "Total Points", value: String(t.t || 0), inline: true },
        { name: "Gym", value: String(t.g || 0), inline: true },
        { name: "Badminton", value: String(t.b || 0), inline: true },
        { name: "Cricket", value: String(t.c || 0), inline: true },
        { name: "Exercise", value: String(t.e || 0), inline: true }
      );
    return i.reply({ embeds: [embed] });
  }

  if (commandName === "remind") {
    const activity = options.getString("activity", true);
    const hours = options.getInteger("hours", true);
    const due = Date.now() + hours * 3600000;
    db.prepare(`INSERT INTO reminders (guild_id,user_id,activity,due_at,every_hours) VALUES (?,?,?,?,?)`)
      .run(guild.id, user.id, activity, due, null);
    return i.reply({ content: `⏰ Okay! I’ll remind you about **${activity}** in **${hours}h**.` });
  }

  if (commandName === "buddy") {
    const u = options.getUser("user");
    if (!u) {
      const b = db.prepare(`SELECT buddy_id FROM buddies WHERE guild_id=? AND user_id=?`).get(guild.id, user.id);
      return i.reply({ content: b?.buddy_id ? `👯 Your buddy is <@${b.buddy_id}>.` : "You have no buddy set. Try `/buddy user:@someone`." , ephemeral: true });
    }
    if (u.id === user.id) return i.reply({ content: "Pick someone else 🤝", ephemeral: true });
    db.prepare(`
      INSERT INTO buddies (guild_id,user_id,buddy_id)
      VALUES (?,?,?)
      ON CONFLICT(guild_id,user_id) DO UPDATE SET buddy_id=excluded.buddy_id
    `).run(guild.id, user.id, u.id);
    return i.reply({ content: `👯 Buddy set! You & <@${u.id}> can keep each other accountable.` });
  }

  if (commandName === "deduct") {
    const target = options.getUser("user", true);
    const amount = options.getInteger("amount", true);
    const category = options.getString("category", true);
    const reason = options.getString("reason") ?? "Manual deduction";

    const result = deductPoints({ guildId: guild.id, userId: target.id, category, amount });
    if (result.deducted <= 0) {
      return i.reply({ content: `ℹ️ <@${target.id}> has no ${category} points to deduct.`, ephemeral: true });
    }

    const reasonLine = reason ? `\nReason: ${reason}` : "";
    await i.reply({ content: `➖ Deducted **${result.deducted}** ${category} points from <@${target.id}>. Total: **${result.row.total}**.${reasonLine}` });
    auditLog(guild, `➖ **Manual deduction**: <@${target.id}> -${result.deducted} in **${category}** • By <@${user.id}> • Reason: ${reason}`);
    return;
  }

  if (commandName === "clearpoints") {
    const target = options.getUser("user", true);
    const reason = options.getString("reason") ?? "Manual clear";
    const result = clearUserPoints(guild.id, target.id);

    if ((result.cleared || 0) <= 0) {
      await i.reply({ content: `ℹ️ <@${target.id}> already has zero points.`, ephemeral: true });
    } else {
      const reasonLine = reason ? `\nReason: ${reason}` : "";
      await i.reply({ content: `🧹 Cleared **${result.cleared}** total points from <@${target.id}>.${reasonLine}` });
    }
    auditLog(guild, `🧹 **Manual clear**: <@${target.id}> reset by <@${user.id}> • Reason: ${reason}`);
    return;
  }

  if (commandName === "award") {
    const target = options.getUser("user", true);
    const amount = options.getInteger("amount", true);
    const category = options.getString("category", true);
    const reason = options.getString("reason") ?? "Manual award";
    addPoints({ guildId: guild.id, userId: target.id, category, amount });
    const row = getUserStmt.get(guild.id, target.id);
    await i.reply({ content: `🎁 Awarded **+${amount}** to <@${target.id}> in **${category}**. Total: **${row.total}**\nReason: ${reason}` });
    auditLog(guild, `🎁 **Manual award**: <@${target.id}> **+${amount}** in **${category}** • By <@${user.id}> • Reason: ${reason}`);
    return;
  }

  if (commandName === "config") {
    const sub = options.getSubcommand();
    if (sub === "setcheckins") {
      const ch = options.getChannel("channel", true);
      upsertConfig.run({ guild_id: guild.id, checkins_channel_id: ch.id, audit_channel_id: null, gym: null, badminton: null, cricket: null, exercise: null });
      return i.reply({ content: `✅ Check-ins channel set to ${ch}.`, ephemeral: true });
    }
    if (sub === "setaudit") {
      const ch = options.getChannel("channel", true);
      upsertConfig.run({ guild_id: guild.id, checkins_channel_id: null, audit_channel_id: ch.id, gym: null, badminton: null, cricket: null, exercise: null });
      return i.reply({ content: `✅ Audit channel set to ${ch}.`, ephemeral: true });
    }
    if (sub === "setcooldowns") {
      const gymH = options.getInteger("gym");
      const badH = options.getInteger("badminton");
      const criH = options.getInteger("cricket");
      const exH  = options.getInteger("exercise");

      upsertConfig.run({
        guild_id: guild.id,
        checkins_channel_id: null,
        audit_channel_id: null,
        gym: gymH ? gymH * 3600000 : null,
        badminton: badH ? badH * 3600000 : null,
        cricket: criH ? criH * 3600000 : null,
        exercise: exH ? exH * 3600000 : null
      });

      return i.reply({ content: "✅ Cooldowns updated (hours).", ephemeral: true });
    }
  }

  /* =========================
     SQUAD SUBCOMMANDS
  ========================= */
  if (commandName === "squad") {
    const sub = options.getSubcommand();

    if (sub === "create") {
      const name = options.getString("name", true).trim();
      const existing = getSquadByName.get(guild.id, name);
      if (existing) return i.reply({ content: "That squad name is taken. Try another.", ephemeral: true });
      const already = getUserSquadRow.get(guild.id, user.id);
      if (already) return i.reply({ content: `You are already in **${already.name}**. Leave first with \`/squad leave\`.`, ephemeral: true });

      const info = createSquadStmt.run(guild.id, user.id, name, Date.now());
      addMemberStmt.run(guild.id, info.lastInsertRowid, user.id, Date.now());
      return i.reply({ content: `🛡️ Squad **${name}** created! You are the owner. Invite friends with \`/squad join name:${name}\`.` });
    }

    if (sub === "join") {
      const name = options.getString("name", true).trim();
      const sq = getSquadByName.get(guild.id, name);
      if (!sq) return i.reply({ content: "No squad by that name.", ephemeral: true });
      const already = getUserSquadRow.get(guild.id, user.id);
      if (already) return i.reply({ content: `You are already in **${already.name}**. Leave first with \`/squad leave\`.`, ephemeral: true });

      addMemberStmt.run(guild.id, sq.squad_id, user.id, Date.now());
      return i.reply({ content: `👥 Joined squad **${sq.name}**!` });
    }

    if (sub === "leave") {
      const sq = getUserSquadRow.get(guild.id, user.id);
      if (!sq) return i.reply({ content: "You are not in a squad.", ephemeral: true });
      if (sq.owner_id === user.id) return i.reply({ content: "You are the owner. Disband with \`/squad disband\` or rename/transfer (not implemented).", ephemeral: true });
      removeMemberStmt.run(guild.id, user.id);
      return i.reply({ content: `👋 You left **${sq.name}**.` });
    }

    if (sub === "rename") {
      const sq = getUserSquadRow.get(guild.id, user.id);
      if (!sq) return i.reply({ content: "You are not in a squad.", ephemeral: true });
      if (sq.owner_id !== user.id) return i.reply({ content: "Only the squad owner can rename the squad.", ephemeral: true });
      const newName = options.getString("name", true).trim();
      if (getSquadByName.get(guild.id, newName)) return i.reply({ content: "That name is already taken.", ephemeral: true });
      renameSquadStmt.run(newName, guild.id, sq.squad_id);
      return i.reply({ content: `✏️ Squad renamed to **${newName}**.` });
    }

    if (sub === "disband") {
      const sq = getUserSquadRow.get(guild.id, user.id);
      if (!sq) return i.reply({ content: "You are not in a squad.", ephemeral: true });
      if (sq.owner_id !== user.id) return i.reply({ content: "Only the squad owner can disband.", ephemeral: true });
      deleteSquadStmt.run(guild.id, sq.squad_id);
      return i.reply({ content: `💥 Squad **${sq.name}** disbanded.` });
    }

    if (sub === "info") {
      const name = options.getString("name");
      const sq = name ? getSquadByName.get(guild.id, name.trim()) : getUserSquadRow.get(guild.id, user.id);
      if (!sq) return i.reply({ content: "No squad found. Specify a name or join one.", ephemeral: true });

      const members = listSquadMembers.all(guild.id, sq.squad_id).map(r => r.user_id);
      const total = members.length
        ? db.prepare(`SELECT COALESCE(SUM(total),0) AS s FROM points WHERE guild_id=? AND user_id IN (${members.map(()=>"?").join(",")})`)
            .get(guild.id, ...members).s || 0
        : 0;

      const embed = new EmbedBuilder()
        .setColor(0x2ecc71)
        .setTitle(`🛡️ Squad: ${sq.name}`)
        .setDescription(members.length ? members.map(id => `• <@${id}>`).join("\n") : "_No members yet_")
        .addFields({ name:"Total Points", value:String(total), inline:true },
                   { name:"Owner", value:`<@${sq.owner_id}>`, inline:true });

      return i.reply({ embeds: [embed] });
    }

    if (sub === "leaderboard") {
      const cat = options.getString("category") ?? "all";
      const period = options.getString("period") ?? "all";

      const squads = db.prepare(`SELECT * FROM squads WHERE guild_id=?`).all(guild.id);
      const results = [];
      for (const sq of squads) {
        const members = listSquadMembers.all(guild.id, sq.squad_id).map(r => r.user_id);
        if (!members.length) { results.push({ name: sq.name, score: 0 }); continue; }

        let score = 0;
        if (period === "all") {
          const column = (cat === "all") ? "total" : cat;
          const total = db.prepare(`SELECT COALESCE(SUM(${column}),0) AS s FROM points WHERE guild_id=? AND user_id IN (${members.map(()=>"?").join(",")})`)
                          .get(guild.id, ...members).s || 0;
          score = total;
        } else {
          const since = period === "week" ? Date.parse(isoWeekStart(new Date())) : Date.parse(monthStart(new Date()));
          const rows = sumForUsersSince(guild.id, members, since, cat === "all" ? "all" : cat);
          score = rows.reduce((a,r) => a + (r.score||0), 0);
        }
        results.push({ name: sq.name, score });
      }
      results.sort((a,b) => b.score - a.score);

      const lines = results.length
        ? results.slice(0,10).map((r,idx) => `${MEDAL(idx+1)} **${r.name}** — **${r.score}**`).join("\n")
        : "_No squads yet._";
      const embed = new EmbedBuilder().setColor(0x1abc9c).setTitle(`🛡️ Squad Leaderboard — ${cat} (${period})`).setDescription(lines);
      return i.reply({ embeds: [embed] });
    }
  }
});

/* Auto-award from #check-in messages, incl. "Exercise + anything" and common typo */
client.on("messageCreate", async (msg) => {
  if (!msg.guild || msg.author.bot) return;
  const cfg = readConfig.get(msg.guild.id);
  if (!cfg?.checkins_channel_id || msg.channelId !== cfg.checkins_channel_id) return;

  const content = msg.content.toLowerCase();
  const authorId = msg.author.id;

  const tryAward = async (category) => {
    const remaining = checkCooldown({ guildId: msg.guild.id, userId: authorId, category });
    if (remaining > 0) return;
    const amount = POINTS[category];
    addPoints({ guildId: msg.guild.id, userId: authorId, category, amount });
    commitCooldown({ guildId: msg.guild.id, userId: authorId, category });
    const row = getUserStmt.get(msg.guild.id, authorId);
    msg.react("✅").catch(() => {});
    auditLog(msg.guild, `📥 Auto-award **+${amount}** to <@${authorId}> in **${category}** • Total: **${row.total}**`);
  };

  const exercisePlusAny = /\bex(?:er|cer)cise\s*\+\s*([^\r\n]+)/i;
  let exerciseAwarded = false;
  if (exercisePlusAny.test(msg.content)) { await tryAward("exercise"); exerciseAwarded = true; }
  if (!exerciseAwarded && /\b(push[-\s]?ups?|dumb(?:bell|ells?)|burpees?|planks?|sit[-\s]?ups?|yoga|walking|jogging|running)\b/i.test(msg.content)) {
    await tryAward("exercise"); exerciseAwarded = true;
  }
  if (content.includes("gym"))                          await tryAward("gym");
  if (content.includes("badminton") || content.includes("🏸")) await tryAward("badminton");
  if (content.includes("cricket")   || content.includes("🏏")) await tryAward("cricket");
});

/* Reminders ticker (DMs users) */
setInterval(async () => {
  const now = Date.now();
  const due = db.prepare(`SELECT rowid, guild_id, user_id, activity FROM reminders WHERE due_at<=?`).all(now);
  for (const r of due) {
    try {
      const guild = await client.guilds.fetch(r.guild_id);
      const member = await guild.members.fetch(r.user_id).catch(() => null);
      if (member) member.send(`⏰ Reminder: log your **${r.activity}** in **${guild.name}**!`).catch(()=>{});
    } catch {}
    db.prepare(`DELETE FROM reminders WHERE rowid=?`).run(r.rowid);
  }
}, 60_000);

/* Helpers */
function formatMs(ms) {
  const s = Math.ceil(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

/* BOOT */
await registerCommands();
client.login(TOKEN);
