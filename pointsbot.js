import 'dotenv/config';
import { Client, GatewayIntentBits, Partials, REST, Routes, SlashCommandBuilder, PermissionFlagsBits, ChannelType, EmbedBuilder } from 'discord.js';
import Database from 'better-sqlite3';

// ---------- Config ----------
const APP_ID = process.env.APPLICATION_ID;                       // 1424263853327388785
const TOKEN  = process.env.DISCORD_TOKEN;
const DEV_GUILD_ID = process.env.DEV_GUILD_ID;                   // register guild commands (fast)

// Default cooldowns (ms)
const COOLDOWNS = {
  gym:        12 * 60 * 60 * 1000, // 12h
  badminton:  12 * 60 * 60 * 1000,
  cricket:    12 * 60 * 60 * 1000,
  exercise:    6 * 60 * 60 * 1000
};

// Points per activity
const POINTS = {
  gym: 2,
  badminton: 5,
  cricket: 5,
  exercise: 1
};

// ---------- Database ----------
const db = new Database('points.db');
db.pragma('journal_mode = WAL');

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

// Helpers
const upsertUser = db.prepare(`
  INSERT INTO points (guild_id, user_id) VALUES (@guild_id, @user_id)
  ON CONFLICT(guild_id, user_id) DO NOTHING
`);

const addPointsStmt = db.prepare(`
  UPDATE points
  SET total = total + @add,
      ${/* category column is dynamic; we build query at runtime */''}
      gym      = CASE WHEN @category = 'gym' THEN gym + @add ELSE gym END,
      badminton= CASE WHEN @category = 'badminton' THEN badminton + @add ELSE badminton END,
      cricket  = CASE WHEN @category = 'cricket' THEN cricket + @add ELSE cricket END,
      exercise = CASE WHEN @category = 'exercise' THEN exercise + @add ELSE exercise END
  WHERE guild_id = @guild_id AND user_id = @user_id
`);

const getUserStmt = db.prepare(`SELECT * FROM points WHERE guild_id=? AND user_id=?`);

const setCooldownStmt = db.prepare(`
  INSERT INTO cooldowns (guild_id, user_id, category, last_ms)
  VALUES (@guild_id, @user_id, @category, @last_ms)
  ON CONFLICT(guild_id, user_id, category) DO UPDATE SET last_ms=excluded.last_ms
`);

const getCooldownStmt = db.prepare(`
  SELECT last_ms FROM cooldowns WHERE guild_id=? AND user_id=? AND category=?
`);

const upsertConfig = db.prepare(`
  INSERT INTO guild_config (guild_id, checkins_channel_id, audit_channel_id,
    gym_cooldown_ms, badminton_cooldown_ms, cricket_cooldown_ms, exercise_cooldown_ms)
  VALUES (@guild_id, @checkins_channel_id, @audit_channel_id, @gym, @badminton, @cricket, @exercise)
  ON CONFLICT(guild_id) DO UPDATE SET
    checkins_channel_id=excluded.checkins_channel_id,
    audit_channel_id=excluded.audit_channel_id,
    gym_cooldown_ms=COALESCE(excluded.gym_cooldown_ms, guild_config.gym_cooldown_ms),
    badminton_cooldown_ms=COALESCE(excluded.badminton_cooldown_ms, guild_config.badminton_cooldown_ms),
    cricket_cooldown_ms=COALESCE(excluded.cricket_cooldown_ms, guild_config.cricket_cooldown_ms),
    exercise_cooldown_ms=COALESCE(excluded.exercise_cooldown_ms, guild_config.exercise_cooldown_ms)
`);

const readConfig = db.prepare(`SELECT * FROM guild_config WHERE guild_id=?`);

// ---------- Discord Client ----------
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ],
  partials: [Partials.Channel]
});

// ---------- Command Builders ----------
const commands = [
  new SlashCommandBuilder().setName('gym').setDescription('Claim +2 for Gym (12h cooldown)'),
  new SlashCommandBuilder().setName('badminton').setDescription('Claim +5 for Badminton (12h cooldown)'),
  new SlashCommandBuilder().setName('cricket').setDescription('Claim +5 for Cricket (12h cooldown)'),
  new SlashCommandBuilder()
    .setName('exercise')
    .setDescription('Claim +1 for Exercise (6h cooldown)')
    .addStringOption(opt =>
      opt.setName('type')
        .setDescription('Exercise type')
        .setRequired(true)
        .addChoices(
          { name: 'pushup', value: 'pushup' },
          { name: 'dumbells', value: 'dumbells' }
        )
    ),
  new SlashCommandBuilder()
    .setName('myscore').setDescription('Show your points and breakdown'),
  new SlashCommandBuilder()
    .setName('leaderboard').setDescription('Show the top 10')
    .addStringOption(o =>
      o.setName('category')
        .setDescription('Category to rank')
        .addChoices(
          { name: 'all (total)', value: 'all' },
          { name: 'gym', value: 'gym' },
          { name: 'badminton', value: 'badminton' },
          { name: 'cricket', value: 'cricket' },
          { name: 'exercise', value: 'exercise' }
        )
    ),
  new SlashCommandBuilder()
    .setName('award')
    .setDescription('Award points to a user (admin)')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addUserOption(o => o.setName('user').setDescription('Member to award').setRequired(true))
    .addIntegerOption(o => o.setName('amount').setDescription('Points').setMinValue(1).setRequired(true))
    .addStringOption(o =>
      o.setName('category').setDescription('Category').setRequired(true)
       .addChoices(
         { name: 'gym', value: 'gym' },
         { name: 'badminton', value: 'badminton' },
         { name: 'cricket', value: 'cricket' },
         { name: 'exercise', value: 'exercise' }
       )
    )
    .addStringOption(o => o.setName('reason').setDescription('Why').setRequired(false)),
  new SlashCommandBuilder()
    .setName('config')
    .setDescription('Configure bot')
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageGuild)
    .addSubcommand(sc =>
      sc.setName('setcheckins')
        .setDescription('Set the Habit Huddle check-ins channel')
        .addChannelOption(o => o.setName('channel').setDescription('Text channel').addChannelTypes(ChannelType.GuildText).setRequired(true))
    )
    .addSubcommand(sc =>
      sc.setName('setaudit')
        .setDescription('Set the audit/log channel')
        .addChannelOption(o => o.setName('channel').setDescription('Text channel').addChannelTypes(ChannelType.GuildText).setRequired(true))
    )
    .addSubcommand(sc =>
      sc.setName('setcooldowns')
        .setDescription('Override cooldowns (hours)')
        .addIntegerOption(o => o.setName('gym').setDescription('Gym cooldown hours'))
        .addIntegerOption(o => o.setName('badminton').setDescription('Badminton cooldown hours'))
        .addIntegerOption(o => o.setName('cricket').setDescription('Cricket cooldown hours'))
        .addIntegerOption(o => o.setName('exercise').setDescription('Exercise cooldown hours'))
    )
].map(c => c.toJSON());

// Register commands on startup (guild for dev = instant; fallback to global)
async function registerCommands() {
  const rest = new REST({ version: '10' }).setToken(TOKEN);
  try {
    if (DEV_GUILD_ID) {
      await rest.put(Routes.applicationGuildCommands(APP_ID, DEV_GUILD_ID), { body: commands });
      console.log('✅ Registered GUILD commands (dev).');
    } else {
      await rest.put(Routes.applicationCommands(APP_ID), { body: commands });
      console.log('✅ Registered GLOBAL commands (may take up to ~1 hour to appear).');
    }
  } catch (e) {
    console.error('Command registration failed:', e);
  }
}

// ---------- Award Engine ----------
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

function formatMs(ms) {
  const s = Math.ceil(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function ensureUserRow(guildId, userId) {
  upsertUser.run({ guild_id: guildId, user_id: userId });
}

function addPoints({ guildId, userId, category, amount }) {
  ensureUserRow(guildId, userId);
  addPointsStmt.run({ guild_id: guildId, user_id: userId, category, add: amount });
}

function checkCooldown({ guildId, userId, category }) {
  const row = getCooldownStmt.get(guildId, userId, category);
  const now = Date.now();
  const cd = getEffectiveCooldownMs(guildId, category);
  if (row && now - row.last_ms < cd) {
    return cd - (now - row.last_ms); // ms remaining
  }
  return 0;
}

function commitCooldown({ guildId, userId, category }) {
  setCooldownStmt.run({ guild_id: guildId, user_id: userId, category, last_ms: Date.now() });
}

async function auditLog(guild, description) {
  const cfg = readConfig.get(guild.id);
  if (!cfg?.audit_channel_id) return;
  const ch = guild.channels.cache.get(cfg.audit_channel_id) || await guild.channels.fetch(cfg.audit_channel_id).catch(() => null);
  if (!ch) return;
  const embed = new EmbedBuilder().setColor(0x44d17a).setDescription(description).setTimestamp(new Date());
  ch.send({ embeds: [embed] }).catch(() => {});
}

// ---------- Handlers ----------
client.on('ready', () => {
  console.log(`🤖 Logged in as ${client.user.tag}`);
});

// Slash commands
client.on('interactionCreate', async (i) => {
  if (!i.isChatInputCommand()) return;
  const { commandName, guild, user, options } = i;
  if (!guild) return i.reply({ content: 'Guild-only.', ephemeral: true });

  const execClaim = async (category, explicitAmount) => {
    const amount = explicitAmount ?? POINTS[category];
    const remaining = checkCooldown({ guildId: guild.id, userId: user.id, category });
    if (!explicitAmount && remaining > 0) {
      return i.reply({ content: `⏳ Cooldown active for **${category}**. Try again in **${formatMs(remaining)}**.`, ephemeral: true });
    }
    addPoints({ guildId: guild.id, userId: user.id, category, amount });
    if (!explicitAmount) commitCooldown({ guildId: guild.id, userId: user.id, category });
    const row = getUserStmt.get(guild.id, user.id);
    await i.reply({ content: `✅ **+${amount}** ${category} points added for <@${user.id}>. Total: **${row.total}**`, ephemeral: false });
    auditLog(guild, `🏅 <@${user.id}> **+${amount}** in **${category}** ${explicitAmount ? '(manual award)' : '(claim)'} • Total: **${row.total}**`);
  };

  if (commandName === 'gym')         return execClaim('gym');
  if (commandName === 'badminton')   return execClaim('badminton');
  if (commandName === 'cricket')     return execClaim('cricket');
  if (commandName === 'exercise')    return execClaim('exercise');

  if (commandName === 'myscore') {
    ensureUserRow(guild.id, user.id);
    const r = getUserStmt.get(guild.id, user.id);
    const embed = new EmbedBuilder()
      .setColor(0x5865F2)
      .setTitle(`🏆 ${i.user.username}'s Score`)
      .addFields(
        { name: 'Total', value: String(r.total), inline: true },
        { name: 'Gym', value: String(r.gym), inline: true },
        { name: 'Badminton', value: String(r.badminton), inline: true },
        { name: 'Cricket', value: String(r.cricket), inline: true },
        { name: 'Exercise', value: String(r.exercise), inline: true }
      );
    return i.reply({ embeds: [embed], ephemeral: false });
  }

  if (commandName === 'leaderboard') {
    const cat = options.getString('category') ?? 'all';
    const col = (cat === 'all') ? 'total' : cat;
    const rows = db.prepare(`SELECT user_id, ${col} as score FROM points WHERE guild_id=? ORDER BY ${col} DESC LIMIT 10`).all(guild.id);
    const lines = rows.length
      ? rows.map((r, idx) => `**${idx + 1}.** <@${r.user_id}> — **${r.score}**`).join('\n')
      : '_No data yet._';
    const embed = new EmbedBuilder().setColor(0xffc857).setTitle(`🏅 Leaderboard — ${cat}`).setDescription(lines);
    return i.reply({ embeds: [embed], ephemeral: false });
  }

  if (commandName === 'award') {
    const target = options.getUser('user', true);
    const amount = options.getInteger('amount', true);
    const category = options.getString('category', true);
    const reason = options.getString('reason') ?? 'Manual award';
    addPoints({ guildId: guild.id, userId: target.id, category, amount });
    const row = getUserStmt.get(guild.id, target.id);
    await i.reply({ content: `🎁 Awarded **+${amount}** to <@${target.id}> in **${category}**. Total: **${row.total}**\nReason: ${reason}` });
    auditLog(guild, `🎁 **Manual award**: <@${target.id}> **+${amount}** in **${category}** • By <@${user.id}> • Reason: ${reason}`);
    return;
  }

  if (commandName === 'config') {
    const sub = options.getSubcommand();
    if (sub === 'setcheckins') {
      const ch = options.getChannel('channel', true);
      upsertConfig.run({
        guild_id: guild.id,
        checkins_channel_id: ch.id,
        audit_channel_id: null,
        gym: null, badminton: null, cricket: null, exercise: null
      });
      return i.reply({ content: `✅ Check-ins channel set to ${ch}.`, ephemeral: true });
    }
    if (sub === 'setaudit') {
      const ch = options.getChannel('channel', true);
      upsertConfig.run({
        guild_id: guild.id,
        checkins_channel_id: null,
        audit_channel_id: ch.id,
        gym: null, badminton: null, cricket: null, exercise: null
      });
      return i.reply({ content: `✅ Audit channel set to ${ch}.`, ephemeral: true });
    }
    if (sub === 'setcooldowns') {
      const gym = options.getInteger('gym');
      const badminton = options.getInteger('badminton');
      const cricket = options.getInteger('cricket');
      const exercise = options.getInteger('exercise');
      upsertConfig.run({
        guild_id: guild.id,
        checkins_channel_id: null,
        audit_channel_id: null,
        gym: gym ? gym * 3600000 : null,
        badminton: badminton ? badminton * 3600000 : null,
        cricket: cricket ? cricket * 3600000 : null,
        exercise: exercise ? exercise * 3600000 : null
      });
      return i.reply({ content: '✅ Cooldowns updated (hours).', ephemeral: true });
    }
  }
});

// Habit Huddle message listener (auto-award)
client.on('messageCreate', async (msg) => {
  if (!msg.guild || msg.author.bot) return;
  const cfg = readConfig.get(msg.guild.id);
  if (!cfg?.checkins_channel_id || msg.channelId !== cfg.checkins_channel_id) return;

  const content = msg.content.toLowerCase();
  const authorId = msg.author.id;

  const tryAward = async (category) => {
    const remaining = checkCooldown({ guildId: msg.guild.id, userId: authorId, category });
    if (remaining > 0) return; // silent ignore to avoid spam
    const amount = POINTS[category];
    addPoints({ guildId: msg.guild.id, userId: authorId, category, amount });
    commitCooldown({ guildId: msg.guild.id, userId: authorId, category });
    const row = getUserStmt.get(msg.guild.id, authorId);
    msg.react('✅').catch(() => {});
    auditLog(msg.guild, `📥 Auto-award **+${amount}** to <@${authorId}> in **${category}** from check-in • Total: **${row.total}**`);
  };

  if (content.includes('exercise + pushup') || content.includes('exercise + pushups') || content.includes('pushup'))
    await tryAward('exercise');
  if (content.includes('exercise + dumbell') || content.includes('dumbell') || content.includes('dumbbells'))
    await tryAward('exercise');
  if (content.includes('gym'))
    await tryAward('gym');
  if (content.includes('badminton') || content.includes('🏸'))
    await tryAward('badminton');
  if (content.includes('cricket') || content.includes('🏏'))
    await tryAward('cricket');
});

await registerCommands();
client.login(TOKEN);
