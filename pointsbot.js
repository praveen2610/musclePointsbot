/* =========================
    COMMAND HANDLERS
========================= */
class CommandHandler {
    constructor(db) { this.db = db; }

    async handleClaim(interaction, category) {
        const { guild, user } = interaction;
        const amount = POINTS[category] || 0;
        const cooldownKey = category;

        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        // Use editReply because the initial reply was already sent
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for **${category}**: ${formatCooldown(remaining)}.` /* flags: Inherited */ });

        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category, amount, reason: `claim:${category}` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.' /* flags: Inherited */ });

        const { cur, need } = nextRankProgress(userRow.total);
        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;

        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} claimed **+${formatNumber(amount)}** pts for **${category}**!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });

        const payload = { content: '', embeds:[embed] }; // Clear placeholder content
        await interaction.editReply(payload); // Edit the initial reply first

        if (achievements.length) {
            // Send achievements as a followup
            return interaction.followUp({ embeds: [new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` })], flags: [MessageFlags.Ephemeral] });
        }
        // No return needed after editReply if no achievements
    }

    async handleDistance(interaction, activity) {
        const { guild, user, options } = interaction;
        const km = options.getNumber('km', true);
        const amount = km * DISTANCE_RATES[activity];
        const cooldownKey = 'exercise';
        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for exercises: ${formatCooldown(remaining)}.` /* flags: Inherited */ });

        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: activity, amount, reason: `distance:${activity}`, notes: `${km}km` });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownKey });

        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.' /* flags: Inherited */ });
        const { cur, need } = nextRankProgress(userRow.total);

        let footerText = `PID: ${BOT_PROCESS_ID}`;
        if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;

        const embed = new EmbedBuilder().setColor(cur.color).setDescription(`${user.toString()} logged **${formatNumber(km)}km** ${activity} → **+${formatNumber(amount)}** pts!`).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });

        const payload = { content: '', embeds:[embed] };
        await interaction.editReply(payload);

         if (achievements.length) {
             return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }) ], flags: [MessageFlags.Ephemeral] });
         }
    }

    async handleExercise(interaction) {
        const { guild, user, options } = interaction;
        const subcommand = options.getSubcommand();
        let amount = 0, description = '', cooldownCategory = 'exercise', logCategory = 'exercise', reasonPrefix = 'exercise', notes = '';

        if (subcommand === 'yoga') { cooldownCategory = 'yoga'; logCategory = 'yoga'; reasonPrefix = 'claim'; }
        else if (subcommand === 'plank') { logCategory = 'plank'; reasonPrefix = 'time'; }
        else if (REP_RATES[subcommand]) { logCategory = subcommand; reasonPrefix = 'reps'; }

        const remaining = this.db.checkCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });
        if (remaining > 0) return interaction.editReply({ content: `⏳ Cooldown for **${subcommand}**: ${formatCooldown(remaining)}.` /* flags: Inherited */ });

        switch (subcommand) {
             case 'yoga': { const minutes = options.getNumber('minutes', true); amount = POINTS.yoga || 0; description = `${user.toString()} claimed **+${formatNumber(amount)}** pts for **Yoga**!`; notes = `${minutes} min`; break; }
             case 'plank': { const minutes = options.getNumber('minutes', true); amount = minutes * PLANK_RATE_PER_MIN; description = `${user.toString()} held **plank** for **${formatNumber(minutes)} min** → **+${formatNumber(amount)}** pts!`; notes = `${minutes} min`; break; }
             case 'reps': { const count = options.getNumber('count', true); amount = count * EXERCISE_RATES.per_rep; description = `${user.toString()} logged **${count} total reps** → **+${formatNumber(amount)}** pts!`; notes = `${count} reps`; break; }
             case 'dumbbells': case 'barbell': case 'pushup': case 'squat': case 'kettlebell': case 'lunge': {
                 const repsInput = options.getInteger('reps', false); const setsInput = options.getInteger('sets', false); let totalReps;
                 if (['squat', 'kettlebell', 'lunge'].includes(subcommand)) { totalReps = repsInput || options.getInteger('reps', true); notes = `${totalReps} reps`; }
                 else { const reps = repsInput ?? 1; const sets = setsInput ?? 1; totalReps = reps * sets; notes = `${sets}x${reps} reps`; }
                 const rate = REP_RATES[subcommand] ?? EXERCISE_RATES.per_rep; amount = totalReps * rate; description = `${user.toString()} logged ${notes} **${subcommand}** → **+${formatNumber(amount)}** pts!`; break;
             }
        }

        const achievements = this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: logCategory, amount, reason: `${reasonPrefix}:${subcommand}`, notes });
        this.db.commitCooldown({ guildId: guild.id, userId: user.id, category: cooldownCategory });
        const userRow = this.db.stmts.getUser.get(guild.id, user.id);
        if (!userRow) return interaction.editReply({ content: 'Error updating score.' /* flags: Inherited */ });
        const { cur, need } = nextRankProgress(userRow.total);
        let footerText = `PID: ${BOT_PROCESS_ID}`; if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;
        const embed = new EmbedBuilder().setColor(cur.color).setDescription(description).addFields({ name: "Total", value: `🏆 ${formatNumber(userRow.total)}`, inline: true }, { name: "Rank", value: cur.name, inline: true }).setThumbnail(user.displayAvatarURL()).setFooter({ text: footerText });
        const payload = { content: '', embeds:[embed] };
        await interaction.editReply(payload);

        if (achievements.length) {
             return interaction.followUp({ embeds: [ new EmbedBuilder().setColor(0xFFD700).setTitle('🏆 Achievement!').setDescription(achievements.map(a => `**${a.name}**: ${a.description}`).join('\n')).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }) ], flags: [MessageFlags.Ephemeral] });
        }
    }

    async handleProtein(interaction) {
        const { guild, user, options } = interaction; const subcommand = options.getSubcommand(); const targetUser = options.getUser('user') || user;
        if (subcommand === 'total') { const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, targetUser.id, since); const totalProtein = result?.total || 0; const embed = new EmbedBuilder().setColor(0x5865F2).setTitle(`🥩 Daily Protein for ${targetUser.displayName}`).setDescription(`Logged **${formatNumber(totalProtein)}g** protein today.`).setThumbnail(targetUser.displayAvatarURL()).setFooter({ text: `PID: ${BOT_PROCESS_ID}` }); return interaction.editReply({ content:'', embeds: [embed] }); }
        let proteinGrams = 0, itemName = '';
        if (subcommand === 'add_item') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const q = options.getInteger('quantity', true); proteinGrams = s.protein_per_unit * q; itemName = `${q} ${s.name}`; }
        else if (subcommand === 'add_grams') { const k = options.getString('item', true); const s = PROTEIN_SOURCES[k]; const g = options.getNumber('grams', true); proteinGrams = s.protein_per_unit * g; itemName = `${g}g of ${s.name}`; }
        else if (subcommand === 'log_direct') { const g = options.getNumber('grams', true); const n = options.getString('source') || 'direct source'; proteinGrams = g; itemName = n; }
        this.db.stmts.addProteinLog.run(guild.id, user.id, itemName, proteinGrams, Math.floor(Date.now() / 1000));
        const since = getPeriodStart('day'); const result = this.db.stmts.getDailyProtein.get(guild.id, user.id, since); const totalProtein = result?.total || 0;
        const embed = new EmbedBuilder().setColor(0x2ECC71).setTitle('✅ Protein Logged!').setDescription(`${user.toString()} added **${formatNumber(proteinGrams)}g** protein from **${itemName}**.`).addFields({ name: 'Daily Total', value: `Today: **${formatNumber(totalProtein)}g** protein.` }).setThumbnail(user.displayAvatarURL()).setFooter({ text: `PID: ${BOT_PROCESS_ID}` });
        return interaction.editReply({ content:'', embeds: [embed] });
    }

    async handleJunk(interaction) {
        const { guild, user, options } = interaction; const item = options.getString('item', true); const deduction = DEDUCTIONS[item]; const msgs = ["Balance is key!", "One step back, two forward!", "Honesty is progress!", "Treats happen!", "Acknowledge and move on!"]; const msg = msgs[Math.floor(Math.random() * msgs.length)];
        this.db.modifyPoints({ guildId: guild.id, userId: user.id, category: 'junk', amount: -deduction.points, reason: `junk:${item}` }); const userRow = this.db.stmts.getUser.get(guild.id, user.id); const total = userRow?.total || 0;
        const embed = new EmbedBuilder().setColor(0xED4245).setDescription(`${user.toString()} logged ${deduction.emoji} **${deduction.label}** (-**${formatNumber(deduction.points)}** pts).`).addFields({ name: "Total", value: `🏆 ${formatNumber(total)}` }).setFooter({ text: `${msg} | PID: ${BOT_PROCESS_ID}` });
        return interaction.editReply({ content:'', embeds: [embed] });
    }

    async handleMyScore(interaction) {
        const { guild, options } = interaction; const targetUser = options.getUser('user') || interaction.user;
        const userRow = this.db.stmts.getUser.get(guild.id, targetUser.id) || { total: 0, current_streak: 0 };
        const { pct, cur, need } = nextRankProgress(userRow.total);
        const ach = this.db.stmts.getUserAchievements.all(guild.id, targetUser.id).map(r => r.achievement_id);
        const embed = new EmbedBuilder().setColor(cur.color).setAuthor({ name: targetUser.displayName, iconURL: targetUser.displayAvatarURL() }).setTitle(`Rank: ${cur.name}`).addFields({ name: 'Points', value: formatNumber(userRow.total), inline: true }, { name: 'Streak', value: `🔥 ${userRow.current_streak || 0}d`, inline: true }, { name: 'Progress', value: progressBar(pct), inline: false }, { name: 'Achievements', value: ach.length > 0 ? ach.map(id => `**${ACHIEVEMENTS.find(a => a.id === id)?.name || id}**`).join(', ') : 'None' });
        let footerText = `PID: ${BOT_PROCESS_ID}`; if (need > 0) footerText = `${formatNumber(need)} pts to next rank! | ${footerText}`;
        embed.setFooter({ text: footerText });
        return interaction.editReply({ content:'', embeds: [embed] });
    }

    async handleLeaderboard(interaction) {
        const { guild, user, options } = interaction; const cat = options.getString('category') || 'all';
        try {
            let rows = []; let subtitle = ''; let selfRank = null;
            subtitle = `All Time • ${cat === 'all' ? 'Total Points' : cat === 'streak' ? 'Current Streak' : cat.charAt(0).toUpperCase() + cat.slice(1)}`;
            if (cat === 'streak') { rows = this.db.stmts.getTopStreaks.all(guild.id); }
            else if (cat === 'all') { rows = this.db.stmts.lbAllFromPoints.all(guild.id); const my = this.db.stmts.selfRankAllFromPoints.get(guild.id, user.id); if (my) selfRank = { userId: user.id, rank: my.rank, score: my.score }; }
            else {
                 const catQueryKey = `lbAllCatFromPoints_${cat}`;
                 if (this.db.stmts[catQueryKey]) { rows = this.db.stmts[catQueryKey].all(guild.id); }
                 else {
                    console.warn(`[Leaderboard Warn] Category ${cat} not found in pre-compiled point statements, falling back to log query.`);
                     let queryCategory = cat; if (cat === 'exercise') queryCategory = EXERCISE_CATEGORIES;
                     const placeholders = Array.isArray(queryCategory) ? queryCategory.map(() => '?').join(',') : '?';
                     const query = `SELECT user_id as userId, SUM(amount) AS score FROM points_log WHERE guild_id=? AND amount <> 0 AND category IN (${placeholders}) GROUP BY user_id HAVING score <> 0 ORDER BY score DESC LIMIT 10`;
                     const params = Array.isArray(queryCategory) ? [guild.id, ...queryCategory] : [guild.id, queryCategory];
                     rows = this.db.db.prepare(query).all(...params);
                     subtitle += " (from log)";
                 }
            }
            if (!rows.length) return interaction.editReply({ content: '📊 No data.' });
            rows = rows.map((r, i) => ({ ...r, rank: i+1 }));
            const userIds = rows.map(r => r.userId);
            const members = await guild.members.fetch({ user: userIds }).catch(() => new Map());
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || `User ${row.userId.substring(0,6)}..`; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\`${cat === 'streak' ? ' days' : ''}`; });
            let footerText = `PID: ${BOT_PROCESS_ID}`;
            if (cat === 'all' && selfRank && !rows.some(r => r.userId === user.id)) footerText = `Your Rank: #${selfRank.rank} (${formatNumber(selfRank.score)} pts) | ${footerText}`;
            const embed = new EmbedBuilder().setTitle(`🏆 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp().setFooter({ text: footerText });
            return interaction.editReply({ content:'', embeds: [embed] });
        } catch (e) { console.error('LB Error:', e); return interaction.editReply({ content: '❌ Error generating leaderboard.' }); }
    }

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
            const entries = rows.map(row => { const m = members.get(row.userId); const n = m?.displayName || `User ${row.userId.substring(0,6)}..`; const s = formatNumber(row.score); const e = { 1: '🥇', 2: '🥈', 3: '🥉' }[row.rank] || `**${row.rank}.**`; return `${e} ${n} - \`${s}\``; });
            const embed = new EmbedBuilder().setTitle(`📅 Leaderboard: ${subtitle}`).setColor(0x3498db).setDescription(entries.join('\n')).setTimestamp().setFooter({ text: `PID: ${BOT_PROCESS_ID}` });
            return interaction.editReply({ content:'', embeds: [embed] });
        } catch (e) { console.error('Period LB Error:', e); return interaction.editReply({ content: '❌ Error generating periodic leaderboard.' }); }
    }

    async handleBuddy(interaction) {
        const { guild, user, options } = interaction; const targetUser = options.getUser('user'); if (!targetUser) { const b = this.db.stmts.getBuddy.get(guild.id, user.id); return interaction.editReply({ content: b?.buddy_id ? `Buddy: <@${b.buddy_id}>` : 'No buddy set!' }); } if (targetUser.id === user.id) return interaction.editReply({ content: 'Cannot be own buddy!' }); this.db.stmts.setBuddy.run(guild.id, user.id, targetUser.id); return interaction.editReply({ content: `✨ ${user.toString()} set <@${targetUser.id}> as buddy!` });
    }
    async handleNudge(interaction) {
        const { guild, user, options } = interaction; const targetUser = options.getUser('user', true); const activity = options.getString('activity', true); if (targetUser.bot || targetUser.id === user.id) return interaction.editReply({ content: "Cannot nudge bots/self." }); const isAdmin = interaction.member.permissions.has(PermissionFlagsBits.ManageGuild); const buddy = this.db.stmts.getBuddy.get(guild.id, user.id); const isBuddy = buddy?.buddy_id === targetUser.id; if (!isAdmin && !isBuddy) return interaction.editReply({ content: "Can only nudge buddy (or ask admin)." }); try { await targetUser.send(`⏰ <@${user.id}> from **${guild.name}** nudges: **${activity}**!`); return interaction.editReply({ content: `✅ Nudge sent to <@${targetUser.id}>.` }); } catch (err) { console.error(`Nudge DM Error for ${targetUser.id}:`, err); return interaction.editReply({ content: `❌ Could not DM user. They may have DMs disabled.` }); }
    }
    async handleRemind(interaction) {
        const { guild, user, options } = interaction; const activity = options.getString('activity', true); const hours = options.getNumber('hours', true); const dueAt = Date.now() + hours * 3600000; this.db.stmts.addReminder.run(guild.id, user.id, activity, dueAt); return interaction.editReply({ content: `⏰ Reminder set for **${activity}** in ${hours}h.` });
    }

    // Includes refined clear_user_data with internal try/catch and checkpoint
    async handleAdmin(interaction) {
        const { guild, user, options } = interaction; const sub = options.getSubcommand();
        const targetUser = options.getUser('user');

        if (sub === 'resetpoints') {
            return this.handleResetPoints(interaction);
        }
        if (sub === 'clear_user_data') {
            if (!targetUser) return interaction.editReply({ content: 'You must specify a user to clear.', flags: [MessageFlags.Ephemeral] });
            const confirm = options.getString('confirm', true);
            if (confirm !== 'CONFIRM') { return interaction.editReply({ content: '❌ Action cancelled. You must type `CONFIRM` to proceed.', flags: [MessageFlags.Ephemeral] }); }
            try {
                this.db.db.transaction(() => {
                    console.log(`[Admin clear_user_data] Starting transaction for ${targetUser.id}`); let totalChanges = 0;
                    try { const i=this.db.stmts.clearUserPoints.run(guild.id, targetUser.id); console.log(`Cleared points: ${i.changes}`); totalChanges+=i.changes; } catch(e){ console.error(`Err clear points:`, e); throw e; }
                    try { const i=this.db.stmts.clearUserLog.run(guild.id, targetUser.id); console.log(`Cleared points_log: ${i.changes}`); totalChanges+=i.changes; if(i.changes===0) console.warn(`WARN: points_log delete reported 0 changes`); } catch(e){ console.error(`Err clear log:`, e); throw e; }
                    try { const i=this.db.stmts.clearUserAchievements.run(guild.id, targetUser.id); console.log(`Cleared achievements: ${i.changes}`); totalChanges+=i.changes; } catch(e){ console.error(`Err clear achievements:`, e); throw e; }
                    try { const i=this.db.stmts.clearUserCooldowns.run(guild.id, targetUser.id); console.log(`Cleared cooldowns: ${i.changes}`); totalChanges+=i.changes; } catch(e){ console.error(`Err clear cooldowns:`, e); throw e; }
                    try { const i=this.db.stmts.clearUserProtein.run(guild.id, targetUser.id); console.log(`Cleared protein_log: ${i.changes}`); totalChanges+=i.changes; } catch(e){ console.error(`Err clear protein:`, e); throw e; }
                    try { const i=this.db.stmts.clearUserBuddy.run(guild.id, targetUser.id); console.log(`Cleared buddies: ${i.changes}`); totalChanges+=i.changes; } catch(e){ console.error(`Err clear buddy:`, e); throw e; }
                    console.log(`[Admin clear_user_data] Transaction finished. Total rows (approx): ${totalChanges}`);
                })();
                try { const cpResult = this.db.db.pragma('wal_checkpoint(FULL)'); console.log(`[Admin clear_user_data] WAL checkpoint OK. Result:`, cpResult); }
                catch (cpErr) { console.error(`[Admin clear_user_data] WAL checkpoint Error:`, cpErr); interaction.followUp({ content: '⚠️ Warn: Checkpoint failed, reads might be stale.', flags: [MessageFlags.Ephemeral] }).catch(()=>{}); }
                return interaction.editReply({ content: `✅ All data for <@${targetUser.id}> permanently deleted.` });
            } catch (err) { console.error(`[Admin clear_user_data] Error:`, err); return interaction.editReply({ content: `❌ Error clearing data. Check logs.` }); }
        }
        if (sub === 'show_table') {
            const tableName = options.getString('table_name', true);
            const allowedTables = ['points', 'points_log', 'cooldowns', 'buddies', 'achievements', 'protein_log', 'reminders'];
            if (!allowedTables.includes(tableName)) { return interaction.editReply({ content: '❌ Invalid table name.', flags: [MessageFlags.Ephemeral] }); }
            try {
                let orderBy = ''; if (['points_log', 'protein_log', 'reminders'].includes(tableName)) { orderBy = 'ORDER BY id DESC'; } else if (tableName === 'points') { orderBy = 'ORDER BY total DESC'; }
                const rows = this.db.db.prepare(`SELECT * FROM ${tableName} ${orderBy} LIMIT 30`).all();
                if (rows.length === 0) { return interaction.editReply({ content: `✅ Table \`${tableName}\` is empty.`, flags: [MessageFlags.Ephemeral] }); }
                const data = JSON.stringify(rows, null, 2); if (Buffer.byteLength(data, 'utf8') > 20*1024*1024) { return interaction.editReply({ content: `❌ Table data too large (> 20MB).`, flags: [MessageFlags.Ephemeral] }); }
                const attachment = new AttachmentBuilder(Buffer.from(data), { name: `${tableName}_dump.json` });
                return interaction.editReply({ content: `✅ Top/last 30 rows from \`${tableName}\`:`, files: [attachment], flags: [MessageFlags.Ephemeral] });
            } catch (err) { console.error(`Error showing table ${tableName}:`, err); return interaction.editReply({ content: `❌ Error fetching table data. Check logs.`, flags: [MessageFlags.Ephemeral] }); }
        }
        if (sub === 'download_all_tables') { return this.handleDownloadAllTables(interaction); }

        if (!targetUser && ['award', 'deduct', 'add_protein', 'deduct_protein'].includes(sub)) {
             return interaction.editReply({ content: `You must specify a user for the '${sub}' command.`, flags: [MessageFlags.Ephemeral] });
        }

        if (sub === 'award' || sub === 'deduct') {
            const amt = options.getNumber('amount', true); const cat = options.getString('category', true); const rsn = options.getString('reason') || `Admin action`; const finalAmt = sub === 'award' ? amt : -amt;
            this.db.modifyPoints({ guildId: guild.id, userId: targetUser.id, category: cat, amount: finalAmt, reason: `admin:${sub}`, notes: rsn });
            const act = sub === 'award' ? 'Awarded' : 'Deducted'; return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(amt))} ${cat} points for <@${targetUser.id}>.` });
        }
        if (sub === 'add_protein' || sub === 'deduct_protein') {
            let g = options.getNumber('grams', true); const rsn = options.getString('reason') || `Admin action`; if (sub === 'deduct_protein') g = -g;
            this.db.stmts.addProteinLog.run(guild.id, targetUser.id, `Admin: ${rsn}`, g, Math.floor(Date.now() / 1000));
            const act = sub === 'add_protein' ? 'Added' : 'Deducted'; return interaction.editReply({ content: `✅ ${act} ${formatNumber(Math.abs(g))}g protein for <@${targetUser.id}>.` });
        }
    }

    // --- NEW: Handler for /admin resetpoints ---
    async handleResetPoints(interaction) {
        const { guild } = interaction;
        const confirmName = interaction.options.getString('confirm', true);

        if (confirmName !== guild.name) {
            return interaction.editReply({ content: `❌ Reset cancelled. You must type the exact server name \`${guild.name}\` to confirm.`, flags: [MessageFlags.Ephemeral] });
        }

        try {
            console.log(`[Admin resetpoints] Starting FULL RESET for guild ${guild.id} (${guild.name}) triggered by ${interaction.user.tag}`);
            this.db.db.transaction((guildId) => {
                const tables = ['Points', 'Log', 'Cooldowns', 'Achievements', 'Buddies', 'Protein', 'Reminders'];
                let totalChanges = 0;
                tables.forEach(t => {
                    try {
                        const stmt = this.db.stmts[`resetGuild${t}`];
                        if (stmt) { const info = stmt.run(guildId); console.log(`Reset ${t}: ${info.changes} rows`); totalChanges += info.changes; }
                        else { console.warn(`Missing reset statement for ${t}`);}
                    } catch (e) { console.error(`Error resetting ${t}:`, e); throw e; } // Abort on error
                });
                 console.log(`[Admin resetpoints] Transaction finished. Total rows (approx): ${totalChanges}`);
            })(guild.id); // Execute transaction

            try { const cpResult = this.db.db.pragma('wal_checkpoint(FULL)'); console.log(`[Admin resetpoints] WAL checkpoint OK. Result:`, cpResult); }
            catch (cpErr) { console.error(`[Admin resetpoints] WAL checkpoint Error:`, cpErr); interaction.followUp({ content: '⚠️ Warn: Reset OK, but DB checkpoint failed. Reads might be stale.', flags: [MessageFlags.Ephemeral] }).catch(()=>{}); }

            return interaction.editReply({ content: `✅ All points, logs, cooldowns, achievements, etc. reset for **${guild.name}**.`, flags: [MessageFlags.Ephemeral] });
        } catch (err) {
            console.error(`[Admin resetpoints] Error during reset for guild ${guild.id}:`, err);
            return interaction.editReply({ content: `❌ Error during reset. Check logs.`, flags: [MessageFlags.Ephemeral] });
        }
    }
    // ------------------------------------

    async handleDownloadAllTables(interaction) {
        console.log(`[Admin download_all_tables] Request received by ${interaction.user.tag}`);
        const attachments = []; let fileCount = 0; const MAX_ATTACHMENTS = 10;
        try {
            const tables = this.db.db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';`).all();
            console.log(`[Admin download_all_tables] Found tables: ${tables.map(t => t.name).join(', ')}`);
            for (const table of tables) {
                if (fileCount >= MAX_ATTACHMENTS) { console.warn(`Reached attachment limit (${MAX_ATTACHMENTS}). Skipping remaining.`); await interaction.followUp({ content: `⚠️ Reached attachment limit (${MAX_ATTACHMENTS}). Some tables skipped.`, flags: MessageFlags.Ephemeral }).catch(()=>{}); break; }
                const tableName = table.name; console.log(`Processing table: ${tableName}`);
                try {
                    const rows = this.db.db.prepare(`SELECT * FROM ${tableName}`).all();
                    if (rows.length === 0) { console.log(`Table ${tableName} is empty. Skipping.`); continue; }
                    const data = JSON.stringify(rows, null, 2); const buffer = Buffer.from(data);
                     if (buffer.byteLength > 20 * 1024 * 1024) { console.warn(`Table ${tableName} data too large (> 20MB). Skipping.`); await interaction.followUp({ content: `⚠️ Data for table \`${tableName}\` too large (> 20MB). Skipped.`, flags: MessageFlags.Ephemeral }).catch(()=>{}); continue; }
                    attachments.push(new AttachmentBuilder(buffer, { name: `${tableName}.json` })); fileCount++;
                    console.log(`Prepared attachment for ${tableName} (${rows.length} rows).`);
                } catch (tableErr) { console.error(`Error fetching table ${tableName}:`, tableErr); await interaction.followUp({ content: `❌ Error fetching table \`${tableName}\`. Check logs.`, flags: MessageFlags.Ephemeral }).catch(()=>{}); }
            } // End for
            if (attachments.length > 0) { await interaction.editReply({ content: `✅ Data tables (up to ${MAX_ATTACHMENTS}):`, files: attachments }); console.log(`Sent ${attachments.length} table dumps.`); }
            else { await interaction.editReply({ content: '✅ No data found/all tables skipped.' }); console.log(`No attachments sent.`); }
        } catch (err) { console.error('❌ [Admin download_all_tables] General error:', err); await interaction.editReply({ content: '❌ Unexpected error preparing table downloads.' }).catch(()=>{}); }
    } // End handleDownloadAllTables

    async handleDbDownload(interaction) {
        const dbPath = CONFIG.dbFile;
        try {
            if (!fs.existsSync(dbPath)) { return interaction.editReply({ content: '❌ Database file not found.' }); }
            const attachment = new AttachmentBuilder(dbPath, { name: 'points.db' });
            await interaction.editReply({ content: '✅ DB Backup:', files: [attachment] });
        } catch (err) { console.error("Error sending DB file:", err); await interaction.editReply({ content: '❌ Could not send DB file.' }).catch(()=>{}); }
    }
} // End CommandHandler


/* =========================
    MAIN BOT INITIALIZATION
========================= */
async function main() {
    console.log("[Startup] Starting main function...");
    createKeepAliveServer();
    if (!CONFIG.token || !CONFIG.appId) { console.error('[Startup Error] Missing DISCORD_TOKEN or APPLICATION_ID env vars!'); process.exit(1); }
    let database; try { console.log("[Startup] Initializing database..."); database = new PointsDatabase(CONFIG.dbFile); } catch (e) { console.error("❌ [Startup FATAL] Failed to initialize Database class:", e); process.exit(1); }
    console.log("[Startup] Starting initial data reconciliation..."); reconcileTotals(database.db); console.log("[Startup] Finished reconcileTotals function call.");
    try { console.log("[Startup] Attempting WAL checkpoint..."); const checkpointResult = database.db.pragma('wal_checkpoint(FULL)'); console.log("[Startup] WAL Checkpoint Result:", checkpointResult); if (checkpointResult?.[0]?.checkpointed > -1) { console.log(`✅ [Startup] Database checkpoint successful (${checkpointResult[0].checkpointed} pages).`); } else { console.warn("⚠️ [Startup] DB checkpoint command executed but result unexpected:", checkpointResult); } } catch (e) { console.error("❌ [Startup Error] Database checkpoint failed:", e); }
    console.log("[Startup] Initializing CommandHandler..."); const handler = new CommandHandler(database);
    console.log("[Startup] Initializing REST client and registering commands..."); const rest = new REST({ version: '10' }).setToken(CONFIG.token);
    try { const route = CONFIG.devGuildId ? Routes.applicationGuildCommands(CONFIG.appId, CONFIG.devGuildId) : Routes.applicationCommands(CONFIG.appId); await rest.put(route, { body: buildCommands() }); console.log('✅ [Startup] Registered application commands.'); }
    catch (err) { console.error('❌ [Startup Error] Command registration failed:', err); if (err.rawError) console.error('Validation Errors:', JSON.stringify(err.rawError, null, 2)); else if (err.errors) console.error('Validation Errors:', JSON.stringify(err.errors, null, 2)); process.exit(1); }
    console.log("[Startup] Initializing Discord Client..."); const client = new Client({ intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMembers] });

    client.once('clientReady', (c) => { console.log(`✅ [Discord] Client is Ready! Logged in as ${c.user.tag}. PID: ${BOT_PROCESS_ID}`); console.log("[Startup] Setting isBotReady = true"); isBotReady = true; setInterval(async () => { if (!isBotReady) return; try { const now = Date.now(); const due = database.stmts.getDueReminders.all(now); for (const r of due) { try { const u = await client.users.fetch(r.user_id); await u.send(`⏰ Reminder: **${r.activity}**!`); } catch (e) { if (e.code !== 50007) { console.error(`[Reminder Error] DM fail for reminder ${r.id} to user ${r.user_id}: ${e.message} (Code: ${e.code})`); } } finally { database.stmts.deleteReminder.run(r.id); } } } catch (e) { console.error("❌ [Reminder Error] Error checking reminders:", e); } }, 60000); });

    client.on('interactionCreate', async (interaction) => {
        const receivedTime = Date.now();
        if (!isBotReady) { try { if (!interaction.replied && !interaction.deferred) { await interaction.reply({ content: "⏳ Bot starting...", flags: MessageFlags.Ephemeral }); } } catch (e) { console.error("Could not send 'not ready' reply:", e); } return; }
        if (!interaction.isChatInputCommand() || !interaction.guild) return;

        let initialReplySuccessful = false;
        try {
            let shouldBeEphemeral = ['buddy', 'nudge', 'remind', 'admin', 'myscore', 'recalculate', 'db_download'].includes(interaction.commandName);
            if (interaction.commandName === 'admin' && ['show_table', 'download_all_tables', 'resetpoints', 'clear_user_data'].includes(interaction.options.getSubcommand())) shouldBeEphemeral = true;
            if (interaction.commandName === 'buddy' && !interaction.options.getUser('user')) shouldBeEphemeral = true;
            if (interaction.commandName === 'protein' && interaction.options.getSubcommand() === 'total') shouldBeEphemeral = true;
            if (interaction.commandName === 'myscore' && interaction.options.getUser('user')) shouldBeEphemeral = false;
            if (interaction.commandName.startsWith('leaderboard')) shouldBeEphemeral = false;

            await interaction.reply({ content: '🔄 Processing...', flags: shouldBeEphemeral ? MessageFlags.Ephemeral : undefined }); initialReplySuccessful = true;
            const { commandName } = interaction; const fixedPointCategories = Object.keys(POINTS);

            if (fixedPointCategories.includes(commandName)) { await handler.handleClaim(interaction, commandName); }
            else if (commandName === 'exercise') { await handler.handleExercise(interaction); }
            else if (['walking', 'jogging', 'running'].includes(commandName)) { await handler.handleDistance(interaction, commandName); }
            else if (commandName === 'protein') { await handler.handleProtein(interaction); }
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
                    case 'recalculate': console.log("[Cmd] /recalculate"); reconcileTotals(database.db); database.db.pragma('wal_checkpoint(FULL)'); console.log("[Cmd] Recalc complete."); await interaction.editReply({ content: `✅ Totals recalculated! | PID: ${BOT_PROCESS_ID}` }); break;
                    case 'db_download': console.log("[Cmd] /db_download"); await handler.handleDbDownload(interaction); break;
                    default: console.warn(`[Cmd Warn] Unhandled: ${commandName}`); await interaction.editReply({ content: "Unknown cmd."});
                }
            }
        } catch (err) {
            const errorTime = Date.now(); console.error(`❌ [Interaction Error] Cmd /${interaction.commandName} by ${interaction.user.tag} at ${errorTime} (Total: ${errorTime - receivedTime}ms):`, err);
            if (!initialReplySuccessful && err.code === 10062) { console.error("❌ CRITICAL: Initial ack failed (10062). Cannot proceed."); return; }
            const errorReply = { content: `❌ Error processing command. Check logs.`}; const errorReplyEphemeral = { ...errorReply, flags: [MessageFlags.Ephemeral]};
            try { if (initialReplySuccessful) { await interaction.editReply(errorReply).catch(editErr => { console.error("❌ Failed editReply w/ error:", editErr); interaction.followUp(errorReplyEphemeral).catch(followUpErr => { console.error("❌ Failed followup after edit fail:", followUpErr); }); }); } else { console.warn("[Warn] Initial reply failed (not 10062). Attempting followup."); interaction.followUp(errorReplyEphemeral).catch(followUpErr => { console.error("❌ Failed followup after non-10062 initial fail:", followUpErr); }); } }
            catch (e) { console.error("❌ CRITICAL: Error sending error reply:", e); }
        }
    }); // End interactionCreate

    const shutdown = (signal) => { console.log(`[Shutdown] Received ${signal}. Shutting down...`); isBotReady = false; console.log('[Shutdown] Destroying Discord client...'); client?.destroy(); setTimeout(() => { console.log('[Shutdown] Closing database...'); database?.close(); console.log("[Shutdown] Exiting."); process.exit(0); }, 1500); };
    process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);

    console.log("[Startup] Attempting client login..."); await client.login(CONFIG.token); console.log("[Startup] client.login() resolved. Waiting for 'clientReady'...");
}

main().catch(err => { console.error('❌ [FATAL ERROR] Uncaught error in main function:', err); process.exit(1); });