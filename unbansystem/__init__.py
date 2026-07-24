import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from typing import Optional, Tuple
import asyncio
import io
import html as html_module
from datetime import datetime, timedelta

class UnbanSystem(commands.Cog):
    """Ein erweitertes System zur übergreifenden Entbannung über zwei Discord-Server."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        default_guild = {
            "main_server_id": None,
            "invite_url": None,
            "ticket_category_id": None,
            "archive_category_id": None,
            "staff_role_id": None,
            "log_channel_id": None,
            "blocklist": [],
            "cooldowns": {},
            "stats": {
                "total_requests": 0,
                "accepted": 0,
                "rejected": 0,
                "withdrawn": 0,
                "total_duration_seconds": 0,
                "moderators": {}
            },
            "active_tickets": {}
        }
        self.config.register_guild(**default_guild)

    # --- Setup Befehle ---
    
    @commands.group(name="unbanset", aliases=["uset"])
    @commands.admin_or_permissions(manage_guild=True)
    async def unbanset(self, ctx: commands.Context):
        """Einstellungen für das Entbannungssystem."""
        pass

    @unbanset.command(name="mainserver")
    async def set_main_server(self, ctx: commands.Context, server_id: int):
        """Setzt die ID des Hauptdiscords, auf dem entbannt werden soll."""
        await self.config.guild(ctx.guild).main_server_id.set(server_id)
        await ctx.send(f"✅ Hauptdiscord-ID wurde auf `{server_id}` gesetzt.")

    @unbanset.command(name="invite")
    async def set_invite(self, ctx: commands.Context, invite_url: str):
        """Setzt den Einladungslink, den Nutzer nach der Entbannung erhalten."""
        await self.config.guild(ctx.guild).invite_url.set(invite_url)
        await ctx.send(f"✅ Einladungslink wurde gesetzt.")

    @unbanset.command(name="category")
    async def set_category(self, ctx: commands.Context, category_id: int):
        """Setzt die Kategorie, in der die Tickets erstellt werden sollen."""
        category = ctx.guild.get_channel(category_id)
        if not category or not isinstance(category, discord.CategoryChannel):
            return await ctx.send("❌ Ungültige Kategorie-ID.")
        await self.config.guild(ctx.guild).ticket_category_id.set(category_id)
        await ctx.send(f"✅ Ticket-Kategorie wurde auf `{category.name}` gesetzt.")

    @unbanset.command(name="archive")
    async def set_archive_category(self, ctx: commands.Context, category_id: int):
        """Setzt die Kategorie, in die geschlossene Tickets verschoben werden."""
        category = ctx.guild.get_channel(category_id)
        if not category or not isinstance(category, discord.CategoryChannel):
            return await ctx.send("❌ Ungültige Kategorie-ID.")
        await self.config.guild(ctx.guild).archive_category_id.set(category_id)
        await ctx.send(f"✅ Archiv-Kategorie wurde auf `{category.name}` gesetzt.")

    @unbanset.command(name="staffrole")
    async def set_staff_role(self, ctx: commands.Context, role_id: int):
        """Setzt die Team-Rolle, die die Tickets sehen und bearbeiten darf."""
        role = ctx.guild.get_role(role_id)
        if not role:
            return await ctx.send("❌ Ungültige Rollen-ID.")
        await self.config.guild(ctx.guild).staff_role_id.set(role_id)
        await ctx.send(f"✅ Team-Rolle wurde auf `{role.name}` gesetzt.")

    @unbanset.command(name="logchannel")
    async def set_log_channel(self, ctx: commands.Context, channel_id: int):
        """Setzt den Channel, in dem Transkripte und Logs gepostet werden."""
        channel = ctx.guild.get_channel(channel_id)
        if not channel or not isinstance(channel, discord.TextChannel):
            return await ctx.send("❌ Ungültige Channel-ID.")
        await self.config.guild(ctx.guild).log_channel_id.set(channel_id)
        await ctx.send(f"✅ Log-Channel wurde auf `{channel.mention}` gesetzt.")

    @unbanset.command(name="block")
    async def block_user(self, ctx: commands.Context, user_id: int):
        """Blockiert einen Nutzer vom Entbannungssystem."""
        async with self.config.guild(ctx.guild).blocklist() as blocklist:
            if user_id not in blocklist:
                blocklist.append(user_id)
                await ctx.send(f"✅ Nutzer `{user_id}` wurde blockiert und kann keine Tickets mehr eröffnen.")
            else:
                await ctx.send("❌ Dieser Nutzer ist bereits blockiert.")

    @unbanset.command(name="unblock")
    async def unblock_user(self, ctx: commands.Context, user_id: int):
        """Entblockt einen Nutzer."""
        async with self.config.guild(ctx.guild).blocklist() as blocklist:
            if user_id in blocklist:
                blocklist.remove(user_id)
                await ctx.send(f"✅ Nutzer `{user_id}` wurde entblockt.")
            else:
                await ctx.send("❌ Dieser Nutzer war nicht blockiert.")

    @unbanset.command(name="clearcooldown")
    async def clear_cooldown(self, ctx: commands.Context, user_id: int):
        """Entfernt den Cooldown eines Nutzers."""
        async with self.config.guild(ctx.guild).cooldowns() as cooldowns:
            if str(user_id) in cooldowns:
                del cooldowns[str(user_id)]
                await ctx.send(f"✅ Cooldown für `{user_id}` wurde entfernt.")
            else:
                await ctx.send("❌ Dieser Nutzer hat keinen aktiven Cooldown.")

    # --- Stats & Panel Befehle ---

    @commands.command(name="unbanstats")
    @commands.admin_or_permissions(manage_guild=True)
    async def unban_stats(self, ctx: commands.Context):
        """Zeigt Statistiken zum Entbannungssystem an."""
        data = await self.config.guild(ctx.guild).stats()
        
        total = data.get("total_requests", 0)
        accepted = data.get("accepted", 0)
        rejected = data.get("rejected", 0)
        withdrawn = data.get("withdrawn", 0)
        
        if total == 0:
            return await ctx.send("Es wurden bisher noch keine Anträge gestellt.")

        avg_duration_sec = data.get("total_duration_seconds", 0) / total
        avg_duration = timedelta(seconds=int(avg_duration_sec))
        
        mods_data = data.get("moderators", {})
        sorted_mods = sorted(mods_data.items(), key=lambda x: x[1]["accepted"] + x[1]["rejected"], reverse=True)
        
        mod_text = ""
        for i, (mod_id, counts) in enumerate(sorted_mods[:3], 1):
            mod_text += f"{i}. <@{mod_id}> (✅ {counts['accepted']} / ❌ {counts['rejected']})\n"
        if not mod_text:
            mod_text = "Keine Daten"

        embed = discord.Embed(title="📊 Entbannungs-Statistiken", color=discord.Color.green())
        embed.add_field(name="📝 Gesamt Anträge", value=str(total), inline=True)
        embed.add_field(name="✅ Akzeptiert", value=str(accepted), inline=True)
        embed.add_field(name="❌ Abgelehnt", value=str(rejected), inline=True)
        embed.add_field(name="↩️ Zurückgezogen", value=str(withdrawn), inline=True)
        embed.add_field(name="⏱️ Ø Bearbeitungszeit", value=str(avg_duration), inline=False)
        embed.add_field(name="🏆 Top Teammitglieder", value=mod_text, inline=False)
        
        await ctx.send(embed=embed)

    @commands.command(name="unbanpanel")
    @commands.admin_or_permissions(manage_guild=True)
    async def unban_panel(self, ctx: commands.Context):
        """Sendet das Panel, um ein Entbannungsticket zu eröffnen."""
        embed = discord.Embed(
            title="🎓 Entbannung beantragen",
            description=(
                "Wenn du auf dem Hauptdiscord gebannt wurdest und Einsicht zeigst, "
                "klicke unten auf den Button, um ein Ticket zu eröffnen.\n\n"
                "**⚠️ Achtung:** Missbrauch des Systems führt zu einer permanenten Blockierung!"
            ),
            color=discord.Color.blue()
        )
        view = TicketCreateView(self)
        await ctx.send(embed=embed, view=view)

    # --- Hilfsfunktionen ---

    async def is_on_cooldown(self, guild: discord.Guild, user_id: int) -> Tuple[bool, str]:
        async with self.config.guild(guild).cooldowns() as cooldowns:
            user_data = cooldowns.get(str(user_id))
            if not user_data:
                return False, ""
            
            if user_data["permanent"]:
                return True, "Du wurdest permanent von der Entbannungsbeantragung ausgeschlossen."
            
            end_time = datetime.fromisoformat(user_data["until"])
            if datetime.now() < end_time:
                remaining = end_time - datetime.now()
                days = remaining.days
                hours, remainder = divmod(int(remaining.total_seconds()), 3600)
                minutes = remainder // 60
                return True, f"Du musst noch warten. Verbleibende Zeit: {days} Tage, {hours} Stunden, {minutes} Minuten."
            else:
                del cooldowns[str(user_id)]
                return False, ""

    async def create_ticket_channel(self, guild: discord.Guild, member: discord.Member) -> discord.TextChannel:
        category_id = await self.config.guild(guild).ticket_category_id()
        staff_role_id = await self.config.guild(guild).staff_role_id()
        
        category = guild.get_channel(category_id) if category_id else None
        staff_role = guild.get_role(staff_role_id) if staff_role_id else None
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
            member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True),
        }
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)
            
        channel_name = f"entbannung-{member.name[:20]}"
        channel = await guild.create_text_channel(channel_name, category=category, overwrites=overwrites, reason=f"Entbannungsticket von {member.name}")
        await channel.edit(topic=f"unban-ticket-{member.id}")
        return channel

    async def send_ticket_control(self, channel: discord.TextChannel, user_id: int, applicant_id: int, ban_info: str, application_text: str):
        embed = discord.Embed(
            title="Entbannungs-Antrag eingegangen",
            description=(
                f"**Antragsteller-ID:** `{user_id}`\n\n"
                f"**Automatischer Bann-Check:**\n{ban_info}\n\n"
                f"**Antragsdaten des Nutzers:**\n{application_text}\n\n"
                "**Team-Aktionen:**\n"
                "🟢 **Entbannen:** Entbannt den Nutzer und sendet ihm den Invite.\n"
                "❌ **Ablehnen:** Öffnet ein Fenster zur Eingabe der Cooldown-Tage (0 = Permanent).\n"
                "🔵 **Claim:** Ticket als 'in Bearbeitung' markieren.\n"
                "➕ **Hinzufügen:** Ein weiteres Teammitglied zum Ticket hinzufügen.\n"
                "💬 **Diskussion:** Eröffnet einen privaten Thread *innerhalb* des Tickets für interne Gespräche.\n\n"
                "**Antragsteller-Aktion:**\n"
                "↩️ **Zurückziehen:** Zieht den Antrag zurück und schließt das Ticket sofort."
            ),
            color=discord.Color.orange()
        )
        view = TicketControlView(self, user_id, applicant_id)
        await channel.send(embed=embed, view=view)

    async def generate_html_transcript(self, channel: discord.TextChannel) -> discord.File:
        """Liest den Channelverlauf aus und erstellt eine .html Datei im Discord-Look."""
        messages = []
        async for msg in channel.history(limit=None, oldest_first=True):
            timestamp = msg.created_at.strftime("%d.%m.%Y %H:%M:%S")
            author_name = html_module.escape(msg.author.display_name)
            author_avatar = msg.author.display_avatar.url
            content = html_module.escape(msg.content).replace("\n", "<br>") if msg.content else "[Kein Text / Nur Anhang]"
            
            msg_html = f"""
            <div class="message">
                <img class="avatar" src="{author_avatar}" alt="Avatar">
                <div class="content">
                    <span class="author">{author_name}</span>
                    <span class="timestamp">{timestamp}</span>
                    <div class="text">{content}</div>
                </div>
            </div>
            """
            messages.append(msg_html)
            
        html_content = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <title>Transkript: {html_module.escape(channel.name)}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #313338; color: #dbdee1; padding: 20px; }}
        .header {{ text-align: center; border-bottom: 2px solid #4e5058; padding-bottom: 10px; margin-bottom: 20px; }}
        .message {{ display: flex; margin-bottom: 15px; }}
        .avatar {{ width: 40px; height: 40px; border-radius: 50%; margin-right: 15px; }}
        .content {{ flex: 1; }}
        .author {{ font-weight: bold; color: #f5f5f5; margin-right: 10px; }}
        .timestamp {{ font-size: 0.8em; color: #949ba4; }}
        .text {{ margin-top: 5px; line-height: 1.4; color: #dcdee1; }}
    </style>
</head>
<body>
    <div class="header"><h2>Transkript für #{html_module.escape(channel.name)}</h2></div>
    {''.join(messages)}
</body>
</html>"""
        file = io.BytesIO(html_content.encode('utf-8'))
        return discord.File(file, filename=f"transcript-{channel.name}.html")

    async def log_action(self, guild: discord.Guild, action: str, user_id: int, moderator: discord.Member, transcript_file: Optional[discord.File] = None):
        log_channel_id = await self.config.guild(guild).log_channel_id()
        if not log_channel_id: return
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel: return
            
        embed = discord.Embed(title="Entbannungs-Log", color=discord.Color.blurple(), timestamp=datetime.now())
        embed.add_field(name="Aktion", value=action, inline=False)
        embed.add_field(name="Betroffener Nutzer", value=f"`{user_id}`", inline=True)
        embed.add_field(name="Moderator", value=f"{moderator.mention} (`{moderator.id}`)", inline=True)
        
        if transcript_file:
            await log_channel.send(embed=embed, file=transcript_file)
        else:
            await log_channel.send(embed=embed)

    async def update_stats(self, guild: discord.Guild, action: str, moderator_id: int, duration_seconds: int):
        async with self.config.guild(guild).stats() as stats:
            if action == "accepted":
                stats["accepted"] += 1
                if str(moderator_id) not in stats["moderators"]:
                    stats["moderators"][str(moderator_id)] = {"accepted": 0, "rejected": 0}
                stats["moderators"][str(moderator_id)]["accepted"] += 1
            elif action == "rejected":
                stats["rejected"] += 1
                if str(moderator_id) not in stats["moderators"]:
                    stats["moderators"][str(moderator_id)] = {"accepted": 0, "rejected": 0}
                stats["moderators"][str(moderator_id)]["rejected"] += 1
            elif action == "withdrawn":
                stats["withdrawn"] += 1
                
            stats["total_duration_seconds"] += duration_seconds

    async def archive_ticket(self, channel: discord.TextChannel, reason: str):
        guild = channel.guild
        archive_cat_id = await self.config.guild(guild).archive_category_id()
        archive_cat = guild.get_channel(archive_cat_id) if archive_cat_id else None
        
        topic = channel.topic or ""
        if "unban-ticket-" in topic:
            try:
                user_id = int(topic.replace("unban-ticket-", ""))
                user = guild.get_member(user_id)
                if user:
                    await channel.set_permissions(user, view_channel=False, send_messages=False, read_message_history=False)
            except:
                pass
                
        if archive_cat:
            await channel.edit(category=archive_cat, name=f"archiv-{channel.name[11:]}", reason=f"Archiviert: {reason}")
        else:
            await channel.edit(name=f"archiv-{channel.name[11:]}", reason=f"Archiviert: {reason}")

    async def process_unban(self, interaction: discord.Interaction, user_id: int):
        guild = interaction.guild
        main_server_id = await self.config.guild(guild).main_server_id()
        invite_url = await self.config.guild(guild).invite_url()
        
        if not main_server_id or not invite_url:
            return await interaction.response.send_message("❌ Setup ist unvollständig.", ephemeral=True)
            
        main_guild = self.bot.get_guild(main_server_id)
        if not main_guild:
            return await interaction.response.send_message("❌ Bot ist nicht auf dem Hauptdiscord.", ephemeral=True)
            
        try:
            await main_guild.unban(discord.Object(id=user_id), reason=f"Entbannt durch {interaction.user}")
        except discord.NotFound:
            pass
        except discord.Forbidden:
            return await interaction.response.send_message("❌ Bot hat keine Rechte zum Entbannen auf dem Hauptdiscord.", ephemeral=True)
            
        user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
        if user:
            try:
                await user.send(f"✅ Du wurdest auf dem Hauptdiscord entbannt! Du kannst hier wieder beitreten: {invite_url}")
            except discord.Forbidden:
                await interaction.channel.send("⚠️ Konnte keine DM an den Nutzer senden.")
                
        async with self.config.guild(guild).cooldowns() as cooldowns:
            if str(user_id) in cooldowns:
                del cooldowns[str(user_id)]
                
        await interaction.response.send_message(f"✅ `{user_id}` wurde entbannt und eingeladen. Ticket wird archiviert...")
        
        transcript = await self.generate_html_transcript(interaction.channel)
        await self.log_action(guild, "Entbannt (Akzeptiert)", user_id, interaction.user, transcript)
        
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(interaction.channel.id))
            if ticket_data:
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                await self.update_stats(guild, "accepted", interaction.user.id, int(duration))
                del active_tickets[str(interaction.channel.id)]
        
        await asyncio.sleep(5)
        await self.archive_ticket(interaction.channel, "Entbannung erfolgreich")

    async def process_reject(self, interaction: discord.Interaction, user_id: int, permanent: bool, days: int = 0):
        guild = interaction.guild
        async with self.config.guild(guild).cooldowns() as cooldowns:
            if permanent:
                cooldowns[str(user_id)] = {"permanent": True, "until": None}
            else:
                until = datetime.now() + timedelta(days=days)
                cooldowns[str(user_id)] = {"permanent": False, "until": until.isoformat()}
                
        user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
        if user:
            try:
                if permanent:
                    await user.send("❌ Dein Entbannungsantrag wurde permanent abgelehnt. Du kannst keine weiteren Anträge mehr stellen.")
                else:
                    await user.send(f"❌ Dein Entbannungsantrag wurde abgelehnt. Du kannst in {days} Tagen erneut einen Antrag stellen.")
            except discord.Forbidden:
                pass
            
        status_text = "permanent abgelehnt" if permanent else f"für {days} Tage abgelehnt"
        await interaction.response.send_message(f"❌ Antrag wurde {status_text}. Ticket wird archiviert...")
        
        transcript = await self.generate_html_transcript(interaction.channel)
        action = "Abgelehnt (Permanent)" if permanent else f"Abgelehnt ({days} Tage)"
        await self.log_action(guild, action, user_id, interaction.user, transcript)
        
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(interaction.channel.id))
            if ticket_data:
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                await self.update_stats(guild, "rejected", interaction.user.id, int(duration))
                del active_tickets[str(interaction.channel.id)]
        
        await asyncio.sleep(5)
        await self.archive_ticket(interaction.channel, f"Antrag abgelehnt von {interaction.user}")

    async def process_withdraw(self, interaction: discord.Interaction, user_id: int):
        guild = interaction.guild
        await interaction.response.send_message("↩️ Dieser Antrag wurde vom Antragsteller zurückgezogen. Das Ticket wird archiviert...", ephemeral=False)
        
        transcript = await self.generate_html_transcript(interaction.channel)
        await self.log_action(guild, "Zurückgezogen durch Antragsteller", user_id, interaction.user, transcript)
        
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(interaction.channel.id))
            if ticket_data:
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                await self.update_stats(guild, "withdrawn", interaction.user.id, int(duration))
                del active_tickets[str(interaction.channel.id)]
                
        await asyncio.sleep(5)
        await self.archive_ticket(interaction.channel, "Antrag vom Nutzer zurückgezogen")

# --- UI Views & Modals ---

class UnbanApplicationModal(discord.ui.Modal, title="Entbannungs-Antrag"):
    def __init__(self, cog: UnbanSystem):
        super().__init__()
        self.cog = cog

    discord_id = discord.ui.TextInput(
        label="Wie lautet deine Discord-ID?",
        placeholder="Rechtsklick auf dich selbst -> ID kopieren (18 Zahlen)",
        required=True,
        min_length=17,
        max_length=19,
    )
    ban_reason = discord.ui.TextInput(
        label="Warum wurdest du gebannt?",
        placeholder="Was hast du getan, das zum Bann geführt hat?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=500,
    )
    apology = discord.ui.TextInput(
        label="Warum sollen wir dich entbannen?",
        placeholder="Erkläre, warum wir dir vergeben sollten.",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        user_id_str = self.discord_id.value
        
        if not user_id_str.isdigit():
            return await interaction.response.send_message("❌ Die Discord-ID darf nur aus Zahlen bestehen.", ephemeral=True)
            
        target_user_id = int(user_id_str)
        
        blocklist = await self.cog.config.guild(guild).blocklist()
        if interaction.user.id in blocklist or target_user_id in blocklist:
            return await interaction.response.send_message("❌ Du oder die angegebene ID stehen auf der Blockliste und können keinen Antrag stellen.", ephemeral=True)
            
        is_cooldown, msg = await self.cog.is_on_cooldown(guild, interaction.user.id)
        if is_cooldown:
            return await interaction.response.send_message(f"❌ Du kannst aktuell kein Ticket eröffnen. {msg}", ephemeral=True)
            
        channel = await self.cog.create_ticket_channel(guild, interaction.user)
        
        async with self.cog.config.guild(guild).active_tickets() as active_tickets:
            active_tickets[str(channel.id)] = {
                "user_id": target_user_id,
                "created_at": datetime.now().isoformat()
            }
            async with self.cog.config.guild(guild).stats() as stats:
                stats["total_requests"] += 1
        
        staff_role_id = await self.cog.config.guild(guild).staff_role_id()
        staff_ping = f"<@&{staff_role_id}>" if staff_role_id else ""
        await channel.send(f"{staff_ping} Ein neuer Entbannungsantrag ist eingegangen!", allowed_mentions=discord.AllowedMentions(roles=True))
        
        main_server_id = await self.cog.config.guild(guild).main_server_id()
        ban_info = "Keine Bann-Informationen gefunden (Bot hat evtl. keine Rechte oder Server-ID fehlt)."
        if main_server_id:
            main_guild = self.cog.bot.get_guild(main_server_id)
            if main_guild:
                try:
                    ban_entry = await main_guild.fetch_ban(discord.Object(id=target_user_id))
                    ban_info = f"✅ **Gebannt gefunden!**\nGrund: `{ban_entry.reason or 'Kein Grund angegeben'}`"
                except discord.NotFound:
                    ban_info = "ℹ️ Dieser Nutzer ist auf dem Hauptdiscord *nicht* gebannt."
                except discord.Forbidden:
                    ban_info = "❌ Bot fehlen die Rechte (Banns einsehen) auf dem Hauptserver."
                    
        application_text = (
            f"**Antragsteller:** {interaction.user.mention}\n"
            f"**Angegeben ID:** `{target_user_id}`\n"
            f"**Bann-Grund (laut Nutzer):** {self.ban_reason.value}\n"
            f"**Warum entbannen?** {self.apology.value}"
        )
        
        await self.cog.send_ticket_control(channel, target_user_id, interaction.user.id, ban_info, application_text)
        await interaction.response.send_message(f"✅ Dein Ticket wurde erstellt: {channel.mention}", ephemeral=True)


class AddUserModal(discord.ui.Modal, title="Teammitglied hinzufügen"):
    def __init__(self, cog: UnbanSystem):
        super().__init__()
        self.cog = cog

    user_id = discord.ui.TextInput(
        label="Discord-ID des Teammitglieds",
        placeholder="18-stellige ID eingeben...",
        required=True,
        min_length=17,
        max_length=19,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not self.user_id.value.isdigit():
            return await interaction.response.send_message("❌ Die ID darf nur aus Zahlen bestehen.", ephemeral=True)
            
        target_id = int(self.user_id.value)
        guild = interaction.guild
        member = guild.get_member(target_id)
        
        if not member:
            return await interaction.response.send_message("❌ Dieser Nutzer ist nicht auf diesem Server.", ephemeral=True)
            
        await interaction.channel.set_permissions(member, view_channel=True, send_messages=True, read_message_history=True, attach_files=True)
        await interaction.response.send_message(f"➕ {member.mention} wurde von {interaction.user.mention} zum Ticket hinzugefügt.")


class RejectModal(discord.ui.Modal, title="Antrag ablehnen"):
    def __init__(self, cog: UnbanSystem, user_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id

    days = discord.ui.TextInput(
        label="Tage bis zur erneuten Antragsstellung (0 = Permanent)",
        placeholder="z.B. 30 für 30 Tage. 0 für permanent.",
        required=True,
        min_length=1,
        max_length=3,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not self.days.value.isdigit():
            return await interaction.response.send_message("❌ Bitte gib eine gültige Zahl ein.", ephemeral=True)
            
        days_int = int(self.days.value)
        permanent = True if days_int == 0 else False
        await self.cog.process_reject(interaction, self.user_id, permanent, days_int)


class TicketCreateView(discord.ui.View):
    def __init__(self, cog: UnbanSystem):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Entbannung beantragen", style=discord.ButtonStyle.primary, custom_id="unban_create_ticket", emoji="📝")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        for channel in interaction.guild.text_channels:
            if channel.topic and f"unban-ticket-{interaction.user.id}" in channel.topic:
                return await interaction.response.send_message(f"❌ Du hast bereits ein offenes Ticket: {channel.mention}", ephemeral=True)
                
        modal = UnbanApplicationModal(self.cog)
        await interaction.response.send_modal(modal)


class TicketControlView(discord.ui.View):
    def __init__(self, cog: UnbanSystem, user_id: int, applicant_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.user_id = user_id
        self.applicant_id = applicant_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        staff_role_id = await self.cog.config.guild(interaction.guild).staff_role_id()
        
        # Wenn es der Zurückziehen-Button ist, darf nur der Antragsteller klicken
        if interaction.data["custom_id"] == "unban_user_close":
            if interaction.user.id != self.applicant_id and not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("❌ Nur der Antragsteller kann den Antrag zurückziehen.", ephemeral=True)
                return False
            return True
            
        # Für alle anderen Buttons gilt: Nur Team
        if staff_role_id is None:
            return True
        
        if staff_role_id not in [role.id for role in interaction.user.roles] and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Du hast keine Berechtigung, diese Buttons zu nutzen.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Entbannen", style=discord.ButtonStyle.success, custom_id="unban_accept", emoji="✅")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.process_unban(interaction, self.user_id)

    @discord.ui.button(label="Ablehnen", style=discord.ButtonStyle.danger, custom_id="unban_reject", emoji="❌")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = RejectModal(self.cog, self.user_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, custom_id="unban_claim", emoji="🔵")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        button.label = f"Claimed by {interaction.user.name}"
        await interaction.message.edit(view=self)
        await interaction.response.send_message(f"🔵 {interaction.user.mention} kümmert sich nun um dieses Ticket.\n\n⏳ {interaction.user.mention}, dein Antrag wird nun geprüft. Bitte habe etwas Geduld.", ephemeral=False)

    @discord.ui.button(label="Hinzufügen", style=discord.ButtonStyle.secondary, custom_id="unban_add_user", emoji="➕")
    async def add_user(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AddUserModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Diskussion", style=discord.ButtonStyle.secondary, custom_id="unban_thread", emoji="💬")
    async def create_thread(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Prüfen ob schon ein Thread existiert
        for thread in interaction.channel.threads:
            if thread.name == "Interne Diskussion":
                return await interaction.response.send_message(f"Ein privater Thread existiert bereits: {thread.mention}", ephemeral=True)
        
        # Privaten Thread erstellen (nur sichtbar für Team und Mods, Antragsteller sieht ihn nicht)
        thread = await interaction.channel.create_thread(
            name="Interne Diskussion",
            type=discord.ChannelType.private_thread,
            auto_archive_duration=10080,
            reason=f"Interne Diskussion für Ticket von {self.user_id}"
        )
        await thread.send(f"🔒 Dies ist ein privater Thread für das Team. Der Antragsteller sieht diesen Thread nicht. Hier könnt ihr intern über den Antrag von <@{self.user_id}> diskutieren.")
        await interaction.response.send_message(f"💬 Ein privater Thread für die interne Diskussion wurde erstellt: {thread.mention}", ephemeral=True)

    @discord.ui.button(label="Antrag zurückziehen", style=discord.ButtonStyle.danger, custom_id="unban_user_close", emoji="↩️")
    async def user_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.process_withdraw(interaction, self.user_id)


async def setup(bot: Red):
    await bot.add_cog(UnbanSystem(bot))
