import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from typing import Optional, Tuple
import asyncio
import io
import html as html_module
import re
from datetime import datetime, timedelta

class UnbanSystem(commands.Cog):
    """Ein erweitertes System zur übergreifenden Unban-Verwaltung über zwei Discord-Server."""

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
        # Persistente Panel-View erstellen (Registrierung erfolgt in cog_load)
        self.panel_view = TicketCreateView(self)

    async def cog_load(self):
        """Registriert persistente Views beim Bot-Start."""
        # Panel-View registrieren
        self.bot.add_view(self.panel_view)
        # Für jedes aktive Ticket eine View registrieren
        for guild in self.bot.guilds:
            active_tickets = await self.config.guild(guild).active_tickets()
            for channel_id_str in active_tickets:
                channel_id = int(channel_id_str)
                view = TicketControlView(self, channel_id)
                self.bot.add_view(view)

    # --- Setup Befehle ---
    
    @commands.group(name="unbanset", aliases=["uset"])
    @commands.admin_or_permissions(manage_guild=True)
    async def unbanset(self, ctx: commands.Context):
        """Einstellungen für das Unban-System."""
        pass

    @unbanset.command(name="mainserver")
    async def set_main_server(self, ctx: commands.Context, server_id: int):
        """Setzt die ID des Hauptdiscords, auf dem entbannt werden soll."""
        await self.config.guild(ctx.guild).main_server_id.set(server_id)
        await ctx.send(f"✅ Hauptdiscord-ID wurde auf `{server_id}` gesetzt.")

    @unbanset.command(name="invite")
    async def set_invite(self, ctx: commands.Context, invite_url: str):
        """Setzt den Einladungslink, den Nutzer nach der Unban erhalten."""
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
        """Blockiert einen Nutzer vom Unban-System."""
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
        """Entfernt den Cooldown (oder permanente Sperre) eines Nutzers."""
        async with self.config.guild(ctx.guild).cooldowns() as cooldowns:
            if str(user_id) in cooldowns:
                del cooldowns[str(user_id)]
                await ctx.send(f"✅ Cooldown/Sperrung für `{user_id}` wurde entfernt.")
            else:
                await ctx.send("❌ Dieser Nutzer hat keinen aktiven Cooldown.")

    # --- Stats & Panel Befehle ---

    @commands.command(name="unbanstats")
    @commands.admin_or_permissions(manage_guild=True)
    async def unban_stats(self, ctx: commands.Context):
        """Zeigt Statistiken zum Unban-System an."""
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

        embed = discord.Embed(title="📊 Unban-Statistiken", color=discord.Color.green())
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
        """Sendet das Panel, um ein Unban-Ticket zu eröffnen."""
        embed = discord.Embed(
            title="🎓 Unban beantragen",
            description=(
                "Wenn du auf dem Hauptdiscord gebannt wurdest und Einsicht zeigst, "
                "klicke unten auf den Button, um ein Ticket zu eröffnen.\n\n"
                "**⚠️ Achtung:** Missbrauch des Systems führt zu einer permanenten Blockierung!"
            ),
            color=discord.Color.blue()
        )
        # Die View ist bereits registriert und persistent
        await ctx.send(embed=embed, view=self.panel_view)

    # --- Hilfsfunktionen ---

    async def is_on_cooldown(self, guild: discord.Guild, user_id: int) -> Tuple[bool, str]:
        async with self.config.guild(guild).cooldowns() as cooldowns:
            user_data = cooldowns.get(str(user_id))
            if not user_data:
                return False, ""
            
            if user_data["permanent"]:
                return True, "Du wurdest permanent von der Unban-Beantragung ausgeschlossen."
            
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
            
        # Kanalname mit Username erstellen (sicher vor Sonderzeichen)
        safe_name = ''.join(c for c in member.name if c.isalnum() or c in '-_').lower()[:20]
        if not safe_name:
            safe_name = "user"
        base_name = f"unban-{safe_name}"
        # Sicherstellen, dass der Name eindeutig ist
        existing_names = [c.name for c in guild.text_channels]
        if base_name in existing_names:
            base_name = f"{base_name}-{member.id}"
        channel = await guild.create_text_channel(
            base_name,
            category=category,
            overwrites=overwrites,
            reason=f"Unban-Ticket von {member.name}"
        )
        await channel.edit(topic=f"unban-ticket-{member.id}")
        return channel

    async def send_ticket_control(self, channel: discord.TextChannel, user_id: int, applicant_id: int, ban_info: str, application_text: str, view: discord.ui.View):
        embed = discord.Embed(
            title="Unban-Antrag eingegangen",
            description=(
                f"**Antragsteller-ID:** `{user_id}`\n\n"
                f"**Automatischer Ban-Check:**\n{ban_info}\n\n"
                f"**Antragsdaten des Nutzers:**\n{application_text}\n\n"
                "**Team-Aktionen:**\n"
                "🟢 **Unban:** Entbannt den Nutzer und sendet ihm den Invite.\n"
                "❌ **Ablehnen:** Öffnet ein Fenster zur Eingabe der Cooldown-Tage (0 = Permanent).\n"
                "🔵 **Claim:** Ticket als 'in Bearbeitung' markieren.\n"
                "➕ **Hinzufügen:** Ein weiteres Teammitglied zum Ticket hinzufügen.\n"
                "💬 **Diskussion:** Eröffnet einen separaten, versteckten Channel für interne Gespräche.\n\n"
                "**Antragsteller-Aktion:**\n"
                "↩️ **Zurückziehen:** Zieht den Antrag zurück und schließt das Ticket sofort."
            ),
            color=discord.Color.orange()
        )
        await channel.send(embed=embed, view=view)

    async def generate_html_transcript(self, channel: discord.TextChannel) -> discord.File:
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

    async def log_action(self, guild: discord.Guild, action: str, user_id: int, actor: discord.Member, transcript_file: Optional[discord.File] = None):
        log_channel_id = await self.config.guild(guild).log_channel_id()
        if not log_channel_id: return
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel: return
            
        embed = discord.Embed(title="Unban-Log", color=discord.Color.blurple(), timestamp=datetime.now())
        embed.add_field(name="Aktion", value=action, inline=False)
        embed.add_field(name="Betroffener Nutzer", value=f"`{user_id}`", inline=True)
        embed.add_field(name="Ausgeführt von", value=f"{actor.mention} (`{actor.id}`)", inline=True)
        
        if transcript_file:
            await log_channel.send(embed=embed, file=transcript_file)
        else:
            await log_channel.send(embed=embed)

    async def update_stats(self, guild: discord.Guild, action: str, moderator_id: Optional[int], duration_seconds: int):
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

    async def archive_ticket(self, channel: discord.TextChannel, reason: str, applicant_id: Optional[int] = None):
        guild = channel.guild
        archive_cat_id = await self.config.guild(guild).archive_category_id()
        archive_cat = guild.get_channel(archive_cat_id) if archive_cat_id else None
        
        # Berechtigungen für Antragsteller entfernen
        if applicant_id:
            applicant = guild.get_member(applicant_id)
            if applicant:
                await channel.set_permissions(applicant, view_channel=False, send_messages=False, read_message_history=False)
        
        # Zugehörigen Diskussionskanal löschen
        for ch in guild.text_channels:
            if ch.topic == f"diskussion-zu-{channel.id}":
                await ch.delete(reason="Zugehöriges Ticket wurde archiviert")
                break
        
        # Kanal umbenennen und in Archiv-Kategorie verschieben
        new_name = f"archiv-{channel.id}"
        if archive_cat:
            await channel.edit(category=archive_cat, name=new_name, reason=f"Archiviert: {reason}")
        else:
            await channel.edit(name=new_name, reason=f"Archiviert: {reason}")

    async def process_unban(self, interaction: discord.Interaction, user_id: int, ban_type: str):
        guild = interaction.guild
        channel = interaction.channel
        
        if ban_type == "discord":
            main_server_id = await self.config.guild(guild).main_server_id()
            invite_url = await self.config.guild(guild).invite_url()
            
            if not main_server_id or not invite_url:
                return await channel.send("❌ Setup ist unvollständig.")
                
            main_guild = self.bot.get_guild(main_server_id)
            if not main_guild:
                return await channel.send("❌ Bot ist nicht auf dem Hauptdiscord.")
                
            try:
                await main_guild.unban(discord.Object(id=user_id), reason=f"Unban durch {interaction.user}")
            except discord.NotFound:
                pass
            except discord.Forbidden:
                return await channel.send("❌ Bot hat keine Rechte zum Unban auf dem Hauptdiscord.")
                
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            if user:
                try:
                    await user.send(f"✅ Du wurdest auf dem Hauptdiscord entbannt! Du kannst hier wieder beitreten: {invite_url}")
                except discord.Forbidden:
                    await channel.send("⚠️ Konnte keine DM an den Nutzer senden.")
                    
            async with self.config.guild(guild).cooldowns() as cooldowns:
                if str(user_id) in cooldowns:
                    del cooldowns[str(user_id)]
        else:  # FiveM-Ban
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            if user:
                try:
                    await user.send("✅ Dein FiveM-Unban-Antrag wurde angenommen. Dein Antrag wurde an das FiveM-Team weitergeleitet. Du wirst benachrichtigt, sobald der Ban aufgehoben wurde.")
                except discord.Forbidden:
                    pass
            await channel.send("✅ FiveM-Unban-Antrag angenommen. Bitte den FiveM-Ban manuell aufheben.")
        
        # Transkript erstellen und loggen
        transcript = await self.generate_html_transcript(channel)
        action = "Unban (Discord)" if ban_type == "discord" else "FiveM-Unban angenommen"
        await self.log_action(guild, action, user_id, interaction.user, transcript)
        
        # Stats aktualisieren und Ticket aus active_tickets entfernen
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(channel.id))
            if ticket_data:
                applicant_id = ticket_data.get("applicant_id")
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                await self.update_stats(guild, "accepted", interaction.user.id, int(duration))
                del active_tickets[str(channel.id)]
        
        await asyncio.sleep(5)
        await self.archive_ticket(channel, "Unban erfolgreich" if ban_type == "discord" else "FiveM-Unban angenommen", applicant_id)

    async def process_reject(self, interaction: discord.Interaction, user_id: int, permanent: bool, days: int = 0):
        guild = interaction.guild
        channel = interaction.channel
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
                    await user.send("❌ Dein Unban-Antrag wurde permanent abgelehnt. Du kannst keine weiteren Anträge mehr stellen.")
                else:
                    await user.send(f"❌ Dein Unban-Antrag wurde abgelehnt. Du kannst in {days} Tagen erneut einen Antrag stellen.")
            except discord.Forbidden:
                pass
        
        transcript = await self.generate_html_transcript(channel)
        action = "Abgelehnt (Permanent)" if permanent else f"Abgelehnt ({days} Tage)"
        await self.log_action(guild, action, user_id, interaction.user, transcript)
        
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(channel.id))
            if ticket_data:
                applicant_id = ticket_data.get("applicant_id")
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                await self.update_stats(guild, "rejected", interaction.user.id, int(duration))
                del active_tickets[str(channel.id)]
        
        await asyncio.sleep(5)
        await self.archive_ticket(channel, f"Antrag abgelehnt von {interaction.user}", applicant_id)

    async def process_withdraw(self, interaction: discord.Interaction, user_id: int, applicant_id: int):
        guild = interaction.guild
        channel = interaction.channel
        await channel.send("↩️ Dieser Antrag wurde vom Antragsteller zurückgezogen. Das Ticket wird archiviert...")
        
        transcript = await self.generate_html_transcript(channel)
        await self.log_action(guild, "Zurückgezogen durch Antragsteller", user_id, interaction.user, transcript)
        
        async with self.config.guild(guild).active_tickets() as active_tickets:
            ticket_data = active_tickets.get(str(channel.id))
            if ticket_data:
                created_at = datetime.fromisoformat(ticket_data["created_at"])
                duration = (datetime.now() - created_at).total_seconds()
                # Für withdrawn keinen Moderator in Stats zählen, daher action "withdrawn"
                await self.update_stats(guild, "withdrawn", None, int(duration))
                del active_tickets[str(channel.id)]
                
        await asyncio.sleep(5)
        await self.archive_ticket(channel, "Antrag vom Nutzer zurückgezogen", applicant_id)

# --- UI Views & Modals ---

class BanTypeSelectView(discord.ui.View):
    """Auswahl zwischen Discord- und FiveM-Ban vor dem Modal."""
    def __init__(self, cog: UnbanSystem):
        super().__init__(timeout=60)  # Kurzer Timeout, da ephemeral
        self.cog = cog

    @discord.ui.button(label="Discord-Ban", style=discord.ButtonStyle.primary)
    async def discord_ban(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = UnbanApplicationModal(self.cog, ban_type="discord")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="FiveM-Ban", style=discord.ButtonStyle.primary)
    async def fivem_ban(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = UnbanApplicationModal(self.cog, ban_type="fivem")
        await interaction.response.send_modal(modal)


class UnbanApplicationModal(discord.ui.Modal, title="Unban-Antrag"):
    def __init__(self, cog: UnbanSystem, ban_type: str):
        super().__init__()
        self.cog = cog
        self.ban_type = ban_type  # "discord" oder "fivem"
        
        # Gemeinsame Felder
        self.ban_reason_input = discord.ui.TextInput(
            label="Warum wurdest du gebannt?",
            placeholder="Was hast du getan, das zum Ban geführt hat?",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=500,
        )
        self.apology_input = discord.ui.TextInput(
            label="Warum sollen wir dich entbannen?",
            placeholder="Erkläre, warum wir dir vergeben sollten.",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1000,
        )
        
        # Zusätzliches Feld für FiveM
        if ban_type == "fivem":
            self.fivem_id_input = discord.ui.TextInput(
                label="FiveM-ID / Steam-ID (optional)",
                placeholder="Deine FiveM- oder Steam-ID, falls vorhanden",
                required=False,
                max_length=100,
            )
        
        # Reihenfolge: erst Pflichtfelder, dann optional
        self.add_item(self.ban_reason_input)
        self.add_item(self.apology_input)
        if ban_type == "fivem":
            self.add_item(self.fivem_id_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        # ID des Antragstellers automatisch verwenden
        target_user_id = interaction.user.id
        
        # Blocklist prüfen (sowohl Antragsteller als auch Ziel – hier identisch)
        blocklist = await self.cog.config.guild(guild).blocklist()
        if interaction.user.id in blocklist or target_user_id in blocklist:
            return await interaction.followup.send("❌ Du oder die angegebene ID stehen auf der Blockliste und können keinen Antrag stellen.", ephemeral=True)
            
        is_cooldown, msg = await self.cog.is_on_cooldown(guild, interaction.user.id)
        if is_cooldown:
            return await interaction.followup.send(f"❌ Du kannst aktuell kein Ticket eröffnen. {msg}", ephemeral=True)
            
        channel = await self.cog.create_ticket_channel(guild, interaction.user)
        
        async with self.cog.config.guild(guild).active_tickets() as active_tickets:
            active_tickets[str(channel.id)] = {
                "user_id": target_user_id,
                "applicant_id": interaction.user.id,
                "ban_type": self.ban_type,
                "created_at": datetime.now().isoformat()
            }
            async with self.cog.config.guild(guild).stats() as stats:
                stats["total_requests"] += 1
        
        staff_role_id = await self.cog.config.guild(guild).staff_role_id()
        staff_ping = f"<@&{staff_role_id}>" if staff_role_id else ""
        await channel.send(f"{staff_ping} Ein neuer Unban-Antrag ist eingegangen!", allowed_mentions=discord.AllowedMentions(roles=True))
        
        # Ban-Informationen je nach Typ
        ban_info = "Keine Ban-Informationen gefunden (Bot hat evtl. keine Rechte oder Server-ID fehlt)."
        if self.ban_type == "discord":
            main_server_id = await self.cog.config.guild(guild).main_server_id()
            if main_server_id:
                main_guild = self.cog.bot.get_guild(main_server_id)
                if main_guild:
                    try:
                        ban_entry = await main_guild.fetch_ban(discord.Object(id=target_user_id))
                        ban_info = f"✅ **Gebannt gefunden!**\nGrund: `{ban_entry.reason or 'Kein Grund angegeben'}`"
                    except discord.NotFound:
                        ban_info = "ℹ️ Dieser Nutzer ist auf dem Hauptdiscord *nicht* gebannt."
                    except discord.Forbidden:
                        ban_info = "❌ Bot fehlen die Rechte (Bans einsehen) auf dem Hauptserver."
        else:
            ban_info = "ℹ️ FiveM-Ban: Keine automatischen Ban-Informationen abgerufen. Bitte manuell prüfen."
            if hasattr(self, 'fivem_id_input') and self.fivem_id_input.value:
                ban_info += f"\n**FiveM-ID/Steam-ID:** `{self.fivem_id_input.value}`"
                    
        application_text = (
            f"**Antragsteller:** {interaction.user.mention}\n"
            f"**Angegebene ID:** `{target_user_id}` (automatisch erkannt)\n"
            f"**Ban-Typ:** {'FiveM' if self.ban_type == 'fivem' else 'Discord'}\n"
            f"**Ban-Grund (laut Nutzer):** {self.ban_reason_input.value}\n"
            f"**Warum entbannen?** {self.apology_input.value}"
        )
        if self.ban_type == "fivem" and hasattr(self, 'fivem_id_input') and self.fivem_id_input.value:
            application_text += f"\n**FiveM-ID/Steam-ID:** `{self.fivem_id_input.value}`"
        
        # View erstellen und registrieren
        view = TicketControlView(self.cog, channel.id)
        self.cog.bot.add_view(view)  # Persistenz für dieses Ticket
        
        await self.cog.send_ticket_control(channel, target_user_id, interaction.user.id, ban_info, application_text, view)
        await interaction.followup.send(f"✅ Dein Ticket wurde erstellt: {channel.mention}", ephemeral=True)


class AddUserModal(discord.ui.Modal, title="Teammitglied hinzufügen"):
    def __init__(self, cog: UnbanSystem):
        super().__init__()
        self.cog = cog
        self.user_id_input = discord.ui.TextInput(
            label="Discord-ID des Teammitglieds",
            placeholder="18-stellige ID eingeben...",
            required=True,
            min_length=17,
            max_length=19,
        )
        self.add_item(self.user_id_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.user_id_input.value.isdigit():
            return await interaction.response.send_message("❌ Die ID darf nur aus Zahlen bestehen.", ephemeral=True)
            
        target_id = int(self.user_id_input.value)
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
        self.days_input = discord.ui.TextInput(
            label="Tage bis zur erneuten Antragsstellung (0 = Permanent)",
            placeholder="z.B. 30 für 30 Tage. 0 für permanent.",
            required=True,
            min_length=1,
            max_length=3,
        )
        self.add_item(self.days_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not self.days_input.value.isdigit():
            return await interaction.response.send_message("❌ Bitte gib eine gültige Zahl ein.", ephemeral=True)
            
        days_int = int(self.days_input.value)
        permanent = True if days_int == 0 else False
        
        status_text = "permanent abgelehnt" if permanent else f"für {days_int} Tage abgelehnt"
        await interaction.response.send_message(f"❌ Antrag wurde {status_text}. Ticket wird archiviert...", ephemeral=False)
        
        await self.cog.process_reject(interaction, self.user_id, permanent, days_int)


class TicketCreateView(discord.ui.View):
    def __init__(self, cog: UnbanSystem):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Unban beantragen", style=discord.ButtonStyle.primary, custom_id="unban_create_ticket", emoji="📝")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Prüfen, ob der Nutzer bereits ein offenes Ticket hat
        guild = interaction.guild
        async with self.cog.config.guild(guild).active_tickets() as active:
            for channel_id_str, data in active.items():
                if data.get("applicant_id") == interaction.user.id:
                    channel = guild.get_channel(int(channel_id_str))
                    if channel:
                        return await interaction.response.send_message(f"❌ Du hast bereits ein offenes Ticket: {channel.mention}", ephemeral=True)
        
        # Cooldown prüfen
        is_cooldown, msg = await self.cog.is_on_cooldown(guild, interaction.user.id)
        if is_cooldown:
            return await interaction.response.send_message(f"❌ {msg}", ephemeral=True)
        
        # Blocklist prüfen
        blocklist = await self.cog.config.guild(guild).blocklist()
        if interaction.user.id in blocklist:
            return await interaction.response.send_message("❌ Du bist blockiert und kannst keine Tickets eröffnen.", ephemeral=True)
        
        # Auswahl der Ban-Art anzeigen
        view = BanTypeSelectView(self.cog)
        await interaction.response.send_message("Bitte wähle die Art deines Bans:", view=view, ephemeral=True)


class TicketControlView(discord.ui.View):
    def __init__(self, cog: UnbanSystem, channel_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.channel_id = channel_id
        
        # Buttons mit eindeutigen Custom-IDs pro Ticket
        self.accept_button = discord.ui.Button(
            label="Unban", style=discord.ButtonStyle.success,
            custom_id=f"unban_accept_{channel_id}", emoji="✅"
        )
        self.accept_button.callback = self.accept
        self.add_item(self.accept_button)
        
        self.reject_button = discord.ui.Button(
            label="Ablehnen", style=discord.ButtonStyle.danger,
            custom_id=f"unban_reject_{channel_id}", emoji="❌"
        )
        self.reject_button.callback = self.reject
        self.add_item(self.reject_button)
        
        self.claim_button = discord.ui.Button(
            label="Claim", style=discord.ButtonStyle.primary,
            custom_id=f"unban_claim_{channel_id}", emoji="🔵"
        )
        self.claim_button.callback = self.claim
        self.add_item(self.claim_button)
        
        self.add_user_button = discord.ui.Button(
            label="Hinzufügen", style=discord.ButtonStyle.secondary,
            custom_id=f"unban_add_user_{channel_id}", emoji="➕"
        )
        self.add_user_button.callback = self.add_user
        self.add_item(self.add_user_button)
        
        self.thread_button = discord.ui.Button(
            label="Diskussion", style=discord.ButtonStyle.secondary,
            custom_id=f"unban_thread_{channel_id}", emoji="💬"
        )
        self.thread_button.callback = self.create_thread
        self.add_item(self.thread_button)
        
        self.withdraw_button = discord.ui.Button(
            label="Antrag zurückziehen", style=discord.ButtonStyle.danger,
            custom_id=f"unban_withdraw_{channel_id}", emoji="↩️"
        )
        self.withdraw_button.callback = self.withdraw
        self.add_item(self.withdraw_button)

    async def _check_staff(self, interaction: discord.Interaction) -> bool:
        staff_role_id = await self.cog.config.guild(interaction.guild).staff_role_id()
        if staff_role_id and staff_role_id not in [r.id for r in interaction.user.roles] and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Keine Berechtigung.", ephemeral=True)
            return False
        return True

    async def _get_ticket_data(self, guild: discord.Guild) -> Optional[dict]:
        async with self.cog.config.guild(guild).active_tickets() as active:
            return active.get(str(self.channel_id))

    async def accept(self, interaction: discord.Interaction):
        if not await self._check_staff(interaction):
            return
        ticket_data = await self._get_ticket_data(interaction.guild)
        if not ticket_data:
            return await interaction.response.send_message("❌ Ticketdaten nicht gefunden.", ephemeral=True)
        
        user_id = ticket_data["user_id"]
        ban_type = ticket_data.get("ban_type", "discord")
        
        # Alle Buttons deaktivieren
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        
        await self.cog.process_unban(interaction, user_id, ban_type)

    async def reject(self, interaction: discord.Interaction):
        if not await self._check_staff(interaction):
            return
        ticket_data = await self._get_ticket_data(interaction.guild)
        if not ticket_data:
            return await interaction.response.send_message("❌ Ticketdaten nicht gefunden.", ephemeral=True)
        
        user_id = ticket_data["user_id"]
        modal = RejectModal(self.cog, user_id)
        try:
            await interaction.response.send_modal(modal)
        except Exception as e:
            await interaction.response.send_message(f"❌ Ein Fehler ist aufgetreten: {e}", ephemeral=True)

    async def claim(self, interaction: discord.Interaction):
        if not await self._check_staff(interaction):
            return
        # Button deaktivieren und Label ändern
        self.claim_button.disabled = True
        self.claim_button.label = f"Claimed by {interaction.user.name}"
        await interaction.response.edit_message(view=self)
        
        ticket_data = await self._get_ticket_data(interaction.guild)
        applicant_id = ticket_data.get("applicant_id") if ticket_data else None
        if applicant_id:
            await interaction.channel.send(f"🔵 {interaction.user.mention} kümmert sich nun um dieses Ticket.\n\n⏳ <@{applicant_id}>, dein Antrag wird nun geprüft. Bitte habe etwas Geduld.")
        else:
            await interaction.channel.send(f"🔵 {interaction.user.mention} kümmert sich nun um dieses Ticket.")

    async def add_user(self, interaction: discord.Interaction):
        if not await self._check_staff(interaction):
            return
        modal = AddUserModal(self.cog)
        await interaction.response.send_modal(modal)

    async def create_thread(self, interaction: discord.Interaction):
        if not await self._check_staff(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        ticket_channel = interaction.channel
        
        for ch in guild.text_channels:
            if ch.topic == f"diskussion-zu-{ticket_channel.id}":
                return await interaction.followup.send(f"Ein Diskussions-Channel existiert bereits: {ch.mention}", ephemeral=True)
        
        category_id = await self.cog.config.guild(guild).ticket_category_id()
        staff_role_id = await self.cog.config.guild(guild).staff_role_id()
        category = guild.get_channel(category_id) if category_id else ticket_channel.category
        staff_role = guild.get_role(staff_role_id) if staff_role_id else None
        
        ticket_data = await self._get_ticket_data(guild)
        applicant_id = ticket_data.get("applicant_id") if ticket_data else None
        applicant = guild.get_member(applicant_id) if applicant_id else None
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
        }
        if applicant:
            overwrites[applicant] = discord.PermissionOverwrite(view_channel=False)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
            
        disc_channel = await guild.create_text_channel(
            name=f"diskussion-{ticket_channel.id}",
            category=category,
            overwrites=overwrites,
            reason=f"Interne Diskussion für Ticket {ticket_channel.name}"
        )
        await disc_channel.edit(topic=f"diskussion-zu-{ticket_channel.id}")
        
        await disc_channel.send(f"🔒 Dies ist der interne Channel für das Team. Der Antragsteller sieht diesen Channel nicht. Hier könnt ihr über den Antrag diskutieren.\nTicket: {ticket_channel.mention}")
        await interaction.followup.send(f"💬 Ein interner Discussions-Channel wurde erstellt: {disc_channel.mention}", ephemeral=True)

    async def withdraw(self, interaction: discord.Interaction):
        # Nur der Antragsteller oder Admin darf zurückziehen
        ticket_data = await self._get_ticket_data(interaction.guild)
        if not ticket_data:
            return await interaction.response.send_message("❌ Ticketdaten nicht gefunden.", ephemeral=True)
        applicant_id = ticket_data.get("applicant_id")
        if interaction.user.id != applicant_id and not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Nur der Antragsteller kann den Antrag zurückziehen.", ephemeral=True)
        
        # Alle Buttons deaktivieren
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        
        await self.cog.process_withdraw(interaction, ticket_data["user_id"], applicant_id)


async def setup(bot: Red):
    await bot.add_cog(UnbanSystem(bot))
