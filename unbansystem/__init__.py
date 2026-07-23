import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from typing import Optional, Tuple
import asyncio
import io
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
            "staff_role_id": None,
            "log_channel_id": None,
            "blocklist": [],
            "cooldowns": {}
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

    # --- Panel Befehl ---

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
            member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True), # FIX: Nutzer darf sehen
        }
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)
            
        channel_name = f"entbannung-{member.name[:20]}"
        channel = await guild.create_text_channel(channel_name, category=category, overwrites=overwrites, reason=f"Entbannungsticket von {member.name}")
        await channel.edit(topic=f"unban-ticket-{member.id}")
        return channel

    async def send_ticket_control(self, channel: discord.TextChannel, user_id: int, ban_info: str, application_text: str):
        embed = discord.Embed(
            title="Entbannungs-Antrag eingegangen",
            description=(
                f"**Antragsteller-ID:** `{user_id}`\n\n"
                f"**Automatischer Bann-Check:**\n{ban_info}\n\n"
                f"**Antragsdaten des Nutzers:**\n{application_text}\n\n"
                "**Team-Aktionen:**\n"
                "🟢 **Entbannen:** Entbannt den Nutzer und sendet ihm den Invite.\n"
                "🟡 **Ablehnen (30 Tage):** Schließt das Ticket. 30 Tage Cooldown.\n"
                "🔴 **Ablehnen (Permanent):** Schließt das Ticket. Permanent blockiert.\n"
                "🔵 **Claim:** Ticket als 'in Bearbeitung' markieren.\n"
                "📝 **Notiz:** Interne Notiz hinzufügen."
            ),
            color=discord.Color.orange()
        )
        view = TicketControlView(self, user_id)
        await channel.send(embed=embed, view=view)

    async def generate_transcript(self, channel: discord.TextChannel) -> discord.File:
        """Liest den Channelverlauf aus und erstellt eine .txt Datei."""
        messages = []
        async for msg in channel.history(limit=None, oldest_first=True):
            timestamp = msg.created_at.strftime("%Y-%m-%d %H:%M:%S")
            author = f"{msg.author} ({msg.author.id})"
            content = msg.content if msg.content else "[Kein Text / Nur Anhang]"
            messages.append(f"[{timestamp}] {author}: {content}")
            
        transcript_text = "\n".join(messages)
        file = io.BytesIO(transcript_text.encode('utf-8'))
        return discord.File(file, filename=f"transcript-{channel.name}.txt")

    async def log_action(self, guild: discord.Guild, action: str, user_id: int, moderator: discord.Member, transcript_file: Optional[discord.File] = None):
        """Loggt Aktionen in den Log-Channel."""
        log_channel_id = await self.config.guild(guild).log_channel_id()
        if not log_channel_id:
            return
            
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel:
            return
            
        embed = discord.Embed(
            title="Entbannungs-Log",
            color=discord.Color.blurple(),
            timestamp=datetime.now()
        )
        embed.add_field(name="Aktion", value=action, inline=False)
        embed.add_field(name="Betroffener Nutzer", value=f"`{user_id}`", inline=True)
        embed.add_field(name="Moderator", value=f"{moderator.mention} (`{moderator.id}`)", inline=True)
        
        if transcript_file:
            await log_channel.send(embed=embed, file=transcript_file)
        else:
            await log_channel.send(embed=embed)

    async def log_channel_send_note(self, guild: discord.Guild, note_text: str, target_user_id: int, moderator: discord.Member):
        """Sendet eine interne Notiz in den Log-Channel."""
        log_channel_id = await self.config.guild(guild).log_channel_id()
        if not log_channel_id: 
            return
        log_channel = guild.get_channel(log_channel_id)
        if not log_channel: 
            return
        
        embed = discord.Embed(title="Interne Team-Notiz", color=discord.Color.yellow(), timestamp=datetime.now())
        embed.add_field(name="Betroffener Nutzer", value=f"`{target_user_id}`", inline=True)
        embed.add_field(name="Moderator", value=f"{moderator.mention}", inline=True)
        embed.add_field(name="Notiz", value=note_text, inline=False)
        await log_channel.send(embed=embed)

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
                
        await interaction.response.send_message(f"✅ `{user_id}` wurde entbannt und eingeladen. Ticket wird in 5 Sekunden geschlossen...")
        
        # Log & Transcript
        transcript = await self.generate_transcript(interaction.channel)
        await self.log_action(guild, "Entbannt (Akzeptiert)", user_id, interaction.user, transcript)
        
        await asyncio.sleep(5)
        await interaction.channel.delete(reason="Entbannung erfolgreich")

    async def process_reject(self, interaction: discord.Interaction, user_id: int, permanent: bool):
        guild = interaction.guild
        async with self.config.guild(guild).cooldowns() as cooldowns:
            if permanent:
                cooldowns[str(user_id)] = {"permanent": True, "until": None}
            else:
                until = datetime.now() + timedelta(days=30)
                cooldowns[str(user_id)] = {"permanent": False, "until": until.isoformat()}
                
        user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
        if user:
            try:
                if permanent:
                    await user.send("❌ Dein Entbannungsantrag wurde permanent abgelehnt. Du kannst keine weiteren Anträge mehr stellen.")
                else:
                    await user.send("❌ Dein Entbannungsantrag wurde abgelehnt. Du kannst in 30 Tagen erneut einen Antrag stellen.")
            except discord.Forbidden:
                pass
            
        status_text = "permanent abgelehnt" if permanent else "für 30 Tage abgelehnt"
        await interaction.response.send_message(f"❌ Antrag wurde {status_text}. Ticket wird in 5 Sekunden geschlossen...")
        
        # Log & Transcript
        transcript = await self.generate_transcript(interaction.channel)
        action = "Abgelehnt (Permanent)" if permanent else "Abgelehnt (30 Tage)"
        await self.log_action(guild, action, user_id, interaction.user, transcript)
        
        await asyncio.sleep(5)
        await interaction.channel.delete(reason=f"Antrag abgelehnt von {interaction.user}")

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
        
        # Prüfen ob ID numerisch ist
        if not user_id_str.isdigit():
            return await interaction.response.send_message("❌ Die Discord-ID darf nur aus Zahlen bestehen.", ephemeral=True)
            
        target_user_id = int(user_id_str)
        
        # Blockliste prüfen
        blocklist = await self.cog.config.guild(guild).blocklist()
        if interaction.user.id in blocklist or target_user_id in blocklist:
            return await interaction.response.send_message("❌ Du oder die angegebene ID stehen auf der Blockliste und können keinen Antrag stellen.", ephemeral=True)
            
        # Cooldown prüfen
        is_cooldown, msg = await self.cog.is_on_cooldown(guild, interaction.user.id)
        if is_cooldown:
            return await interaction.response.send_message(f"❌ Du kannst aktuell kein Ticket eröffnen. {msg}", ephemeral=True)
            
        # Channel erstellen
        channel = await self.cog.create_ticket_channel(guild, interaction.user)
        
        # Team ping (FIX)
        staff_role_id = await self.cog.config.guild(guild).staff_role_id()
        staff_ping = f"<@&{staff_role_id}>" if staff_role_id else ""
        await channel.send(f"{staff_ping} Ein neuer Entbannungsantrag ist eingegangen!", allowed_mentions=discord.AllowedMentions(roles=True))
        
        # Bann-Info vom Hauptserver abrufen
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
                    
        # Antragsdaten formatieren
        application_text = (
            f"**Antragsteller:** {interaction.user.mention}\n"
            f"**Angegeben ID:** `{target_user_id}`\n"
            f"**Bann-Grund (laut Nutzer):** {self.ban_reason.value}\n"
            f"**Warum entbannen?** {self.apology.value}"
        )
        
        await self.cog.send_ticket_control(channel, target_user_id, ban_info, application_text)
        await interaction.response.send_message(f"✅ Dein Ticket wurde erstellt: {channel.mention}", ephemeral=True)


class TicketCreateView(discord.ui.View):
    def __init__(self, cog: UnbanSystem):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Entbannung beantragen", style=discord.ButtonStyle.primary, custom_id="unban_create_ticket", emoji="📝")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Prüfen ob schon ein Ticket offen ist
        for channel in interaction.guild.text_channels:
            if channel.topic and f"unban-ticket-{interaction.user.id}" in channel.topic:
                return await interaction.response.send_message(f"❌ Du hast bereits ein offenes Ticket: {channel.mention}", ephemeral=True)
                
        modal = UnbanApplicationModal(self.cog)
        await interaction.response.send_modal(modal)


class NoteModal(discord.ui.Modal, title="Interne Notiz"):
    def __init__(self, cog: UnbanSystem, user_id: int, ticket_channel: discord.TextChannel):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.ticket_channel = ticket_channel

    note = discord.ui.TextInput(
        label="Notiz (nur fürs Team sichtbar im Log)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.log_action(interaction.guild, f"Interne Notiz im Ticket von `{self.user_id}`", self.user_id, interaction.user)
        await self.cog.log_channel_send_note(interaction.guild, self.note.value, self.user_id, interaction.user)
        await interaction.response.send_message(f"📝 {interaction.user.mention} hat eine interne Notiz hinzugefügt.", ephemeral=False)


class TicketControlView(discord.ui.View):
    def __init__(self, cog: UnbanSystem, user_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        staff_role_id = await self.cog.config.guild(interaction.guild).staff_role_id()
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

    @discord.ui.button(label="Ablehnen (30 Tage)", style=discord.ButtonStyle.secondary, custom_id="unban_reject_30", emoji="⏳")
    async def reject_30(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.process_reject(interaction, self.user_id, permanent=False)

    @discord.ui.button(label="Ablehnen (Permanent)", style=discord.ButtonStyle.danger, custom_id="unban_reject_perm", emoji="🚫")
    async def reject_perm(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.process_reject(interaction, self.user_id, permanent=True)

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.primary, custom_id="unban_claim", emoji="🔵")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        button.disabled = True
        button.label = f"Claimed by {interaction.user.name}"
        await interaction.message.edit(view=self)
        await interaction.response.send_message(f"🔵 {interaction.user.mention} kümmert sich nun um dieses Ticket.", ephemeral=False)

    @discord.ui.button(label="Notiz", style=discord.ButtonStyle.secondary, custom_id="unban_note", emoji="📝")
    async def add_note(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = NoteModal(self.cog, self.user_id, interaction.channel)
        await interaction.response.send_modal(modal)


async def setup(bot: Red):
    await bot.add_cog(UnbanSystem(bot))
