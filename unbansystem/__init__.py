import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from typing import Optional
import asyncio
from datetime import datetime, timedelta

class UnbanSystem(commands.Cog):
    """Ein System zur bergreifenden Entbannung über zwei Discord-Server."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        default_guild = {
            "main_server_id": None,
            "invite_url": None,
            "ticket_category_id": None,
            "staff_role_id": None,
            "cooldowns": {} # Speichert User IDs und deren Entbannungs-Cooldowns
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

    @unbanset.command(name="clearcooldown")
    async def clear_cooldown(self, ctx: commands.Context, user_id: int):
        """Entfernt den Cooldown eines Nutzers, damit er wieder ein Ticket öffnen kann."""
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
                "Wenn du auf dem Hauptdiscord gebannt wurdest und Einsicht einsehst, "
                "klicke unten auf den Button, um ein Ticket zu eröffnen.\n\n"
                "**⚠️ Achtung:** Missbrauch des Systems führt zu einem permanenten Ausschluss aus dem Entbannungs-Discord."
            ),
            color=discord.Color.blue()
        )
        view = TicketCreateView(self)
        await ctx.send(embed=embed, view=view)

    # --- Hilfsfunktionen ---

    async def is_on_cooldown(self, guild: discord.Guild, user_id: int) -> tuple[bool, str]:
        """Prüft, ob ein Nutzer auf Cooldown ist."""
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
                # Cooldown abgelaufen, löschen
                del cooldowns[str(user_id)]
                return False, ""
                
        return False, ""

    async def create_ticket_channel(self, interaction: discord.Interaction, user: discord.Member) -> discord.TextChannel:
        guild = interaction.guild
        category_id = await self.config.guild(guild).ticket_category_id()
        staff_role_id = await self.config.guild(guild).staff_role_id()
        
        category = guild.get_channel(category_id) if category_id else None
        staff_role = guild.get_role(staff_role_id) if staff_role_id else None
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
        }
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)
            
        channel_name = f"entbannung-{user.name}"
        channel = await guild.create_text_channel(channel_name, category=category, overwrites=overwrites, reason=f"Entbannungsticket von {user}")
        return channel

    async def send_ticket_control(self, channel: discord.TextChannel, user: discord.Member):
        embed = discord.Embed(
            title="Entbannungs-Antrag eingegangen",
            description=(
                f"**Nutzer:** {user.mention} (`{user.id}`)\n\n"
                "Bitte schildere deinen Fall. Das Team wird sich dies ansehen.\n\n"
                "**Team-Aktionen:**\n"
                "🟢 **Entbannen:** Entbannt den Nutzer auf dem Hauptdiscord und sendet ihm den Invite.\n"
                "🟡 **Ablehnen (30 Tage):** Schließt das Ticket. Der Nutzer kann in 30 Tagen erneut beantragen.\n"
                "🔴 **Ablehnen (Permanent):** Schließt das Ticket. Der Nutzer kann nie wieder beantragen."
            ),
            color=discord.Color.orange()
        )
        view = TicketControlView(self, user.id)
        await channel.send(embed=embed, view=view)

    async def process_unban(self, interaction: discord.Interaction, user_id: int):
        guild = interaction.guild
        main_server_id = await self.config.guild(guild).main_server_id()
        invite_url = await self.config.guild(guild).invite_url()
        
        if not main_server_id or not invite_url:
            return await interaction.response.send_message("❌ Setup ist unvollständig (Hauptserver-ID oder Invite fehlt).", ephemeral=True)
            
        main_guild = self.bot.get_guild(main_server_id)
        if not main_guild:
            return await interaction.response.send_message("❌ Bot ist nicht auf dem Hauptdiscord.", ephemeral=True)
            
        try:
            # Entbanne den Nutzer auf dem Hauptserver
            await main_guild.unban(discord.Object(id=user_id), reason=f"Entbannt durch Team auf Entbannungs-Discord von {interaction.user}")
        except discord.NotFound:
            pass # War nicht gebannt, trotzdem einladen
        except discord.Forbidden:
            return await interaction.response.send_message("❌ Bot hat keine Rechte zum Entbannen auf dem Hauptdiscord.", ephemeral=True)
            
        # Sende DM an den Nutzer
        user = interaction.guild.get_member(user_id) or await self.bot.fetch_user(user_id)
        try:
            await user.send(f"✅ Du wurdest auf dem Hauptdiscord entbannt! Du kannst hier wieder beitreten: {invite_url}")
        except discord.Forbidden:
            await interaction.channel.send("⚠️ Konnte keine DM an den Nutzer senden. Bitte ihn manuell einzuladen.")
            
        # Cooldown entfernen falls vorhanden
        async with self.config.guild(guild).cooldowns() as cooldowns:
            if str(user_id) in cooldowns:
                del cooldowns[str(user_id)]
                
        await interaction.response.send_message(f"✅ {user.mention} wurde entbannt und eingeladen. Ticket wird in 5 Sekunden geschlossen...")
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
                
        user = interaction.guild.get_member(user_id) or await self.bot.fetch_user(user_id)
        try:
            if permanent:
                await user.send("❌ Dein Entbannungsantrag wurde permanent abgelehnt. Du kannst keine weiteren Anträge mehr stellen.")
            else:
                await user.send("❌ Dein Entbannungsantrag wurde abgelehnt. Du kannst in 30 Tagen erneut einen Antrag stellen.")
        except discord.Forbidden:
            pass
            
        status_text = "permanent abgelehnt" if permanent else "für 30 Tage abgelehnt"
        await interaction.response.send_message(f"❌ Antrag wurde {status_text}. Ticket wird in 5 Sekunden geschlossen...")
        await asyncio.sleep(5)
        await interaction.channel.delete(reason=f"Antrag abgelehnt von {interaction.user}")

# --- UI Views ---

class TicketCreateView(discord.ui.View):
    def __init__(self, cog: UnbanSystem):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Entbannung beantragen", style=discord.ButtonStyle.primary, custom_id="unban_create_ticket", emoji="📝")
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_cooldown, msg = await self.cog.is_on_cooldown(interaction.guild, interaction.user.id)
        if is_cooldown:
            return await interaction.response.send_message(f"❌ Du kannst aktuell kein Ticket eröffnen. {msg}", ephemeral=True)
            
        # Prüfen ob schon ein Ticket offen ist
        for channel in interaction.guild.text_channels:
            if channel.topic and str(interaction.user.id) in channel.topic and "unban-ticket" in channel.topic:
                return await interaction.response.send_message(f"❌ Du hast bereits ein offenes Ticket: {channel.mention}", ephemeral=True)
                
        channel = await self.cog.create_ticket_channel(interaction, interaction.user)
        await channel.edit(topic=f"unban-ticket-{interaction.user.id}")
        
        await self.cog.send_ticket_control(channel, interaction.user)
        await interaction.response.send_message(f"✅ Dein Ticket wurde erstellt: {channel.mention}", ephemeral=True)

class TicketControlView(discord.ui.View):
    def __init__(self, cog: UnbanSystem, user_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        staff_role_id = await self.cog.config.guild(interaction.guild).staff_role_id()
        if staff_role_id is None:
            return True # Wenn keine Rolle gesetzt, darf jeder (sollte man aber setzen)
        
        if staff_role_id not in [role.id for role in interaction.user.roles] and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Du hast keine Berechtigung, diese Buttons zu nutzen.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Entbannen", style=discord.ButtonStyle.success, custom_id="unban_accept", emoji="✅")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_unban(interaction, self.user_id)

    @discord.ui.button(label="Ablehnen (30 Tage)", style=discord.ButtonStyle.secondary, custom_id="unban_reject_30", emoji="⏳")
    async def reject_30(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_reject(interaction, self.user_id, permanent=False)

    @discord.ui.button(label="Ablehnen (Permanent)", style=discord.ButtonStyle.danger, custom_id="unban_reject_perm", emoji="🚫")
    async def reject_perm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.process_reject(interaction, self.user_id, permanent=True)

async def setup(bot: Red):
    await bot.add_cog(UnbanSystem(bot))