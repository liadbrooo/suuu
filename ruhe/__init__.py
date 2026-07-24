import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import humanize_list, info

class RuheCog(commands.Cog):
    """Ein Cog, um Voice Channel von Usern zu befreien, wenn man seine Ruhe braucht."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        # Standard-Konfiguration für jeden Server
        default_guild = {
            "exempt_role": None
        }
        self.config.register_guild(**default_guild)

    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @commands.command(name="ruhe")
    async def ruhe(self, ctx):
        """
        Kickt alle User aus deinem aktuellen Voice Channel (außer dir und der verschonten Rolle).
        """
        # Prüfen, ob der Befehls-Autor überhaupt in einem Voice Channel ist
        if not ctx.author.voice or not ctx.author.voice.channel:
            await ctx.send("❌ Du musst dich in einem Voice Channel befinden, um diesen Befehl zu nutzen.")
            return

        voice_channel = ctx.author.voice.channel
        exempt_role_id = await self.config.guild(ctx.guild).exempt_role()
        exempt_role = ctx.guild.get_role(exempt_role_id) if exempt_role_id else None

        kicked_users = []
        skipped_users = []
        dm_failed = []

        # Liste kopieren, da wir sie während der Iteration nicht verändern sollten
        members = list(voice_channel.members)

        for member in members:
            # Den Befehls-Autor nicht kicken
            if member.id == ctx.author.id:
                continue
            
            # Bots generell nicht kicken
            if member.bot:
                skipped_users.append(member)
                continue

            # User mit der Ausnahme-Rolle nicht kicken
            if exempt_role and exempt_role in member.roles:
                skipped_users.append(member)
                continue

            # DM an den User senden
            dm_message = (
                f"🤫 **Ruhephase eingeläutet** 🤫\n\n"
                f"Hallo {member.display_name},\n"
                f"**{ctx.author.display_name}** benötigt gerade etwas Ruhe und hat dich daher aus dem Sprachkanal `{voice_channel.name}` entfernt.\n"
                f"Bitte habe Verständnis dafür und störe vorerst nicht weiter. Danke!"
            )
            
            try:
                await member.send(dm_message)
            except discord.Forbidden:
                # User hat DMs deaktiviert
                dm_failed.append(member)
            except Exception:
                # Andere Fehler abfangen
                pass

            # User aus dem Voice Channel kicken
            try:
                await member.move_to(None, reason=f"Ruhe-Befehl von {ctx.author}")
                kicked_users.append(member)
            except discord.HTTPException:
                pass

        # Feedback an den Befehls-Autor
        response = f"🤫 Du hast jetzt deine Ruhe, {ctx.author.mention}!\n\n"
        
        if kicked_users:
            response += f"**Aus dem Kanal entfernt:** {humanize_list([m.mention for m in kicked_users])}\n"
        if skipped_users:
            response += f"**Verschont (Ausnahme-Rolle/Bot):** {humanize_list([m.mention for m in skipped_users])}\n"
        if dm_failed:
            response += f"⚠️ **DMs konnten nicht gesendet werden an:** {humanize_list([m.mention for m in dm_failed])}\n"

        await ctx.send(response)

    @commands.guild_only()
    @commands.has_permissions(administrator=True)
    @commands.command(name="setruherolle")
    async def setruherolle(self, ctx, role: discord.Role = None):
        """
        Legt die Rolle fest, die beim `ruhe`-Befehl nicht gekickt wird.
        Lass die Rolle weg, um die Einstellung zurückzusetzen.
        """
        if role is None:
            await self.config.guild(ctx.guild).exempt_role.clear()
            await ctx.send(info("✅ Die Ausnahme-Rolle wurde zurückgesetzt. Ab jetzt werden alle (außer dir) gekickt."))
            return

        await self.config.guild(ctx.guild).exempt_role.set(role.id)
        await ctx.send(info(f"✅ Die Rolle {role.mention} wird ab sofort beim `ruhe`-Befehl verschont."))

# Setup-Funktion für Red-DiscordBot
async def setup(bot):
    await bot.add_cog(RuheCog(bot))