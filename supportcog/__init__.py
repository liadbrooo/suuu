"""
Support Warteraum Cog für RedBot mit On-Duty System & Button-Interface

Dieser Cog erkennt, wenn ein Nutzer einen Support-Warteraum betritt oder verlässt
und sendet eine Nachricht in einem konfigurierten Text-Channel.
Enthält ein On-Duty System für Support-Teammitglieder mit Button-Interface.

Installation:
1. Kopiere den gesamten 'supportcog' Ordner in deinen RedBot cogs Ordner
   (normalerweise ~/.local/share/Red-DiscordBot/data/[DEIN_BOT_NAME]/cogs/)
2. Lade den Cog mit: [p]load supportcog
3. Konfiguriere mit:
   - [p]supportset channel #textchannel  (Setzt den Text-Channel für Benachrichtigungen)
   - [p]supportset room @VoiceChannel    (Setzt den Voice-Warteraum)
   - [p]supportset role @Rolle           (Setzt die Rolle, die gepingt wird)
   - [p]supportset dutychannel #channel  (Optional: Separater Channel für Duty-Nachrichten)
   - ODER verwende [p]supportset setup für einen interaktiven Einrichtungsassistenten

Nutzung:
- Wenn jemand den konfigurierten Voice-Channel betritt, wird automatisch
  eine schöne Nachricht im Text-Channel gesendet.
- Support-Teamler können sich per Button an- und abmelden
- Eine automatische "On Duty" Rolle wird erstellt und verwaltet
- Nur Teamler mit der Duty-Rolle werden gepingt!
"""

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from datetime import datetime, timedelta
from typing import Optional
import asyncio


class SupportCog(commands.Cog):
    """Cog für Support-Warteraum Benachrichtigungen mit Button-Duty-System"""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=12345678901234567890)

        default_guild_settings = {
            "channel": None,  # Text-Channel ID für Support-Benachrichtigungen
            "room": None,     # Voice-Channel ID des Warteraums
            "role": None,     # Rolle ID die gepingt wird (Basis-Supportrolle)
            "duty_role": None,  # Automatisch erstellte Duty-Rolle
            "use_embed": True,  # Ob Embeds verwendet werden sollen
            "enabled": True,   # Ob der Cog aktiv ist
            "duty_channel": None,  # Channel für Duty-Nachrichten
            "auto_remove_duty": True,  # Automatisch Duty entfernen nach X Stunden
            "duty_timeout": 4,  # Stunden nach denen Duty automatisch entfernt wird
            "duty_message_id": None  # Message ID der permanenten Duty-Nachricht
        }

        # Speichert On-Duty Status pro User
        default_member_settings = {
            "on_duty": False,
            "duty_start": None
        }

        self.config.register_guild(**default_guild_settings)
        self.config.register_member(**default_member_settings)

        # Cache für aktive Duty-User (wird bei Bot-Start neu aufgebaut)
        self.duty_cache = {}

    async def get_or_create_duty_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        """Erstellt oder holt die automatische Duty-Rolle"""
        duty_role_id = await self.config.guild(guild).duty_role()
        
        if duty_role_id:
            duty_role = guild.get_role(duty_role_id)
            if duty_role:
                return duty_role
        
        # Rolle existiert nicht mehr, erstelle neue
        try:
            duty_role = await guild.create_role(
                name="🟢 On Duty",
                color=discord.Color.green(),
                mentionable=True,
                reason="Automatische Duty-Rolle für Support-System"
            )
            await self.config.guild(guild).duty_role.set(duty_role.id)
            return duty_role
        except discord.Forbidden:
            return None

    async def update_duty_role(self, member: discord.Member, on_duty: bool):
        """Fügt oder entfernt die Duty-Rolle eines Members"""
        guild = member.guild
        duty_role = await self.get_or_create_duty_role(guild)
        
        if not duty_role:
            return
        
        try:
            if on_duty and duty_role not in member.roles:
                await member.add_roles(duty_role, reason="Support-Duty gestartet")
            elif not on_duty and duty_role in member.roles:
                await member.remove_roles(duty_role, reason="Support-Duty beendet")
        except discord.Forbidden:
            pass

    async def create_duty_message(self, ctx: commands.Context):
        """Erstellt eine permanente Duty-Nachricht mit Buttons"""
        guild = ctx.guild
        duty_channel_id = await self.config.guild(guild).duty_channel()
        
        if duty_channel_id:
            channel = guild.get_channel(duty_channel_id)
        else:
            channel_id = await self.config.guild(guild).channel()
            channel = guild.get_channel(channel_id) if channel_id else ctx.channel
        
        if not channel or not isinstance(channel, discord.TextChannel):
            channel = ctx.channel
        
        embed = discord.Embed(
            title="🎧 Support Duty System",
            description="Klicke auf die Buttons unten um dich für den Support-Dienst an- oder abzumelden.\n\n**Grüner Button** = Duty starten\n**Roter Button** = Duty beenden",
            color=discord.Color.blue()
        )
        embed.set_footer(text="Nur Mitglieder mit der Support-Rolle können sich auf Duty setzen")
        
        view = DutyButtonView(self)
        message = await channel.send(embed=embed, view=view)
        
        await self.config.guild(guild).duty_message_id.set(message.id)
        return message

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """Hört auf Voice-Channel Änderungen und sendet Benachrichtigungen"""

        # Ignoriere Bots
        if member.bot:
            return

        guild = member.guild
        if not guild:
            return

        # Prüfen ob Cog für dieses Guild aktiviert ist
        enabled = await self.config.guild(guild).enabled()
        if not enabled:
            return

        # Hole Konfiguration
        room_id = await self.config.guild(guild).room()
        channel_id = await self.config.guild(guild).channel()
        role_id = await self.config.guild(guild).role()
        use_embed = await self.config.guild(guild).use_embed()
        duty_channel_id = await self.config.guild(guild).duty_channel()

        # Prüfen ob alle erforderlichen Einstellungen gesetzt sind
        if not all([room_id, channel_id, role_id]):
            return

        # Prüfen ob es der konfigurierte Warteraum ist
        # User muss DEN Warteraum BETRETEN (vorher woanders oder offline, jetzt im Warteraum)
        if after.channel is None or after.channel.id != room_id:
            return
        if before.channel is not None and before.channel.id == room_id:
            # User war bereits im Warteraum - keine Aktion
            return

        # User hat den Warteraum soeben betreten
        try:
            channel = guild.get_channel(channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                return

            base_role = guild.get_role(role_id)
            if not base_role:
                return

            # Hole alle On-Duty User MIT DER DUTY ROLLE
            duty_mentions = []
            duty_members = []

            duty_role = await self.get_or_create_duty_role(guild)
            
            for m in base_role.members:
                # Prüfe ob User die Duty-Rolle hat
                if duty_role and duty_role in m.roles:
                    is_on_duty = await self.config.member(m).on_duty()
                    if is_on_duty:
                        duty_mentions.append(f"<@{m.id}>")
                        duty_members.append(m)

            user_mention = member.mention
            user_avatar = member.display_avatar.url

            # Bestimme den Channel für die Support-Anfrage Nachricht
            notify_channel = channel
            if duty_channel_id:
                duty_ch = guild.get_channel(duty_channel_id)
                if duty_ch and isinstance(duty_ch, discord.TextChannel):
                    notify_channel = duty_ch

            if use_embed:
                # Erstelle ein schönes Embed
                embed = discord.Embed(
                    title="🎧 Neue Support-Anfrage",
                    description=f"{user_mention} hat den Support-Warteraum betreten und wartet auf Hilfe!",
                    color=discord.Color.orange(),
                    timestamp=datetime.utcnow()
                )

                embed.set_thumbnail(url=user_avatar)
                embed.add_field(
                    name="👤 Nutzer",
                    value=f"{user_mention}\n(`{member.display_name}`)",
                    inline=True
                )

                if duty_members:
                    # Zeige nur On-Duty Teamler an
                    duty_list = "\n".join([f"• {m.display_name}" for m in duty_members[:5]])
                    if len(duty_members) > 5:
                        duty_list += f"\n• ...und {len(duty_members) - 5} weitere"
                    embed.add_field(
                        name="🟢 Verfügbare Supporter",
                        value=duty_list,
                        inline=True
                    )
                    # Pinge alle On-Duty User über die Duty-Rolle
                    ping_content = f"{duty_role.mention}" if duty_role else " ".join(duty_mentions)
                else:
                    embed.add_field(
                        name="🔴 Keine Supporter verfügbar",
                        value=f"Niemand ist gerade im Dienst! {base_role.mention}",
                        inline=True
                    )
                    # Fallback: Pinge die Basis-Rolle wenn niemand Duty hat
                    ping_content = f"{base_role.mention}"

                embed.add_field(
                    name="📍 Channel",
                    value=f"{after.channel.mention}",
                    inline=True
                )
                embed.set_footer(text="Support Warteraum System • On-Duty aktiv")

                # Sende das Embed mit Role-Ping im Content IM RICHTIGEN CHANNEL
                await notify_channel.send(content=ping_content, embed=embed)
            else:
                # Einfache Textnachricht (Fallback)
                if duty_members:
                    ping_content = f"{duty_role.mention}" if duty_role else " ".join(duty_mentions)
                    message = f"🎧 {ping_content} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({after.channel.mention})"
                else:
                    message = f"🎧 {base_role.mention} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({after.channel.mention}) - Niemand im Duty!"
                await notify_channel.send(message)

        except discord.Forbidden:
            # Bot hat keine Berechtigung zum Senden
            pass
        except Exception as e:
            # Logge Fehler
            print(f"Fehler in SupportCog: {e}")

    @commands.group(name="supportset", aliases=["supportconfig"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def supportset(self, ctx: commands.Context):
        """Konfiguriere den Support Warteraum Cog"""
        pass

    @supportset.command(name="channel")
    async def supportset_channel(self, ctx: commands.Context, channel: str):
        """Setze den Text-Channel für Support-Benachrichtigungen (ID oder Mention)"""
        # Unterstützt sowohl Mentions als auch IDs
        channel_id = None
        
        # Versuche Channel-Mention zu parsen
        if channel.startswith("<#") and channel.endswith(">"):
            channel_id = int(channel[2:-1])
        else:
            # Versuche als ID zu parsen
            try:
                channel_id = int(channel)
            except ValueError:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein (z.B. #channel oder 123456789)")
                return
        
        ch = ctx.guild.get_channel(channel_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Channel nicht gefunden oder kein Text-Channel!")
            return
            
        await self.config.guild(ctx.guild).channel.set(channel_id)
        await ctx.send(f"✅ Text-Channel auf {ch.mention} gesetzt.")

    @supportset.command(name="room")
    async def supportset_room(self, ctx: commands.Context, room: str):
        """Setze den Voice-Channel als Support-Warteraum (ID oder Mention)"""
        room_id = None
        
        # Versuche Channel-Mention zu parsen
        if room.startswith("<#") and room.endswith(">"):
            room_id = int(room[2:-1])
        else:
            # Versuche als ID zu parsen
            try:
                room_id = int(room)
            except ValueError:
                await ctx.send("❌ Bitte gib eine gültige Voice-Channel-ID oder Mention ein!")
                return
        
        ch = ctx.guild.get_channel(room_id)
        if not ch or not isinstance(ch, discord.VoiceChannel):
            await ctx.send("❌ Voice-Channel nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).room.set(room_id)
        await ctx.send(f"✅ Voice-Warteraum auf {ch.mention} gesetzt.")

    @supportset.command(name="role")
    async def supportset_role(self, ctx: commands.Context, role: str):
        """Setze die Basis-Supportrolle (ID oder Mention)"""
        role_id = None
        
        # Versuche Role-Mention zu parsen
        if role.startswith("<@&") and role.endswith(">"):
            role_id = int(role[3:-1])
        else:
            # Versuche als ID zu parsen
            try:
                role_id = int(role)
            except ValueError:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
        
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).role.set(role_id)
        await ctx.send(f"✅ Support-Basisrolle auf {r.mention} gesetzt.\nℹ️ Die automatische Duty-Rolle wird beim ersten Duty-Start erstellt.")

    @supportset.command(name="embed")
    async def supportset_embed(self, ctx: commands.Context, enabled: bool = None):
        """
        Aktiviere oder deaktiviere Embed-Nachrichten.

        Embeds sehen besser aus und zeigen mehr Informationen.
        Ohne Parameter wird der aktuelle Status umgeschaltet.
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).use_embed()
            await self.config.guild(ctx.guild).use_embed.set(not current)
            status = "aktiviert" if not current else "deaktiviert"
        else:
            await self.config.guild(ctx.guild).use_embed.set(enabled)
            status = "aktiviert" if enabled else "deaktiviert"

        await ctx.send(f"✅ Embed-Nachrichten {status}.")

    @supportset.command(name="toggle")
    async def supportset_toggle(self, ctx: commands.Context):
        """Aktiviere oder deaktiviere den Support Cog für diesen Server"""
        current = await self.config.guild(ctx.guild).enabled()
        await self.config.guild(ctx.guild).enabled.set(not current)
        status = "aktiviert" if not current else "deaktiviert"
        await ctx.send(f"✅ Support Cog {status}.")

    @supportset.command(name="show")
    async def supportset_show(self, ctx: commands.Context):
        """Zeige die aktuelle Konfiguration"""
        guild_data = await self.config.guild(ctx.guild).all()

        channel_id = guild_data.get("channel")
        room_id = guild_data.get("room")
        role_id = guild_data.get("role")
        duty_role_id = guild_data.get("duty_role")
        use_embed = guild_data.get("use_embed", True)
        enabled = guild_data.get("enabled")
        duty_channel_id = guild_data.get("duty_channel")
        auto_duty = guild_data.get("auto_remove_duty", True)
        duty_timeout = guild_data.get("duty_timeout", 4)

        channel_mention = f"<#{channel_id}>" if channel_id else "❌ Nicht gesetzt"
        room_mention = f"<#{room_id}>" if room_id else "❌ Nicht gesetzt"
        role_mention = f"<@&{role_id}>" if role_id else "❌ Nicht gesetzt"
        duty_role_mention = f"<@&{duty_role_id}>" if duty_role_id else "❌ Noch nicht erstellt"
        duty_channel_mention = f"<#{duty_channel_id}>" if duty_channel_id else "Gleicher wie Support-Channel"
        embed_status = "✅ Aktiv" if use_embed else "❌ Deaktiviert"
        cog_status = "✅ Aktiv" if enabled else "❌ Deaktiviert"
        auto_duty_status = f"✅ Aktiv ({duty_timeout}h)" if auto_duty else "❌ Deaktiviert"

        # Zähle aktive Duty-User
        duty_count = 0
        duty_role = ctx.guild.get_role(duty_role_id) if duty_role_id else None
        if role_id:
            base_role = ctx.guild.get_role(role_id)
            if base_role:
                for m in base_role.members:
                    if duty_role and duty_role in m.roles:
                        is_on_duty = await self.config.member(m).on_duty()
                        if is_on_duty:
                            duty_count += 1

        embed = discord.Embed(
            title="🛠️ Support Warteraum Konfiguration",
            color=discord.Color.blue()
        )
        embed.add_field(name="Cog Status", value=cog_status, inline=False)
        embed.add_field(name="Embeds", value=embed_status, inline=True)
        embed.add_field(name="Aktive Duty", value=f"🟢 {duty_count} Supporter", inline=True)
        embed.add_field(name="Auto-Duty-Ende", value=auto_duty_status, inline=True)
        embed.add_field(name="Text-Channel", value=channel_mention, inline=True)
        embed.add_field(name="Voice-Warteraum", value=room_mention, inline=True)
        embed.add_field(name="Support-Basisrolle", value=role_mention, inline=True)
        embed.add_field(name="Duty-Rolle", value=duty_role_mention, inline=True)
        embed.add_field(name="Duty-Log-Channel", value=duty_channel_mention, inline=True)

        await ctx.send(embed=embed)

    @supportset.command(name="setup")
    async def supportset_setup(self, ctx: commands.Context):
        """
        Interaktiver Einrichtungsassistent für den Support Cog.
        Führt dich Schritt für Schritt durch die Einrichtung.
        """
        await ctx.send("🔧 **Willkommen beim Support-Cog Einrichtungsassistenten!**\n\nIch werde dich jetzt durch die Einrichtung führen. Bitte antworte auf die folgenden Fragen.")

        questions = [
            ("1️⃣ Welcher **Text-Channel** soll für Support-Benachrichtigungen genutzt werden?", "channel", "text"),
            ("2️⃣ Welcher **Voice-Channel** ist der Support-Warteraum?", "room", "voice"),
            ("3️⃣ Welche **Rolle** ist die Basis-Supportrolle?", "role", "role"),
            ("4️⃣ (Optional) In welchem Channel sollen **Duty-Nachrichten** erscheinen? (Antworte mit 'skip' zum Überspringen)", "duty_channel", "text", True),
        ]

        answers = {}

        for question_data in questions:
            optional = len(question_data) > 3 and question_data[3]

            embed = discord.Embed(
                title=question_data[0],
                description="Sende deine Antwort als Nachricht hier im Channel.\n• Erwähne den Channel/die Rolle einfach mit @ oder #\n• Oder kopiere die ID (Rechtsklick -> ID kopieren)\n• Bei Frage 4 kannst du 'skip' schreiben um zu überspringen",
                color=discord.Color.orange()
            )
            await ctx.send(embed=embed)

            # Warte auf Antwort
            try:
                def check(m):
                    return m.author == ctx.author and m.channel == ctx.channel

                msg = await ctx.bot.wait_for("message", timeout=60.0, check=check)

                if question_data[2] in ["text", "voice"]:
                    if msg.content.lower() == "skip" and optional:
                        answers[question_data[1]] = None
                        continue
                    
                    # Versuche Mention zu parsen
                    if msg.content.startswith("<#") and msg.content.endswith(">"):
                        obj_id = int(msg.content[2:-1])
                    else:
                        # Versuche als ID
                        try:
                            obj_id = int(msg.content)
                        except ValueError:
                            await ctx.send("❌ Bitte erwähne einen gültigen Channel mit # oder gib die ID ein!")
                            return
                    
                    obj = ctx.guild.get_channel(obj_id)
                    if not obj:
                        await ctx.send("❌ Channel nicht gefunden!")
                        return
                    answers[question_data[1]] = obj
                    
                elif question_data[2] == "role":
                    # Versuche Role-Mention zu parsen
                    if msg.content.startswith("<@&") and msg.content.endswith(">"):
                        obj_id = int(msg.content[3:-1])
                    else:
                        try:
                            obj_id = int(msg.content)
                        except ValueError:
                            await ctx.send("❌ Bitte erwähne eine gültige Rolle mit @ oder gib die ID ein!")
                            return
                    
                    obj = ctx.guild.get_role(obj_id)
                    if not obj:
                        await ctx.send("❌ Rolle nicht gefunden!")
                        return
                    answers[question_data[1]] = obj

            except asyncio.TimeoutError:
                await ctx.send("❌ Zeitüberschreitung! Bitte starte den Assistenten neu mit `[p]supportset setup`")
                return

        # Speichere alle Einstellungen
        await self.config.guild(ctx.guild).channel.set(answers["channel"].id)
        await self.config.guild(ctx.guild).room.set(answers["room"].id)
        await self.config.guild(ctx.guild).role.set(answers["role"].id)

        if answers.get("duty_channel"):
            await self.config.guild(ctx.guild).duty_channel.set(answers["duty_channel"].id)

        embed = discord.Embed(
            title="✅ Einrichtung erfolgreich!",
            description="Der Support-Cog ist jetzt konfiguriert und bereit!\n\n**Zusammenfassung:**\n"
                        f"• 📝 Text-Channel: {answers['channel'].mention}\n"
                        f"• 🎤 Voice-Warteraum: {answers['room'].mention}\n"
                        f"• 👥 Support-Rolle: {answers['role'].mention}",
            color=discord.Color.green()
        )

        if answers.get("duty_channel"):
            embed.description += f"\n• 📢 Duty-Channel: {answers['duty_channel'].mention}"

        embed.description += "\n\n**Nächste Schritte:**\n"
        embed.description += "• Erstelle die Duty-Nachricht mit `[p]supportset createdutymessage`\n"
        embed.description += "• Support-Teamler können sich dann per Button an-/abmelden\n"
        embed.description += "• Eine automatische 🟢 On Duty Rolle wird erstellt"

        await ctx.send(embed=embed)

    @supportset.command(name="dutychannel")
    async def supportset_dutychannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Duty-Nachrichten.
        Ohne Channel-Angabe wird der normale Support-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).duty_channel.set(None)
            await ctx.send("✅ Duty-Nachrichten werden jetzt im normalen Support-Channel angezeigt.")
        else:
            # Parse channel ID or mention
            channel_id = None
            if channel.startswith("<#") and channel.endswith(">"):
                channel_id = int(channel[2:-1])
            else:
                try:
                    channel_id = int(channel)
                except ValueError:
                    await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                    return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).duty_channel.set(channel_id)
            await ctx.send(f"✅ Duty-Nachrichten werden jetzt in {ch.mention} angezeigt.")

    @supportset.command(name="autoduty")
    async def supportset_autoduty(self, ctx: commands.Context, hours: int = None):
        """
        Konfiguriere automatisches Duty-Ende nach X Stunden.

        - `0` oder `off`: Automatisches Beenden deaktivieren
        - `1-24`: Anzahl der Stunden nach denen Duty automatisch endet
        """
        if hours is None or hours <= 0:
            await self.config.guild(ctx.guild).auto_remove_duty.set(False)
            await ctx.send("✅ Automatisches Duty-Ende deaktiviert.")
        else:
            if hours > 24:
                hours = 24
            await self.config.guild(ctx.guild).auto_remove_duty.set(True)
            await self.config.guild(ctx.guild).duty_timeout.set(hours)
            await ctx.send(f"✅ Duty wird automatisch nach {hours} Stunden beendet.")

    @supportset.command(name="createdutymessage")
    async def supportset_createdutymessage(self, ctx: commands.Context):
        """
        Erstellt eine permanente Duty-Nachricht mit Buttons.
        Diese Nachricht sollte im Duty-Channel gepinnt werden.
        """
        await ctx.send("🔄 Erstelle Duty-Nachricht mit Buttons...")
        await self.create_duty_message(ctx)
        await ctx.send("✅ Duty-Nachricht wurde erstellt! Du kannst die Buttons jetzt verwenden um dich an-/abzumelden.")

    @supportset.command(name="refreshdutymessage")
    async def supportset_refreshdutymessage(self, ctx: commands.Context):
        """
        Aktualisiert die Duty-Nachricht falls sie gelöscht wurde.
        """
        old_message_id = await self.config.guild(ctx.guild).duty_message_id()
        if old_message_id:
            try:
                duty_channel_id = await self.config.guild(ctx.guild).duty_channel()
                if duty_channel_id:
                    channel = ctx.guild.get_channel(duty_channel_id)
                else:
                    channel_id = await self.config.guild(ctx.guild).channel()
                    channel = ctx.guild.get_channel(channel_id) if channel_id else ctx.channel
                
                if channel:
                    old_msg = await channel.fetch_message(old_message_id)
                    await old_msg.delete()
            except:
                pass
        
        await self.create_duty_message(ctx)
        await ctx.send("✅ Duty-Nachricht wurde aktualisiert!")


class DutyButtonView(discord.ui.View):
    """Button-View für Duty An-/Abmeldung"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Duty Starten", style=discord.ButtonStyle.green, emoji="🟢", custom_id="duty_start")
    async def start_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Startet den Duty-Modus"""
        guild = interaction.guild
        ctx = await self.cog.bot.get_context(interaction)
        
        role_id = await self.cog.config.guild(guild).role()
        
        if not role_id:
            await interaction.response.send_message("❌ Es wurde keine Support-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Die konfigurierte Support-Rolle existiert nicht mehr!", ephemeral=True)
            return
        
        if base_role not in interaction.user.roles:
            await interaction.response.send_message(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!", ephemeral=True)
            return
        
        # Prüfen ob bereits auf Duty
        is_on_duty = await self.cog.config.member(interaction.user).on_duty()
        if is_on_duty:
            await interaction.response.send_message("⚠️ Du bist bereits im Duty-Modus!", ephemeral=True)
            return
        
        # Duty aktivieren und Rolle geben
        await self.cog.config.member(interaction.user).on_duty.set(True)
        start_time = datetime.utcnow()
        await self.cog.config.member(interaction.user).duty_start.set(start_time.timestamp())
        
        # Duty-Rolle hinzufügen
        await self.cog.update_duty_role(interaction.user, True)
        
        # Auto-Duty Timer starten falls aktiviert
        auto_duty = await self.cog.config.guild(guild).auto_remove_duty()
        duty_timeout = await self.cog.config.guild(guild).duty_timeout()
        
        # Nachricht senden
        duty_channel_id = await self.cog.config.guild(guild).duty_channel()
        notify_channel = interaction.channel
        if duty_channel_id:
            dc = guild.get_channel(duty_channel_id)
            if dc and isinstance(dc, discord.TextChannel):
                notify_channel = dc
        
        embed = discord.Embed(
            title="🟢 Duty Gestartet",
            description=f"{interaction.user.mention} hat sich für den Support-Dienst angemeldet!",
            color=discord.Color.green(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{interaction.user.display_name}", inline=True)
        
        if auto_duty:
            end_time = start_time + timedelta(hours=duty_timeout)
            embed.add_field(name="⏰ Automatische Abmeldung", value=f"Nach {duty_timeout} Stunden\n(<t:{int(end_time.timestamp())}:R>)", inline=True)
        
        # Zähle alle aktiven Duty-User
        duty_count = 0
        duty_role = await self.cog.get_or_create_duty_role(guild)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.cog.config.member(m).on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Aktive Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_time.strftime('%d.%m.%Y %H:%M')}")
        
        await notify_channel.send(embed=embed)
        await interaction.response.send_message("✅ Du bist jetzt im Duty-Modus! Du wirst bei neuen Support-Anfragen gepingt.", ephemeral=True)
    
    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Duty-Modus"""
        is_on_duty = await self.cog.config.member(interaction.user).on_duty()
        
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Duty-Modus.", ephemeral=True)
            return
        
        # Hole Startzeit für Statistik
        start_time = await self.cog.config.member(interaction.user).duty_start()
        duration = "Unbekannt"
        if start_time:
            start_dt = datetime.fromtimestamp(start_time)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
        
        # Duty deaktivieren und Rolle entfernen
        await self.cog.config.member(interaction.user).on_duty.set(False)
        await self.cog.config.member(interaction.user).duty_start.set(None)
        
        # Duty-Rolle entfernen
        await self.cog.update_duty_role(interaction.user, False)
        
        # Nachricht senden
        guild = interaction.guild
        duty_channel_id = await self.cog.config.guild(guild).duty_channel()
        notify_channel = interaction.channel
        if duty_channel_id:
            dc = guild.get_channel(duty_channel_id)
            if dc and isinstance(dc, discord.TextChannel):
                notify_channel = dc
        
        role_id = await self.cog.config.guild(guild).role()
        base_role = guild.get_role(role_id) if role_id else None
        
        embed = discord.Embed(
            title="🔴 Duty Beendet",
            description=f"{interaction.user.mention} hat sich vom Support-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=interaction.user.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{interaction.user.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)
        
        # Zähle verbleibende aktive Duty-User
        duty_count = 0
        duty_role = await self.cog.get_or_create_duty_role(guild)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.cog.config.member(m).on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Verbleibende Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}")
        
        await notify_channel.send(embed=embed)
        await interaction.response.send_message("✅ Du hast den Duty-Modus verlassen.", ephemeral=True)


async def setup(bot: Red):
    """Lädt den Cog"""
    cog = SupportCog(bot)
    # Registere die persistent View für Buttons
    bot.add_view(DutyButtonView(cog))
    await bot.add_cog(cog)


async def teardown(bot: Red):
    """Entfernt den Cog"""
    bot.remove_view(DutyButtonView)
    await bot.remove_cog("SupportCog")
