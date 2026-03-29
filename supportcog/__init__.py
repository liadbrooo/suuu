"""
Support & Whitelist Warteraum Cog für RedBot mit On-Duty System & Button-Interface
Verbesserte Version mit getrennten Panel/Log Channels und besserer UX

Dieser Cog erkennt, wenn ein Nutzer einen Support-Warteraum oder Whitelist-Warteraum betritt oder verlässt
und sendet eine Nachricht in einem konfigurierten Text-Channel.
Enthält ein On-Duty System für Support-Teammitglieder und Whitelist-Handler mit Button-Interface.

Installation:
1. Kopiere den gesamten 'supportcog' Ordner in deinen RedBot cogs Ordner
   (normalerweise ~/.local/share/Red-DiscordBot/data/[DEIN_BOT_NAME]/cogs/)
2. Lade den Cog mit: [p]load supportcog
3. Konfiguriere mit:
   SUPPORT SYSTEM:
   - [p]supportset channel #textchannel  (Setzt den Channel für Team-Pings bei neuen Anfragen)
   - [p]supportset room @VoiceChannel    (Setzt den Voice-Warteraum)
   - [p]supportset role @Rolle           (Setzt die Basis-Supportrolle)
   - [p]supportset panelchannel #channel (Channel für das Duty-Panel mit Buttons)
   - [p]supportset logchannel #channel   (Channel NUR für Duty-Logs: An-/Abmeldungen)
   
   WHITELIST SYSTEM:
   - [p]whitelistset channel #textchannel  (Setzt den Channel für Whitelist-Benachrichtigungen)
   - [p]whitelistset room @VoiceChannel    (Setzt den Whitelist-Warteraum)
   - [p]whitelistset role @Rolle           (Setzt die Basis-Whitelist-Handler-Rolle)
   - [p]whitelistset panelchannel #channel (Channel für das Whitelist-Duty-Panel)
   - [p]whitelistset logchannel #channel   (Channel für Whitelist-Duty-Logs)
   - [p]whitelistset approvedrole @Rolle   (Die Rolle die Spieler nach Whitelist erhalten)
   
   ALLGEMEIN:
   - [p]supportset autoduty <stunden|off>  (Automatische Abmeldung nach X Stunden oder ausschalten)
   - [p]supportset feedbackchannel #channel (Channel für Feedback-Logs)
   - [p]supportset callchannel #channel     (Channel für Support-Aufrufe)
   - [p]supportset callroom @VoiceChannel   (Voice-Channel für Support-Calls)
   - ODER verwende [p]supportset setup für einen interaktiven Einrichtungsassistenten

Nutzung:
- Wenn jemand den konfigurierten Voice-Channel betritt, wird automatisch
  eine schöne Nachricht im entsprechenden Channel gesendet mit Ping aller Duty-Mitglieder
- Duty-Logs (An-/Abmeldungen) landen separat im Log-Channel
- Teammitglieder können sich per Button am Duty-Panel an- und abmelden
- Automatische Duty-Rollen werden erstellt und verwaltet ("🟢 On Duty" / "📋 Whitelist Duty")
- Modal-Eingabe ermöglicht das direkte Eingeben von Spieler-ID oder Name zum Whitelisten
- ALLE Duty-Mitglieder werden INDIVIDUELL gepingt für garantierte Benachrichtigung!
"""

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from datetime import datetime, timedelta
from typing import Optional
import asyncio
import random


class SupportCog(commands.Cog):
    """Cog für Support-Warteraum Benachrichtigungen mit Button-Duty-System"""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=12345678901234567890)

        default_guild_settings = {
            # SUPPORT SYSTEM SETTINGS
            "channel": None,  # Text-Channel ID für Support-Benachrichtigungen (Fallback)
            "room": None,     # Voice-Channel ID des Warteraums
            "role": None,     # Rolle ID die gepingt wird (Basis-Supportrolle)
            "duty_role": None,  # Automatisch erstellte Duty-Rolle
            "use_embed": True,  # Ob Embeds verwendet werden sollen
            "enabled": True,   # Ob der Cog aktiv ist
            "panel_channel": None,  # Channel für das Duty-Panel mit Buttons
            "log_channel": None,    # Channel NUR für Duty-Logs: An-/Abmeldungen
            "auto_remove_duty": True,  # Automatisch Duty entfernen nach X Stunden
            "duty_timeout": 4,  # Stunden nach denen Duty automatisch entfernt wird
            "panel_message_id": None,  # Message ID der permanenten Panel-Nachricht
            
            # FEEDBACK SYSTEM
            "feedback_channel": None,  # Channel für Feedback-Logs
            "feedback_panel_message_id": None,  # Message ID des Feedback-Panels
            
            # SUPPORT CALL SYSTEM
            "support_call_channel": None,  # Channel für Support-Aufrufe
            "call_room": None,  # Voice-Channel für Support-Calls
            
            # WHITELIST SYSTEM SETTINGS
            "whitelist_channel": None,  # Text-Channel für Whitelist-Benachrichtigungen
            "whitelist_room": None,  # Voice-Channel ID des Whitelist-Warteraums
            "whitelist_role": None,  # Basis-Whitelist-Handler-Rolle
            "whitelist_duty_role": None,  # Automatische Whitelist Duty-Rolle
            "whitelist_approved_role": None,  # Rolle die Spieler nach Whitelist erhalten
            "whitelist_panel_channel": None,  # Channel für Whitelist-Duty-Panel
            "whitelist_log_channel": None,  # Channel für Whitelist-Duty-Logs
            "whitelist_auto_remove_duty": True,
            "whitelist_duty_timeout": 4,
            "whitelist_panel_message_id": None,
        }

        # Speichert On-Duty Status pro User (für beide Systeme)
        default_member_settings = {
            "on_duty": False,
            "duty_start": None,
            "whitelist_on_duty": False,
            "whitelist_duty_start": None
        }

        self.config.register_guild(**default_guild_settings)
        self.config.register_member(**default_member_settings)

        # Cache für aktive Duty-User (wird bei Bot-Start neu aufgebaut)
        self.duty_cache = {}

    async def get_or_create_duty_role(self, guild: discord.Guild, whitelist: bool = False) -> Optional[discord.Role]:
        """Erstellt oder holt die automatische Duty-Rolle"""
        if whitelist:
            duty_role_id = await self.config.guild(guild).whitelist_duty_role()
            role_name = "📋 Whitelist Duty"
            color = discord.Color.blue()
            reason = "Automatische Duty-Rolle für Whitelist-System"
        else:
            duty_role_id = await self.config.guild(guild).duty_role()
            role_name = "🟢 On Duty"
            color = discord.Color.green()
            reason = "Automatische Duty-Rolle für Support-System"
        
        if duty_role_id:
            duty_role = guild.get_role(duty_role_id)
            if duty_role:
                return duty_role
        
        # Rolle existiert nicht mehr, erstelle neue
        try:
            duty_role = await guild.create_role(
                name=role_name,
                color=color,
                mentionable=True,
                reason=reason
            )
            if whitelist:
                await self.config.guild(guild).whitelist_duty_role.set(duty_role.id)
            else:
                await self.config.guild(guild).duty_role.set(duty_role.id)
            return duty_role
        except discord.Forbidden:
            return None

    async def update_duty_role(self, member: discord.Member, on_duty: bool, whitelist: bool = False):
        """Fügt oder entfernt die Duty-Rolle eines Members"""
        guild = member.guild
        duty_role = await self.get_or_create_duty_role(guild, whitelist=whitelist)
        
        if not duty_role:
            return
        
        try:
            if on_duty and duty_role not in member.roles:
                reason = "Whitelist-Duty gestartet" if whitelist else "Support-Duty gestartet"
                await member.add_roles(duty_role, reason=reason)
            elif not on_duty and duty_role in member.roles:
                reason = "Whitelist-Duty beendet" if whitelist else "Support-Duty beendet"
                await member.remove_roles(duty_role, reason=reason)
        except discord.Forbidden:
            pass

    # WHITELIST SYSTEM HELPER METHODS
    async def get_whitelist_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Whitelist-Benachrichtigungs-Channel"""
        channel_id = await self.config.guild(guild).whitelist_channel()
        
        if channel_id:
            channel = guild.get_channel(channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf log_channel wenn kein spezieller channel gesetzt
        return await self.get_whitelist_log_channel(guild)

    async def get_whitelist_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Log-Channel für Whitelist-Duty-Logs"""
        log_channel_id = await self.config.guild(guild).whitelist_log_channel()
        
        if log_channel_id:
            channel = guild.get_channel(log_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Kein separater Log-Channel gesetzt - Logs landen im Whitelist-Channel
        return await self.get_whitelist_channel(guild)

    async def get_whitelist_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Panel-Channel für das Whitelist-Duty-Interface"""
        panel_channel_id = await self.config.guild(guild).whitelist_panel_channel()
        
        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf Log-Channel
        return await self.get_whitelist_log_channel(guild)

    async def get_whitelist_approved_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        """Holt die Whitelist-Approved-Rolle"""
        role_id = await self.config.guild(guild).whitelist_approved_role()
        
        if role_id:
            role = guild.get_role(role_id)
            if role:
                return role
        return None

    async def create_whitelist_panel_message(self, ctx: commands.Context):
        """Erstellt eine permanente Whitelist-Panel-Nachricht mit Buttons"""
        guild = ctx.guild
        channel = await self.get_whitelist_panel_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        embed = discord.Embed(
            title="📋 Whitelist Duty Panel",
            description=(
                "**Willkommen zum Whitelist-Duty System!**\n\n"
                "Klicke auf die Buttons unten um dich für den Whitelist-Dienst an- oder abzumelden.\n\n"
                "🔵 **Duty Starten** - Du wirst bei neuen Whitelist-Anfragen gepingt\n"
                "🔴 **Duty Beenden** - Du erhältst keine Pings mehr\n\n"
                "🎮 **Spieler whitelisten** - Öffnet ein Eingabefeld für Spieler-ID oder Name\n\n"
                "**Aktuell im Dienst:** Niemand"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Die 📋 Whitelist Duty Rolle wird automatisch zugewiesen/entfernt")
        
        view = WhitelistButtonView(self, guild)
        message = await channel.send(embed=embed, view=view)
        
        await self.config.guild(guild).whitelist_panel_message_id.set(message.id)
        return message

    async def get_support_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Support-Benachrichtigungs-Channel (für Team-Pings bei neuen Anfragen)"""
        channel_id = await self.config.guild(guild).channel()
        
        if channel_id:
            channel = guild.get_channel(channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf log_channel wenn kein spezieller support channel gesetzt
        return await self.get_log_channel(guild)

    async def get_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Log-Channel NUR für Duty-Logs (An/Abmeldungen)"""
        log_channel_id = await self.config.guild(guild).log_channel()
        
        if log_channel_id:
            channel = guild.get_channel(log_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Kein separater Log-Channel gesetzt - Logs landen im Support-Channel
        return await self.get_support_channel(guild)

    async def get_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Panel-Channel für das Duty-Interface"""
        panel_channel_id = await self.config.guild(guild).panel_channel()
        
        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf Log-Channel
        return await self.get_log_channel(guild)

    async def get_feedback_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Feedback-Channel für Feedback-Logs"""
        feedback_channel_id = await self.config.guild(guild).feedback_channel()
        
        if feedback_channel_id:
            channel = guild.get_channel(feedback_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf log_channel
        return await self.get_log_channel(guild)

    async def get_support_call_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für Support-Aufrufe"""
        call_channel_id = await self.config.guild(guild).support_call_channel()
        
        if call_channel_id:
            channel = guild.get_channel(call_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf support_channel
        return await self.get_support_channel(guild)

    async def get_call_room(self, guild: discord.Guild) -> Optional[discord.VoiceChannel]:
        """Holt den Voice-Channel für Support-Calls"""
        call_room_id = await self.config.guild(guild).call_room()
        
        if call_room_id:
            channel = guild.get_channel(call_room_id)
            if channel and isinstance(channel, discord.VoiceChannel):
                return channel
        
        return None

    async def create_panel_message(self, ctx: commands.Context):
        """Erstellt eine permanente Panel-Nachricht mit Buttons"""
        guild = ctx.guild
        channel = await self.get_panel_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        embed = discord.Embed(
            title="🎧 Support Duty Panel",
            description=(
                "**Willkommen zum Support-Duty System!**\n\n"
                "Klicke auf die Buttons unten um dich für den Support-Dienst an- oder abzumelden.\n\n"
                "🟢 **Duty Starten** - Du wirst bei neuen Anfragen gepingt\n"
                "🔴 **Duty Beenden** - Du erhältst keine Pings mehr\n\n"
                "**Aktuell im Dienst:** Niemand"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Die 🟢 On Duty Rolle wird automatisch zugewiesen/entfernt")
        
        view = DutyButtonView(self)
        message = await channel.send(embed=embed, view=view)
        
        await self.config.guild(guild).panel_message_id.set(message.id)
        return message

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        """Hört auf Voice-Channel Änderungen und sendet Benachrichtigungen für Support UND Whitelist"""

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

        # SUPPORT SYSTEM
        await self._handle_support_voice_update(member, before, after, guild)
        
        # WHITELIST SYSTEM
        await self._handle_whitelist_voice_update(member, before, after, guild)

    async def _handle_support_voice_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState, guild: discord.Guild):
        """Verarbeitet Voice-Updates für das Support-System"""
        
        room_id = await self.config.guild(guild).room()
        role_id = await self.config.guild(guild).role()
        use_embed = await self.config.guild(guild).use_embed()

        # Prüfen ob alle erforderlichen Einstellungen gesetzt sind
        if not all([room_id, role_id]):
            return

        # Prüfen ob es der konfigurierte Support-Warteraum ist
        if after.channel is None or after.channel.id != room_id:
            return
        if before.channel is not None and before.channel.id == room_id:
            # User war bereits im Warteraum - keine Aktion
            return

        # User hat den Support-Warteraum soeben betreten
        try:
            support_channel = await self.get_support_channel(guild)
            if not support_channel:
                return

            base_role = guild.get_role(role_id)
            if not base_role:
                return

            # Hole alle On-Duty User MIT DER DUTY ROLLE
            duty_members = []
            duty_role = await self.get_or_create_duty_role(guild, whitelist=False)
            
            for m in base_role.members:
                # Prüfe ob User die Duty-Rolle hat UND on_duty flag gesetzt ist
                if duty_role and duty_role in m.roles:
                    is_on_duty = await self.config.member(m).on_duty()
                    if is_on_duty:
                        duty_members.append(m)

            user_mention = member.mention
            user_avatar = member.display_avatar.url

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
                else:
                    embed.add_field(
                        name="🔴 Keine Supporter verfügbar",
                        value=f"Niemand ist gerade im Dienst!\n*Benutze `/supportset panelchannel` und klicke auf 'Duty Starten'*",
                        inline=True
                    )

                embed.add_field(
                    name="📍 Channel",
                    value=f"{after.channel.mention}",
                    inline=True
                )
                embed.set_footer(text="Support Warteraum System • On-Duty aktiv")

                # Sende das Embed mit Role-Ping IM SUPPORT-CHANNEL (nicht Log!)
                # WICHTIG: allowed_mentions erzwingt den Ping der Rolle
                if duty_role and duty_members:
                    # Ping die Duty-Rolle direkt - das pingt ALLE Mitglieder reliably
                    await support_channel.send(content=duty_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    # Fallback zur Basis-Rolle wenn niemand Duty hat
                    await support_channel.send(content=base_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await support_channel.send(embed=embed)
            else:
                # Einfache Textnachricht (Fallback)
                if duty_role and duty_members:
                    message = f"🎧 {duty_role.mention} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({after.channel.mention})"
                    await support_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    message = f"🎧 {base_role.mention} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({after.channel.mention}) - Niemand im Duty!"
                    await support_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await support_channel.send(f"🎧 {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({after.channel.mention})")

        except discord.Forbidden as e:
            # Bot hat keine Berechtigung zum Senden
            print(f"[SupportCog] Forbidden Fehler beim Senden in Channel: {e}")
        except Exception as e:
            # Logge Fehler mit mehr Details
            import traceback
            print(f"[SupportCog] Fehler in Support Warteraum: {type(e).__name__}: {e}")
            print(traceback.format_exc())

    async def _handle_whitelist_voice_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState, guild: discord.Guild):
        """Verarbeitet Voice-Updates für das Whitelist-System"""
        
        # Hole Whitelist-Konfiguration
        room_id = await self.config.guild(guild).whitelist_room()
        role_id = await self.config.guild(guild).whitelist_role()
        use_embed = await self.config.guild(guild).use_embed()

        # Prüfen ob alle erforderlichen Einstellungen gesetzt sind
        if not all([room_id, role_id]):
            return

        # Prüfen ob es der konfigurierte Whitelist-Warteraum ist
        if after.channel is None or after.channel.id != room_id:
            return
        if before.channel is not None and before.channel.id == room_id:
            # User war bereits im Warteraum - keine Aktion
            return

        # User hat den Whitelist-Warteraum soeben betreten
        try:
            whitelist_channel = await self.get_whitelist_channel(guild)
            if not whitelist_channel:
                return

            base_role = guild.get_role(role_id)
            if not base_role:
                return

            # Hole alle On-Duty User MIT DER DUTY ROLLE
            duty_members = []
            duty_role = await self.get_or_create_duty_role(guild, whitelist=True)
            
            for m in base_role.members:
                # Prüfe ob User die Duty-Rolle hat UND whitelist_on_duty flag gesetzt ist
                if duty_role and duty_role in m.roles:
                    is_on_duty = await self.config.member(m).whitelist_on_duty()
                    if is_on_duty:
                        duty_members.append(m)

            user_mention = member.mention
            user_avatar = member.display_avatar.url

            if use_embed:
                # Erstelle ein schönes Embed
                embed = discord.Embed(
                    title="📋 Neue Whitelist-Anfrage",
                    description=f"{user_mention} hat den Whitelist-Warteraum betreten und wartet auf Bearbeitung!",
                    color=discord.Color.blue(),
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
                        name="🔵 Verfügbare Whitelist-Handler",
                        value=duty_list,
                        inline=True
                    )
                else:
                    embed.add_field(
                        name="🔴 Keine Handler verfügbar",
                        value=f"Niemand ist gerade im Dienst!",
                        inline=True
                    )

                embed.add_field(
                    name="📍 Channel",
                    value=f"{after.channel.mention}",
                    inline=True
                )
                embed.set_footer(text="Whitelist Warteraum System • On-Duty aktiv")

                # Sende das Embed mit Role-Ping IM WHITELIST-CHANNEL
                if duty_role and duty_members:
                    await whitelist_channel.send(content=duty_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    await whitelist_channel.send(content=base_role.mention, embed=embed, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await whitelist_channel.send(embed=embed)
            else:
                # Einfache Textnachricht (Fallback)
                if duty_role and duty_members:
                    message = f"📋 {duty_role.mention} | {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({after.channel.mention})"
                    await whitelist_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    message = f"📋 {base_role.mention} | {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({after.channel.mention}) - Niemand im Duty!"
                    await whitelist_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await whitelist_channel.send(f"📋 {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({after.channel.mention})")

        except discord.Forbidden:
            # Bot hat keine Berechtigung zum Senden
            pass
        except Exception as e:
            # Logge Fehler
            print(f"Fehler in SupportCog (Whitelist): {e}")
    @commands.group(name="supportset", aliases=["supportconfig"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def supportset(self, ctx: commands.Context):
        """Konfiguriere den Support Warteraum Cog"""
        pass

    @supportset.command(name="channel")
    async def supportset_channel(self, ctx: commands.Context, channel: str):
        """Setze den Text-Channel für Support-Benachrichtigungen (ID oder Mention)"""
        channel_id = self._parse_channel_id(channel)
        
        if channel_id is None:
            await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein (z.B. #channel oder 123456789)")
            return
        
        ch = ctx.guild.get_channel(channel_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Channel nicht gefunden oder kein Text-Channel!")
            return
            
        await self.config.guild(ctx.guild).channel.set(channel_id)
        await ctx.send(f"✅ Text-Channel auf {ch.mention} gesetzt.")

    def _parse_channel_id(self, channel: str) -> Optional[int]:
        """Parses a channel mention or ID to an integer ID"""
        if channel.startswith("<#") and channel.endswith(">"):
            return int(channel[2:-1])
        try:
            return int(channel)
        except ValueError:
            return None

    def _parse_voice_channel_id(self, channel: str) -> Optional[int]:
        """Parses a voice channel mention or ID to an integer ID"""
        if channel.startswith("<#") and channel.endswith(">"):
            return int(channel[2:-1])
        try:
            return int(channel)
        except ValueError:
            return None

    def _parse_role_id(self, role: str) -> Optional[int]:
        """Parses a role mention or ID to an integer ID"""
        if role.startswith("<@&") and role.endswith(">"):
            return int(role[3:-1])
        try:
            return int(role)
        except ValueError:
            return None

    @supportset.command(name="room")
    async def supportset_room(self, ctx: commands.Context, room: str):
        """Setze den Voice-Channel als Support-Warteraum (ID oder Mention)"""
        room_id = self._parse_voice_channel_id(room)
        
        if room_id is None:
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
        role_id = self._parse_role_id(role)
        
        if role_id is None:
            await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
            return
        
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).role.set(role_id)
        await ctx.send(f"✅ Support-Basisrolle auf {r.mention} gesetzt.\nℹ️ Die automatische Duty-Rolle wird beim ersten Duty-Start erstellt.")

    @supportset.command(name="panelchannel")
    async def supportset_panelchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für das Duty-Panel mit Buttons.
        Ohne Channel-Angabe wird der normale Support-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).panel_channel.set(None)
            await ctx.send("✅ Duty-Panel wird im Support-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).panel_channel.set(channel_id)
            await ctx.send(f"✅ Duty-Panel wird jetzt in {ch.mention} angezeigt.")

    @supportset.command(name="logchannel")
    async def supportset_logchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Support-Anfragen und Duty-Logs.
        Ohne Channel-Angabe wird der normale Support-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).log_channel.set(None)
            await ctx.send("✅ Logs werden im Support-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).log_channel.set(channel_id)
            await ctx.send(f"✅ Logs werden jetzt in {ch.mention} angezeigt.")

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
        panel_channel_id = guild_data.get("panel_channel")
        log_channel_id = guild_data.get("log_channel")
        auto_duty = guild_data.get("auto_remove_duty", True)
        duty_timeout = guild_data.get("duty_timeout", 4)
        feedback_channel_id = guild_data.get("feedback_channel")
        support_call_channel_id = guild_data.get("support_call_channel")
        call_room_id = guild_data.get("call_room")

        channel_mention = f"<#{channel_id}>" if channel_id else "❌ Nicht gesetzt"
        room_mention = f"<#{room_id}>" if room_id else "❌ Nicht gesetzt"
        role_mention = f"<@&{role_id}>" if role_id else "❌ Nicht gesetzt"
        duty_role_mention = f"<@&{duty_role_id}>" if duty_role_id else "❌ Noch nicht erstellt"
        panel_channel_mention = f"<#{panel_channel_id}>" if panel_channel_id else "Gleicher wie Support-Channel"
        log_channel_mention = f"<#{log_channel_id}>" if log_channel_id else "Gleicher wie Support-Channel"
        feedback_channel_mention = f"<#{feedback_channel_id}>" if feedback_channel_id else "Gleicher wie Log-Channel"
        call_channel_mention = f"<#{support_call_channel_id}>" if support_call_channel_id else "Gleicher wie Support-Channel"
        call_room_mention = f"<#{call_room_id}>" if call_room_id else "❌ Nicht gesetzt"
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
        embed.add_field(name="📝 Support-Channel", value=channel_mention, inline=True)
        embed.add_field(name="🎤 Voice-Warteraum", value=room_mention, inline=True)
        embed.add_field(name="👥 Support-Basisrolle", value=role_mention, inline=True)
        embed.add_field(name="🟢 Duty-Rolle", value=duty_role_mention, inline=True)
        embed.add_field(name="📋 Panel-Channel", value=panel_channel_mention, inline=True)
        embed.add_field(name="📜 Log-Channel", value=log_channel_mention, inline=True)
        embed.add_field(name="💬 Feedback-Channel", value=feedback_channel_mention, inline=True)
        embed.add_field(name="📞 Call-Channel", value=call_channel_mention, inline=True)
        embed.add_field(name="🎤 Call-Room", value=call_room_mention, inline=True)

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
            ("4️⃣ (Optional) In welchem Channel soll das **Duty-Panel** erscheinen? (Antworte mit 'skip' zum Überspringen)", "panel_channel", "text", True),
            ("5️⃣ (Optional) In welchem Channel sollen **Support-Anfragen & Logs** erscheinen? (Antworte mit 'skip' zum Überspringen)", "log_channel", "text", True),
        ]

        answers = {}

        for question_data in questions:
            optional = len(question_data) > 3 and question_data[3]

            embed = discord.Embed(
                title=question_data[0],
                description="Sende deine Antwort als Nachricht hier im Channel.\n• Erwähne den Channel/die Rolle einfach mit @ oder #\n• Oder kopiere die ID (Rechtsklick -> ID kopieren)\n• Bei optionalen Fragen kannst du 'skip' schreiben um zu überspringen",
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

        if answers.get("panel_channel"):
            await self.config.guild(ctx.guild).panel_channel.set(answers["panel_channel"].id)
        
        if answers.get("log_channel"):
            await self.config.guild(ctx.guild).log_channel.set(answers["log_channel"].id)

        embed = discord.Embed(
            title="✅ Einrichtung erfolgreich!",
            description="Der Support-Cog ist jetzt konfiguriert und bereit!\n\n**Zusammenfassung:**\n"
                        f"• 📝 Support-Channel: {answers['channel'].mention}\n"
                        f"• 🎤 Voice-Warteraum: {answers['room'].mention}\n"
                        f"• 👥 Support-Rolle: {answers['role'].mention}",
            color=discord.Color.green()
        )

        if answers.get("panel_channel"):
            embed.description += f"\n• 📋 Panel-Channel: {answers['panel_channel'].mention}"
        else:
            embed.description += "\n• 📋 Panel-Channel: Gleicher wie Support-Channel"
            
        if answers.get("log_channel"):
            embed.description += f"\n• 📜 Log-Channel: {answers['log_channel'].mention}"
        else:
            embed.description += "\n• 📜 Log-Channel: Gleicher wie Support-Channel"

        embed.description += "\n\n**Nächste Schritte:**\n"
        embed.description += "• Erstelle das Duty-Panel mit `[p]supportset createpanel`\n"
        embed.description += "• Support-Teamler können sich dann per Button an-/abmelden\n"
        embed.description += "• Eine automatische 🟢 On Duty Rolle wird erstellt"

        await ctx.send(embed=embed)

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

    @supportset.command(name="createpanel")
    async def supportset_createpanel(self, ctx: commands.Context):
        """
        Erstellt ein permanentes Duty-Panel mit Buttons.
        Diese Nachricht sollte im Panel-Channel gepinnt werden.
        """
        await ctx.send("🔄 Erstelle Duty-Panel mit Buttons...")
        await self.create_panel_message(ctx)
        await ctx.send("✅ Duty-Panel wurde erstellt! Du kannst die Buttons jetzt verwenden um dich an-/abzumelden.")

    @supportset.command(name="refreshpanel")
    async def supportset_refreshpanel(self, ctx: commands.Context):
        """
        Aktualisiert das Duty-Panel falls es gelöscht wurde.
        """
        old_message_id = await self.config.guild(ctx.guild).panel_message_id()
        if old_message_id:
            try:
                channel = await self.get_panel_channel(ctx.guild)
                if channel:
                    old_msg = await channel.fetch_message(old_message_id)
                    await old_msg.delete()
            except:
                pass
        
        await self.create_panel_message(ctx)
        await ctx.send("✅ Duty-Panel wurde aktualisiert!")

    @supportset.command(name="feedbackchannel")
    async def supportset_feedbackchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Feedback-Logs.
        Ohne Channel-Angabe wird der Log-Channel verwendet.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).feedback_channel.set(None)
            await ctx.send("✅ Feedback wird im Log-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).feedback_channel.set(channel_id)
            await ctx.send(f"✅ Feedback wird jetzt in {ch.mention} angezeigt.")

    @supportset.command(name="callchannel")
    async def supportset_callchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Support-Aufrufe.
        Ohne Channel-Angabe wird der Support-Channel verwendet.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).support_call_channel.set(None)
            await ctx.send("✅ Support-Aufrufe werden im Support-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).support_call_channel.set(channel_id)
            await ctx.send(f"✅ Support-Aufrufe werden jetzt in {ch.mention} angezeigt.")

    @supportset.command(name="callroom")
    async def supportset_callroom(self, ctx: commands.Context, room: str = None):
        """
        Setze den Voice-Channel für Support-Calls (Treffpunkt).
        Ohne Channel-Angabe wird zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if room is None or room.lower() == "reset":
            await self.config.guild(ctx.guild).call_room.set(None)
            await ctx.send("✅ Call-Room zurückgesetzt.")
        else:
            room_id = self._parse_voice_channel_id(room)
            if room_id is None:
                await ctx.send("❌ Bitte gib eine gültige Voice-Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(room_id)
            if not ch or not isinstance(ch, discord.VoiceChannel):
                await ctx.send("❌ Voice-Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).call_room.set(room_id)
            await ctx.send(f"✅ Call-Room auf {ch.mention} gesetzt.")

    @supportset.command(name="createfeedbackpanel")
    async def supportset_createfeedbackpanel(self, ctx: commands.Context):
        """
        Erstellt ein permanentes Feedback-Panel mit Buttons.
        Diese Nachricht sollte im Panel-Channel gepinnt werden.
        """
        guild = ctx.guild
        channel = await self.get_panel_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        embed = discord.Embed(
            title="💬 Feedback Panel",
            description=(
                "**Wir freuen uns über dein Feedback!**\n\n"
                "Klicke auf einen der Buttons unten um uns dein Feedback zu geben.\n\n"
                "😊 **Positives Feedback** - Das lief gut!\n"
                "😞 **Negatives Feedback** - Das muss verbessert werden\n"
                "💡 **Vorschlag** - Du hast eine Idee?"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Dein Feedback hilft uns besser zu werden!")
        
        view = FeedbackPanelView(self)
        message = await channel.send(embed=embed, view=view)
        
        await self.config.guild(guild).feedback_panel_message_id.set(message.id)
        await ctx.send("✅ Feedback-Panel wurde erstellt!")

    @supportset.command(name="createcallpanel")
    async def supportset_createcallpanel(self, ctx: commands.Context):
        """
        Erstellt ein Panel mit einem Button um Support zu rufen.
        """
        guild = ctx.guild
        channel = await self.get_panel_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        embed = discord.Embed(
            title="📞 Support benötigt?",
            description=(
                "**Brauchst du Hilfe von unserem Support-Team?**\n\n"
                "Klicke auf den Button unten um einen Supporter zu rufen.\n"
                "Ein verfügbares Teammitglied wird dich kontaktieren!"
            ),
            color=discord.Color.orange()
        )
        embed.set_footer(text="Bitte habe etwas Geduld nachdem du geklickt hast.")
        
        view = SupportCallView(self)
        message = await channel.send(embed=embed, view=view)
        
        await ctx.send("✅ Support-Aufruf-Panel wurde erstellt!")

    # WHITELIST SYSTEM COMMANDS
    @commands.group(name="whitelistset", aliases=["whitelistconfig"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def whitelistset(self, ctx: commands.Context):
        """Konfiguriere den Whitelist Warteraum Cog"""
        pass

    @whitelistset.command(name="channel")
    async def whitelistset_channel(self, ctx: commands.Context, channel: str):
        """Setze den Text-Channel für Whitelist-Benachrichtigungen (ID oder Mention)"""
        channel_id = self._parse_channel_id(channel)
        
        if channel_id is None:
            await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein (z.B. #channel oder 123456789)")
            return
        
        ch = ctx.guild.get_channel(channel_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Channel nicht gefunden oder kein Text-Channel!")
            return
            
        await self.config.guild(ctx.guild).whitelist_channel.set(channel_id)
        await ctx.send(f"✅ Whitelist-Channel auf {ch.mention} gesetzt.")

    @whitelistset.command(name="room")
    async def whitelistset_room(self, ctx: commands.Context, room: str):
        """Setze den Voice-Channel als Whitelist-Warteraum (ID oder Mention)"""
        room_id = self._parse_voice_channel_id(room)
        
        if room_id is None:
            await ctx.send("❌ Bitte gib eine gültige Voice-Channel-ID oder Mention ein!")
            return
        
        ch = ctx.guild.get_channel(room_id)
        if not ch or not isinstance(ch, discord.VoiceChannel):
            await ctx.send("❌ Voice-Channel nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).whitelist_room.set(room_id)
        await ctx.send(f"✅ Whitelist-Warteraum auf {ch.mention} gesetzt.")

    @whitelistset.command(name="role")
    async def whitelistset_role(self, ctx: commands.Context, role: str):
        """Setze die Basis-Whitelist-Handler-Rolle (ID oder Mention)"""
        role_id = self._parse_role_id(role)
        
        if role_id is None:
            await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
            return
        
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).whitelist_role.set(role_id)
        await ctx.send(f"✅ Whitelist-Handler-Rolle auf {r.mention} gesetzt.\nℹ️ Die automatische Duty-Rolle wird beim ersten Duty-Start erstellt.")

    @whitelistset.command(name="approvedrole")
    async def whitelistset_approvedrole(self, ctx: commands.Context, role: str):
        """Setze die Rolle die Spieler nach Whitelist-Genehmigung erhalten (ID oder Mention)"""
        role_id = self._parse_role_id(role)
        
        if role_id is None:
            await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
            return
        
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
            
        await self.config.guild(ctx.guild).whitelist_approved_role.set(role_id)
        await ctx.send(f"✅ Whitelist-Approved-Rolle auf {r.mention} gesetzt.")

    @whitelistset.command(name="panelchannel")
    async def whitelistset_panelchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für das Whitelist-Duty-Panel mit Buttons.
        Ohne Channel-Angabe wird der normale Whitelist-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_panel_channel.set(None)
            await ctx.send("✅ Whitelist-Duty-Panel wird im Whitelist-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_panel_channel.set(channel_id)
            await ctx.send(f"✅ Whitelist-Duty-Panel wird jetzt in {ch.mention} angezeigt.")

    @whitelistset.command(name="logchannel")
    async def whitelistset_logchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Whitelist-Duty-Logs.
        Ohne Channel-Angabe wird der normale Whitelist-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_log_channel.set(None)
            await ctx.send("✅ Whitelist-Logs werden im Whitelist-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_log_channel.set(channel_id)
            await ctx.send(f"✅ Whitelist-Logs werden jetzt in {ch.mention} angezeigt.")

    @whitelistset.command(name="show")
    async def whitelistset_show(self, ctx: commands.Context):
        """Zeige die aktuelle Whitelist-Konfiguration"""
        guild_data = await self.config.guild(ctx.guild).all()

        channel_id = guild_data.get("whitelist_channel")
        room_id = guild_data.get("whitelist_room")
        role_id = guild_data.get("whitelist_role")
        approved_role_id = guild_data.get("whitelist_approved_role")
        duty_role_id = guild_data.get("whitelist_duty_role")
        panel_channel_id = guild_data.get("whitelist_panel_channel")
        log_channel_id = guild_data.get("whitelist_log_channel")
        auto_duty = guild_data.get("whitelist_auto_remove_duty", True)
        duty_timeout = guild_data.get("whitelist_duty_timeout", 4)

        channel_mention = f"<#{channel_id}>" if channel_id else "❌ Nicht gesetzt"
        room_mention = f"<#{room_id}>" if room_id else "❌ Nicht gesetzt"
        role_mention = f"<@&{role_id}>" if role_id else "❌ Nicht gesetzt"
        approved_role_mention = f"<@&{approved_role_id}>" if approved_role_id else "❌ Nicht gesetzt"
        duty_role_mention = f"<@&{duty_role_id}>" if duty_role_id else "❌ Noch nicht erstellt"
        panel_channel_mention = f"<#{panel_channel_id}>" if panel_channel_id else "Gleicher wie Whitelist-Channel"
        log_channel_mention = f"<#{log_channel_id}>" if log_channel_id else "Gleicher wie Whitelist-Channel"
        auto_duty_status = f"✅ Aktiv ({duty_timeout}h)" if auto_duty else "❌ Deaktiviert"

        # Zähle aktive Duty-User
        duty_count = 0
        duty_role = ctx.guild.get_role(duty_role_id) if duty_role_id else None
        if role_id:
            base_role = ctx.guild.get_role(role_id)
            if base_role:
                for m in base_role.members:
                    if duty_role and duty_role in m.roles:
                        is_on_duty = await self.config.member(m).whitelist_on_duty()
                        if is_on_duty:
                            duty_count += 1

        embed = discord.Embed(
            title="📋 Whitelist Warteraum Konfiguration",
            color=discord.Color.blue()
        )
        embed.add_field(name="Aktive Duty", value=f"🔵 {duty_count} Handler", inline=True)
        embed.add_field(name="Auto-Duty-Ende", value=auto_duty_status, inline=True)
        embed.add_field(name="📝 Whitelist-Channel", value=channel_mention, inline=True)
        embed.add_field(name="🎤 Voice-Warteraum", value=room_mention, inline=True)
        embed.add_field(name="👥 Handler-Rolle", value=role_mention, inline=True)
        embed.add_field(name="✅ Approved-Rolle", value=approved_role_mention, inline=True)
        embed.add_field(name="🔵 Duty-Rolle", value=duty_role_mention, inline=True)
        embed.add_field(name="📋 Panel-Channel", value=panel_channel_mention, inline=True)
        embed.add_field(name="📜 Log-Channel", value=log_channel_mention, inline=True)

        await ctx.send(embed=embed)

    @whitelistset.command(name="createpanel")
    async def whitelistset_createpanel(self, ctx: commands.Context):
        """
        Erstellt ein permanentes Whitelist-Duty-Panel mit Buttons.
        Diese Nachricht sollte im Panel-Channel gepinnt werden.
        
        Das Panel enthält:
        - 🔵 Duty Starten Button
        - 🔴 Duty Beenden Button
        - 🎮 Spieler whitelisten Button (öffnet Modal für ID/Name-Eingabe)
        """
        await ctx.send("🔄 Erstelle Whitelist-Duty-Panel mit Buttons...")
        await self.create_whitelist_panel_message(ctx)
        await ctx.send("✅ Whitelist-Duty-Panel wurde erstellt! Du kannst die Buttons jetzt verwenden.")

    @whitelistset.command(name="autoduty")
    async def whitelistset_autoduty(self, ctx: commands.Context, hours: int = None):
        """
        Konfiguriere automatisches Whitelist-Duty-Ende nach X Stunden.

        - `0` oder `off`: Automatisches Beenden deaktivieren
        - `1-24`: Anzahl der Stunden nach denen Duty automatisch endet
        """
        if hours is None or hours <= 0:
            await self.config.guild(ctx.guild).whitelist_auto_remove_duty.set(False)
            await ctx.send("✅ Automatisches Whitelist-Duty-Ende deaktiviert.")
        else:
            if hours > 24:
                hours = 24
            await self.config.guild(ctx.guild).whitelist_auto_remove_duty.set(True)
            await self.config.guild(ctx.guild).whitelist_duty_timeout.set(hours)
            await ctx.send(f"✅ Whitelist-Duty wird automatisch nach {hours} Stunden beendet.")


class DutyButtonView(discord.ui.View):
    """Button-View für Duty An-/Abmeldung"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Duty Starten", style=discord.ButtonStyle.green, emoji="🟢", custom_id="duty_start")
    async def start_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Startet den Duty-Modus"""
        guild = interaction.guild
        member = interaction.user
        
        role_id = await self.cog.config.guild(guild).role()
        
        if not role_id:
            await interaction.response.send_message("❌ Es wurde keine Support-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Die konfigurierte Support-Rolle existiert nicht mehr!", ephemeral=True)
            return
        
        if base_role not in member.roles:
            await interaction.response.send_message(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!", ephemeral=True)
            return
        
        # Prüfen ob bereits auf Duty
        is_on_duty = await self.cog.config.member(member).on_duty()
        if is_on_duty:
            await interaction.response.send_message("⚠️ Du bist bereits im Duty-Modus!", ephemeral=True)
            return
        
        # Duty aktivieren und Rolle geben
        await self.cog.config.member(member).on_duty.set(True)
        start_time = datetime.utcnow()
        await self.cog.config.member(member).duty_start.set(start_time.timestamp())
        
        # Duty-Rolle hinzufügen (wichtig: erst Rolle, dann Log)
        await self.cog.update_duty_role(member, True)
        
        # Auto-Duty Timer starten falls aktiviert
        auto_duty = await self.cog.config.guild(guild).auto_remove_duty()
        duty_timeout = await self.cog.config.guild(guild).duty_timeout()
        
        # Nachricht im LOG-Channel senden (separat vom Support-Ping!)
        log_channel = await self.cog.get_log_channel(guild)
        
        embed = discord.Embed(
            title="🟢 Duty Gestartet",
            description=f"{member.mention} hat sich für den Support-Dienst angemeldet!",
            color=discord.Color.green(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        
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
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update the panel message to show current duty count
        await self.update_panel_display(guild)
        
        await interaction.response.send_message("✅ Du bist jetzt im Duty-Modus! Du wirst bei neuen Support-Anfragen gepingt.", ephemeral=True)
    
    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Duty-Modus"""
        member = interaction.user
        is_on_duty = await self.cog.config.member(member).on_duty()
        
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Duty-Modus.", ephemeral=True)
            return
        
        # Hole Startzeit für Statistik
        start_time = await self.cog.config.member(member).duty_start()
        duration = "Unbekannt"
        if start_time:
            start_dt = datetime.fromtimestamp(start_time)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
        
        # Duty deaktivieren und Rolle entfernen
        await self.cog.config.member(member).on_duty.set(False)
        await self.cog.config.member(member).duty_start.set(None)
        
        # Duty-Rolle entfernen
        await self.cog.update_duty_role(member, False)
        
        # Nachricht im Log-Channel senden
        guild = interaction.guild
        log_channel = await self.cog.get_log_channel(guild)
        
        embed = discord.Embed(
            title="🔴 Duty Beendet",
            description=f"{member.mention} hat sich vom Support-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
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
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update the panel message to show current duty count
        await self.update_panel_display(guild)
        
        await interaction.response.send_message("✅ Du hast den Duty-Modus verlassen.", ephemeral=True)
    
    async def update_panel_display(self, guild: discord.Guild):
        """Updates the panel message to show current duty members"""
        panel_message_id = await self.cog.config.guild(guild).panel_message_id()
        if not panel_message_id:
            return
        
        panel_channel = await self.cog.get_panel_channel(guild)
        if not panel_channel:
            return
        
        try:
            panel_message = await panel_channel.fetch_message(panel_message_id)
            
            # Get current duty members
            duty_count = 0
            duty_list = []
            duty_role = await self.cog.get_or_create_duty_role(guild)
            role_id = await self.cog.config.guild(guild).role()
            
            if role_id and duty_role:
                base_role = guild.get_role(role_id)
                if base_role:
                    for m in base_role.members:
                        if duty_role in m.roles:
                            is_duty = await self.cog.config.member(m).on_duty()
                            if is_duty:
                                duty_count += 1
                                duty_list.append(f"• {m.display_name}")
            
            # Create new embed with updated info
            if duty_count > 0:
                duty_text = "\n".join(duty_list[:10])
                if len(duty_list) > 10:
                    duty_text += f"\n• ...und {duty_count - 10} weitere"
            else:
                duty_text = "Niemand"
            
            new_embed = discord.Embed(
                title="🎧 Support Duty Panel",
                description=(
                    "**Willkommen zum Support-Duty System!**\n\n"
                    "Klicke auf die Buttons unten um dich für den Support-Dienst an- oder abzumelden.\n\n"
                    "🟢 **Duty Starten** - Du wirst bei neuen Anfragen gepingt\n"
                    "🔴 **Duty Beenden** - Du erhältst keine Pings mehr"
                ),
                color=discord.Color.blue()
            )
            new_embed.add_field(
                name="🟢 Aktuell im Dienst",
                value=duty_text,
                inline=False
            )
            new_embed.set_footer(text=f"Aktive Supporter: {duty_count} • Die 🟢 On Duty Rolle wird automatisch zugewiesen/entfernt")
            
            await panel_message.edit(embed=new_embed)
        except:
            pass  # Ignore errors if panel message was deleted


class WhitelistButtonView(discord.ui.View):
    """Button-View für Whitelist Duty An-/Abmeldung mit Button für Spieler-Verwaltung"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild = guild
    
    @discord.ui.button(label="Duty Starten", style=discord.ButtonStyle.green, emoji="🔵", custom_id="whitelist_duty_start")
    async def start_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Startet den Whitelist-Duty-Modus"""
        guild = interaction.guild
        member = interaction.user
        
        role_id = await self.cog.config.guild(guild).whitelist_role()
        
        if not role_id:
            await interaction.response.send_message("❌ Es wurde keine Whitelist-Handler-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Die konfigurierte Whitelist-Rolle existiert nicht mehr!", ephemeral=True)
            return
        
        if base_role not in member.roles:
            await interaction.response.send_message(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!", ephemeral=True)
            return
        
        # Prüfen ob bereits auf Duty
        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        if is_on_duty:
            await interaction.response.send_message("⚠️ Du bist bereits im Whitelist-Duty-Modus!", ephemeral=True)
            return
        
        # Duty aktivieren und Rolle geben
        await self.cog.config.member(member).whitelist_on_duty.set(True)
        start_time = datetime.utcnow()
        await self.cog.config.member(member).whitelist_duty_start.set(start_time.timestamp())
        
        # Duty-Rolle hinzufügen
        await self.cog.update_duty_role(member, True, whitelist=True)
        
        # Auto-Duty Timer starten falls aktiviert
        auto_duty = await self.cog.config.guild(guild).whitelist_auto_remove_duty()
        duty_timeout = await self.cog.config.guild(guild).whitelist_duty_timeout()
        
        # Nachricht im LOG-Channel senden
        log_channel = await self.cog.get_whitelist_log_channel(guild)
        
        embed = discord.Embed(
            title="🔵 Whitelist Duty Gestartet",
            description=f"{member.mention} hat sich für den Whitelist-Dienst angemeldet!",
            color=discord.Color.blue(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        
        if auto_duty:
            end_time = start_time + timedelta(hours=duty_timeout)
            embed.add_field(name="⏰ Automatische Abmeldung", value=f"Nach {duty_timeout} Stunden\n(<t:{int(end_time.timestamp())}:R>)", inline=True)
        
        # Zähle alle aktiven Duty-User
        duty_count = 0
        duty_role = await self.cog.get_or_create_duty_role(guild, whitelist=True)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.cog.config.member(m).whitelist_on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Aktive Handler", value=f"🔵 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_time.strftime('%d.%m.%Y %H:%M')}")
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update the panel message to show current duty count
        await self.update_panel_display(guild)
        
        await interaction.response.send_message("✅ Du bist jetzt im Whitelist-Duty-Modus! Du wirst bei neuen Anfragen gepingt.", ephemeral=True)
    
    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="whitelist_duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Whitelist-Duty-Modus"""
        member = interaction.user
        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Whitelist-Duty-Modus.", ephemeral=True)
            return
        
        # Hole Startzeit für Statistik
        start_time = await self.cog.config.member(member).whitelist_duty_start()
        duration = "Unbekannt"
        if start_time:
            start_dt = datetime.fromtimestamp(start_time)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
        
        # Duty deaktivieren und Rolle entfernen
        await self.cog.config.member(member).whitelist_on_duty.set(False)
        await self.cog.config.member(member).whitelist_duty_start.set(None)
        
        # Duty-Rolle entfernen
        await self.cog.update_duty_role(member, False, whitelist=True)
        
        # Nachricht im Log-Channel senden
        guild = interaction.guild
        log_channel = await self.cog.get_whitelist_log_channel(guild)
        
        embed = discord.Embed(
            title="🔴 Whitelist Duty Beendet",
            description=f"{member.mention} hat sich vom Whitelist-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)
        
        # Zähle verbleibende aktive Duty-User
        duty_count = 0
        duty_role = await self.cog.get_or_create_duty_role(guild, whitelist=True)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.cog.config.member(m).whitelist_on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Verbleibende Handler", value=f"🔵 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}")
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update the panel message to show current duty count
        await self.update_panel_display(guild)
        
        await interaction.response.send_message("✅ Du hast den Whitelist-Duty-Modus verlassen.", ephemeral=True)
    
    @discord.ui.button(label="Spieler whitelisten", style=discord.ButtonStyle.primary, emoji="🎮", custom_id="whitelist_player_modal")
    async def open_whitelist_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal zum Eingeben der Spieler-ID oder des Benutzernamens"""
        guild = interaction.guild
        member = interaction.user
        
        # Prüfe ob User Whitelist-Duty hat
        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        if not is_on_duty:
            await interaction.response.send_message("❌ Du musst im Whitelist-Duty sein um Spieler hinzuzufügen!", ephemeral=True)
            return
        
        try:
            # Öffne das WhitelistSearchModal (das jetzt direkt die Verarbeitung übernimmt)
            modal = WhitelistSearchModal(self.cog, guild)
            await interaction.response.send_modal(modal)
        except discord.errors.InteractionResponded:
            # Interaktion wurde bereits beantwortet, ignoriere
            pass
        except Exception as e:
            await interaction.response.send_message(f"❌ Fehler beim Öffnen des Modals: `{str(e)}`", ephemeral=True)
    
    async def update_panel_display(self, guild: discord.Guild):
        """Updates the whitelist panel message to show current duty members"""
        panel_message_id = await self.cog.config.guild(guild).whitelist_panel_message_id()
        if not panel_message_id:
            return
        
        panel_channel = await self.cog.get_whitelist_panel_channel(guild)
        if not panel_channel:
            return
        
        try:
            panel_message = await panel_channel.fetch_message(panel_message_id)
            
            # Get current duty members
            duty_count = 0
            duty_list = []
            duty_role = await self.cog.get_or_create_duty_role(guild, whitelist=True)
            role_id = await self.cog.config.guild(guild).whitelist_role()
            
            if role_id and duty_role:
                base_role = guild.get_role(role_id)
                if base_role:
                    for m in base_role.members:
                        if duty_role in m.roles:
                            is_duty = await self.cog.config.member(m).whitelist_on_duty()
                            if is_duty:
                                duty_count += 1
                                duty_list.append(f"• {m.display_name}")
            
            # Create new embed with updated info
            if duty_count > 0:
                duty_text = "\n".join(duty_list[:10])
                if len(duty_list) > 10:
                    duty_text += f"\n• ...und {duty_count - 10} weitere"
            else:
                duty_text = "Niemand"
            
            new_embed = discord.Embed(
                title="📋 Whitelist Duty Panel",
                description=(
                    "**Willkommen zum Whitelist-Duty System!**\n\n"
                    "Klicke auf die Buttons unten um dich für den Whitelist-Dienst an- oder abzumelden.\n\n"
                    "🔵 **Duty Starten** - Du wirst bei neuen Anfragen gepingt\n"
                    "🔴 **Duty Beenden** - Du erhältst keine Pings mehr\n\n"
                    "Klicke auf \"🎮 Spieler whitelisten\" um einen Spieler zur Whitelist hinzuzufügen."
                ),
                color=discord.Color.blue()
            )
            new_embed.add_field(
                name="🔵 Aktuell im Dienst",
                value=duty_text,
                inline=False
            )
            new_embed.set_footer(text=f"Aktive Handler: {duty_count} • Die 📋 Whitelist Duty Rolle wird automatisch zugewiesen/entfernt")
            
            await panel_message.edit(embed=new_embed)
        except:
            pass  # Ignore errors if panel message was deleted


class WhitelistPlayerSelect(discord.ui.Select):
    """Dropdown-Menü zum Auswählen von Spielern für Whitelist - Mit Suchfunktion"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild, search_query: str = ""):
        self.cog = cog
        self.guild = guild
        self.search_query = search_query.lower().strip() if search_query else ""
        
        # Sammle alle Mitglieder für die Dropdown-Optionen
        options = []
        for member in guild.members:
            # Überspringe Bots
            if member.bot:
                continue
            
            # Filtere nach Suchbegriff falls vorhanden
            if self.search_query:
                if (self.search_query not in member.name.lower() and 
                    self.search_query not in member.display_name.lower() and 
                    self.search_query not in str(member.id)):
                    continue
            
            # Kürze den Namen falls zu lang (max 100 Zeichen für Label)
            display_name = member.display_name[:90] + "..." if len(member.display_name) > 93 else member.display_name
            
            # Füge Avatar-Emoji hinzu wenn möglich
            emoji = "👤"
            if member.avatar:
                emoji = "✅" if any(role.name.lower() in ["whitelist", "approved", "verified"] for role in member.roles) else "👤"
            
            options.append(
                discord.SelectOption(
                    label=display_name,
                    description=f"{member.name} | ID: {member.id}",
                    value=f"user_{member.id}",
                    emoji=emoji
                )
            )
        
        # Begrenze auf max 25 Optionen (Discord Limit) - zeige die ersten 25 Treffer
        if len(options) > 25:
            options = options[:25]
        
        # Falls keine Spieler verfügbar sind oder keine Treffer bei Suche
        if not options:
            if self.search_query:
                options.append(
                    discord.SelectOption(
                        label=f"Keine Treffer für '{search_query}'",
                        description="Versuche einen anderen Suchbegriff",
                        value="no_results",
                        emoji="❌"
                    )
                )
            else:
                options.append(
                    discord.SelectOption(
                        label="Keine Spieler verfügbar",
                        description="Keine Mitglieder gefunden",
                        value="none",
                        emoji="ℹ️"
                    )
                )
        
        placeholder = "Spieler suchen & auswählen..." if not self.search_query else f"Treffer für '{search_query}'..."
        
        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=1,
            options=options,
            custom_id="whitelist_player_select"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn ein Spieler ausgewählt wird"""
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        member = interaction.user  # Der Handler der die Aktion durchführt
        
        # Prüfe ob User Whitelist-Duty hat
        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        if not is_on_duty:
            await interaction.followup.send("❌ Du musst im Whitelist-Duty sein um Spieler hinzuzufügen!", ephemeral=True)
            return
        
        # Hole die Approved-Rolle
        approved_role = await self.cog.get_whitelist_approved_role(guild)
        if not approved_role:
            await interaction.followup.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        # Hole den ausgewählten User (gespeichert in der value als User-ID)
        selected_value = self.values[0]
        
        if selected_value in ["none", "no_results"]:
            await interaction.followup.send("ℹ️ Kein Spieler ausgewählt oder keine Treffer!", ephemeral=True)
            return
        
        if selected_value.startswith("user_"):
            user_id = int(selected_value.split("_")[1])
            try:
                target_user = await guild.fetch_member(user_id)
                
                # Prüfe ob der Spieler bereits die Approved-Rolle hat
                if approved_role not in target_user.roles:
                    await target_user.add_roles(approved_role, reason=f"Whitelist genehmigt von {member.display_name}")
                    
                    # Sende Bestätigung dem Handler
                    embed_success = discord.Embed(
                        title="✅ Whitelist genehmigt",
                        description=f"{target_user.mention} wurde erfolgreich zur Whitelist hinzugefügt!",
                        color=discord.Color.green(),
                        timestamp=datetime.utcnow()
                    )
                    embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention} ({member.display_name})", inline=True)
                    embed_success.add_field(name="🎮 Spieler", value=f"{target_user.display_name}", inline=True)
                    
                    # Logge die Aktion im Log-Channel
                    log_channel = await self.cog.get_whitelist_log_channel(guild)
                    if log_channel:
                        # Detailliertes Log für den Log-Channel
                        log_embed = discord.Embed(
                            title="📋 Whitelist Eintrag erstellt",
                            description=f"**{target_user.mention}** wurde zur Whitelist hinzugefügt.",
                            color=discord.Color.blue(),
                            timestamp=datetime.utcnow()
                        )
                        log_embed.set_thumbnail(url=target_user.display_avatar.url)
                        log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                        log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                        log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                        log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                        log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {member.display_name}")
                        
                        await log_channel.send(embed=log_embed)
                    
                    # Benachrichtige den Spieler privat falls möglich
                    try:
                        dm_embed = discord.Embed(
                            title="🎉 Herzlichen Glückwunsch!",
                            description=f"Du wurdest von **{member.display_name}** zur Whitelist hinzugefügt!",
                            color=discord.Color.green(),
                            timestamp=datetime.utcnow()
                        )
                        dm_embed.add_field(name="✅ Rolle erhalten", value=f"{approved_role.mention}", inline=False)
                        dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt die freigeschalteten Features nutzen!", inline=False)
                        dm_embed.set_footer(text=f"{guild.name} Whitelist System")
                        await target_user.send(embed=dm_embed)
                    except discord.Forbidden:
                        pass  # DMs deaktiviert
                    
                    await interaction.followup.send(f"✅ {target_user.mention} hat die Whitelist-Rolle erhalten!\n📝 Die Aktion wurde im Log-Channel protokolliert.", ephemeral=True)
                else:
                    await interaction.followup.send(f"ℹ️ {target_user.mention} hat bereits die Whitelist-Rolle!", ephemeral=True)
                    
            except discord.NotFound:
                await interaction.followup.send("❌ Der ausgewählte Spieler wurde nicht gefunden!", ephemeral=True)
            except Exception as e:
                await interaction.followup.send(f"❌ Fehler beim Hinzufügen: {e}", ephemeral=True)


class WhitelistInputModal(discord.ui.Modal):
    """Modal zum direkten Eingeben der Spieler-ID oder des Benutzernamens für Whitelist"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild):
        self.cog = cog
        self.guild = guild
        
        super().__init__(title="🎮 Spieler whitelisten")
        
        self.player_input = discord.ui.TextInput(
            label="Spielername oder ID",
            style=discord.TextStyle.short,
            placeholder="Gib den Discord-Namen oder die User-ID ein...",
            min_length=1,
            max_length=50,
            required=True,
            custom_id="whitelist_player_input"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn das Modal abgesendet wird"""
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        member = interaction.user  # Der Handler der die Aktion durchführt
        search_query = self.player_input.value.strip()
        
        # Prüfe ob User Whitelist-Duty hat (bereits im Button geprüft, aber sicherheitshalber)
        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        if not is_on_duty:
            await interaction.followup.send("❌ Du musst im Whitelist-Duty sein um Spieler hinzuzufügen!", ephemeral=True)
            return
        
        # Hole die Approved-Rolle
        approved_role = await self.cog.get_whitelist_approved_role(guild)
        if not approved_role:
            await interaction.followup.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        # Versuche den Spieler zu finden
        target_user = None
        
        # Versuch 1: Als User-ID parsen
        try:
            user_id = int(search_query)
            target_user = await guild.fetch_member(user_id)
        except (ValueError, discord.NotFound):
            pass
        
        # Versuch 2: Nach Namen suchen (mit Discriminator oder neuem Namenssystem)
        if not target_user:
            for m in guild.members:
                if m.bot:
                    continue
                # Prüfe Name, Display-Name und ID als String
                if (search_query.lower() == m.name.lower() or 
                    search_query.lower() == m.display_name.lower() or
                    search_query == str(m.id)):
                    target_user = m
                    break
            
            # Wenn nicht exakt gefunden, suche nach Teilübereinstimmung
            if not target_user:
                for m in guild.members:
                    if m.bot:
                        continue
                    if (search_query.lower() in m.name.lower() or 
                        search_query.lower() in m.display_name.lower()):
                        target_user = m
                        break
        
        if not target_user:
            await interaction.followup.send(
                f"❌ Kein Spieler gefunden für '**{search_query}**'!\n"
                "Bitte überprüfe die Schreibweise oder verwende die User-ID.",
                ephemeral=True
            )
            return
        
        # Prüfe ob der Spieler bereits die Approved-Rolle hat
        if approved_role in target_user.roles:
            await interaction.followup.send(
                f"ℹ️ {target_user.mention} hat bereits die Whitelist-Rolle!",
                ephemeral=True
            )
            return
        
        # Füge die Approved-Rolle hinzu
        try:
            await target_user.add_roles(approved_role, reason=f"Whitelist genehmigt von {member.display_name}")
            
            # Sende Bestätigung dem Handler
            embed_success = discord.Embed(
                title="✅ Whitelist genehmigt",
                description=f"{target_user.mention} wurde erfolgreich zur Whitelist hinzugefügt!",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention} ({member.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{target_user.display_name}", inline=True)
            
            await interaction.followup.send(embed=embed_success, ephemeral=True)
            
            # Logge die Aktion im Log-Channel
            log_channel = await self.cog.get_whitelist_log_channel(guild)
            if log_channel:
                log_embed = discord.Embed(
                    title="📋 Whitelist Eintrag erstellt",
                    description=f"**{target_user.mention}** wurde zur Whitelist hinzugefügt.",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )
                log_embed.set_thumbnail(url=target_user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {member.display_name}")
                
                await log_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler privat falls möglich
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=(
                        f"**{target_user.display_name}**, deine Whitelist-Anfrage wurde genehmigt!\n\n"
                        f"Du hast jetzt Zugriff auf alle Bereiche des Servers.\n"
                        f"Genehmigt von: **{member.display_name}**"
                    ),
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                dm_embed.set_thumbnail(url=guild.icon.url if guild.icon else None)
                await target_user.send(embed=dm_embed)
            except discord.Forbidden:
                pass  # DMs deaktiviert
            
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!",
                ephemeral=True
            )
        except Exception as e:
            await interaction.followup.send(
                f"❌ Ein Fehler ist aufgetreten: `{str(e)}`",
                ephemeral=True
            )


class WhitelistSearchModal(discord.ui.Modal):
    """Modal zum Suchen von Spielern für die Whitelist - Verarbeitet direkt die Whitelist"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild):
        self.cog = cog
        self.guild = guild
        
        super().__init__(title="🎮 Spieler whitelisten", timeout=600)
        
        self.search_input = discord.ui.TextInput(
            label="Spielername oder ID",
            style=discord.TextStyle.short,
            placeholder="Gib den Discord-Namen oder die User-ID ein (z.B. 123456789012345678)",
            min_length=1,
            max_length=50,
            required=True
        )
        # Füge das TextInput-Feld zum Modal hinzu
        self.add_item(self.search_input)
    
    async def callback(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn das Modal abgesendet wird - Verarbeitet die Whitelist direkt"""
        await interaction.response.defer(ephemeral=True, thinking=True)
        
        try:
            # Hole die eingegebene ID oder den Namen
            search_query = self.search_input.value.strip()
            
            member = interaction.user
            is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
            if not is_on_duty:
                await interaction.followup.send("❌ Du musst im Whitelist-Duty sein um Spieler hinzuzufügen!", ephemeral=True)
                return
            
            approved_role = await self.cog.get_whitelist_approved_role(self.guild)
            if not approved_role:
                await interaction.followup.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
                return
            
            # Versuche den Spieler zu finden
            target_user = None
            
            # Versuch 1: Als User-ID parsen
            try:
                user_id = int(search_query)
                target_user = await self.guild.fetch_member(user_id)
            except (ValueError, discord.NotFound, discord.HTTPException):
                pass
            
            # Versuch 2: Nach Namen suchen
            if not target_user:
                for m in self.guild.members:
                    if m.bot:
                        continue
                    if (search_query.lower() == m.name.lower() or 
                        search_query.lower() == m.display_name.lower() or
                        search_query == str(m.id)):
                        target_user = m
                        break
                
                # Teilübereinstimmung
                if not target_user:
                    for m in self.guild.members:
                        if m.bot:
                            continue
                        if (search_query.lower() in m.name.lower() or 
                            search_query.lower() in m.display_name.lower()):
                            target_user = m
                            break
            
            if not target_user:
                await interaction.followup.send(
                    f"❌ Kein Spieler gefunden für '**{search_query}**'!\n"
                    "Bitte überprüfe die Schreibweise oder verwende die User-ID.",
                    ephemeral=True
                )
                return
            
            if approved_role in target_user.roles:
                await interaction.followup.send(
                    f"ℹ️ {target_user.mention} hat bereits die Whitelist-Rolle!",
                    ephemeral=True
                )
                return
            
            # Füge die Approved-Rolle hinzu
            try:
                await target_user.add_roles(approved_role, reason=f"Whitelist genehmigt von {member.display_name}")
                
                embed_success = discord.Embed(
                    title="✅ Whitelist genehmigt",
                    description=f"{target_user.mention} wurde erfolgreich zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention} ({member.display_name})", inline=True)
                embed_success.add_field(name="🎮 Spieler", value=f"{target_user.display_name}", inline=True)
                
                await interaction.followup.send(embed=embed_success, ephemeral=True)
                
                # Logge die Aktion
                log_channel = await self.cog.get_whitelist_log_channel(self.guild)
                if log_channel:
                    log_embed = discord.Embed(
                        title="📋 Whitelist Eintrag erstellt",
                        description=f"**{target_user.mention}** wurde zur Whitelist hinzugefügt.",
                        color=discord.Color.blue(),
                        timestamp=datetime.utcnow()
                    )
                    log_embed.set_thumbnail(url=target_user.display_avatar.url)
                    log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                    log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                    log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                    log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                    log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {member.display_name}")
                    
                    await log_channel.send(embed=log_embed)
                
                # Benachrichtige den Spieler
                try:
                    dm_embed = discord.Embed(
                        title="🎉 Herzlichen Glückwunsch!",
                        description=f"Du wurdest von **{member.display_name}** zur Whitelist hinzugefügt!",
                        color=discord.Color.green(),
                        timestamp=datetime.utcnow()
                    )
                    dm_embed.add_field(name="✅ Rolle erhalten", value=f"{approved_role.mention}", inline=False)
                    dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt die freigeschalteten Features nutzen!", inline=False)
                    dm_embed.set_footer(text=f"{self.guild.name} Whitelist System")
                    await target_user.send(embed=dm_embed)
                except discord.Forbidden:
                    pass
                
            except discord.Forbidden:
                await interaction.followup.send(
                    "❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!",
                    ephemeral=True
                )
            except Exception as e:
                await interaction.followup.send(
                    f"❌ Ein Fehler ist beim Hinzufügen der Rolle aufgetreten: `{str(e)}`",
                    ephemeral=True
                )
                
        except Exception as e:
            # Fangen alle anderen Fehler ab
            error_msg = f"❌ Ein unerwarteter Fehler ist aufgetreten: `{type(e).__name__}: {str(e)}`"
            try:
                await interaction.followup.send(error_msg, ephemeral=True)
            except:
                pass


class WhitelistDutyView(discord.ui.View):
    """Button-View für Whitelist-Duty - Nur für interne Suche (nicht im Panel verwendet)"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild = None, search_query: str = ""):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild = guild
        self.search_query = search_query
    
    @discord.ui.button(label="🔍 Spieler suchen", style=discord.ButtonStyle.secondary, emoji="🔎", custom_id="whitelist_search_player", row=1)
    async def search_player(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal zur Spielersuche"""
        is_on_duty = await self.cog.config.member(interaction.user).whitelist_on_duty()
        if not is_on_duty:
            await interaction.response.send_message("❌ Du musst im Whitelist-Duty sein um Spieler zu suchen!", ephemeral=True)
            return
        
        modal = WhitelistSearchModal(self.cog, interaction.guild)
        await interaction.response.send_modal(modal)


class FeedbackPanelView(discord.ui.View):
    """Button-View für Feedback"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Positives Feedback geben", style=discord.ButtonStyle.green, emoji="😊", custom_id="feedback_positive")
    async def positive_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für positives Feedback"""
        modal = FeedbackModal(self.cog, "positive")
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Negatives Feedback geben", style=discord.ButtonStyle.red, emoji="😞", custom_id="feedback_negative")
    async def negative_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für negatives Feedback"""
        modal = FeedbackModal(self.cog, "negative")
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Vorschlag machen", style=discord.ButtonStyle.blurple, emoji="💡", custom_id="feedback_suggestion")
    async def suggestion_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für Vorschläge"""
        modal = FeedbackModal(self.cog, "suggestion")
        await interaction.response.send_modal(modal)


class FeedbackModal(discord.ui.Modal):
    """Modal für Feedback-Eingabe"""
    
    def __init__(self, cog: SupportCog, feedback_type: str):
        self.cog = cog
        self.feedback_type = feedback_type
        
        type_titles = {
            "positive": "😊 Positives Feedback",
            "negative": "😞 Negatives Feedback",
            "suggestion": "💡 Vorschlag"
        }
        
        super().__init__(title=type_titles.get(feedback_type, "Feedback"))
        
        self.feedback_text = discord.ui.TextInput(
            label="Dein Feedback",
            style=discord.TextStyle.long,
            placeholder="Beschreibe dein Feedback hier...",
            min_length=10,
            max_length=2000,
            required=True
        )
        self.add_item(self.feedback_text)
    
    async def on_submit(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn das Modal abgeschickt wird"""
        guild = interaction.guild
        member = interaction.user
        feedback_content = self.feedback_text.value
        
        feedback_channel = await self.cog.get_feedback_channel(guild)
        
        if not feedback_channel:
            await interaction.response.send_message("❌ Kein Feedback-Channel konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        type_emojis = {
            "positive": "😊",
            "negative": "😞",
            "suggestion": "💡"
        }
        type_names = {
            "positive": "Positives Feedback",
            "negative": "Negatives Feedback",
            "suggestion": "Vorschlag"
        }
        
        embed = discord.Embed(
            title=f"{type_emojis[self.feedback_type]} {type_names[self.feedback_type]}",
            description=feedback_content,
            color=discord.Color.green() if self.feedback_type == "positive" else (discord.Color.red() if self.feedback_type == "negative" else discord.Color.blurple()),
            timestamp=datetime.utcnow()
        )
        embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        embed.add_field(name="👤 Nutzer", value=f"{member.mention}\n(`{member.id}`)", inline=True)
        embed.add_field(name="📍 Channel", value=interaction.channel.mention, inline=True)
        embed.set_footer(text=f"Feedback-ID: {hash(feedback_content)}")
        
        await feedback_channel.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ Dein {type_names[self.feedback_type].lower()} wurde erfolgreich übermittelt! Vielen Dank für dein Feedback.",
            ephemeral=True
        )
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        """Fehlerbehandlung"""
        await interaction.response.send_message("❌ Ein Fehler ist aufgetreten. Bitte versuche es später erneut.", ephemeral=True)
        print(f"Fehler im Feedback-Modal: {error}")


class SupportCallView(discord.ui.View):
    """Button-View für Support-Aufruf"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Support rufen", style=discord.ButtonStyle.red, emoji="📞", custom_id="support_call")
    async def call_support(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Ruft einen Supporter"""
        guild = interaction.guild
        member = interaction.user
        
        # Hole Duty-Rolle und Duty-Mitglieder
        role_id = await self.cog.config.guild(guild).role()
        if not role_id:
            await interaction.response.send_message("❌ Keine Support-Rolle konfiguriert!", ephemeral=True)
            return
        
        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Support-Rolle nicht gefunden!", ephemeral=True)
            return
        
        # Hole Duty-Mitglieder
        duty_members = []
        duty_role = await self.cog.get_or_create_duty_role(guild)
        if duty_role:
            for m in base_role.members:
                if duty_role in m.roles:
                    is_on_duty = await self.cog.config.member(m).on_duty()
                    if is_on_duty:
                        duty_members.append(m)
        
        if not duty_members:
            # Fallback zur Basis-Rolle wenn niemand Duty hat
            await interaction.response.send_message(
                f"🔴 Aktuell ist kein Supporter im Dienst! Die {base_role.mention} wurde benachrichtigt.",
                ephemeral=True
            )
            # Trotzdem Benachrichtigung senden
            call_channel = await self.cog.get_support_call_channel(guild)
            if call_channel:
                embed = discord.Embed(
                    title="📞 Support-Anfrage",
                    description=f"{member.mention} benötigt Support!",
                    color=discord.Color.orange(),
                    timestamp=datetime.utcnow()
                )
                embed.add_field(name="👤 Anfragender", value=f"{member.display_name}\n(`{member.id}`)", inline=True)
                embed.add_field(name="📍 Ursprungs-Channel", value=interaction.channel.mention, inline=True)
                embed.set_footer(text="🔴 Niemand im Duty - Basis-Rolle wird gepingt")
                
                await call_channel.send(content=base_role.mention, embed=embed, 
                                       allowed_mentions=discord.AllowedMentions(roles=[base_role]))
            return
        
        # Wähle zufälligen Duty-Supporter aus
        import random
        selected_supporter = random.choice(duty_members)
        
        # Hole Call-Room
        call_room = await self.cog.get_call_room(guild)
        call_room_mention = call_room.mention if call_room else "einem Voice-Channel"
        
        # Sende Benachrichtigung im Call-Channel
        call_channel = await self.cog.get_support_call_channel(guild)
        
        embed = discord.Embed(
            title="📞 Support-Aufruf",
            description=f"{member.mention} ruft nach Support!",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Anfragender", value=f"{member.display_name}\n(`{member.id}`)", inline=True)
        embed.add_field(name="📍 Ursprungs-Channel", value=interaction.channel.mention, inline=True)
        embed.add_field(name="🎤 Treffpunkt", value=f"Bitte begib dich zu {call_room_mention}", inline=True)
        embed.set_footer(text=f"Support-System • {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}")
        
        if call_channel:
            await call_channel.send(content=selected_supporter.mention, embed=embed,
                                   allowed_mentions=discord.AllowedMentions(users=[selected_supporter]))
        
        await interaction.response.send_message(
            f"✅ {selected_supporter.mention} wurde gerufen! Bitte warte kurz, der Supporter wird sich bei dir melden oder du sollst dich in einem bestimmten Channel einfinden.",
            ephemeral=True
        )


async def setup(bot: Red):
    """Lädt den Cog"""
    cog = SupportCog(bot)
    # Registere die persistent Views für Buttons
    bot.add_view(DutyButtonView(cog))
    bot.add_view(WhitelistButtonView(cog))
    bot.add_view(FeedbackPanelView(cog))
    bot.add_view(SupportCallView(cog))
    await bot.add_cog(cog)


async def teardown(bot: Red):
    """Entfernt den Cog"""
    bot.remove_view(DutyButtonView)
    bot.remove_view(WhitelistButtonView)
    bot.remove_view(FeedbackPanelView)
    bot.remove_view(SupportCallView)
    await bot.remove_cog("SupportCog")
