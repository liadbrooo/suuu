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
   - [p]supportset dutygrantrole @Rolle    (Setzt die Rolle die anderen Support-Duty geben darf)
   - [p]supportset feedbackchannel #channel (Channel für Feedback-Logs)
   - [p]supportset feedbackpanelchannel #channel (Channel NUR für das Feedback-Panel - GETRENNT vom Log!)
   - [p]supportset callchannel #channel     (Channel für Support-Aufrufe)
   - [p]supportset callroom @VoiceChannel   (Voice-Channel für Support-Calls)
   - ODER verwende [p]supportset setup für einen interaktiven Einrichtungsassistenten

BEFEHLE (AUSWAHL):
   SUPPORT & DUTY:
   - [p]supportstats - Zeigt Support-Statistiken an
   - [p]dutylist - Zeigt alle aktuell im Duty befindlichen Mitglieder (Support & Whitelist)
   - [p]staffinfo [@user] - Zeigt Informationen über ein Teammitglied
   - [p]supportinfo - Zeigt Informationen über das Support-System
   - [p]supportdutygrant @user - Gib einem Nutzer die Support-Duty-Berechtigung (nur mit Grant-Rolle)
   
   WHITELIST DUTY:
   - [p]whitelistset dutygrantrole @Rolle  (Setzt die Rolle die anderen Whitelist-Duty geben darf)
   - [p]whitelistdutygrant @user - Gib einem Nutzer die Whitelist-Duty-Berechtigung (nur mit Grant-Rolle)
   - [p]whitelistinfo - Zeigt Informationen über das Whitelist-System
   
   FEEDBACK:
   - [p]feedbackpanel - Erstellt ein Feedback-Panel mit Buttons (Positiv/Negativ/Vorschlag)
   
   MODERATION:
   - [p]warn <user> [grund] - Verwarnt einen Benutzer
   - [p]mute <user> [dauer] [grund] - Setzt Timeout
   - [p]unmute <user> [grund] - Hebt Stummschaltung auf
   - [p]kick <user> [grund] - Kickt einen Benutzer
   - [p]ban <user> [grund] - Bannt einen Benutzer
   - [p]unban <user-id> [grund] - Hebt Bann auf
   - [p]modlog [limit] - Zeigt letzte Moderations-Logs
   - [p]purge <anzahl> - Löscht Nachrichten
   - [p]slowmode <sekunden> - Setzt Slowmode
   - [p]lock/unlock - Sperrt/Entsperrt Channel
   
   WHITELIST:
   - [p]whitelistlist - Zeigt alle whitelisted Benutzer
   - [p]whitelistuser <user> <spielername> - Fügt zur Whitelist hinzu
   - [p]removewhitelist <user> - Entfernt von Whitelist
   
   INFO:
   - [p]serverinfo - Zeigt Server-Informationen
   - [p]roleinfo <rolle> - Zeigt Rollen-Informationen
   - [p]nick <user> [nickname] - Ändert Nickname
   - [p]callpanel - Erstellt Support-Aufruf-Panel
   - [p]teampanel - Erstellt Team-Übersichts-Panel

Nutzung:
- Wenn jemand den konfigurierten Voice-Channel betritt, wird automatisch
  eine schöne Nachricht im entsprechenden Channel gesendet mit Ping aller Duty-Mitglieder
- Duty-Logs (An-/Abmeldungen) landen separat im Log-Channel
- Feedback-Panel kann in einem VÖLLIG GETRENNTEN Channel erstellt werden (feedbackpanelchannel)
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
            "panel_channel": None,  # Channel für das Duty-Panel mit Buttons UND Feedback/Call Panels
            "log_channel": None,    # Channel NUR für Duty-Logs: An-/Abmeldungen
            "team_channel": None,   # Channel für das Team-Übersichts-Panel
            "auto_remove_duty": True,  # Automatisch Duty entfernen nach X Stunden
            "duty_timeout": 4,  # Stunden nach denen Duty automatisch entfernt wird
            "panel_message_id": None,  # Message ID der permanenten Panel-Nachricht
            "team_panel_message_id": None,  # Message ID des Team-Panels
            "support_always_allowed_role": None,  # Rolle die immer User holen darf (ohne Duty-Pflicht)
            
            # ERWEITERTE DUTY FUNKTIONEN
            "duty_status_display_channel": None,  # Channel für das erweiterte Duty-Status-Display
            "duty_status_display_message_id": None,  # Message ID des Status-Displays
            "max_duty_hours_per_day": 8,  # Maximale Duty-Stunden pro Tag
            "min_break_minutes": 15,  # Minimale Pausenzeit nach X Minuten Duty
            "duty_handover_enabled": False,  # Duty-Übergabe-System aktivieren
            "duty_schedule_channel": None,  # Channel für Duty-Planung
            "allow_status_messages": True,  # Benutzerdefinierte Status-Nachrichten erlauben
            
            # FEEDBACK SYSTEM
            "feedback_channel": None,  # Channel für Feedback-Logs
            "feedback_panel_channel": None,  # Channel NUR für das Feedback-Panel (getrennt vom Log)
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
            "whitelist_grant_role": None,  # Rolle die bei "Whitelist freischalten" Button vergeben wird
            "whitelist_panel_channel": None,  # Channel für Whitelist-Duty-Panel
            "whitelist_log_channel": None,  # Channel für Whitelist-Duty-Logs
            "whitelist_entries_channel": None,  # Channel für Whitelist-Einträge (Hinzufügungen/Entfernungen)
            "whitelist_auto_remove_duty": True,
            "whitelist_duty_timeout": 4,
            "whitelist_panel_message_id": None,
            "whitelist_always_allowed_role": None,  # Rolle die immer User holen darf (ohne Duty-Pflicht)
            "whitelist_duty_grant_role": None,  # Rolle die berechtigt ist anderen die Whitelist-Duty-Rolle zu geben
            
            # SUPPORT CASE TRACKING
            "active_support_cases": {},  # {message_id: {"user_id": user_id, "helper_id": helper_id, "timestamp": timestamp, "channel": channel_id}}
            "active_whitelist_cases": {},  # {message_id: {"user_id": user_id, "helper_id": helper_id, "timestamp": timestamp, "channel": channel_id}}
            
            # TICKET SYSTEM
            "ticket_category": None,  # Kategorie für Tickets
            "ticket_panel_channel": None,  # Channel für Ticket-Panel
            "ticket_panel_message_id": None,  # Message ID des Ticket-Panels
            "ticket_support_role": None,  # Rolle die Tickets bearbeiten kann
            
            # MODERATION SYSTEM
            "mod_log_channel": None,  # Channel für Moderations-Logs
            "warn_threshold": 3,  # Anzahl Warns vor Auto-Mute
            "mute_duration": 60,  # Standard Mute-Dauer in Minuten
            
            # STATS & TRACKING
            "track_stats": True,  # Ob Statistiken getrackt werden sollen
            "support_stats_channel": None,  # Channel für Support-Statistiken
            
            # DUTY GRANT ROLE SETTINGS
            "support_duty_grant_role": None,  # Rolle die berechtigt ist anderen die Support-Duty-Rolle zu geben
        }

        # Speichert On-Duty Status pro User (für beide Systeme)
        default_member_settings = {
            "on_duty": False,
            "duty_start": None,
            "whitelist_on_duty": False,
            "whitelist_duty_start": None,
            "total_duty_time": 0,  # Gesamte Duty-Zeit in Sekunden (Support)
            "total_whitelist_duty_time": 0,  # Gesamte Duty-Zeit in Sekunden (Whitelist)
            
            # ERWEITERTE DUTY FUNKTIONEN
            "duty_status": "available",  # available, busy, break, away
            "duty_status_message": None,  # Benutzerdefinierter Status-Text
            "duty_scheduled": [],  # Geplante Duty-Schichten [{"start": timestamp, "end": timestamp}]
            "duty_handover_target": None,  # User-ID für geplante Übergabe
            "duty_handover_time": None,  # Timestamp für geplante Übergabe
            "duty_session_count": 0,  # Anzahl der Duty-Sitzungen
            "last_duty_end": None,  # Timestamp des letzten Duty-Endes
            "duty_break_count": 0,  # Anzahl der Pausen in aktueller Sitzung
            "duty_total_break_time": 0,  # Gesamte Pausenzeit in Sekunden
            "current_break_start": None,  # Startzeit der aktuellen Pause
        }

        self.config.register_guild(**default_guild_settings)
        self.config.register_member(**default_member_settings)

        # Cache für aktive Duty-User (wird bei Bot-Start neu aufgebaut)
        self.duty_cache = {}

    async def cog_load(self):
        """Wird beim Laden des Cogs aufgerufen - registriert persistente Views"""
        # Registere die persistent Views für Buttons
        self.bot.add_view(DutyButtonView(self))
        self.bot.add_view(WhitelistButtonView(self))
        self.bot.add_view(FeedbackPanelView())  # Keine cog Referenz für persistente View
        self.bot.add_view(SupportCallView(self))
        self.bot.add_view(PersistentWhitelistGrantView(self))  # Persistente View für Whitelist-Rollenvergabe

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
        """Holt den Log-Channel für Whitelist-Duty-Logs (An-/Abmeldungen)"""
        log_channel_id = await self.config.guild(guild).whitelist_log_channel()
        
        if log_channel_id:
            channel = guild.get_channel(log_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Kein separater Log-Channel gesetzt - Logs landen im Whitelist-Channel
        return await self.get_whitelist_channel(guild)

    async def get_whitelist_entries_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für Whitelist-Einträge (Hinzufügungen/Entfernungen von Spielern)"""
        entries_channel_id = await self.config.guild(guild).whitelist_entries_channel()
        
        if entries_channel_id:
            channel = guild.get_channel(entries_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf log_channel wenn kein spezieller entries_channel gesetzt
        return await self.get_whitelist_log_channel(guild)

    async def get_whitelist_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Panel-Channel für das Whitelist-Duty-Interface"""
        panel_channel_id = await self.config.guild(guild).whitelist_panel_channel()
        
        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf Log-Channel
        return await self.get_whitelist_log_channel(guild)

    async def update_whitelist_panel_display(self, guild: discord.Guild):
        """Updates the whitelist panel message to show current duty members and grant role button"""
        try:
            panel_message_id = await self.config.guild(guild).whitelist_panel_message_id()
            if not panel_message_id:
                return
            
            panel_channel = await self.get_whitelist_panel_channel(guild)
            if not panel_channel:
                return
            
            panel_message = await panel_channel.fetch_message(panel_message_id)
            
            # Get current duty members
            duty_count = 0
            duty_list = []
            duty_role = await self.get_or_create_duty_role(guild, whitelist=True)
            role_id = await self.config.guild(guild).whitelist_role()
            
            if role_id and duty_role:
                base_role = guild.get_role(role_id)
                if base_role:
                    for m in base_role.members:
                        if duty_role in m.roles:
                            is_duty = await self.config.member(m).whitelist_on_duty()
                            if is_duty:
                                duty_count += 1
                                duty_list.append(f"• {m.display_name}")
            
            # Check if grant role is configured
            grant_role_id = await self.config.guild(guild).whitelist_grant_role()
            has_grant_role = grant_role_id is not None
            
            # Create new embed with updated info
            if duty_count > 0:
                duty_text = "\n".join(duty_list[:10])
                if len(duty_list) > 10:
                    duty_text += f"\n• ...und {duty_count - 10} weitere"
            else:
                duty_text = "Niemand"
            
            description = (
                "**Willkommen zum Whitelist-Duty System!**\n\n"
                "Klicke auf die Buttons unten um dich für den Whitelist-Dienst an- oder abzumelden.\n\n"
                "🔵 **Duty Starten** - Du wirst bei neuen Anfragen gepingt\n"
                "🔴 **Duty Beenden** - Du erhältst keine Pings mehr"
            )
            
            if has_grant_role:
                description += "\n\n✅ **Whitelist freischalten** - Spieler zur Whitelist hinzufügen"
            
            new_embed = discord.Embed(
                title="📋 Whitelist Duty Panel",
                description=description,
                color=discord.Color.blue()
            )
            new_embed.add_field(
                name="🔵 Aktuell im Dienst",
                value=duty_text,
                inline=False
            )
            new_embed.set_footer(text=f"Aktive Handler: {duty_count} • Die 🔵 On Duty Rolle wird automatisch zugewiesen/entfernt")
            
            # Re-create view with grant role button if configured
            new_view = WhitelistButtonView(self, guild)
            # Füge persistenten Whitelist-Grant Button hinzu wenn Rolle konfiguriert ist
            if has_grant_role:
                grant_view = PersistentWhitelistGrantView(self, guild)
                # Füge den Grant Button zur bestehenden View hinzu
                for item in grant_view.children:
                    new_view.add_item(item)
            
            await panel_message.edit(embed=new_embed, view=new_view)
        except Exception as e:
            pass  # Ignore errors if panel message was deleted

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
        """Holt den Panel-Channel für das Duty-Interface (Feedback/Call Panels)"""
        panel_channel_id = await self.config.guild(guild).panel_channel()
        
        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf Log-Channel
        return await self.get_log_channel(guild)

    async def get_team_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für das Team-Übersichts-Panel"""
        team_channel_id = await self.config.guild(guild).team_channel()
        
        if team_channel_id:
            channel = guild.get_channel(team_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf panel_channel
        return await self.get_panel_channel(guild)

    async def get_feedback_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Feedback-Channel für Feedback-Logs"""
        feedback_channel_id = await self.config.guild(guild).feedback_channel()
        
        if feedback_channel_id:
            channel = guild.get_channel(feedback_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel
        
        # Fallback auf panel_channel
        return await self.get_panel_channel(guild)

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

    async def get_feedback_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel NUR für das Feedback-Panel (getrennt vom Log-Channel)"""
        panel_channel_id = await self.config.guild(guild).feedback_panel_channel()

        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf feedback_channel
        return await self.get_feedback_channel(guild)

    async def get_status_display_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für das erweiterte Duty-Status-Display"""
        status_channel_id = await self.config.guild(guild).duty_status_display_channel()

        if status_channel_id:
            channel = guild.get_channel(status_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf team_channel
        return await self.get_team_channel(guild)

    async def get_duty_schedule_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für Duty-Planung"""
        schedule_channel_id = await self.config.guild(guild).duty_schedule_channel()

        if schedule_channel_id:
            channel = guild.get_channel(schedule_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf panel_channel
        return await self.get_panel_channel(guild)

    async def get_mod_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Moderations-Log-Channel"""
        mod_log_id = await self.config.guild(guild).mod_log_channel()

        if mod_log_id:
            channel = guild.get_channel(mod_log_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf allgemeinen log_channel
        return await self.get_log_channel(guild)

    async def get_ticket_category(self, guild: discord.Guild) -> Optional[discord.CategoryChannel]:
        """Holt die Ticket-Kategorie"""
        category_id = await self.config.guild(guild).ticket_category()

        if category_id:
            category = guild.get_channel(category_id)
            if category and isinstance(category, discord.CategoryChannel):
                return category

        return None

    async def get_ticket_panel_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für das Ticket-Panel"""
        panel_channel_id = await self.config.guild(guild).ticket_panel_channel()

        if panel_channel_id:
            channel = guild.get_channel(panel_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf support_channel
        return await self.get_support_channel(guild)

    async def get_ticket_support_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        """Holt die Rolle die Tickets bearbeiten kann"""
        role_id = await self.config.guild(guild).ticket_support_role()

        if role_id:
            role = guild.get_role(role_id)
            if role:
                return role

        # Fallback auf Support-Basisrolle
        role_id = await self.config.guild(guild).role()
        if role_id:
            return guild.get_role(role_id)

        return None

    async def get_stats_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Holt den Channel für Support-Statistiken"""
        stats_channel_id = await self.config.guild(guild).support_stats_channel()

        if stats_channel_id:
            channel = guild.get_channel(stats_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        # Fallback auf team_channel
        return await self.get_team_channel(guild)

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

                # Buttons für Duty-Mitglieder hinzufügen (User im Warteraum zu sich holen)
                view = discord.ui.View(timeout=None)  # Kein Timeout damit Buttons dauerhaft funktionieren
                if duty_members:
                    # Button nur anzeigen wenn Duty-Mitglieder verfügbar sind
                    # Button ruft den User zum Teamler (nicht umgekehrt!)
                    button = discord.ui.Button(label="User zu mir holen", style=discord.ButtonStyle.green, emoji="🎧", custom_id=f"fetch_user_{member.id}")
                    callback = self.create_fetch_user_callback(member, after.channel)
                    button.callback = callback
                    view.add_item(button)

                # Sende das Embed mit Role-Ping IM SUPPORT-CHANNEL (nicht Log!)
                # WICHTIG: allowed_mentions erzwingt den Ping der Rolle
                if duty_role and duty_members:
                    # Nur die Duty-Rolle pingen statt alle Mitglieder einzeln
                    await support_channel.send(content=duty_role.mention, embed=embed, view=view, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    # Fallback zur Basis-Rolle wenn niemand Duty hat
                    await support_channel.send(content=base_role.mention, embed=embed, view=view, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await support_channel.send(embed=embed, view=view)
            else:
                # Einfache Textnachricht (Fallback)
                if duty_role and duty_members:
                    # Nur die Duty-Rolle pingen
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

                # Buttons für Duty-Mitglieder hinzufügen (User im Warteraum zu sich holen)
                view = discord.ui.View(timeout=None)  # Kein Timeout damit Buttons dauerhaft funktionieren
                if duty_members:
                    # Button nur anzeigen wenn Duty-Mitglieder verfügbar sind
                    # Button ruft den User zum Teamler (nicht umgekehrt!)
                    button = discord.ui.Button(label="User zu mir holen", style=discord.ButtonStyle.green, emoji="📋", custom_id=f"fetch_whitelist_user_{member.id}")
                    callback = self.create_fetch_user_callback(member, after.channel, whitelist=True)
                    button.callback = callback
                    view.add_item(button)

                # "Whitelist freischalten" Button hinzufügen wenn Rolle konfiguriert ist
                grant_role_id = await self.config.guild(guild).whitelist_grant_role()
                if grant_role_id:
                    # Die User-ID des Antragstellers im custom_id speichern
                    grant_button = GrantWhitelistButton(self, guild, member.id)
                    view.add_item(grant_button)

                # Sende das Embed mit Role-Ping IM WHITELIST-CHANNEL
                if duty_role and duty_members:
                    # Nur die Whitelist-Duty-Rolle pingen statt alle Mitglieder einzeln
                    await whitelist_channel.send(content=duty_role.mention, embed=embed, view=view, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    await whitelist_channel.send(content=base_role.mention, embed=embed, view=view, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await whitelist_channel.send(embed=embed, view=view)
            else:
                # Einfache Textnachricht (Fallback)
                if duty_role and duty_members:
                    # Nur die Whitelist-Duty-Rolle pingen
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

    # HELPER METHODS - MÜSSEN VOR DEN COMMANDS DEFINIERT WERDEN!
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

    def create_fetch_user_callback(self, user_to_fetch: discord.Member, target_channel: discord.VoiceChannel, whitelist: bool = False):
        """Erstellt eine Callback-Funktion für den 'User zu mir holen' Button
        
        Args:
            user_to_fetch: Der User der geholt werden soll
            target_channel: Der Voice-Channel in dem sich der User befindet
            whitelist: True wenn es sich um einen Whitelist-Fall handelt, False für Support
        """
        async def callback(interaction: discord.Interaction):
            """Callback wenn ein Teamler auf den Button klickt"""
            teamler = interaction.user
            
            # Prüfen ob der Teamler berechtigt ist (Duty-Mitglied oder entsprechende Rolle)
            guild = interaction.guild
            
            is_authorized = False
            if whitelist:
                # WHITELIST SYSTEM
                role_id = await self.config.guild(guild).whitelist_role()
                base_role = guild.get_role(role_id) if role_id else None
                
                # Prüfen ob die "immer erlaubt" Rolle vorhanden ist und der User diese hat
                always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
                always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
                
                if always_allowed_role and always_allowed_role in teamler.roles:
                    # Diese Rolle darf immer, auch ohne Duty
                    is_authorized = True
                elif base_role and base_role in teamler.roles:
                    # Prüfen ob im Whitelist-Duty
                    duty_role = await self.get_or_create_duty_role(guild, whitelist=True)
                    if duty_role and duty_role in teamler.roles:
                        is_on_duty = await self.config.member(teamler).whitelist_on_duty()
                        if is_on_duty:
                            is_authorized = True
            else:
                # SUPPORT SYSTEM
                role_id = await self.config.guild(guild).role()
                base_role = guild.get_role(role_id) if role_id else None
                
                # Prüfen ob die "immer erlaubt" Rolle vorhanden ist und der User diese hat
                always_allowed_role_id = await self.config.guild(guild).support_always_allowed_role()
                always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
                
                if always_allowed_role and always_allowed_role in teamler.roles:
                    # Diese Rolle darf immer, auch ohne Duty
                    is_authorized = True
                elif base_role and base_role in teamler.roles:
                    # Prüfen ob im Support-Duty
                    duty_role = await self.get_or_create_duty_role(guild, whitelist=False)
                    if duty_role and duty_role in teamler.roles:
                        is_on_duty = await self.config.member(teamler).on_duty()
                        if is_on_duty:
                            is_authorized = True
            
            if not is_authorized:
                system_name = "Whitelist" if whitelist else "Support"
                await interaction.response.send_message(f"❌ Nur {system_name}-Duty-Mitglieder (oder Inhaber der entsprechenden 'immer erlaubt' Rolle) können User holen!", ephemeral=True)
                return
            
            # Prüfen ob der User noch im Warteraum ist
            if user_to_fetch.voice is None or user_to_fetch.voice.channel != target_channel:
                await interaction.response.send_message(f"ℹ️ {user_to_fetch.display_name} ist nicht mehr im Warteraum.", ephemeral=True)
                return
            
            # Hole den Voice-Channel des Teamlers
            if teamler.voice is None or teamler.voice.channel is None:
                await interaction.response.send_message("❌ Du bist nicht in einem Voice-Channel!", ephemeral=True)
                return
            
            target_vc = teamler.voice.channel
            
            # Versuche den User zum Teamler zu bewegen
            try:
                await user_to_fetch.move_to(target_vc)
                system_name = "Whitelist" if whitelist else "Support"
                await interaction.response.send_message(f"✅ {user_to_fetch.display_name} wurde zu dir in {target_vc.mention} geholt!", ephemeral=True)
                
                # Logge die Aktion
                log_channel = await self.get_log_channel(guild) if not whitelist else await self.get_whitelist_log_channel(guild)
                if log_channel:
                    embed = discord.Embed(
                        title=f"🎧 User geholt ({system_name})",
                        description=f"{teamler.mention} hat {user_to_fetch.mention} aus dem Warteraum geholt.",
                        color=discord.Color.green(),
                        timestamp=datetime.utcnow()
                    )
                    embed.add_field(name="Von", value=target_channel.mention, inline=True)
                    embed.add_field(name="Zu", value=target_vc.mention, inline=True)
                    await log_channel.send(embed=embed)
            except discord.Forbidden:
                await interaction.response.send_message("❌ Ich habe keine Berechtigung um diesen User zu bewegen!", ephemeral=True)
            except Exception as e:
                await interaction.response.send_message(f"❌ Fehler: {str(e)}", ephemeral=True)
        
        return callback

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

    @supportset.command(name="alwaysallowedrole")
    async def supportset_alwaysallowedrole(self, ctx: commands.Context, role: str = None):
        """
        Setze die Rolle die IMMER User holen darf (ohne Duty-Pflicht).
        Ohne Rollen-Angabe wird die Einstellung zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).support_always_allowed_role.set(None)
            await ctx.send("✅ 'Immer erlaubt' Rolle zurückgesetzt. Nur noch Duty-Mitglieder können User holen.")
        else:
            role_id = self._parse_role_id(role)
            if role_id is None:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
            
            r = ctx.guild.get_role(role_id)
            if not r:
                await ctx.send("❌ Rolle nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).support_always_allowed_role.set(role_id)
            await ctx.send(f"✅ {r.mention} kann jetzt IMMER User holen (auch ohne Duty).")

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

    @supportset.command(name="teamchannel")
    async def supportset_teamchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für das Team-Übersichts-Panel.
        Ohne Channel-Angabe wird der Panel-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).team_channel.set(None)
            await ctx.send("✅ Team-Panel wird im Panel-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).team_channel.set(channel_id)
            await ctx.send(f"✅ Team-Panel wird jetzt in {ch.mention} angezeigt.")

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
        team_channel_id = guild_data.get("team_channel")
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
        team_channel_mention = f"<#{team_channel_id}>" if team_channel_id else "Gleicher wie Panel-Channel"
        feedback_channel_mention = f"<#{feedback_channel_id}>" if feedback_channel_id else "Gleicher wie Panel-Channel"
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
        embed.add_field(name="📋 Panel-Channel (Feedback/Call)", value=panel_channel_mention, inline=True)
        embed.add_field(name="📜 Log-Channel", value=log_channel_mention, inline=True)
        embed.add_field(name="👥 Team-Channel", value=team_channel_mention, inline=True)
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

    @supportset.command(name="dutygrantrole")
    async def supportset_dutygrantrole(self, ctx: commands.Context, role: str = None):
        """
        Setze die Rolle die berechtigt ist anderen Nutzern die Support-Duty-Rolle zu geben.
        
        Dies ermöglicht es bestimmten Rollen (z.B. "Support Admin"), anderen Nutzern
        die Berechtigung zu geben, Support-Duty zu machen, ohne dass sie selbst
        die Support-Basisrolle benötigen.
        
        Ohne Rollen-Angabe wird die Einstellung zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).support_duty_grant_role.set(None)
            await ctx.send("✅ Support-Duty-Grant-Rolle zurückgesetzt.")
        else:
            role_id = self._parse_role_id(role)
            if role_id is None:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
            
            r = ctx.guild.get_role(role_id)
            if not r:
                await ctx.send("❌ Rolle nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).support_duty_grant_role.set(role_id)
            await ctx.send(f"✅ {r.mention} kann jetzt anderen Nutzern die Support-Duty-Berechtigung geben.")
    
    @commands.command(name="supportdutygrant", aliases=["grantduty", "grantsupportduty"])
    @commands.guild_only()
    async def supportdutygrant(self, ctx: commands.Context, target_user: discord.Member):
        """
        Gib einem anderen Nutzer die Support-Duty-Berechtigung.
        
        Dies setzt den Nutzer direkt auf Support-Duty, auch wenn er keine
        Support-Basisrolle hat. Erfordert die konfigurierte Grant-Rolle.
        """
        guild = ctx.guild
        author = ctx.author
        
        # Prüfe ob Autor die Grant-Rolle hat
        grant_role_id = await self.config.guild(guild).support_duty_grant_role()
        
        if not grant_role_id:
            await ctx.send("❌ Es wurde keine Support-Duty-Grant-Rolle konfiguriert! Nutze `[p]supportset dutygrantrole <Rolle>` um sie festzulegen.")
            return
        
        has_grant_role = False
        grant_role = guild.get_role(grant_role_id)
        if grant_role and grant_role in author.roles:
            has_grant_role = True
        
        if not has_grant_role:
            missing_role_msg = f"❌ Du benötigst die {grant_role.mention if grant_role else 'konfigurierte'} Rolle um anderen Nutzern Support-Duty zu geben!"
            await ctx.send(missing_role_msg)
            return
        
        # Prüfe ob Target bereits auf Duty ist
        is_on_duty = await self.config.member(target_user).on_duty()
        if is_on_duty:
            await ctx.send(f"ℹ️ {target_user.mention} ist bereits im Support-Duty!")
            return
        
        # Setze Target auf Duty
        await self.config.member(target_user).on_duty.set(True)
        start_time = datetime.utcnow()
        await self.config.member(target_user).duty_start.set(start_time.timestamp())
        
        # Duty-Rolle hinzufügen
        await self.update_duty_role(target_user, True, whitelist=False)
        
        # Log-Nachricht senden
        log_channel = await self.get_log_channel(guild)
        if log_channel:
            embed = discord.Embed(
                title="🟢 Support Duty zugewiesen",
                description=f"{target_user.mention} wurde von {author.mention} auf Duty gesetzt!",
                color=discord.Color.green(),
                timestamp=start_time
            )
            embed.set_thumbnail(url=target_user.display_avatar.url)
            embed.add_field(name="👤 Zugewiesen von", value=f"{author.display_name}", inline=True)
            embed.add_field(name="⏰ Automatische Abmeldung", value="Nach konfigurierter Zeit", inline=True)
            try:
                await log_channel.send(embed=embed)
            except discord.Forbidden:
                pass
        
        await ctx.send(f"✅ {target_user.mention} ist jetzt im Support-Duty!")

    @supportset.command(name="createpanel")
    async def supportset_createpanel(self, ctx: commands.Context):
        """
        Erstellt ein permanentes Duty-Panel mit Buttons.
        Diese Nachricht sollte im Panel-Channel gepinnt werden.
        """
        await ctx.send("🔄 Erstelle Duty-Panel mit Buttons...")
        await self.create_panel_message(ctx)
        await ctx.send("✅ Duty-Panel wurde erstellt! Du kannst die Buttons jetzt verwenden um dich an-/abzumelden.")

    @supportset.command(name="createstatusdisplay")
    async def supportset_createstatusdisplay(self, ctx: commands.Context):
        """
        Erstellt das erweiterte Duty-Status-Display mit detaillierter Übersicht.
        Diese Nachricht zeigt alle Teammitglieder nach Status sortiert an.
        """
        guild = ctx.guild
        channel = await self.get_status_display_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        # Erstelle initiales Embed
        embed = discord.Embed(
            title="📊 Live Duty Status Übersicht",
            description="**Aktueller Status aller Teammitglieder im Dienst**\n\n_Lade Status..._",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="🟢 Verfügbar", value="Keine", inline=True)
        embed.add_field(name="🔵 Beschäftigt", value="Keine", inline=True)
        embed.add_field(name="☕ In Pause", value="Keine", inline=True)
        embed.add_field(name="🟡 Abwesend", value="Keine", inline=False)
        embed.add_field(name="📈 Statistik", value="**0** im Duty", inline=False)
        
        message = await channel.send(embed=embed)
        await self.config.guild(guild).duty_status_display_message_id.set(message.id)
        
        await ctx.send(f"✅ Erweitertes Status-Display wurde in {channel.mention} erstellt!")

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

    @supportset.command(name="feedbackpanelchannel")
    async def supportset_feedbackpanelchannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel NUR für das Feedback-Panel (GETRENNT vom Feedback-Log!).
        Ohne Channel-Angabe wird der Panel-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).feedback_panel_channel.set(None)
            await ctx.send("✅ Feedback-Panel wird im Panel-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).feedback_panel_channel.set(channel_id)
            await ctx.send(f"✅ Feedback-Panel wird jetzt in {ch.mention} angezeigt (Feedback-Logs bleiben im separaten Channel)!")

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

    @supportset.command(name="statusdisplaychannel")
    async def supportset_statusdisplaychannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für das erweiterte Duty-Status-Display.
        Ohne Channel-Angabe wird der Team-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).duty_status_display_channel.set(None)
            await ctx.send("✅ Status-Display wird im Team-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).duty_status_display_channel.set(channel_id)
            await ctx.send(f"✅ Erweitertes Status-Display wird jetzt in {ch.mention} angezeigt.")

    @supportset.command(name="statusmessage")
    async def supportset_statusmessage(self, ctx: commands.Context, enabled: bool = None):
        """
        Aktiviere oder deaktiviere benutzerdefinierte Status-Nachrichten.
        Ohne Angabe wird der aktuelle Status angezeigt.
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).allow_status_messages()
            await ctx.send(f"{'✅' if current else '❌'} Benutzerdefinierte Status-Nachrichten sind aktuell {'aktiviert' if current else 'deaktiviert'}.")
        else:
            await self.config.guild(ctx.guild).allow_status_messages.set(enabled)
            await ctx.send(f"✅ Benutzerdefinierte Status-Nachrichten wurden {'aktiviert' if enabled else 'deaktiviert'}.")

    @supportset.command(name="maxdutyhours")
    async def supportset_maxdutyhours(self, ctx: commands.Context, hours: int = None):
        """
        Setze die maximale Duty-Stunden pro Tag.
        Ohne Angabe wird der aktuelle Wert angezeigt.
        """
        if hours is None:
            current = await self.config.guild(ctx.guild).max_duty_hours_per_day()
            await ctx.send(f"⏰ Maximale Duty-Stunden pro Tag: **{current}**")
        elif hours < 1 or hours > 24:
            await ctx.send("❌ Bitte gib eine gültige Stundenzahl zwischen 1 und 24 an!")
        else:
            await self.config.guild(ctx.guild).max_duty_hours_per_day.set(hours)
            await ctx.send(f"✅ Maximale Duty-Stunden pro Tag auf **{hours}** gesetzt.")

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

    @supportset.command(name="createteampanel")
    async def supportset_createteampanel(self, ctx: commands.Context):
        """
        Erstellt ein permanentes Team-Übersichts-Panel.
        Zeigt alle Teammitglieder und deren Status an.
        """
        guild = ctx.guild
        channel = await self.get_team_channel(guild)
        
        if not channel:
            channel = ctx.channel
        
        # Erstelle das Team-Panel
        await self.update_team_panel(channel, guild)
        await ctx.send("✅ Team-Panel wurde erstellt/aktualisiert!")

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

    @whitelistset.command(name="alwaysallowedrole")
    async def whitelistset_alwaysallowedrole(self, ctx: commands.Context, role: str = None):
        """
        Setze die Rolle die IMMER User holen darf (ohne Duty-Pflicht).
        Ohne Rollen-Angabe wird die Einstellung zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_always_allowed_role.set(None)
            await ctx.send("✅ 'Immer erlaubt' Rolle zurückgesetzt. Nur noch Duty-Mitglieder können User holen.")
        else:
            role_id = self._parse_role_id(role)
            if role_id is None:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
            
            r = ctx.guild.get_role(role_id)
            if not r:
                await ctx.send("❌ Rolle nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_always_allowed_role.set(role_id)
            await ctx.send(f"✅ {r.mention} kann jetzt IMMER User holen (auch ohne Duty).")

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
        Setze den Channel für Whitelist-Duty-Logs (An-/Abmeldungen).
        Ohne Channel-Angabe wird der normale Whitelist-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_log_channel.set(None)
            await ctx.send("✅ Whitelist-Duty-Logs werden im Whitelist-Channel angezeigt.")
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
            await ctx.send(f"✅ Whitelist-Duty-Logs werden jetzt in {ch.mention} angezeigt.")

    @whitelistset.command(name="entrieschannel")
    async def whitelistset_entrieschannel(self, ctx: commands.Context, channel: str = None):
        """
        Setze den Channel für Whitelist-Einträge (Hinzufügungen/Entfernungen von Spielern).
        Ohne Channel-Angabe wird der Duty-Log-Channel verwendet.
        Verwende 'reset' um zurückzusetzen.
        Unterstützt ID oder Mention.
        """
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_entries_channel.set(None)
            await ctx.send("✅ Whitelist-Einträge werden im Duty-Log-Channel angezeigt.")
        else:
            channel_id = self._parse_channel_id(channel)
            if channel_id is None:
                await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein!")
                return
            
            ch = ctx.guild.get_channel(channel_id)
            if not ch or not isinstance(ch, discord.TextChannel):
                await ctx.send("❌ Channel nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_entries_channel.set(channel_id)
            await ctx.send(f"✅ Whitelist-Einträge werden jetzt in {ch.mention} angezeigt.")

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
        entries_channel_id = guild_data.get("whitelist_entries_channel")
        auto_duty = guild_data.get("whitelist_auto_remove_duty", True)
        duty_timeout = guild_data.get("whitelist_duty_timeout", 4)

        channel_mention = f"<#{channel_id}>" if channel_id else "❌ Nicht gesetzt"
        room_mention = f"<#{room_id}>" if room_id else "❌ Nicht gesetzt"
        role_mention = f"<@&{role_id}>" if role_id else "❌ Nicht gesetzt"
        approved_role_mention = f"<@&{approved_role_id}>" if approved_role_id else "❌ Nicht gesetzt"
        duty_role_mention = f"<@&{duty_role_id}>" if duty_role_id else "❌ Noch nicht erstellt"
        panel_channel_mention = f"<#{panel_channel_id}>" if panel_channel_id else "Gleicher wie Whitelist-Channel"
        log_channel_mention = f"<#{log_channel_id}>" if log_channel_id else "Gleicher wie Whitelist-Channel"
        entries_channel_mention = f"<#{entries_channel_id}>" if entries_channel_id else "Gleicher wie Duty-Log-Channel"
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
        embed.add_field(name="📜 Duty-Log-Channel", value=log_channel_mention, inline=True)
        embed.add_field(name="📄 Einträge-Channel", value=entries_channel_mention, inline=True)

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

    @whitelistset.command(name="grantrole")
    async def whitelistset_grantrole(self, ctx: commands.Context, role: str = None):
        """
        Setze die Rolle die bei Klick auf "Whitelist freischalten" vergeben wird.
        Ohne Rollen-Angabe wird die Einstellung zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_grant_role.set(None)
            await ctx.send("✅ 'Whitelist freischalten' Rolle zurückgesetzt. Der Button wird nicht mehr angezeigt.")
        else:
            role_id = self._parse_role_id(role)
            if role_id is None:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
            
            r = ctx.guild.get_role(role_id)
            if not r:
                await ctx.send("❌ Rolle nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_grant_role.set(role_id)
            await ctx.send(f"✅ {r.mention} wird jetzt bei Klick auf 'Whitelist freischalten' vergeben.\n\n📝 **Wichtig:** Der Button erscheint jetzt im Whitelist-Duty-Panel. Falls das Panel noch nicht existiert, erstelle es mit `{ctx.prefix}whitelistset createpanel`")
            
            # Update the panel to show the new button immediately
            await self.update_whitelist_panel_display(ctx.guild)

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

    @whitelistset.command(name="dutygrantrole")
    async def whitelistset_dutygrantrole(self, ctx: commands.Context, role: str = None):
        """
        Setze die Rolle die berechtigt ist anderen Nutzern die Whitelist-Duty-Rolle zu geben.
        
        Dies ermöglicht es bestimmten Rollen (z.B. "Whitelist Admin"), anderen Nutzern
        die Berechtigung zu geben, Whitelist-Duty zu machen, ohne dass sie selbst
        die Whitelist-Handler-Rolle benötigen.
        
        Ohne Rollen-Angabe wird die Einstellung zurückgesetzt.
        Unterstützt ID oder Mention.
        """
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).whitelist_duty_grant_role.set(None)
            await ctx.send("✅ Whitelist-Duty-Grant-Rolle zurückgesetzt.")
        else:
            role_id = self._parse_role_id(role)
            if role_id is None:
                await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein!")
                return
            
            r = ctx.guild.get_role(role_id)
            if not r:
                await ctx.send("❌ Rolle nicht gefunden!")
                return
                
            await self.config.guild(ctx.guild).whitelist_duty_grant_role.set(role_id)
            await ctx.send(f"✅ {r.mention} kann jetzt anderen Nutzern die Whitelist-Duty-Berechtigung geben.")
    
    @commands.command(name="whitelistdutygrant", aliases=["wlgrantduty", "grantwhitelistduty"])
    @commands.guild_only()
    async def whitelistdutygrant(self, ctx: commands.Context, target_user: discord.Member):
        """
        Gib einem anderen Nutzer die Whitelist-Duty-Berechtigung.
        
        Dies setzt den Nutzer direkt auf Whitelist-Duty, auch wenn er keine
        Whitelist-Handler-Rolle hat. Erfordert die konfigurierte Grant-Rolle.
        """
        guild = ctx.guild
        author = ctx.author
        
        # Prüfe ob Autor die Grant-Rolle hat
        grant_role_id = await self.config.guild(guild).whitelist_duty_grant_role()
        
        if not grant_role_id:
            await ctx.send("❌ Es wurde keine Whitelist-Duty-Grant-Rolle konfiguriert! Nutze `[p]whitelistset dutygrantrole <Rolle>` um sie festzulegen.")
            return
        
        has_grant_role = False
        grant_role = guild.get_role(grant_role_id)
        if grant_role and grant_role in author.roles:
            has_grant_role = True
        
        if not has_grant_role:
            missing_role_msg = f"❌ Du benötigst die {grant_role.mention if grant_role else 'konfigurierte'} Rolle um anderen Nutzern Whitelist-Duty zu geben!"
            await ctx.send(missing_role_msg)
            return
        
        # Prüfe ob Target bereits auf Duty ist
        is_on_duty = await self.config.member(target_user).whitelist_on_duty()
        if is_on_duty:
            await ctx.send(f"ℹ️ {target_user.mention} ist bereits im Whitelist-Duty!")
            return
        
        # Setze Target auf Duty
        await self.config.member(target_user).whitelist_on_duty.set(True)
        start_time = datetime.utcnow()
        await self.config.member(target_user).whitelist_duty_start.set(start_time.timestamp())
        
        # Duty-Rolle hinzufügen
        await self.update_duty_role(target_user, True, whitelist=True)
        
        # Log-Nachricht senden
        log_channel = await self.get_whitelist_log_channel(guild)
        if log_channel:
            embed = discord.Embed(
                title="🔵 Whitelist Duty zugewiesen",
                description=f"{target_user.mention} wurde von {author.mention} auf Duty gesetzt!",
                color=discord.Color.blue(),
                timestamp=start_time
            )
            embed.set_thumbnail(url=target_user.display_avatar.url)
            embed.add_field(name="👤 Zugewiesen von", value=f"{author.display_name}", inline=True)
            embed.add_field(name="⏰ Automatische Abmeldung", value="Nach konfigurierter Zeit", inline=True)
            try:
                await log_channel.send(embed=embed)
            except discord.Forbidden:
                pass
        
        await ctx.send(f"✅ {target_user.mention} ist jetzt im Whitelist-Duty!")

    @commands.command(name="whitelistinfo", aliases=["wlinfo"])
    async def whitelistinfo(self, ctx: commands.Context):
        """
        Zeigt Informationen zur aktuellen Whitelist-Konfiguration und Status an.
        
        Dieser Befehl zeigt:
        - Konfigurierte Channels (Whitelist-Channel, Voice-Warteraum, Panel-Channel, Log-Channel, Entries-Channel)
        - Konfigurierte Rollen (Handler-Rolle, Approved-Rolle, Duty-Rolle)
        - Aktive Duty-Handler
        - Auto-Duty-Einstellungen
        """
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()

        channel_id = guild_data.get("whitelist_channel")
        room_id = guild_data.get("whitelist_room")
        role_id = guild_data.get("whitelist_role")
        approved_role_id = guild_data.get("whitelist_approved_role")
        duty_role_id = guild_data.get("whitelist_duty_role")
        panel_channel_id = guild_data.get("whitelist_panel_channel")
        log_channel_id = guild_data.get("whitelist_log_channel")
        entries_channel_id = guild_data.get("whitelist_entries_channel")
        auto_duty = guild_data.get("whitelist_auto_remove_duty", True)
        duty_timeout = guild_data.get("whitelist_duty_timeout", 4)

        channel_mention = f"<#{channel_id}>" if channel_id else "❌ Nicht gesetzt"
        room_mention = f"<#{room_id}>" if room_id else "❌ Nicht gesetzt"
        role_mention = f"<@&{role_id}>" if role_id else "❌ Nicht gesetzt"
        approved_role_mention = f"<@&{approved_role_id}>" if approved_role_id else "❌ Nicht gesetzt"
        duty_role_mention = f"<@&{duty_role_id}>" if duty_role_id else "❌ Noch nicht erstellt"
        panel_channel_mention = f"<#{panel_channel_id}>" if panel_channel_id else "Gleicher wie Whitelist-Channel"
        log_channel_mention = f"<#{log_channel_id}>" if log_channel_id else "Gleicher wie Whitelist-Channel"
        entries_channel_mention = f"<#{entries_channel_id}>" if entries_channel_id else "Gleicher wie Duty-Log-Channel"
        auto_duty_status = f"✅ Aktiv ({duty_timeout}h)" if auto_duty else "❌ Deaktiviert"
        
        # Grant-Rolle anzeigen
        grant_role_id = guild_data.get("whitelist_duty_grant_role")
        grant_role_mention = f"<@&{grant_role_id}>" if grant_role_id else "❌ Nicht gesetzt"

        # Zähle aktive Duty-User
        duty_count = 0
        duty_members_list = []
        duty_role = guild.get_role(duty_role_id) if duty_role_id else None
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and duty_role:
                for m in base_role.members:
                    if duty_role in m.roles:
                        is_on_duty = await self.config.member(m).whitelist_on_duty()
                        if is_on_duty:
                            duty_count += 1
                            duty_members_list.append(m.display_name)

        active_duty_display = "\n".join([f"• {name}" for name in duty_members_list[:10]]) if duty_members_list else "Niemand im Duty"
        if len(duty_members_list) > 10:
            active_duty_display += f"\n...und {len(duty_members_list) - 10} weitere"

        embed = discord.Embed(
            title="📋 Whitelist Information",
            description="Aktuelle Konfiguration und Status des Whitelist-Systems",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="🔵 Aktive Duty-Handler", value=f"{duty_count} Handler\n{active_duty_display}", inline=False)
        embed.add_field(name="⏰ Auto-Duty-Ende", value=auto_duty_status, inline=True)
        embed.add_field(name="🎖️ Duty-Grant-Rolle", value=grant_role_mention, inline=True)
        embed.add_field(name="📝 Whitelist-Channel", value=channel_mention, inline=True)
        embed.add_field(name="🎤 Voice-Warteraum", value=room_mention, inline=True)
        embed.add_field(name="👥 Handler-Rolle", value=role_mention, inline=True)
        embed.add_field(name="✅ Approved-Rolle", value=approved_role_mention, inline=True)
        embed.add_field(name="🔵 Duty-Rolle", value=duty_role_mention, inline=True)
        embed.add_field(name="📋 Panel-Channel", value=panel_channel_mention, inline=True)
        embed.add_field(name="📜 Duty-Log-Channel", value=log_channel_mention, inline=True)
        embed.add_field(name="📄 Einträge-Channel", value=entries_channel_mention, inline=True)
        embed.set_footer(text=f"Whitelist-Info • {guild.name}")

        await ctx.send(embed=embed)
    
    @commands.command(name="whitelistuser", aliases=["wluser", "addwhitelist", "wl"])
    async def whitelistuser(self, ctx: commands.Context, user: discord.Member):
        """
        Fügt einen Spieler zur Whitelist hinzu.
        
        Verwendung: [p]whitelistuser @User oder [p]whitelistuser USER_ID
        
        - Erfordert Whitelist-Handler-Rolle ODER im Whitelist-Duty
        - Loggt die Aktion automatisch im Whitelist-Log-Channel
        - Trennt Duty-Logs von Eintrag-Logs
        """
        guild = ctx.guild
        member = ctx.author
        
        # Prüfe Berechtigung: Whitelist-Handler-Rolle ODER im Duty
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True
        
        is_on_duty = await self.config.member(member).whitelist_on_duty()
        
        if not has_base_role and not is_on_duty:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle oder musst im Whitelist-Duty sein!")
            return
        
        approved_role = await self.get_whitelist_approved_role(guild)
        if not approved_role:
            await ctx.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.")
            return
        
        # Prüfe ob der User bereits die Rolle hat
        if approved_role in user.roles:
            await ctx.send(f"ℹ️ {user.mention} hat bereits die Whitelist-Rolle!")
            return
        
        # Füge die Approved-Rolle hinzu
        try:
            await user.add_roles(approved_role, reason=f"Whitelist genehmigt von {member.display_name}")
            
            embed_success = discord.Embed(
                title="✅ Whitelist genehmigt",
                description=f"{user.mention} wurde erfolgreich zur Whitelist hinzugefügt!",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention} ({member.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{user.display_name}", inline=True)
            
            await ctx.send(embed=embed_success)
            
            # Logge die Aktion im Whitelist-Einträge-Channel (getrennt von Duty-Logs!)
            entries_channel = await self.get_whitelist_entries_channel(guild)
            if entries_channel:
                log_embed = discord.Embed(
                    title="📋 Whitelist Eintrag erstellt",
                    description=f"**{user.mention}** wurde zur Whitelist hinzugefügt.",
                    color=discord.Color.gold(),
                    timestamp=datetime.utcnow()
                )
                log_embed.set_thumbnail(url=user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{user.mention}\n*{user.display_name}* (ID: `{user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                log_embed.set_footer(text="Whitelist-Eintrag • Genehmigt von " + member.display_name)
                
                await entries_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=f"Du wurdest von **{member.display_name}** zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{approved_role.name}**", inline=False)
                dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen.", inline=False)
                dm_embed.set_footer(text=f"{guild.name} Whitelist System")
                await user.send(embed=dm_embed)
            except discord.Forbidden:
                pass
            
        except discord.Forbidden:
            await ctx.send("❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!")
        except Exception as e:
            await ctx.send(f"❌ Ein Fehler ist beim Hinzufügen der Rolle aufgetreten: `{str(e)}`")
    
    @commands.command(name="removewhitelist", aliases=["unwhitelist", "wlremove", "delwhitelist"])
    async def removewhitelist(self, ctx: commands.Context, user: discord.Member):
        """
        Entfernt die Whitelist-Rolle von einem Spieler.
        
        Verwendung: [p]removewhitelist @User oder [p]removewhitelist USER_ID
        
        - Erfordert Whitelist-Handler-Rolle ODER im Whitelist-Duty
        - Loggt die Aktion automatisch im Whitelist-Log-Channel
        """
        guild = ctx.guild
        member = ctx.author
        
        # Prüfe Berechtigung: Whitelist-Handler-Rolle ODER im Duty
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True
        
        is_on_duty = await self.config.member(member).whitelist_on_duty()
        
        if not has_base_role and not is_on_duty:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle oder musst im Whitelist-Duty sein!")
            return
        
        approved_role = await self.get_whitelist_approved_role(guild)
        if not approved_role:
            await ctx.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.")
            return
        
        # Prüfe ob der User die Rolle hat
        if approved_role not in user.roles:
            await ctx.send(f"ℹ️ {user.mention} hat die Whitelist-Rolle nicht!")
            return
        
        # Entferne die Approved-Rolle
        try:
            await user.remove_roles(approved_role, reason=f"Whitelist entfernt von {member.display_name}")
            
            embed_success = discord.Embed(
                title="❌ Whitelist entfernt",
                description=f"{user.mention} wurde die Whitelist-Rolle entzogen!",
                color=discord.Color.red(),
                timestamp=datetime.utcnow()
            )
            embed_success.add_field(name="👤 Entfernt von", value=f"{member.mention} ({member.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{user.display_name}", inline=True)
            
            await ctx.send(embed=embed_success)
            
            # Logge die Aktion im Whitelist-Einträge-Channel (getrennt von Duty-Logs!)
            entries_channel = await self.get_whitelist_entries_channel(guild)
            if entries_channel:
                log_embed = discord.Embed(
                    title="🗑️ Whitelist Eintrag entfernt",
                    description=f"**{user.mention}** wurde die Whitelist entzogen.",
                    color=discord.Color.dark_red(),
                    timestamp=datetime.utcnow()
                )
                log_embed.set_thumbnail(url=user.display_avatar.url)
                log_embed.add_field(name="🔹 Entfernt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{user.mention}\n*{user.display_name}* (ID: `{user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                log_embed.set_footer(text="Whitelist-Entfernung • Durchgeführt von " + member.display_name)
                
                await entries_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="⚠️ Whitelist entfernt",
                    description=f"Deine Whitelist wurde von **{member.display_name}** entfernt.",
                    color=discord.Color.orange(),
                    timestamp=datetime.utcnow()
                )
                dm_embed.add_field(name="❌ Rolle entfernt", value=f"{approved_role.mention}", inline=False)
                dm_embed.set_footer(text=f"{guild.name} Whitelist System")
                await user.send(embed=dm_embed)
            except discord.Forbidden:
                pass
            
        except discord.Forbidden:
            await ctx.send("❌ Ich habe keine Berechtigung um diese Rolle zu entfernen!")
        except Exception as e:
            await ctx.send(f"❌ Ein Fehler ist beim Entfernen der Rolle aufgetreten: `{str(e)}`")
    
    @commands.command(name="checkwhitelist", aliases=["wlcheck", "whoadded", "wlcheckinfo"])
    async def checkwhitelist(self, ctx: commands.Context, user: discord.Member):
        """
        Überprüft wer einem Spieler die Whitelist gegeben hat.
        
        Verwendung: [p]checkwhitelist @User oder [p]checkwhitelist USER_ID
        
        - Zeigt den Genehmiger und Zeitpunkt der Whitelist
        - Durchsucht die Logs des Whitelist-Log-Channels
        - Erfordert Whitelist-Handler-Rolle
        """
        guild = ctx.guild
        member = ctx.author
        
        # Prüfe Berechtigung: Whitelist-Handler-Rolle
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True
        
        if not has_base_role:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle für diesen Befehl!")
            return
        
        approved_role = await self.get_whitelist_approved_role(guild)
        if not approved_role:
            await ctx.send("❌ Keine Whitelist-Approved-Rolle konfiguriert!")
            return
        
        # Prüfe ob der User die Rolle hat
        if approved_role not in user.roles:
            await ctx.send(f"ℹ️ {user.mention} hat die Whitelist-Rolle nicht!")
            return
        
        # Suche in den Logs nach dem Eintrag
        log_channel = await self.get_whitelist_log_channel(guild)
        if not log_channel:
            await ctx.send("❌ Kein Whitelist-Log-Channel konfiguriert!")
            return
        
        await ctx.send("🔍 Durchsuche Whitelist-Logs...")
        
        found_entry = None
        approver = None
        grant_time = None
        
        try:
            # Durchsuche die letzten 1000 Nachrichten im Log-Channel
            async for message in log_channel.history(limit=1000):
                if not message.embeds:
                    continue
                
                embed = message.embeds[0]
                
                # Prüfe ob es ein Whitelist-Eintrag für diesen User ist
                if embed.title and ("Eintrag erstellt" in embed.title or "hinzugefügt" in embed.description.lower()):
                    # Prüfe ob der User im Embed erwähnt wird
                    if user.mention in str(embed.description) or user.id == int(user.id):
                        # Finde den Genehmiger im Embed
                        for field in embed.fields:
                            if "genehmigt von" in field.name.lower() or "von" in field.name.lower():
                                # Extrahiere den Mention des Genehmigers
                                mention_parts = field.value.split(">")
                                if len(mention_parts) > 1:
                                    approver_mention = mention_parts[0] + ">"
                                    approver = approver_mention
                                
                                if "zeitpunkt" in field.name.lower() or "time" in field.name.lower():
                                    grant_time = field.value
                        
                        found_entry = message
                        break
            
            if found_entry and approver:
                # Versuche den Genehmiger als Member zu finden
                approver_member = None
                if "<@" in approver:
                    try:
                        approver_id = int(approver.replace("<@", "").replace(">", ""))
                        approver_member = await guild.fetch_member(approver_id)
                    except:
                        pass
                
                embed_result = discord.Embed(
                    title="📋 Whitelist Information",
                    description=f"Informationen zur Whitelist von {user.mention}",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )
                embed_result.add_field(
                    name="✅ Genehmigt von",
                    value=approver_member.mention if approver_member else approver,
                    inline=True
                )
                embed_result.add_field(
                    name="🎮 Spieler",
                    value=f"{user.mention}\n*{user.display_name}*",
                    inline=True
                )
                embed_result.add_field(
                    name="⏰ Zeitpunkt",
                    value=grant_time if grant_time else "Unbekannt",
                    inline=False
                )
                embed_result.add_field(
                    name="📝 Rolle",
                    value=approved_role.mention,
                    inline=True
                )
                embed_result.set_footer(text=f"Whitelist-Check • Durchgeführt von {member.display_name}")
                
                await ctx.send(embed=embed_result)
            else:
                await ctx.send(f"ℹ️ Kein Whitelist-Eintrag für {user.mention} in den Logs gefunden.\nDie Whitelist wurde möglicherweise vor der Log-Einrichtung oder manuell vergeben.")
        
        except Exception as e:
            await ctx.send(f"❌ Fehler beim Durchsuchen der Logs: `{str(e)}`")
    
    @commands.command(name="whitelistlog", aliases=["wllog", "whitelistlogs"])
    async def whitelistlog(self, ctx: commands.Context, limit: int = 10):
        """
        Zeigt die letzten Whitelist-Einträge an.
        
        Verwendung: [p]whitelistlog [Anzahl, Standard: 10]
        
        - Zeigt die letzten X Whitelist-Genehmigungen und Entfernungen
        - Erfordert Whitelist-Handler-Rolle
        """
        guild = ctx.guild
        member = ctx.author
        
        # Prüfe Berechtigung: Whitelist-Handler-Rolle
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True
        
        if not has_base_role:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle für diesen Befehl!")
            return
        
        log_channel = await self.get_whitelist_log_channel(guild)
        if not log_channel:
            await ctx.send("❌ Kein Whitelist-Log-Channel konfiguriert!")
            return
        
        # Begrenze auf max 50 Einträge
        limit = min(limit, 50)
        
        entries = []
        try:
            async for message in log_channel.history(limit=100):
                if not message.embeds:
                    continue
                
                embed = message.embeds[0]
                if embed.title and ("Eintrag erstellt" in embed.title or "Eintrag entfernt" in embed.title or "hinzugefügt" in embed.title.lower() or "entfernt" in embed.title.lower()):
                    entries.append({
                        "title": embed.title,
                        "description": embed.description,
                        "color": embed.color,
                        "timestamp": embed.timestamp if embed.timestamp else message.created_at,
                        "fields": embed.fields[:2] if embed.fields else []  # Nur erste 2 Felder für Übersicht
                    })
                
                if len(entries) >= limit:
                    break
            
            if not entries:
                await ctx.send("ℹ️ Keine Whitelist-Einträge in den Logs gefunden.")
                return
            
            # Erstelle eine übersichtliche Liste
            embed_list = discord.Embed(
                title="📋 Whitelist Verlauf",
                description=f"Die letzten {len(entries)} Whitelist-Aktionen",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            for i, entry in enumerate(entries, 1):
                action_type = "✅" if "erstellt" in entry["title"].lower() or "hinzugefügt" in entry["title"].lower() else "❌"
                desc_short = entry["description"][:80] + "..." if len(entry["description"]) > 80 else entry["description"]
                time_str = f"<t:{int(entry['timestamp'].timestamp())}:R>" if entry["timestamp"] else "Unbekannt"
                
                embed_list.add_field(
                    name=f"{action_type} Eintrag #{i} • {time_str}",
                    value=desc_short,
                    inline=False
                )
            
            embed_list.set_footer(text=f"Whitelist-Log • Angezeigt von {member.display_name}")
            
            await ctx.send(embed=embed_list)
        
        except Exception as e:
            await ctx.send(f"❌ Fehler beim Abrufen der Logs: `{str(e)}`")

    # ============================================
    # NEUE SUPPORT & MODERATION BEFEHLE
    # ============================================
    # HINWEIS: Der Alias "supportstatistik" wurde entfernt um Konflikte zu vermeiden.
    # Verwende stattdessen "supportstats" oder "stats".

    @commands.command(name="supportstats", aliases=["stats"])
    async def supportstats(self, ctx: commands.Context):
        """
        Zeigt Support-Statistiken für diesen Server.
        
        - Anzahl der aktiven Duty-Mitglieder
        - Gesamte Support-Anfragen (wenn getrackt)
        - Durchschnittliche Wartezeit
        """
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()
        
        # Zähle aktive Duty-User
        duty_count = 0
        duty_members_list = []
        duty_role_id = guild_data.get("duty_role")
        role_id = guild_data.get("role")
        
        if role_id and duty_role_id:
            base_role = guild.get_role(role_id)
            duty_role = guild.get_role(duty_role_id)
            if base_role and duty_role:
                for m in base_role.members:
                    if duty_role in m.roles:
                        is_on_duty = await self.config.member(m).on_duty()
                        if is_on_duty:
                            duty_count += 1
                            duty_members_list.append(m.display_name)
        
        # Hole Duty-Zeiten
        total_duty_time = 0
        members_with_duty = 0
        
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role:
                for m in base_role.members:
                    duty_start = await self.config.member(m).duty_start()
                    if duty_start:
                        start_dt = datetime.fromtimestamp(duty_start)
                        duration = (datetime.utcnow() - start_dt).total_seconds()
                        total_duty_time += duration
                        members_with_duty += 1
        
        avg_duty_hours = (total_duty_time / 3600) / max(members_with_duty, 1)
        
        embed = discord.Embed(
            title="📊 Support Statistiken",
            description="Aktuelle Übersicht des Support-Systems",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        active_duty_display = "\n".join([f"• {name}" for name in duty_members_list[:10]]) if duty_members_list else "Niemand im Duty"
        
        embed.add_field(name="🟢 Aktive Duty-Mitglieder", value=f"{duty_count}\n{active_duty_display}", inline=False)
        embed.add_field(name="⏱️ Ø Duty-Zeit pro Member", value=f"{avg_duty_hours:.1f} Stunden", inline=True)
        embed.add_field(name="👥 Teammitglieder gesamt", value=str(len(base_role.members)) if base_role else "N/A", inline=True)
        
        stats_channel = await self.get_stats_channel(guild)
        if stats_channel:
            embed.add_field(name="📈 Stats Channel", value=stats_channel.mention, inline=True)
        
        embed.set_footer(text=f"Support-Stats • {guild.name}")
        
        await ctx.send(embed=embed)

    @commands.command(name="feedbackpanel", aliases=["feedbackcreate", "createfeedback"])
    @checks.admin_or_permissions(manage_guild=True)
    async def feedbackpanel(self, ctx: commands.Context):
        """
        Erstellt ein Feedback-Panel mit Buttons für positives/negatives Feedback und Vorschläge.
        """
        guild = ctx.guild
        channel = await self.get_feedback_panel_channel(guild)
        
        if not channel:
            await ctx.send("❌ Kein Feedback-Channel konfiguriert!")
            return
        
        embed = discord.Embed(
            title="💬 Feedback Panel",
            description=(
                "**Wir freuen uns über dein Feedback!**\n\n"
                "Klicke auf einen der Buttons unten um uns dein Feedback zu geben.\n\n"
                "😊 **Positives Feedback** - Das läuft gut!\n"
                "😞 **Negatives Feedback** - Das muss verbessert werden\n"
                "💡 **Vorschlag machen** - Hast du eine Idee?"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Dein Feedback hilft uns besser zu werden!")
        
        view = FeedbackPanelView(self)
        message = await channel.send(embed=embed, view=view)
        
        await self.config.guild(guild).feedback_panel_message_id.set(message.id)
        
        await ctx.send(f"✅ Feedback-Panel wurde in {channel.mention} erstellt!")

    @commands.command(name="teampanel", aliases=["teamupdate", "updateteam"])
    async def teampanel(self, ctx: commands.Context):
        """
        Aktualisiert oder erstellt das Team-Übersichts-Panel.
        Zeigt alle Teammitglieder und deren Duty-Status.
        """
        guild = ctx.guild
        channel = await self.get_team_channel(guild)
        
        if not channel:
            await ctx.send("❌ Kein Team-Channel konfiguriert!")
            return
        
        # Hole View-Klasse und aktualisiere Panel
        from discord.ui import View
        
        role_id = await self.config.guild(guild).role()
        duty_role = await self.get_or_create_duty_role(guild)
        
        team_members = []
        on_duty_count = 0
        off_duty_count = 0
        
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role:
                for m in sorted(base_role.members, key=lambda x: x.display_name.lower()):
                    is_duty = await self.config.member(m).on_duty()
                    status_emoji = "🟢" if is_duty else "🔴"
                    if is_duty:
                        on_duty_count += 1
                    else:
                        off_duty_count += 1
                    team_members.append(f"{status_emoji} {m.display_name}")
        
        embed = discord.Embed(
            title="👥 Team Übersicht",
            description="**Unser Support-Team**\n\nHier siehst du alle Teammitglieder und deren aktuellen Status.",
            color=discord.Color.blue()
        )
        
        if team_members:
            embed.add_field(name=f"🟢 Im Dienst ({on_duty_count})", value=f"Insgesamt {len(team_members)} Teammitglieder", inline=False)
            embed.add_field(name="📋 Teammitglieder", value="\n".join(team_members[:20]) + (f"\n...und {len(team_members) - 20} weitere" if len(team_members) > 20 else ""), inline=False)
        else:
            embed.add_field(name="Keine Teammitglieder", value="Es wurden noch keine Teammitglieder mit der Support-Basisrolle ausgestattet.", inline=False)
        
        embed.set_footer(text=f"On Duty: {on_duty_count} | Off Duty: {off_duty_count}")
        
        team_panel_message_id = await self.config.guild(guild).team_panel_message_id()
        
        if team_panel_message_id:
            try:
                message = await channel.fetch_message(team_panel_message_id)
                await message.edit(embed=embed)
                await ctx.send(f"✅ Team-Panel aktualisiert!")
                return
            except:
                pass
        
        message = await channel.send(embed=embed)
        await self.config.guild(guild).team_panel_message_id.set(message.id)
        await ctx.send(f"✅ Team-Panel in {channel.mention} erstellt!")

    # ============================================
    # WEITERE NEUE BEFEHLE FÜR SUPPORT & MODERATION
    # ============================================
    # HINWEIS: Moderationsbefehle wie warn, mute, kick, ban, etc. wurden entfernt,
    # da diese bereits in den offiziellen Red-Cogs (warnings, mod, admin) enthalten sind.
    # Bitte verwende die offiziellen Cogs für Moderationsaufgaben.

    @commands.command(name="whitelistlist", aliases=["wllist", "whitelisted", "allwhitelist"])
    async def whitelistlist(self, ctx: commands.Context):
        """
        Zeigt alle whitelisted Benutzer an.
        """
        guild = ctx.guild
        approved_role = await self.get_whitelist_approved_role(guild)
        
        if not approved_role:
            await ctx.send("❌ Es wurde keine Whitelist-Approved-Rolle konfiguriert!")
            return
        
        members = approved_role.members
        if not members:
            await ctx.send(f"📋 Keine Mitglieder mit der {approved_role.mention} Rolle gefunden.")
            return
        
        embed = discord.Embed(
            title="📋 Whitelist Mitglieder",
            description=f"**{len(members)}** Mitglieder sind whitelisted",
            color=discord.Color.green()
        )
        
        member_list = "\n".join([f"• {m.display_name}" for m in list(members)[:50]])
        if len(members) > 50:
            member_list += f"\n... und {len(members) - 50} weitere"
        
        embed.add_field(name="Whitelisted Spieler", value=member_list, inline=False)
        embed.set_footer(text=f"Whitelist Liste • {guild.name}")
        
        await ctx.send(embed=embed)

    # HINWEIS: kick, ban, unban wurden entfernt - verwende offizielle Red-Cogs (mod, admin)

    @commands.command(name="supportinfo", aliases=["support", "helpinfo"])
    async def supportinfo(self, ctx: commands.Context):
        """
        Zeigt Informationen über das Support-System an.
        """
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()
        
        embed = discord.Embed(
            title="ℹ️ Support System Informationen",
            description="Übersicht über das Support-System dieses Servers",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        # Support Channel Info
        support_channel = await self.get_support_channel(guild)
        log_channel = await self.get_log_channel(guild)
        panel_channel = await self.get_panel_channel(guild)
        
        embed.add_field(
            name="📝 Channels",
            value=(
                f"**Support:** {support_channel.mention if support_channel else 'Nicht gesetzt'}\n"
                f"**Logs:** {log_channel.mention if log_channel else 'Nicht gesetzt'}\n"
                f"**Panel:** {panel_channel.mention if panel_channel else 'Nicht gesetzt'}"
            ),
            inline=False
        )
        
        # Duty Info
        duty_role_id = guild_data.get("duty_role")
        duty_role = guild.get_role(duty_role_id) if duty_role_id else None
        
        base_role_id = guild_data.get("role")
        base_role = guild.get_role(base_role_id) if base_role_id else None
        
        embed.add_field(
            name="👥 Rollen",
            value=(
                f"**Base Role:** {base_role.mention if base_role else 'Nicht gesetzt'}\n"
                f"**Duty Role:** {duty_role.mention if duty_role else 'Nicht gesetzt'}"
            ),
            inline=False
        )
        
        # Auto-Duty Info
        auto_duty = guild_data.get("auto_remove_duty", True)
        duty_timeout = guild_data.get("duty_timeout", 4)
        
        embed.add_field(
            name="⏱️ Auto-Duty",
            value=f"**Aktiv:** {'Ja' if auto_duty else 'Nein'}\n**Timeout:** {duty_timeout} Stunden",
            inline=True
        )
        
        embed.set_footer(text=f"Support Info • {guild.name}")
        
        await ctx.send(embed=embed)

    # ============================================
    # DUTY COMMANDS - Neue Textbefehle für Duty
    # ============================================
    
    @commands.group(name="duty")
    @commands.guild_only()
    async def duty_group(self, ctx: commands.Context):
        """
        Duty-System Befehle für Support-Mitarbeiter.
        
        Verwende `[p]duty help` für eine Liste aller Unterbefehle.
        """
        pass
    
    @duty_group.command(name="start", aliases=["on", "begin"])
    async def duty_start_command(self, ctx: commands.Context):
        """
        Startet deinen Duty-Dienst.
        
        Du wirst bei neuen Support-Anfragen gepingt.
        """
        guild = ctx.guild
        member = ctx.author
        
        role_id = await self.config.guild(guild).role()
        
        if not role_id:
            await ctx.send("❌ Es wurde keine Support-Rolle konfiguriert! Bitte wende dich an einen Admin.")
            return
        
        base_role = guild.get_role(role_id)
        if not base_role:
            await ctx.send("❌ Die konfigurierte Support-Rolle existiert nicht mehr!")
            return
        
        if base_role not in member.roles:
            await ctx.send(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!")
            return
        
        # Prüfen ob bereits auf Duty
        is_on_duty = await self.config.member(member).on_duty()
        if is_on_duty:
            status = await self.config.member(member).duty_status()
            status_emoji = {"available": "🟢", "busy": "🔵", "break": "☕", "away": "⚪", "off_duty": "⚪"}.get(status, "🟢")
            await ctx.send(f"⚠️ Du bist bereits im Duty-Modus! Status: {status_emoji}")
            return
        
        # Duty aktivieren und Rolle geben
        await self.config.member(member).on_duty.set(True)
        start_time = datetime.utcnow()
        await self.config.member(member).duty_start.set(start_time.timestamp())
        
        # Duty-Status auf "available" setzen
        await self.config.member(member).duty_status.set("available")
        await self.config.member(member).duty_status_message.set(None)
        
        # Session-Count erhöhen
        current_sessions = await self.config.member(member).duty_session_count()
        await self.config.member(member).duty_session_count.set(current_sessions + 1)
        
        # Pausen-Zähler zurücksetzen
        await self.config.member(member).duty_break_count.set(0)
        await self.config.member(member).duty_total_break_time.set(0)
        
        # Duty-Rolle hinzufügen
        await self.update_duty_role(member, True)
        
        # Nachricht im Log-Channel senden
        log_channel = await self.get_log_channel(guild)
        
        embed = discord.Embed(
            title="🟢 Duty Gestartet",
            description=f"{member.mention} hat sich für den Support-Dienst angemeldet!",
            color=discord.Color.green(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        
        session_count = await self.config.member(member).duty_session_count()
        embed.add_field(name="📊 Sessions", value=f"Session #{session_count}", inline=True)
        
        # Zähle alle aktiven Duty-User
        duty_count = 0
        duty_role = await self.get_or_create_duty_role(guild)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.config.member(m).on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Aktive Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_time.strftime('%d.%m.%Y %H:%M')}")
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update Displays
        await self.update_panel_display(guild)
        await self.update_status_display(guild)
        
        await ctx.send("✅ Du bist jetzt im Duty-Modus! Du wirst bei neuen Support-Anfragen gepingt.")
    
    @duty_group.command(name="stop", aliases=["off", "end"])
    async def duty_stop_command(self, ctx: commands.Context):
        """
        Beendet deinen Duty-Dienst.
        
        Deine gesammelte Zeit wird zur Statistik hinzugefügt.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("ℹ️ Du bist aktuell nicht im Duty-Modus.")
            return
        
        # Hole Startzeit für Statistik
        start_time_ts = await self.config.member(member).duty_start()
        duration = "Unbekannt"
        duration_seconds = 0
        if start_time_ts:
            start_dt = datetime.fromtimestamp(start_time_ts)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
            duration_seconds = int(delta.total_seconds())
        
        # Gesamte Duty-Zeit aktualisieren
        total_time = await self.config.member(member).total_duty_time()
        await self.config.member(member).total_duty_time.set(total_time + duration_seconds)
        
        # Duty deaktivieren
        await self.config.member(member).on_duty.set(False)
        await self.config.member(member).duty_start.set(None)
        await self.config.member(member).last_duty_end.set(datetime.utcnow().timestamp())
        await self.config.member(member).duty_status.set("off_duty")
        await self.config.member(member).duty_status_message.set(None)
        
        # Duty-Rolle entfernen
        await self.update_duty_role(member, False)
        
        # Nachricht im Log-Channel senden
        guild = ctx.guild
        log_channel = await self.get_log_channel(guild)
        
        embed = discord.Embed(
            title="🔴 Duty Beendet",
            description=f"{member.mention} hat sich vom Support-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)
        
        # Pausen-Info
        break_count = await self.config.member(member).duty_break_count()
        total_break_time = await self.config.member(member).duty_total_break_time()
        break_minutes = total_break_time // 60
        if break_count > 0:
            embed.add_field(name="☕ Pausen", value=f"{break_count} Pausen ({break_minutes} min)", inline=True)
        
        # Zähle verbleibende aktive Duty-User
        duty_count = 0
        duty_role = await self.get_or_create_duty_role(guild)
        if duty_role:
            for m in duty_role.members:
                is_duty = await self.config.member(m).on_duty()
                if is_duty:
                    duty_count += 1
        
        embed.add_field(name="📊 Verbleibende Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {datetime.utcnow().strftime('%d.%m.%Y %H:%M')}")
        
        if log_channel:
            await log_channel.send(embed=embed)
        
        # Update Displays
        await self.update_panel_display(guild)
        await self.update_status_display(guild)
        
        await ctx.send(f"✅ Du hast den Duty-Modus verlassen. Gesamte Zeit: {duration}")
    
    @duty_group.command(name="pause", aliases=["break", "coffee"])
    async def duty_pause_command(self, ctx: commands.Context):
        """
        Setzt deinen Status auf Pause.
        
        Nützlich für kurze Pausen während des Duty.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("❌ Du musst im Duty sein um eine Pause zu machen!")
            return
        
        current_status = await self.config.member(member).duty_status()
        if current_status == "break":
            await ctx.send("☕ Du bist bereits in Pause!")
            return
        
        # Pausen-Startzeit speichern
        await self.config.member(member).current_break_start.set(datetime.utcnow().timestamp())
        break_count = await self.config.member(member).duty_break_count()
        await self.config.member(member).duty_break_count.set(break_count + 1)
        
        # Status setzen
        await self.config.member(member).duty_status.set("break")
        
        # Status-Display aktualisieren
        guild = ctx.guild
        await self.update_status_display(guild)
        
        await ctx.send("☕ Du bist jetzt in Pause! Vergiss nicht `duty resume` zu verwenden wenn du zurückkommst.")
    
    @duty_group.command(name="resume", aliases=["continue", "back"])
    async def duty_resume_command(self, ctx: commands.Context):
        """
        Setzt deinen Status von Pause zurück auf Verfügbar.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("❌ Du bist nicht im Duty!")
            return
        
        current_status = await self.config.member(member).duty_status()
        if current_status != "break":
            await ctx.send(f"ℹ️ Du bist nicht in Pause. Dein aktueller Status: {current_status}")
            return
        
        # Pausenzeit berechnen und speichern
        break_start = await self.config.member(member).current_break_start()
        if break_start:
            break_duration = int(datetime.utcnow().timestamp() - break_start)
            total_break = await self.config.member(member).duty_total_break_time()
            await self.config.member(member).duty_total_break_time.set(total_break + break_duration)
            await self.config.member(member).current_break_start.set(None)
        
        # Status setzen
        await self.config.member(member).duty_status.set("available")
        
        # Status-Display aktualisieren
        guild = ctx.guild
        await self.update_status_display(guild)
        
        await ctx.send("✅ Willkommen zurück! Du bist jetzt wieder verfügbar.")
    
    @duty_group.command(name="busy", aliases=["occupied"])
    async def duty_busy_command(self, ctx: commands.Context):
        """
        Setzt deinen Status auf Beschäftigt.
        
        Zeigt an dass du gerade ein Ticket bearbeitest.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("❌ Du musst im Duty sein um deinen Status zu ändern!")
            return
        
        await self.config.member(member).duty_status.set("busy")
        
        # Status-Display aktualisieren
        guild = ctx.guild
        await self.update_status_display(guild)
        
        await ctx.send("🔵 Du bist jetzt als beschäftigt markiert.")
    
    @duty_group.command(name="away", aliases=["afk"])
    async def duty_away_command(self, ctx: commands.Context):
        """
        Setzt deinen Status auf Abwesend.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("❌ Du musst im Duty sein um deinen Status zu ändern!")
            return
        
        await self.config.member(member).duty_status.set("away")
        
        # Status-Display aktualisieren
        guild = ctx.guild
        await self.update_status_display(guild)
        
        await ctx.send("⚪ Du bist jetzt als abwesend markiert.")
    
    @duty_group.command(name="status", aliases=["me", "current"])
    async def duty_status_command(self, ctx: commands.Context):
        """
        Zeigt deinen aktuellen Duty-Status an.
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("ℹ️ Du bist aktuell nicht im Duty-Modus.")
            return
        
        status = await self.config.member(member).duty_status()
        status_message = await self.config.member(member).duty_status_message()
        
        status_emojis = {
            "available": ("🟢", "Verfügbar"),
            "busy": ("🔵", "Beschäftigt"),
            "break": ("☕", "Pause"),
            "away": ("⚪", "Abwesend"),
            "off_duty": ("⚫", "Nicht im Duty")
        }
        
        emoji, status_name = status_emojis.get(status, ("⚪", "Unbekannt"))
        
        # Berechne aktuelle Session-Zeit
        start_time_ts = await self.config.member(member).duty_start()
        session_time = "Unbekannt"
        if start_time_ts:
            start_dt = datetime.fromtimestamp(start_time_ts)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            session_time = f"{hours}h {minutes}m"
        
        embed = discord.Embed(
            title=f"{emoji} Dein Duty-Status",
            description=f"**Status:** {status_name}\n**Session-Zeit:** {session_time}",
            color=discord.Color.green()
        )
        
        if status_message:
            embed.add_field(name="📝 Status-Nachricht", value=status_message, inline=False)
        
        # Pausen-Info
        break_count = await self.config.member(member).duty_break_count()
        total_break_time = await self.config.member(member).duty_total_break_time()
        if break_count > 0:
            break_minutes = total_break_time // 60
            embed.add_field(name="☕ Pausen heute", value=f"{break_count} Pausen ({break_minutes} min)", inline=True)
        
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_footer(text=f"Duty Status • {member.display_name}")
        
        await ctx.send(embed=embed)
    
    @duty_group.command(name="today", aliases=["daily", "shift"])
    async def duty_today_command(self, ctx: commands.Context, member: discord.Member = None):
        """
        Zeigt deine heutige Duty-Übersicht an.
        
        Optional kannst du auch einen anderen User angeben.
        """
        if not member:
            member = ctx.author
        
        guild = ctx.guild
        is_on_duty = await self.config.member(member).on_duty()
        
        # Hole Statistiken
        total_duty_time = await self.config.member(member).total_duty_time()
        session_count = await self.config.member(member).duty_session_count()
        break_count = await self.config.member(member).duty_break_count()
        total_break_time = await self.config.member(member).duty_total_break_time()
        
        # Konvertiere Zeiten
        duty_hours = total_duty_time // 3600
        duty_minutes = (total_duty_time % 3600) // 60
        break_minutes = total_break_time // 60
        
        status = await self.config.member(member).duty_status()
        status_emojis = {
            "available": "🟢 Verfügbar",
            "busy": "🔵 Beschäftigt",
            "break": "☕ Pause",
            "away": "⚪ Abwesend",
            "off_duty": "⚫ Nicht im Duty"
        }
        status_text = status_emojis.get(status, "Unbekannt")
        
        embed = discord.Embed(
            title=f"📊 Duty Übersicht für {member.display_name}",
            description=f"**Aktueller Status:** {status_text}",
            color=member.color
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        embed.add_field(name="⏱️ Gesamtzeit", value=f"{duty_hours}h {duty_minutes}m", inline=True)
        embed.add_field(name="🔄 Sessions", value=str(session_count), inline=True)
        embed.add_field(name="☕ Pausen", value=f"{break_count} ({break_minutes} min)", inline=True)
        
        if is_on_duty:
            start_time_ts = await self.config.member(member).duty_start()
            if start_time_ts:
                start_dt = datetime.fromtimestamp(start_time_ts)
                embed.add_field(name="🕐 Session Start", value=start_dt.strftime("%d.%m.%Y %H:%M"), inline=True)
        
        embed.set_footer(text=f"Duty Today • {ctx.guild.name}")
        
        await ctx.send(embed=embed)
    
    @duty_group.command(name="setmessage", aliases=["msg", "statusmsg"])
    async def duty_setmessage_command(self, ctx: commands.Context, *, message: str = None):
        """
        Setze eine benutzerdefinierte Status-Nachricht.
        
        Ohne Nachricht wird die aktuelle Nachricht gelöscht.
        Beispiel: `[p]duty setmessage Bearbeite gerade Tickets`
        """
        member = ctx.author
        is_on_duty = await self.config.member(member).on_duty()
        
        if not is_on_duty:
            await ctx.send("❌ Du musst im Duty sein um eine Status-Nachricht zu setzen!")
            return
        
        # Prüfen ob Feature aktiviert ist
        guild = ctx.guild
        allow_messages = await self.config.guild(guild).allow_status_messages()
        
        if message:
            if not allow_messages:
                await ctx.send("❌ Benutzerdefinierte Status-Nachrichten sind derzeit deaktiviert!")
                return
            
            if len(message) > 100:
                await ctx.send("❌ Die Nachricht darf maximal 100 Zeichen lang sein!")
                return
            
            await self.config.member(member).duty_status_message.set(message)
            await self.update_status_display(guild)
            await ctx.send(f"✅ Deine Status-Nachricht wurde gesetzt: \"{message}\"")
        else:
            await self.config.member(member).duty_status_message.set(None)
            await self.update_status_display(guild)
            await ctx.send("✅ Deine Status-Nachricht wurde gelöscht.")
    
    @duty_group.command(name="clearmessage", aliases=["removemsg"])
    async def duty_clearmessage_command(self, ctx: commands.Context):
        """
        Löscht deine benutzerdefinierte Status-Nachricht.
        """
        member = ctx.author
        await self.config.member(member).duty_status_message.set(None)
        
        guild = ctx.guild
        await self.update_status_display(guild)
        
        await ctx.send("✅ Deine Status-Nachricht wurde gelöscht.")

    @commands.command(name="dutylist", aliases=["activestaff"])
    async def dutylist(self, ctx: commands.Context):
        """
        Zeigt alle aktuell im Duty befindlichen Teammitglieder an (Support & Whitelist).
        """
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()
        
        duty_role_id = guild_data.get("duty_role")
        base_role_id = guild_data.get("role")
        wl_duty_role_id = guild_data.get("whitelist_duty_role")
        wl_role_id = guild_data.get("whitelist_role")
        
        # Support Duty
        support_on_duty = []
        if duty_role_id and base_role_id:
            duty_role = guild.get_role(duty_role_id)
            base_role = guild.get_role(base_role_id)
            
            if duty_role and base_role:
                for member in base_role.members:
                    if duty_role in member.roles:
                        is_on_duty = await self.config.member(member).on_duty()
                        if is_on_duty:
                            duty_start = await self.config.member(member).duty_start()
                            if duty_start:
                                start_time = datetime.fromtimestamp(duty_start)
                                duration = datetime.utcnow() - start_time
                                hours = int(duration.total_seconds() // 3600)
                                minutes = int((duration.total_seconds() % 3600) // 60)
                                support_on_duty.append((member, f"{hours}h {minutes}m"))
                            else:
                                support_on_duty.append((member, "Unbekannt"))
        
        # Whitelist Duty
        whitelist_on_duty = []
        if wl_duty_role_id and wl_role_id:
            wl_duty_role = guild.get_role(wl_duty_role_id)
            wl_base_role = guild.get_role(wl_role_id)
            
            if wl_duty_role and wl_base_role:
                for member in wl_base_role.members:
                    if wl_duty_role in member.roles:
                        is_on_duty = await self.config.member(member).whitelist_on_duty()
                        if is_on_duty:
                            duty_start = await self.config.member(member).whitelist_duty_start()
                            if duty_start:
                                start_time = datetime.fromtimestamp(duty_start)
                                duration = datetime.utcnow() - start_time
                                hours = int(duration.total_seconds() // 3600)
                                minutes = int((duration.total_seconds() % 3600) // 60)
                                whitelist_on_duty.append((member, f"{hours}h {minutes}m"))
                            else:
                                whitelist_on_duty.append((member, "Unbekannt"))
        
        if not support_on_duty and not whitelist_on_duty:
            await ctx.send("🟢🔵 Aktuell ist niemand im Duty (weder Support noch Whitelist)!")
            return
        
        embed = discord.Embed(
            title="👥 Aktuell im Duty",
            description=f"**{len(support_on_duty) + len(whitelist_on_duty)}** Teammitglieder sind im Einsatz",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        if support_on_duty:
            support_list = "\n".join([f"• {m.mention} - seit {time}" for m, time in support_on_duty[:10]])
            if len(support_on_duty) > 10:
                support_list += f"\n... und {len(support_on_duty) - 10} weitere"
            embed.add_field(name=f"🟢 Support-Duty ({len(support_on_duty)})", value=support_list, inline=False)
        
        if whitelist_on_duty:
            wl_list = "\n".join([f"• {m.mention} - seit {time}" for m, time in whitelist_on_duty[:10]])
            if len(whitelist_on_duty) > 10:
                wl_list += f"\n... und {len(whitelist_on_duty) - 10} weitere"
            embed.add_field(name=f"🔵 Whitelist-Duty ({len(whitelist_on_duty)})", value=wl_list, inline=False)
        
        embed.set_footer(text=f"Duty Liste • {guild.name}")
        
        await ctx.send(embed=embed)

    @commands.command(name="staffinfo", aliases=["teaminfo", "memberinfo"])
    async def staffinfo(self, ctx: commands.Context, member: discord.Member = None):
        """
        Zeigt Informationen über ein Teammitglied an.
        """
        if not member:
            member = ctx.author
        
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()
        
        base_role_id = guild_data.get("role")
        duty_role_id = guild_data.get("duty_role")
        wl_duty_role_id = guild_data.get("whitelist_duty_role")
        
        base_role = guild.get_role(base_role_id) if base_role_id else None
        duty_role = guild.get_role(duty_role_id) if duty_role_id else None
        wl_duty_role = guild.get_role(wl_duty_role_id) if wl_duty_role_id else None
        
        is_support = base_role in member.roles if base_role else False
        is_on_duty = await self.config.member(member).on_duty()
        is_wl_duty = await self.config.member(member).whitelist_on_duty()
        
        embed = discord.Embed(
            title=f"👤 {member.display_name}",
            color=member.color,
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        status_info = []
        if is_support:
            status_info.append("✅ Support Team")
        if is_on_duty:
            status_info.append("🟢 On Duty (Support)")
        if is_wl_duty:
            status_info.append("📋 On Duty (Whitelist)")
        
        if not status_info:
            status_info.append("🔹 Community Mitglied")
        
        embed.add_field(name="Status", value="\n".join(status_info), inline=False)
        
        if is_on_duty:
            duty_start = await self.config.member(member).duty_start()
            if duty_start:
                start_time = datetime.fromtimestamp(duty_start)
                duration = datetime.utcnow() - start_time
                hours = int(duration.total_seconds() // 3600)
                minutes = int((duration.total_seconds() % 3600) // 60)
                embed.add_field(name="Duty Dauer", value=f"{hours}h {minutes}m", inline=True)
        
        embed.add_field(name="Rollen", value=f"{len(member.roles)} Rollen", inline=True)
        embed.add_field(name="Beigetreten", value=member.joined_at.strftime("%d.%m.%Y") if member.joined_at else "Unbekannt", inline=True)
        
        embed.set_footer(text=f"Staff Info • {guild.name}")
        
        await ctx.send(embed=embed)

    @commands.command(name="dutytime", aliases=["dt"])
    async def dutytime(self, ctx: commands.Context, member: discord.Member = None):
        """
        Zeigt die gesammelte Duty-Zeit eines Users an.
        
        - `member`: Der User dessen Duty-Zeit angezeigt werden soll (optional, standardmäßig du selbst)
        """
        if not member:
            member = ctx.author
        
        guild = ctx.guild
        
        # Hole Duty-Zeiten
        total_duty_time = await self.config.member(member).total_duty_time()
        total_wl_duty_time = await self.config.member(member).total_whitelist_duty_time()
        
        # Konvertiere zu Stunden und Minuten
        support_hours = total_duty_time // 3600
        support_minutes = (total_duty_time % 3600) // 60
        wl_hours = total_wl_duty_time // 3600
        wl_minutes = (total_wl_duty_time % 3600) // 60
        
        embed = discord.Embed(
            title=f"⏱️ Duty-Zeit von {member.display_name}",
            color=member.color,
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        embed.add_field(
            name="🟢 Support Duty",
            value=f"**{support_hours}h {support_minutes}m**\n({total_duty_time:,} Sekunden)",
            inline=True
        )
        embed.add_field(
            name="📋 Whitelist Duty",
            value=f"**{wl_hours}h {wl_minutes}m**\n({total_wl_duty_time:,} Sekunden)",
            inline=True
        )
        
        total_hours = (total_duty_time + total_wl_duty_time) // 3600
        total_minutes = ((total_duty_time + total_wl_duty_time) % 3600) // 60
        embed.add_field(
            name="📊 Gesamt",
            value=f"**{total_hours}h {total_minutes}m**",
            inline=False
        )
        
        embed.set_footer(text=f"Duty Zeit • {guild.name}")
        
        await ctx.send(embed=embed)

    @commands.command(name="dutyleaderboard", aliases=["dutylb", "dutyranking"])
    async def dutyleaderboard(self, ctx: commands.Context, limit: int = 10):
        """
        Zeigt ein Ranking der aktivsten Staff-Mitglieder nach Duty-Zeit.
        
        - `limit`: Anzahl der anzuzeigenden Einträge (max. 25)
        """
        if limit > 25:
            limit = 25
        if limit < 1:
            limit = 10
        
        guild = ctx.guild
        guild_data = await self.config.guild(guild).all()
        
        role_id = guild_data.get("role")
        wl_role_id = guild_data.get("whitelist_role")
        
        if not role_id and not wl_role_id:
            await ctx.send("❌ Es wurden keine Support- oder Whitelist-Rollen konfiguriert!")
            return
        
        # Sammle alle relevanten Mitglieder
        members_to_check = set()
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role:
                members_to_check.update(base_role.members)
        if wl_role_id:
            wl_base_role = guild.get_role(wl_role_id)
            if wl_base_role:
                members_to_check.update(wl_base_role.members)
        
        if not members_to_check:
            await ctx.send("❌ Keine Teammitglieder gefunden!")
            return
        
        # Sammle Duty-Zeiten
        duty_times = []
        for member in members_to_check:
            if member.bot:
                continue
            total_support = await self.config.member(member).total_duty_time()
            total_wl = await self.config.member(member).total_whitelist_duty_time()
            total = total_support + total_wl
            if total > 0:
                duty_times.append((member, total, total_support, total_wl))
        
        if not duty_times:
            await ctx.send("📊 Noch keine Duty-Zeiten erfasst!")
            return
        
        # Sortiere nach Gesamtzeit (absteigend)
        duty_times.sort(key=lambda x: x[1], reverse=True)
        
        # Erstelle Leaderboard
        leaderboard_entries = []
        for i, (member, total, support, wl) in enumerate(duty_times[:limit], 1):
            total_hours = total // 3600
            total_minutes = (total % 3600) // 60
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            entry = f"{medal} **{member.display_name}** - {total_hours}h {total_minutes}m"
            if support > 0 and wl > 0:
                supp_h = support // 3600
                wl_h = wl // 3600
                entry += f"\n   └ 🟢 {supp_h}h Support | 📋 {wl_h}h Whitelist"
            leaderboard_entries.append(entry)
        
        embed = discord.Embed(
            title="🏆 Duty Leaderboard",
            description="Die aktivsten Staff-Mitglieder nach gesamter Duty-Zeit\n\n" + "\n\n".join(leaderboard_entries),
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )
        embed.set_footer(text=f"Duty Ranking • {len(duty_times)} Mitglieder mit Duty-Zeit • {guild.name}")
        
        await ctx.send(embed=embed)

    # HINWEIS: clearwarns, slowmode, purge, lock, unlock, nick, removenick wurden entfernt - verwende offizielle Red-Cogs (mod, admin, roletools)
    # HINWEIS: serverinfo, roleinfo wurden entfernt - verwende offiziellen Red-Cog (info)

    # ENDE DER BEFEHLE - Alle weiteren Befehle (serverinfo, roleinfo) wurden entfernt da sie im offiziellen 'info' Cog enthalten sind


class DutyButtonView(discord.ui.View):
    """Button-View für Duty An-/Abmeldung mit erweiterten Funktionen"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Duty Starten", style=discord.ButtonStyle.green, emoji="🟢", custom_id="duty_start")
    async def start_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Startet den Duty-Modus mit erweitertem Status-System"""
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
        
        # Duty-Status auf "available" setzen
        await self.cog.config.member(member).duty_status.set("available")
        await self.cog.config.member(member).duty_status_message.set(None)
        
        # Session-Count erhöhen
        current_sessions = await self.cog.config.member(member).duty_session_count()
        await self.cog.config.member(member).duty_session_count.set(current_sessions + 1)
        
        # Pausen-Zähler zurücksetzen
        await self.cog.config.member(member).duty_break_count.set(0)
        
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
        
        session_count = await self.cog.config.member(member).duty_session_count()
        embed.add_field(name="📊 Sessions", value=f"Session #{session_count}", inline=True)
        
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
        await self.update_status_display(guild)
        
        await interaction.response.send_message("✅ Du bist jetzt im Duty-Modus! Du wirst bei neuen Support-Anfragen gepingt.", ephemeral=True)
    
    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Duty-Modus mit erweiterter Statistik"""
        member = interaction.user
        is_on_duty = await self.cog.config.member(member).on_duty()
        
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Duty-Modus.", ephemeral=True)
            return
        
        # Hole Startzeit für Statistik
        start_time_ts = await self.cog.config.member(member).duty_start()
        duration = "Unbekannt"
        duration_seconds = 0
        if start_time_ts:
            start_dt = datetime.fromtimestamp(start_time_ts)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
            duration_seconds = int(delta.total_seconds())
        
        # Gesamte Duty-Zeit aktualisieren (Statistik)
        total_time = await self.cog.config.member(member).total_duty_time()
        await self.cog.config.member(member).total_duty_time.set(total_time + duration_seconds)
        
        # Duty deaktivieren und Rolle entfernen
        await self.cog.config.member(member).on_duty.set(False)
        await self.cog.config.member(member).duty_start.set(None)
        await self.cog.config.member(member).last_duty_end.set(datetime.utcnow().timestamp())
        await self.cog.config.member(member).duty_status.set("off_duty")
        await self.cog.config.member(member).duty_status_message.set(None)
        
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
        
        # Pausen-Info anzeigen
        break_count = await self.cog.config.member(member).duty_break_count()
        total_break_time = await self.cog.config.member(member).duty_total_break_time()
        break_minutes = total_break_time // 60
        if break_count > 0:
            embed.add_field(name="☕ Pausen", value=f"{break_count} Pausen ({break_minutes} min)", inline=True)
        
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
        await self.update_status_display(guild)
        
        await interaction.response.send_message("✅ Du hast den Duty-Modus verlassen.", ephemeral=True)
    
    @discord.ui.button(label="Status Ändern", style=discord.ButtonStyle.secondary, emoji="📝", custom_id="duty_status")
    async def change_status(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Zeigt ein Modal zum Ändern des Duty-Status an"""
        is_on_duty = await self.cog.config.member(interaction.user).on_duty()
        
        if not is_on_duty:
            await interaction.response.send_message("❌ Du musst im Duty sein um deinen Status zu ändern!", ephemeral=True)
            return
        
        # Erstelle eine View mit Status-Buttons
        view = StatusSelectView(self.cog)
        await interaction.response.send_message(
            "Wähle deinen aktuellen Status:",
            view=view,
            ephemeral=True
        )
    
    async def update_panel_display(self, guild: discord.Guild):
        """Updates the panel message to show current duty members"""
        try:
            panel_message_id = await self.cog.config.guild(guild).panel_message_id()
            if not panel_message_id:
                return
            
            panel_channel = await self.cog.get_panel_channel(guild)
            if not panel_channel:
                return
            
            panel_message = await panel_channel.fetch_message(panel_message_id)
            
            # Get current duty members with their status
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
                                # Status-Emoji holen
                                status = await self.cog.config.member(m).duty_status()
                                status_emoji = {"available": "🟢", "busy": "🔵", "break": "☕", "away": "🟡"}.get(status, "🟢")
                                duty_list.append(f"{status_emoji} {m.display_name}")
            
            # Create new embed with updated info
            if duty_count > 0:
                duty_text = "\n".join(duty_list[:15])
                if len(duty_list) > 15:
                    duty_text += f"\n• ...und {duty_count - 15} weitere"
            else:
                duty_text = "Niemand"
            
            new_embed = discord.Embed(
                title="🎧 Support Duty Panel",
                description=(
                    "**Willkommen zum Support-Duty System!**\n\n"
                    "Klicke auf die Buttons unten um dich für den Support-Dienst an- oder abzumelden.\n\n"
                    "🟢 **Duty Starten** - Du wirst bei neuen Anfragen gepingt\n"
                    "🔴 **Duty Beenden** - Du erhältst keine Pings mehr\n"
                    "📝 **Status Ändern** - Setze deinen aktuellen Status (Verfügbar/Beschäftigt/Pause)"
                ),
                color=discord.Color.blue()
            )
            new_embed.add_field(name=f"🟢 Aktuell im Dienst ({duty_count})", value=duty_text or "Niemand", inline=False)
            new_embed.set_footer(text="Die 🟢 On Duty Rolle wird automatisch zugewiesen/entfernt")
            
            await panel_message.edit(embed=new_embed)
        except Exception as e:
            print(f"Fehler beim Aktualisieren des Duty-Panels: {e}")
    
    async def update_status_display(self, guild: discord.Guild):
        """Aktualisiert das erweiterte Duty-Status-Display mit detaillierten Informationen"""
        try:
            status_display_id = await self.cog.config.guild(guild).duty_status_display_message_id()
            if not status_display_id:
                return
            
            status_channel = await self.cog.get_status_display_channel(guild)
            if not status_channel:
                return
            
            status_message = await status_channel.fetch_message(status_display_id)
            
            # Sammle alle Duty-Mitglieder mit ihren Details
            duty_role = await self.cog.get_or_create_duty_role(guild)
            role_id = await self.cog.config.guild(guild).role()
            
            available_list = []
            busy_list = []
            break_list = []
            away_list = []
            
            if role_id and duty_role:
                base_role = guild.get_role(role_id)
                if base_role:
                    for m in base_role.members:
                        if duty_role in m.roles:
                            is_duty = await self.cog.config.member(m).on_duty()
                            if is_duty:
                                status = await self.cog.config.member(m).duty_status()
                                status_msg = await self.cog.config.member(m).duty_status_message()
                                
                                entry = f"• {m.display_name}"
                                if status_msg:
                                    entry += f"\n  └ _{status_msg}_"
                                
                                if status == "available":
                                    available_list.append(entry)
                                elif status == "busy":
                                    busy_list.append(entry)
                                elif status == "break":
                                    break_list.append(entry)
                                elif status == "away":
                                    away_list.append(entry)
            
            # Erstelle detailliertes Embed
            embed = discord.Embed(
                title="📊 Live Duty Status Übersicht",
                description="**Aktueller Status aller Teammitglieder im Dienst**",
                color=discord.Color.blue(),
                timestamp=datetime.utcnow()
            )
            
            # Verfügbarkeit anzeigen
            avail_text = "\n".join(available_list[:10]) if available_list else "Keine"
            if len(available_list) > 10:
                avail_text += f"\n_...und {len(available_list) - 10} weitere_"
            embed.add_field(name="🟢 Verfügbar", value=avail_text, inline=True)
            
            busy_text = "\n".join(busy_list[:10]) if busy_list else "Keine"
            if len(busy_list) > 10:
                busy_text += f"\n_...und {len(busy_list) - 10} weitere_"
            embed.add_field(name="🔵 Beschäftigt", value=busy_text, inline=True)
            
            break_text = "\n".join(break_list[:10]) if break_list else "Keine"
            if len(break_list) > 10:
                break_text += f"\n_...und {len(break_list) - 10} weitere_"
            embed.add_field(name="☕ In Pause", value=break_text, inline=True)
            
            away_text = "\n".join(away_list[:10]) if away_list else "Keine"
            if len(away_list) > 10:
                away_text += f"\n_...und {len(away_list) - 10} weitere_"
            embed.add_field(name="🟡 Abwesend", value=away_text, inline=False)
            
            # Gesamtstatistik
            total = len(available_list) + len(busy_list) + len(break_list) + len(away_list)
            embed.add_field(
                name="📈 Statistik",
                value=f"**{total}** im Duty | 🟢 {len(available_list)} frei | 🔵 {len(busy_list)} beschäftigt | ☕ {len(break_list)} Pause",
                inline=False
            )
            
            embed.set_footer(text=f"Zuletzt aktualisiert")
            embed.timestamp = datetime.utcnow()
            
            await status_message.edit(embed=embed)
        except Exception as e:
            print(f"Fehler beim Aktualisieren des Status-Displays: {e}")


class StatusSelectView(discord.ui.View):
    """View zur Auswahl des Duty-Status"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=180)  # 3 Minuten Timeout
        self.cog = cog
    
    @discord.ui.button(label="Verfügbar", style=discord.ButtonStyle.green, emoji="🟢")
    async def set_available(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_status(interaction, "available", "Du bist jetzt verfügbar.")
    
    @discord.ui.button(label="Beschäftigt", style=discord.ButtonStyle.blurple, emoji="🔵")
    async def set_busy(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_status(interaction, "busy", "Du bist jetzt beschäftigt.")
    
    @discord.ui.button(label="Pause", style=discord.ButtonStyle.secondary, emoji="☕")
    async def set_break(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = interaction.user
        # Pausen-Startzeit speichern
        await self.cog.config.member(member).current_break_start.set(datetime.utcnow().timestamp())
        break_count = await self.cog.config.member(member).duty_break_count()
        await self.cog.config.member(member).duty_break_count.set(break_count + 1)
        await self._set_status(interaction, "break", "Du bist jetzt in Pause.")
    
    @discord.ui.button(label="Abwesend", style=discord.ButtonStyle.secondary, emoji="⚪")
    async def set_away(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_status(interaction, "away", "Du bist jetzt abwesend.")
    
    async def _set_status(self, interaction: discord.Interaction, status: str, message: str):
        member = interaction.user
        await self.cog.config.member(member).duty_status.set(status)
        
        # Status-Display aktualisieren
        guild = interaction.guild
        await self.cog.update_status_display(guild)
        
        await interaction.response.edit_message(content=f"✅ {message}", view=None)


class WhitelistButtonView(discord.ui.View):
    """Button-View für Whitelist Duty An-/Abmeldung"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild = guild
        # Hinweis: Der "Whitelist freischalten" Button gehört nicht hierher,
        # sondern wird dynamisch bei den Whitelist-Anfrage-Nachrichten hinzugefügt.
    
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
        await self.cog.update_whitelist_panel_display(guild)
        
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
        start_time_ts = await self.cog.config.member(member).whitelist_duty_start()
        duration = "Unbekannt"
        duration_seconds = 0
        if start_time_ts:
            start_dt = datetime.fromtimestamp(start_time_ts)
            delta = datetime.utcnow() - start_dt
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            duration = f"{hours}h {minutes}min"
            duration_seconds = int(delta.total_seconds())
        
        # Gesamte Duty-Zeit aktualisieren (Statistik)
        total_time = await self.cog.config.member(member).total_whitelist_duty_time()
        await self.cog.config.member(member).total_whitelist_duty_time.set(total_time + duration_seconds)
        
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
        await self.cog.update_whitelist_panel_display(guild)
        
        await interaction.response.send_message("✅ Du hast den Whitelist-Duty-Modus verlassen.", ephemeral=True)


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
                        dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{approved_role.name}**", inline=False)
                        dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen.", inline=False)
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
    
    def __init__(self, cog: SupportCog, guild: discord.Guild, grant_role_mode: bool = False):
        self.cog = cog
        self.guild = guild
        self.grant_role_mode = grant_role_mode
        
        title = "🎮 Spieler whitelisten" if not grant_role_mode else "✅ Whitelist-Rolle vergeben"
        
        super().__init__(title=title, timeout=600)
        
        placeholder_text = "Gib den Discord-Namen oder die User-ID ein (z.B. 123456789012345678)"
        if grant_role_mode:
            placeholder_text = "Gib den Discord-Namen oder die User-ID des Spielers ein dem du die Whitelist-Rolle geben möchtest"
        
        self.search_input = discord.ui.TextInput(
            label="Spielername oder ID",
            style=discord.TextStyle.short,
            placeholder=placeholder_text,
            min_length=1,
            max_length=50,
            required=True
        )
        # Füge das TextInput-Feld zum Modal hinzu
        self.add_item(self.search_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn das Modal abgesendet wird - Verarbeitet die Whitelist direkt"""
        # Defer immediately to prevent timeout - NO thinking parameter
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Hole die eingegebene ID oder den Namen
            search_query = self.search_input.value.strip()
            
            member = interaction.user
            
            # Prüfe Berechtigung: Whitelist-Handler-Rolle ODER im Duty
            role_id = await self.cog.config.guild(self.guild).whitelist_role()
            has_base_role = False
            if role_id:
                base_role = self.guild.get_role(role_id)
                if base_role and base_role in member.roles:
                    has_base_role = True
            
            is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
            
            if not has_base_role and not is_on_duty:
                await interaction.followup.send("❌ Du benötigst die Whitelist-Handler-Rolle oder musst im Whitelist-Duty sein!", ephemeral=True)
                return
            
            # Im grant_role_mode verwenden wir die whitelist_grant_role, sonst whitelist_approved_role
            if self.grant_role_mode:
                approved_role_id = await self.cog.config.guild(self.guild).whitelist_grant_role()
                if not approved_role_id:
                    await interaction.followup.send("❌ Keine 'Whitelist freischalten' Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
                    return
                approved_role = self.guild.get_role(approved_role_id)
                if not approved_role:
                    await interaction.followup.send("❌ Die konfigurierte 'Whitelist freischalten' Rolle existiert nicht mehr!", ephemeral=True)
                    return
            else:
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
                    f"ℹ️ {target_user.mention} hat bereits diese Rolle!",
                    ephemeral=True
                )
                return
            
            # Füge die Approved-Rolle hinzu
            try:
                await target_user.add_roles(approved_role, reason=f"{'Whitelist genehmigt' if not self.grant_role_mode else 'Whitelist-Rolle vergeben'} von {member.display_name}")
                
                embed_success = discord.Embed(
                    title="✅ Whitelist genehmigt" if not self.grant_role_mode else "✅ Whitelist-Rolle vergeben",
                    description=f"{target_user.mention} wurde erfolgreich {'zur Whitelist hinzugefügt' if not self.grant_role_mode else 'die Whitelist-Rolle zugewiesen'}!",
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
                        title="📋 Whitelist Eintrag erstellt" if not self.grant_role_mode else "📋 Whitelist-Rolle vergeben",
                        description=f"**{target_user.mention}** wurde {'zur Whitelist hinzugefügt' if not self.grant_role_mode else 'die Whitelist-Rolle zugewiesen'}.",
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
                        title="🎉 Herzlichen Glückwunsch!" if not self.grant_role_mode else "✅ Whitelist-Rolle erhalten",
                        description=f"Du wurdest von **{member.display_name}** {'zur Whitelist hinzugefügt' if not self.grant_role_mode else 'die Whitelist-Rolle zugewiesen'}!",
                        color=discord.Color.green(),
                        timestamp=datetime.utcnow()
                    )
                    dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{approved_role.name}**", inline=False)
                    dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen." if not self.grant_role_mode else "Deine Whitelist wurde aktiviert.", inline=False)
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


class PersistentWhitelistGrantView(discord.ui.View):
    """Persistente View für Whitelist-Rollenvergabe im Panel - OHNE Ziel-User Bindung"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild = guild
    
    @discord.ui.button(label="Whitelist freischalten", style=discord.ButtonStyle.success, emoji="✅", custom_id="whitelist_grant_role_persistent", row=2)
    async def grant_whitelist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal zur Eingabe des Spielers der die Rolle erhalten soll"""
        # Prüfen ob User im Whitelist-Duty ist ODER die always_allowed_role hat
        is_on_duty = await self.cog.config.member(interaction.user).whitelist_on_duty()
        
        # Prüfen auf always_allowed_role
        always_allowed_role_id = await self.cog.config.guild(self.guild).whitelist_always_allowed_role()
        always_allowed_role = self.guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in interaction.user.roles
        
        if not is_on_duty and not has_always_allowed:
            await interaction.response.send_message("❌ Du musst im Whitelist-Duty sein oder die 'Always Allowed' Rolle haben um Spieler zur Whitelist hinzuzufügen!", ephemeral=True)
            return
        
        # Hole die zu vergebende Rolle
        grant_role_id = await self.cog.config.guild(self.guild).whitelist_grant_role()
        if not grant_role_id:
            await interaction.response.send_message("❌ Keine 'Whitelist freischalten' Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        grant_role = self.guild.get_role(grant_role_id)
        if not grant_role:
            await interaction.response.send_message("❌ Die konfigurierte 'Whitelist freischalten' Rolle existiert nicht mehr!", ephemeral=True)
            return
        
        # Öffne Modal zur Eingabe des Ziel-Users (ID, Mention oder Name)
        modal = WhitelistGrantRoleModal(self.cog, self.guild, grant_role)
        await interaction.response.send_modal(modal)


class WhitelistGrantRoleModal(discord.ui.Modal):
    """Modal zur Eingabe eines Spielers für die Whitelist-Rollenvergabe"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild, grant_role: discord.Role):
        super().__init__(title="🎮 Spieler für Whitelist eingeben", timeout=600)
        self.cog = cog
        self.guild = guild
        self.grant_role = grant_role
        
        self.player_input = discord.ui.TextInput(
            label="Spieler eingeben",
            placeholder="Discord ID, @Mention oder Benutzername",
            min_length=1,
            max_length=100,
            required=True,
            custom_id="player_input"
        )
        self.add_item(self.player_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn das Modal abgesendet wird"""
        player_query = self.player_input.value.strip()
        
        # Versuche den Ziel-User zu finden
        target_user = None
        
        # Versuch 1: Als ID parsen
        if player_query.isdigit():
            try:
                target_user = await self.guild.fetch_member(int(player_query))
            except (discord.NotFound, discord.HTTPException):
                pass
        
        # Versuch 2: Als Mention parsen (<@123456789>)
        if not target_user and player_query.startswith("<@") and player_query.endswith(">"):
            user_id = player_query[2:-1].lstrip("!")
            if user_id.isdigit():
                try:
                    target_user = await self.guild.fetch_member(int(user_id))
                except (discord.NotFound, discord.HTTPException):
                    pass
        
        # Versuch 3: Als Username/Nickname suchen
        if not target_user:
            for member in self.guild.members:
                if (player_query.lower() in member.name.lower() or 
                    player_query.lower() in member.display_name.lower()):
                    target_user = member
                    break
        
        if not target_user:
            await interaction.response.send_message(f"❌ Kein Benutzer mit '{player_query}' gefunden!", ephemeral=True)
            return
        
        # Prüfen ob der User die Rolle schon hat
        if self.grant_role in target_user.roles:
            await interaction.response.send_message(f"ℹ️ {target_user.mention} hat bereits diese Rolle!", ephemeral=True)
            return
        
        # Füge die Rolle hinzu
        try:
            await target_user.add_roles(self.grant_role, reason=f"Whitelist-Rolle vergeben von {interaction.user.display_name} via Panel Button")
            
            embed_success = discord.Embed(
                title="✅ Whitelist-Rolle vergeben",
                description=f"{target_user.mention} wurde erfolgreich die Whitelist-Rolle zugewiesen!",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{interaction.user.mention} ({interaction.user.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{target_user.display_name}", inline=True)
            
            await interaction.response.send_message(embed=embed_success, ephemeral=True)
            
            # Logge die Aktion
            log_channel = await self.cog.get_whitelist_log_channel(self.guild)
            if log_channel:
                log_embed = discord.Embed(
                    title="📋 Whitelist-Rolle vergeben",
                    description=f"**{target_user.mention}** wurde die Whitelist-Rolle zugewiesen.",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )
                log_embed.set_thumbnail(url=target_user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{interaction.user.mention}\n*{interaction.user.display_name}* (ID: `{interaction.user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{self.grant_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {interaction.user.display_name}")
                
                await log_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=f"Du wurdest von **{interaction.user.display_name}** zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{self.grant_role.name}**", inline=False)
                dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen.", inline=False)
                dm_embed.set_footer(text=f"{self.guild.name} Whitelist System")
                await target_user.send(embed=dm_embed)
            except discord.Forbidden:
                pass
            
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Ein Fehler ist beim Hinzufügen der Rolle aufgetreten: `{str(e)}`",
                ephemeral=True
            )


class GrantWhitelistButton(discord.ui.Button):
    """Button zum Freischalten der Whitelist für einen Spieler"""
    
    def __init__(self, cog: SupportCog, guild: discord.Guild, target_user_id: int):
        super().__init__(label="Whitelist freischalten", style=discord.ButtonStyle.success, emoji="✅", custom_id=f"whitelist_grant_role_{target_user_id}")
        self.cog = cog
        self.guild = guild
        self.target_user_id = target_user_id
    
    async def callback(self, interaction: discord.Interaction):
        """Wird ausgelöst wenn der Button geklickt wird - vergibt Rolle automatisch an den Antragsteller"""
        # Prüfen ob User im Whitelist-Duty ist ODER die always_allowed_role hat
        is_on_duty = await self.cog.config.member(interaction.user).whitelist_on_duty()
        
        # Prüfen auf always_allowed_role
        always_allowed_role_id = await self.cog.config.guild(self.guild).whitelist_always_allowed_role()
        always_allowed_role = self.guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in interaction.user.roles
        
        if not is_on_duty and not has_always_allowed:
            await interaction.response.send_message("❌ Du musst im Whitelist-Duty sein oder die 'Always Allowed' Rolle haben um Spieler zur Whitelist hinzuzufügen!", ephemeral=True)
            return
        
        # Hole den Ziel-User aus der gespeicherten ID
        try:
            target_user = await self.guild.fetch_member(self.target_user_id)
        except (discord.NotFound, discord.HTTPException):
            await interaction.response.send_message("❌ Der ursprüngliche Antragsteller wurde nicht gefunden!", ephemeral=True)
            return
        
        # Hole die zu vergebende Rolle
        grant_role_id = await self.cog.config.guild(self.guild).whitelist_grant_role()
        if not grant_role_id:
            await interaction.response.send_message("❌ Keine 'Whitelist freischalten' Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return
        
        grant_role = self.guild.get_role(grant_role_id)
        if not grant_role:
            await interaction.response.send_message("❌ Die konfigurierte 'Whitelist freischalten' Rolle existiert nicht mehr!", ephemeral=True)
            return
        
        # Prüfen ob der User die Rolle schon hat
        if grant_role in target_user.roles:
            await interaction.response.send_message(f"ℹ️ {target_user.mention} hat bereits diese Rolle!", ephemeral=True)
            return
        
        # Füge die Rolle hinzu
        try:
            await target_user.add_roles(grant_role, reason=f"Whitelist-Rolle vergeben von {interaction.user.display_name} via Button")
            
            embed_success = discord.Embed(
                title="✅ Whitelist-Rolle vergeben",
                description=f"{target_user.mention} wurde erfolgreich die Whitelist-Rolle zugewiesen!",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{interaction.user.mention} ({interaction.user.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{target_user.display_name}", inline=True)
            
            await interaction.response.send_message(embed=embed_success, ephemeral=True)
            
            # Logge die Aktion
            log_channel = await self.cog.get_whitelist_log_channel(self.guild)
            if log_channel:
                log_embed = discord.Embed(
                    title="📋 Whitelist-Rolle vergeben",
                    description=f"**{target_user.mention}** wurde die Whitelist-Rolle zugewiesen.",
                    color=discord.Color.blue(),
                    timestamp=datetime.utcnow()
                )
                log_embed.set_thumbnail(url=target_user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{interaction.user.mention}\n*{interaction.user.display_name}* (ID: `{interaction.user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{grant_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(datetime.utcnow().timestamp())}:F>\n(<t:{int(datetime.utcnow().timestamp())}:R>)", inline=True)
                log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {interaction.user.display_name}")
                
                await log_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=f"Du wurdest von **{interaction.user.display_name}** zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{grant_role.name}**", inline=False)
                dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen.", inline=False)
                dm_embed.set_footer(text=f"{self.guild.name} Whitelist System")
                await target_user.send(embed=dm_embed)
            except discord.Forbidden:
                pass
            
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Ein Fehler ist beim Hinzufügen der Rolle aufgetreten: `{str(e)}`",
                ephemeral=True
            )


class FeedbackPanelView(discord.ui.View):
    """Button-View für Feedback - Persistent"""
    
    def __init__(self, cog: SupportCog = None):
        super().__init__(timeout=None)
        self.cog = cog
    
    async def get_cog(self, interaction: discord.Interaction) -> Optional[SupportCog]:
        """Holt den Cog aus der Interaktion wenn nicht gesetzt"""
        if self.cog:
            return self.cog
        # Versuche den Cog aus dem Bot zu holen
        try:
            cog = interaction.client.get_cog("SupportCog")
            if cog and isinstance(cog, SupportCog):
                return cog
        except Exception:
            pass
        return None
    
    @discord.ui.button(label="Positives Feedback geben", style=discord.ButtonStyle.green, emoji="😊", custom_id="feedback_positive")
    async def positive_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für positives Feedback"""
        cog = await self.get_cog(interaction)
        if not cog:
            await interaction.response.send_message("❌ Cog nicht gefunden. Bitte wende dich an einen Admin.", ephemeral=True)
            return
        modal = FeedbackModal(cog, "positive")
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Negatives Feedback geben", style=discord.ButtonStyle.red, emoji="😞", custom_id="feedback_negative")
    async def negative_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für negatives Feedback"""
        cog = await self.get_cog(interaction)
        if not cog:
            await interaction.response.send_message("❌ Cog nicht gefunden. Bitte wende dich an einen Admin.", ephemeral=True)
            return
        modal = FeedbackModal(cog, "negative")
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Vorschlag machen", style=discord.ButtonStyle.blurple, emoji="💡", custom_id="feedback_suggestion")
    async def suggestion_feedback(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal für Vorschläge"""
        cog = await self.get_cog(interaction)
        if not cog:
            await interaction.response.send_message("❌ Cog nicht gefunden. Bitte wende dich an einen Admin.", ephemeral=True)
            return
        modal = FeedbackModal(cog, "suggestion")
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
        
        # ALLE Duty-Supporter pingen (nicht nur einen zufälligen!)
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
            # Nur die Duty-Rolle pingen statt alle Mitglieder einzeln
            await call_channel.send(content=duty_role.mention, embed=embed,
                                   allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
        
        supporter_names = ", ".join([m.display_name for m in duty_members[:5]])
        if len(duty_members) > 5:
            supporter_names += f" und {len(duty_members) - 5} weitere"
        
        await interaction.response.send_message(
            f"✅ {supporter_names} wurden gerufen! Bitte warte kurz, die Supporter werden sich bei dir melden oder du sollst dich in einem bestimmten Channel einfinden.",
            ephemeral=True
        )


async def setup(bot: Red):
    """Lädt den Cog"""
    cog = SupportCog(bot)
    # Die persistent Views werden jetzt in cog_load() registriert
    await bot.add_cog(cog)


async def teardown(bot: Red):
    """Entfernt den Cog"""
    await bot.remove_cog("SupportCog")
