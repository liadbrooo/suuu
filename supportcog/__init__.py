"""
Support & Whitelist Warteraum Cog für RedBot mit On-Duty System & Button-Interface
Verbesserte Version mit getrennten Panel/Log Channels und besserer UX
Version: 2.1 (BanSync + Anti-Nuke + Auto-Role-Sync + Setup-Wizard)

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

from __future__ import annotations

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from redbot.core.i18n import Translator
from datetime import datetime, timedelta, timezone
from typing import Optional, Union
import asyncio
import hashlib
import logging
import re
import random

_ = Translator("SupportCog", __file__)
log = logging.getLogger("red.supportcog")

# Stable, deterministic Config identifier (derived from module name).
# Avoids the magic-number smell of the previous hard-coded literal.
_CONF_IDENTIFIER = int(hashlib.md5(__name__.encode()).hexdigest()[:12], 16)

# Background-loop intervals
_DUTY_SWEEP_INTERVAL = 60  # seconds between duty-expiry sweeps
_TEMP_WL_SWEEP_INTERVAL = 60  # seconds between temp-whitelist sweeps


def _now() -> datetime:
    """Timezone-aware UTC now. Replaces deprecated _now()."""
    return datetime.now(timezone.utc)


def _now_ts() -> int:
    """Current POSIX timestamp (seconds, UTC-safe)."""
    return int(_now().timestamp())


def _from_ts(ts: float) -> datetime:
    """Parse a POSIX timestamp into a timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _fmt_h_m(seconds: int) -> str:
    """Format a duration as 'Xh Ym'."""
    seconds = int(seconds)
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


class SupportCog(commands.Cog):
    """Cog für Support-Warteraum Benachrichtigungen mit Button-Duty-System"""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=_CONF_IDENTIFIER)

        # Per-(guild, member) locks to prevent race conditions in duty start/stop
        self._duty_locks: dict[tuple[int, int], asyncio.Lock] = {}
        # Background task handles
        self._duty_expiry_task: Optional[asyncio.Task] = None

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
            
            # NEUE WHITELIST FEATURES
            "whitelist_welcome_message": None,  # Individuelle Willkommensnachricht nach Whitelist
            "whitelist_cooldown_minutes": 5,  # Cooldown in Minuten für Whitelist desselben Users
            "whitelist_notes": {},  # Notizen zu Usern: {user_id: [{\"author_id\": id, \"note\": text, \"timestamp\": ts}]}
            "whitelist_temp_entries": {},  # Temporäre Whitelist-Einträge: {user_id: expiry_timestamp}
            
            # SUPPORT CASE TRACKING
            "active_support_cases": {},  # {message_id: {"user_id": user_id, "helper_id": helper_id, "timestamp": timestamp, "channel": channel_id}}
            "active_whitelist_cases": {},  # {message_id: {"user_id": user_id, "helper_id": helper_id, "timestamp": timestamp, "channel": channel_id}}
            
            # TICKET SYSTEM
            "ticket_category": None,  # Kategorie für Tickets
            "ticket_panel_channel": None,  # Channel für Ticket-Panel
            "ticket_panel_message_id": None,  # Message ID des Ticket-Panels
            "ticket_support_role": None,  # Rolle die Tickets bearbeiten kann
            "ticket_log_channel": None,  # Channel für Ticket-Erstellungs/Schließungs-Logs
            "ticket_counter": 0,  # Laufende Nummer für Ticket-Namen (ticket-001, ticket-002, ...)
            # Erweiterte Ticket-Optionen
            "ticket_welcome_message": "Willkommen zu deinem Ticket! Ein Teammitglied wird sich gleich um dich kümmern.",
            "ticket_panel_emoji": "🎫",
            "ticket_panel_color": "blurple",  # blurple, red, green, grey
            "ticket_panel_title": "🎫 Ticket erstellen",
            "ticket_panel_description": "Brauchst du Hilfe? Klicke auf den Button unten um ein Ticket zu erstellen.",
            "ticket_modal_enabled": True,
            "ticket_modal_question": "Worum geht es in deinem Ticket?",
            "ticket_modal_placeholder": "Kurze Beschreibung deines Anliegens...",
            "ticket_dm_on_close": False,
            "ticket_auto_close_hours": 0,  # 0 = deaktiviert
            "ticket_transcript": True,  # Sende Transcript bei Schließung in den Log-Channel
            "ticket_user_can_close": True,  # User darf eigenes Ticket schließen
            "ticket_claim_enabled": True,  # Claim-System aktiv
            "ticket_blacklist": [],  # Liste von User-IDs die keine Tickets erstellen dürfen
            "ticket_max_open": 1,  # Maximal gleichzeitig offene Tickets pro User
            "ticket_active": {},  # Runtime: {user_id: [channel_ids]} — aktuell offene Tickets
            "ticket_panel_button_text": "Ticket erstellen",
            # MULTI-KATEGORIE-SYSTEM
            # ticket_categories: {
            #   "support": {
            #     "name": "Allgemeiner Support", "emoji": "🎫", "color": "blurple",
            #     "button_text": "Support", "category_id": 123, "support_role_id": 456,
            #     "description": "Allgemeine Fragen und Probleme",
            #     "welcome_message": "Ein Teammitglied hilft dir gleich!",
            #     "modal_question": "Was ist dein Problem?",
            #     "modal_placeholder": "Beschreibe dein Anliegen..."
            #   },
            #   "report": { ... }, ...
            # }
            "ticket_categories": {},  # Dict von Kategorie-Key → Kategorie-Config
            "ticket_panel_multi_enabled": False,  # Wenn True: Multi-Button-Panel statt einzelne Panels

            # AWAY AUTO-RETURN
            "away_auto_return_minutes": 15,  # Auto-Revert von "away" zu "available" nach X Min (0 = deaktiviert)

            # SMART ESCALATION
            "escalation_enabled": True,  # Wenn nach X Min niemand den User holt, Basis-Rolle pingen
            "escalation_minutes": 5,  # Minuten bis zur Eskalation
            "pending_support_requests": {},  # {message_id: {"user_id": u, "channel_id": c, "sent_ts": ts, "escalated": bool}}
            "pending_whitelist_requests": {},  # gleiche Struktur für Whitelist

            # SUPPORT BLOCKLIST
            "support_blocklist": {},  # {user_id: {"blocked_by": id, "reason": text, "timestamp": ts}}
            "support_blocklist_role": None,  # Rolle die blocken/entblocken darf (Fallback: manage_guild)

            # MODERATION SYSTEM
            "mod_log_channel": None,  # Channel für Moderations-Logs
            "warn_threshold": 3,  # Anzahl Warns vor Auto-Mute
            "mute_duration": 60,  # Standard Mute-Dauer in Minuten
            
            # STATS & TRACKING
            "track_stats": True,  # Ob Statistiken getrackt werden sollen
            "support_stats_channel": None,  # Channel für Support-Statistiken
            
            # DUTY GRANT ROLE SETTINGS
            "support_duty_grant_role": None,  # Rolle die berechtigt ist anderen die Support-Duty-Rolle zu geben

            # ============================================
            # CROSS-SERVER SYNC (BanSync / ModSync)
            # ============================================
            "sync_enabled": False,                # Master-Schalter
            "sync_master_guild_id": None,         # Wenn gesetzt: nur dieser Server ist Master (bei master_to_all)
            "sync_direction": "master_to_all",    # "master_to_all" | "bidirectional"
            "sync_bans": True,
            "sync_unbans": True,
            "sync_timeouts": True,
            "sync_kicks": True,
            "sync_warns": True,
            "sync_audit_log": False,              # Audit-Logs auswerten für manuelle Aktionen
            "sync_log_channel": None,             # Log-Channel für Sync-Events
            "sync_excluded_guilds": [],           # Liste von Guild-IDs die nicht synchronisiert werden
            "sync_role_map": {},                  # Auto-Role-Sync: {source_role_id: [{guild_id, role_id}]}
            "sync_role_sync_enabled": False,
            # Interne Tracking-Daten (runtime, nicht von User gesetzt):
            "sync_recent_actions": {},            # {action_key: ts} — verhindert Rekursion beim Sync

            # ============================================
            # ANTI-NUKE
            # ============================================
            "antinuke_enabled": False,
            "antinuke_ban_threshold": 10,         # Maximale Banns im Zeitfenster
            "antinuke_kick_threshold": 10,
            "antinuke_channel_delete_threshold": 5,
            "antinuke_role_delete_threshold": 5,
            "antinuke_window_seconds": 60,        # Zeitfenster in Sekunden
            "antinuke_action": "strip",           # "strip" (Mod-Rechte entfernen) | "notify" (nur loggen)
            "antinuke_log_channel": None,
            "antinuke_whitelist_roles": [],       # Rollen-IDs die immun sind
            "antinuke_whitelist_users": [],       # User-IDs die immun sind
            "antinuke_tracker": {},               # Runtime: {user_id: {action: [ts, ts, ...]}}
        }

        # Speichert On-Duty Status pro User (für beide Systeme)
        default_member_settings = {
            "on_duty": False,
            "duty_start": None,
            "whitelist_on_duty": False,
            "whitelist_duty_start": None,
            "total_duty_time": 0,  # Gesamte Duty-Zeit in Sekunden (Support)
            "total_whitelist_duty_time": 0,  # Gesamte Duty-Zeit in Sekunden (Whitelist)
            "whitelist_last_added": 0,  # POSIX timestamp of last whitelist grant (for cooldown)
            
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
            "away_since": None,  # Timestamp wann der User auf "away" gesetzt wurde (für Auto-Return)
            "cases_handled": 0,  # Anzahl bearbeiteter Support-Cases (für CSV-Export/Leaderboard)
            "whitelist_cases_handled": 0,  # Anzahl bearbeiteter Whitelist-Cases
        }

        self.config.register_guild(**default_guild_settings)
        self.config.register_member(**default_member_settings)

    def _lock_for(self, guild_id: int, user_id: int) -> asyncio.Lock:
        """Get (or lazily create) an asyncio.Lock keyed by (guild_id, user_id).

        Used to serialize duty start/stop transitions for the same member to
        prevent race conditions that double-count sessions or corrupt stats.
        """
        key = (guild_id, user_id)
        if key not in self._duty_locks:
            self._duty_locks[key] = asyncio.Lock()
        return self._duty_locks[key]

    async def cog_load(self):
        """Wird beim Laden des Cogs aufgerufen - registriert persistente Views"""
        # Registere die persistent Views für Buttons
        self.bot.add_view(DutyButtonView(self))
        self.bot.add_view(WhitelistButtonView(self))
        self.bot.add_view(FeedbackPanelView(self))  # Cog-Referenz mitgeben, vermeidet Reload-Fallback
        self.bot.add_view(SupportCallView(self))
        self.bot.add_view(PersistentWhitelistGrantView(self))  # Persistente View für Whitelist-Rollenvergabe
        # Ticket-System persistente Views
        self.bot.add_view(TicketPanelView(self))
        self.bot.add_view(TicketControlView(self, claim_enabled=True))
        self.bot.add_view(TicketCloseView(self))
        # Multi-Kategorie Panel: dummy View mit sample-Kategorien registrieren
        # (damit persistente Buttons nach Reload wieder funktionieren)
        # Die eigentliche View wird bei `[p]ticketset createmulti` neu erzeugt.
        # Hier registrieren wir nur eine "leere" View mit dem custom_id Schema
        # damit Discord die Buttons nach Restart noch erkennt.
        try:
            self.bot.add_view(TicketMultiPanelView(self, []))
        except Exception:
            pass  # ignore if registration fails (e.g. no categories)
        # Background loop: auto-expire duty sessions + temp whitelists
        if self._duty_expiry_task is None or self._duty_expiry_task.done():
            self._duty_expiry_task = asyncio.create_task(self._background_sweep_loop())
        # Background loop: ticket auto-close
        if not hasattr(self, "_ticket_auto_close_task") or self._ticket_auto_close_task is None or self._ticket_auto_close_task.done():
            self._ticket_auto_close_task = asyncio.create_task(self._ticket_auto_close_loop())

    async def cog_unload(self):
        """Cleanup beim Entladen - Background-Tasks abbrechen."""
        if self._duty_expiry_task is not None and not self._duty_expiry_task.done():
            self._duty_expiry_task.cancel()
            try:
                await self._duty_expiry_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Fehler beim Stoppen des Background-Sweep-Loops")
        if hasattr(self, "_ticket_auto_close_task") and self._ticket_auto_close_task is not None and not self._ticket_auto_close_task.done():
            self._ticket_auto_close_task.cancel()
            try:
                await self._ticket_auto_close_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Fehler beim Stoppen des Ticket-Auto-Close-Loops")

    async def _background_sweep_loop(self):
        """Background loop that periodically:
        - auto-removes duty sessions past their configured timeout
        - expires temporary whitelist entries
        - auto-reverts "away" status after configurable minutes (Away Auto-Return)
        - escalates unanswered support/whitelist requests (Smart Escalation)
        """
        await self.bot.wait_until_red_ready()
        while True:
            try:
                async for guild in self.bot.fetch_guilds(limit=None):
                    try:
                        await self._sweep_expired_duty(guild)
                        await self._sweep_expired_temp_whitelist(guild)
                        await self._sweep_away_auto_return(guild)
                        await self._sweep_pending_requests(guild)
                    except Exception:
                        log.exception("Fehler im Sweep für Guild %s", guild.id)
            except Exception:
                log.exception("Schwerer Fehler im Background-Sweep-Loop")
            await asyncio.sleep(_DUTY_SWEEP_INTERVAL)

    async def _sweep_expired_duty(self, guild: discord.Guild):
        """Auto-end duty sessions that exceeded the configured timeout."""
        # Support duty sweep
        cfg = self.config.guild(guild)
        auto_remove = await cfg.auto_remove_duty()
        if auto_remove:
            timeout_hours = await cfg.duty_timeout()
            if timeout_hours and timeout_hours > 0:
                cutoff = _now_ts() - timeout_hours * 3600
                all_members = await self.config.all_members(guild)
                for uid, data in all_members.items():
                    if not data.get("on_duty"):
                        continue
                    start_ts = data.get("duty_start")
                    if start_ts and start_ts < cutoff:
                        member = guild.get_member(uid)
                        if member is None:
                            continue
                        try:
                            async with self._lock_for(guild.id, uid):
                                await self._finalize_duty_stop(member, whitelist=False, reason="Auto-Abmeldung (Timeout)")
                        except Exception:
                            log.exception("Auto-Abmeldung fehlgeschlagen für User %s", uid)
        # Whitelist duty sweep
        wl_auto_remove = await cfg.whitelist_auto_remove_duty()
        if wl_auto_remove:
            timeout_hours = await cfg.whitelist_duty_timeout()
            if timeout_hours and timeout_hours > 0:
                cutoff = _now_ts() - timeout_hours * 3600
                all_members = await self.config.all_members(guild)
                for uid, data in all_members.items():
                    if not data.get("whitelist_on_duty"):
                        continue
                    start_ts = data.get("whitelist_duty_start")
                    if start_ts and start_ts < cutoff:
                        member = guild.get_member(uid)
                        if member is None:
                            continue
                        try:
                            async with self._lock_for(guild.id, uid):
                                await self._finalize_duty_stop(member, whitelist=True, reason="Auto-Abmeldung (Timeout)")
                        except Exception:
                            log.exception("WL Auto-Abmeldung fehlgeschlagen für User %s", uid)

    async def _sweep_expired_temp_whitelist(self, guild: discord.Guild):
        """Remove expired temporary whitelist entries."""
        temp_entries = await self.config.guild(guild).whitelist_temp_entries()
        if not temp_entries:
            return
        now = _now_ts()
        approved_role = await self.get_whitelist_approved_role(guild)
        changed = False
        for uid_str, exp_ts in list(temp_entries.items()):
            if exp_ts > now:
                continue
            try:
                uid = int(uid_str)
            except (TypeError, ValueError):
                temp_entries.pop(uid_str, None)
                changed = True
                continue
            member = guild.get_member(uid)
            if member and approved_role and member.get_role(approved_role.id) is not None:
                try:
                    await member.remove_roles(approved_role, reason="Temporäre Whitelist abgelaufen")
                except discord.HTTPException:
                    log.warning("Konnte temp. WL-Rolle nicht entfernen für User %s", uid)
            # Log the auto-removal in the entries channel
            entries_channel = await self.get_whitelist_entries_channel(guild)
            if entries_channel:
                try:
                    embed = discord.Embed(
                        title="⌛ Temporärer Whitelist-Eintrag abgelaufen",
                        description=f"{member.mention if member else f'`{uid}`'} wurde automatisch nach Ablauf entfernt.",
                        color=discord.Color.dark_grey(),
                        timestamp=_now(),
                    )
                    await entries_channel.send(embed=embed)
                except discord.HTTPException:
                    pass
            temp_entries.pop(uid_str, None)
            changed = True
        if changed:
            await self.config.guild(guild).whitelist_temp_entries.set(temp_entries)

    async def _sweep_away_auto_return(self, guild: discord.Guild):
        """Reverts duty_status from 'away' to 'available' after the configured timeout.

        This is a quality-of-life feature: staff who forgot to set themselves
        back to available will be auto-reverted, so they don't miss pings.
        Set away_auto_return_minutes to 0 to disable.
        """
        minutes = await self.config.guild(guild).away_auto_return_minutes() or 0
        if minutes <= 0:
            return
        cutoff = _now_ts() - minutes * 60
        all_members = await self.config.all_members(guild)
        for uid, data in all_members.items():
            if data.get("duty_status") != "away":
                continue
            if not data.get("on_duty"):
                continue
            away_since = data.get("away_since")
            if not away_since or int(away_since) > cutoff:
                continue
            member = guild.get_member(uid)
            if member is None:
                continue
            try:
                async with self._lock_for(guild.id, uid):
                    await self._set_duty_status(member, "available")
                log.info("Away-Auto-Return für User %s in Guild %s", uid, guild.id)
            except Exception:
                log.exception("Away-Auto-Return fehlgeschlagen für User %s", uid)

    async def _sweep_pending_requests(self, guild: discord.Guild):
        """Smart Escalation: ping the base role if a support/whitelist request
        hasn't been picked up within `escalation_minutes`.

        Also cleans up stale entries from pending_support_requests /
        pending_whitelist_requests (older than 1 hour, regardless of escalation).
        """
        if not await self.config.guild(guild).escalation_enabled():
            return
        esc_minutes = await self.config.guild(guild).escalation_minutes() or 0
        if esc_minutes <= 0:
            return
        esc_cutoff = _now_ts() - esc_minutes * 60
        cleanup_cutoff = _now_ts() - 3600  # 1 h

        for key, channel_getter, base_role_key, system_name in [
            ("pending_support_requests", self.get_support_channel, "role", "Support"),
            ("pending_whitelist_requests", self.get_whitelist_channel, "whitelist_role", "Whitelist"),
        ]:
            pending = await self.config.guild(guild).get_attr(key)()
            if not pending:
                continue
            changed = False
            channel = await channel_getter(guild)
            base_role_id = await self.config.guild(guild).get_attr(base_role_key)()
            base_role = guild.get_role(base_role_id) if base_role_id else None
            for msg_id_str, info in list(pending.items()):
                sent_ts = info.get("sent_ts", 0)
                escalated = info.get("escalated", False)
                # Cleanup stale entries
                if sent_ts < cleanup_cutoff:
                    pending.pop(msg_id_str, None)
                    changed = True
                    continue
                # Escalate if not yet escalated and cutoff reached
                if not escalated and sent_ts < esc_cutoff:
                    if channel and base_role:
                        try:
                            user_id = info.get("user_id")
                            user_mention = f"<@{user_id}>" if user_id else "Unbekannt"
                            embed = discord.Embed(
                                title=f"⚠️ {system_name}-Eskalation",
                                description=(
                                    f"{user_mention} wartet seit **{esc_minutes} Minuten** im Warteraum!\n"
                                    f"Niemand im Duty hat reagiert — Basis-Rolle wird gepingt."
                                ),
                                color=discord.Color.red(),
                                timestamp=_now(),
                            )
                            embed.set_footer(text=f"{system_name}-Eskalation • Auto-Ping")
                            await channel.send(
                                content=base_role.mention,
                                embed=embed,
                                allowed_mentions=discord.AllowedMentions(roles=[base_role]),
                            )
                        except discord.HTTPException:
                            log.warning("Konnte Eskalation nicht senden (Guild %s)", guild.id)
                    info["escalated"] = True
                    pending[msg_id_str] = info
                    changed = True
            if changed:
                await self.config.guild(guild).get_attr(key).set(pending)

    async def _finalize_duty_stop(self, member: discord.Member, *, whitelist: bool, reason: str = ""):
        """Internal helper that performs the actual config/role updates for duty stop.
        Assumes the caller already holds _lock_for(guild.id, member.id).
        """
        guild = member.guild
        if whitelist:
            start_ts = await self.config.member(member).whitelist_duty_start()
            if not start_ts:
                # Already stopped.
                return
            duration = max(0, _now_ts() - int(start_ts))
            total = await self.config.member(member).total_whitelist_duty_time() or 0
            await self.config.member(member).total_whitelist_duty_time.set(total + duration)
            await self.config.member(member).whitelist_on_duty.set(False)
            await self.config.member(member).whitelist_duty_start.set(None)
            await self.config.member(member).duty_status.set("available")
        else:
            start_ts = await self.config.member(member).duty_start()
            if not start_ts:
                return
            duration = max(0, _now_ts() - int(start_ts))
            total = await self.config.member(member).total_duty_time() or 0
            await self.config.member(member).total_duty_time.set(total + duration)
            await self.config.member(member).on_duty.set(False)
            await self.config.member(member).duty_start.set(None)
            await self.config.member(member).duty_status.set("available")
            await self.config.member(member).last_duty_end.set(_now_ts())
        # Remove role (ignore forbidden - logged)
        ok = await self.update_duty_role(member, on_duty=False, whitelist=whitelist)
        if not ok:
            log.warning("Konnte Duty-Rolle nicht entfernen für %s in Guild %s", member.id, guild.id)
        # Refresh panels / status displays
        try:
            if whitelist:
                await self.update_whitelist_panel_display(guild)
            else:
                await self.update_panel_display(guild)
                await self.update_status_display(guild)
        except Exception:
            log.exception("Panel-Update nach Duty-Stop fehlgeschlagen")
        # Send log entry
        try:
            log_channel = await (self.get_whitelist_log_channel(guild) if whitelist else self.get_log_channel(guild))
            if log_channel:
                system_name = "Whitelist" if whitelist else "Support"
                embed = discord.Embed(
                    title=f"⏰ {system_name}-Duty automatisch beendet",
                    description=f"{member.mention} wurde nach Ablauf des Timeouts automatisch abgemeldet.\nDauer: {_fmt_h_m(duration)}",
                    color=discord.Color.orange(),
                    timestamp=_now(),
                )
                if reason:
                    embed.add_field(name="Grund", value=reason, inline=False)
                await log_channel.send(embed=embed)
        except Exception:
            log.exception("Konnte Auto-Duty-Stop Log nicht senden")

    async def get_or_create_duty_role(self, guild: discord.Guild, whitelist: bool = False) -> Optional[discord.Role]:
        """Erstellt oder holt die automatische Duty-Rolle (mit_side_effect: legt sie ggf. an)."""
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
                permissions=discord.Permissions.none(),  # Pure ping-marker role
                mentionable=True,
                reason=reason
            )
            if whitelist:
                await self.config.guild(guild).whitelist_duty_role.set(duty_role.id)
            else:
                await self.config.guild(guild).duty_role.set(duty_role.id)
            return duty_role
        except (discord.Forbidden, discord.HTTPException):
            log.warning("Konnte Duty-Rolle nicht erstellen in Guild %s", guild.id)
            return None

    async def get_duty_role(self, guild: discord.Guild, whitelist: bool = False) -> Optional[discord.Role]:
        """Read-only lookup of the duty role. Never creates a new role.
        Use this in display/stats paths; reserve get_or_create_duty_role for duty-start.
        """
        duty_role_id = await (self.config.guild(guild).whitelist_duty_role() if whitelist
                              else self.config.guild(guild).duty_role())
        if duty_role_id:
            return guild.get_role(duty_role_id)
        return None

    async def update_duty_role(self, member: discord.Member, on_duty: bool, whitelist: bool = False) -> bool:
        """Fügt oder entfernt die Duty-Rolle eines Members.
        Returns True on success, False if the role couldn't be applied/removed.
        Caller should roll back Config flags on False.
        """
        guild = member.guild
        duty_role = await self.get_or_create_duty_role(guild, whitelist=whitelist)
        if not duty_role:
            return False
        try:
            has_role = member.get_role(duty_role.id) is not None
            if on_duty and not has_role:
                reason = "Whitelist-Duty gestartet" if whitelist else "Support-Duty gestartet"
                await member.add_roles(duty_role, reason=reason)
            elif not on_duty and has_role:
                reason = "Whitelist-Duty beendet" if whitelist else "Support-Duty beendet"
                await member.remove_roles(duty_role, reason=reason)
            return True
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Anpassen der Duty-Rolle in Guild %s", guild.id)
            return False
        except discord.HTTPException as e:
            log.warning("HTTP-Fehler beim Anpassen der Duty-Rolle in Guild %s: %s", guild.id, e)
            return False

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
        """Holt den Log-Channel für Whitelist-Duty-Logs (An-/Abmeldungen).

        PRIMARY resolver for the whitelist domain. Must NOT fall back to
        get_whitelist_channel — that creates an infinite recursion cycle when
        neither channel is configured. Callers must handle None.
        """
        log_channel_id = await self.config.guild(guild).whitelist_log_channel()

        if log_channel_id:
            channel = guild.get_channel(log_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        return None

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
        """Updates the whitelist panel message to show current duty members and grant role button.

        Uses get_duty_role (read-only) so this is safe to call from stats/display paths —
        it will never spawn a new duty role as a side effect.
        """
        panel_message_id = await self.config.guild(guild).whitelist_panel_message_id()
        if not panel_message_id:
            return
        panel_channel = await self.get_whitelist_panel_channel(guild)
        if not panel_channel:
            return
        try:
            panel_message = await panel_channel.fetch_message(panel_message_id)
        except discord.NotFound:
            # Message was deleted — clear the stale ID so we stop retrying.
            await self.config.guild(guild).whitelist_panel_message_id.set(None)
            return
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Abrufen des WL-Panels in Guild %s", guild.id)
            return
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Abrufen des WL-Panels in Guild %s", guild.id)
            return

        # Get current duty members (read-only — never create role here)
        duty_count = 0
        duty_list = []
        duty_role = await self.get_duty_role(guild, whitelist=True)
        role_id = await self.config.guild(guild).whitelist_role()

        if role_id and duty_role:
            base_role = guild.get_role(role_id)
            if base_role:
                # Single Config.all_members call instead of N per-member reads.
                all_members = await self.config.all_members(guild)
                for m in base_role.members:
                    if m.get_role(duty_role.id) is None:
                        continue
                    if all_members.get(m.id, {}).get("whitelist_on_duty"):
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
        new_embed.set_footer(text=f"Aktive Handler: {duty_count} • Die 📋 Whitelist Duty Rolle wird automatisch zugewiesen/entfernt")

        # Re-create view with grant role button if configured
        new_view = WhitelistButtonView(self, guild)
        if has_grant_role:
            grant_view = PersistentWhitelistGrantView(self, guild)
            # Note: re-binding items across View instances relies on undocumented
            # discord.py internals and is fragile. We keep it for backwards compat
            # but log if anything looks off.
            try:
                for item in grant_view.children:
                    new_view.add_item(item)
            except Exception:
                log.exception("Konnte Grant-Button nicht an WL-View anhängen in Guild %s", guild.id)

        try:
            await panel_message.edit(embed=new_embed, view=new_view)
        except discord.NotFound:
            await self.config.guild(guild).whitelist_panel_message_id.set(None)
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Editieren des WL-Panels in Guild %s", guild.id)
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Editieren des WL-Panels in Guild %s", guild.id)

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
        """Holt den Log-Channel NUR für Duty-Logs (An/Abmeldungen).

        This is the PRIMARY channel resolver for the support domain.
        It must NOT fall back to get_support_channel — that would create an
        infinite recursion cycle (support_channel <-> log_channel) when neither
        is configured. Returning None here is correct; callers must handle it.
        """
        log_channel_id = await self.config.guild(guild).log_channel()

        if log_channel_id:
            channel = guild.get_channel(log_channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                return channel

        return None

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
        """Verarbeitet Voice-Updates für das Support-System (JOIN + LEAVE)."""
        room_id = await self.config.guild(guild).room()
        role_id = await self.config.guild(guild).role()
        use_embed = await self.config.guild(guild).use_embed()

        if not all([room_id, role_id]):
            return

        joined = after.channel is not None and after.channel.id == room_id
        left = (
            before.channel is not None
            and before.channel.id == room_id
            and (after.channel is None or after.channel.id != room_id)
        )
        # If the user just moved within the same room (impossible) — skip.
        if not joined and not left:
            return
        # If both branches hit (e.g. rapid channel swap), prefer JOIN.
        if joined and left:
            left = False

        if joined:
            await self._notify_support_join(member, after.channel, guild, use_embed, role_id, room_id)
        elif left:
            await self._notify_support_leave(member, before.channel, guild, use_embed, role_id)

    async def _notify_support_join(self, member: discord.Member, channel: discord.VoiceChannel, guild: discord.Guild, use_embed: bool, role_id: int, room_id: int):
        """User hat den Support-Warteraum soeben betreten."""
        try:
            # Blocklist-Prüfung: blockierte User keine Notification
            blocklist = await self.config.guild(guild).support_blocklist() or {}
            if str(member.id) in blocklist:
                log.info("Blockierter User %s hat Support-Raum betreten (Guild %s) — keine Notification", member.id, guild.id)
                return

            support_channel = await self.get_support_channel(guild)
            if not support_channel:
                return

            base_role = guild.get_role(role_id)
            if not base_role:
                return

            # Read-only lookup of the duty role — do NOT create one as a side effect here.
            duty_role = await self.get_duty_role(guild, whitelist=False)

            # Hole alle On-Duty User MIT DER DUTY ROLLE (single Config.all_members call)
            duty_members = []
            if duty_role:
                all_members = await self.config.all_members(guild)
                for m in base_role.members:
                    if m.get_role(duty_role.id) is None:
                        continue
                    if all_members.get(m.id, {}).get("on_duty"):
                        duty_members.append(m)

            user_mention = member.mention
            user_avatar = member.display_avatar.url

            if use_embed:
                embed = discord.Embed(
                    title="🎧 Neue Support-Anfrage",
                    description=f"{user_mention} hat den Support-Warteraum betreten und wartet auf Hilfe!",
                    color=discord.Color.orange(),
                    timestamp=_now()
                )
                embed.set_thumbnail(url=user_avatar)
                embed.add_field(
                    name="👤 Nutzer",
                    value=f"{user_mention}\n(`{member.display_name}`)",
                    inline=True
                )
                if duty_members:
                    duty_list = "\n".join([f"• {m.display_name}" for m in duty_members[:5]])
                    if len(duty_members) > 5:
                        duty_list += f"\n• ...und {len(duty_members) - 5} weitere"
                    embed.add_field(name="🟢 Verfügbare Supporter", value=duty_list, inline=True)
                else:
                    embed.add_field(
                        name="🔴 Keine Supporter verfügbar",
                        value="Niemand ist gerade im Dienst!\n*Benutze `[p]supportset panelchannel` und klicke auf 'Duty Starten'*",
                        inline=True
                    )
                embed.add_field(name="📍 Channel", value=channel.mention, inline=True)
                embed.set_footer(text="Support Warteraum System • On-Duty aktiv")

                view = discord.ui.View(timeout=None)
                if duty_members:
                    button = discord.ui.Button(
                        label="User zu mir holen",
                        style=discord.ButtonStyle.green,
                        emoji="🎧",
                        custom_id=f"fetch_user_{member.id}",
                    )
                    button.callback = self.create_fetch_user_callback(member, channel)
                    view.add_item(button)

                if duty_role and duty_members:
                    sent_msg = await support_channel.send(
                        content=duty_role.mention, embed=embed, view=view,
                        allowed_mentions=discord.AllowedMentions(roles=[duty_role]),
                    )
                elif base_role:
                    sent_msg = await support_channel.send(
                        content=base_role.mention, embed=embed, view=view,
                        allowed_mentions=discord.AllowedMentions(roles=[base_role]),
                    )
                else:
                    sent_msg = await support_channel.send(embed=embed, view=view)

                # Pending-Request registrieren für Smart-Escalation
                if sent_msg is not None:
                    await self._register_pending_request(guild, sent_msg.id, member.id, whitelist=False)
            else:
                if duty_role and duty_members:
                    message = f"🎧 {duty_role.mention} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({channel.mention})"
                    await support_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    message = f"🎧 {base_role.mention} | {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({channel.mention}) - Niemand im Duty!"
                    await support_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await support_channel.send(f"🎧 {user_mention} (`{member.display_name}`) ist im Support-Warteraum ({channel.mention})")
        except discord.Forbidden as e:
            log.warning("Forbidden beim Senden in Support-Channel (Guild %s): %s", guild.id, e)
        except discord.HTTPException:
            log.exception("HTTP-Fehler in Support-Warteraum (Guild %s)", guild.id)
        except Exception:
            log.exception("Unerwarteter Fehler in _notify_support_join (Guild %s)", guild.id)

    async def _notify_support_leave(self, member: discord.Member, channel: discord.VoiceChannel, guild: discord.Guild, use_embed: bool, role_id: int):
        """User hat den Support-Warteraum verlassen."""
        try:
            support_channel = await self.get_support_channel(guild)
            if not support_channel:
                return
            if use_embed:
                embed = discord.Embed(
                    title="🎧 Support-Warteraum verlassen",
                    description=f"{member.mention} (`{member.display_name}`) hat den Warteraum verlassen.",
                    color=discord.Color.light_gray(),
                    timestamp=_now(),
                )
                embed.add_field(name="📍 Channel", value=channel.mention, inline=True)
                embed.set_footer(text="Support Warteraum System")
                await support_channel.send(embed=embed)
            else:
                await support_channel.send(f"🎧 {member.mention} (`{member.display_name}`) hat den Support-Warteraum verlassen.")
        except discord.Forbidden as e:
            log.warning("Forbidden beim Senden des Leave-Events (Guild %s): %s", guild.id, e)
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Senden des Leave-Events (Guild %s)", guild.id)
        except Exception:
            log.exception("Unerwarteter Fehler in _notify_support_leave (Guild %s)", guild.id)

    async def _handle_whitelist_voice_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState, guild: discord.Guild):
        """Verarbeitet Voice-Updates für das Whitelist-System (JOIN + LEAVE)."""
        room_id = await self.config.guild(guild).whitelist_room()
        role_id = await self.config.guild(guild).whitelist_role()
        use_embed = await self.config.guild(guild).use_embed()

        if not all([room_id, role_id]):
            return

        joined = after.channel is not None and after.channel.id == room_id
        left = (
            before.channel is not None
            and before.channel.id == room_id
            and (after.channel is None or after.channel.id != room_id)
        )
        if not joined and not left:
            return
        if joined and left:
            left = False

        if joined:
            await self._notify_whitelist_join(member, after.channel, guild, use_embed, role_id)
        elif left:
            await self._notify_whitelist_leave(member, before.channel, guild, use_embed)

    async def _notify_whitelist_join(self, member: discord.Member, channel: discord.VoiceChannel, guild: discord.Guild, use_embed: bool, role_id: int):
        """User hat den Whitelist-Warteraum soeben betreten."""
        try:
            # Blocklist-Prüfung (selbe Liste wie Support)
            blocklist = await self.config.guild(guild).support_blocklist() or {}
            if str(member.id) in blocklist:
                log.info("Blockierter User %s hat WL-Raum betreten (Guild %s) — keine Notification", member.id, guild.id)
                return

            whitelist_channel = await self.get_whitelist_channel(guild)
            if not whitelist_channel:
                return

            base_role = guild.get_role(role_id)
            if not base_role:
                return

            # Read-only lookup — never create role as side effect of a voice event.
            duty_role = await self.get_duty_role(guild, whitelist=True)

            duty_members = []
            if duty_role:
                all_members = await self.config.all_members(guild)
                for m in base_role.members:
                    if m.get_role(duty_role.id) is None:
                        continue
                    if all_members.get(m.id, {}).get("whitelist_on_duty"):
                        duty_members.append(m)

            user_mention = member.mention
            user_avatar = member.display_avatar.url

            if use_embed:
                embed = discord.Embed(
                    title="📋 Neue Whitelist-Anfrage",
                    description=f"{user_mention} hat den Whitelist-Warteraum betreten und wartet auf Bearbeitung!",
                    color=discord.Color.blue(),
                    timestamp=_now()
                )
                embed.set_thumbnail(url=user_avatar)
                embed.add_field(
                    name="👤 Nutzer",
                    value=f"{user_mention}\n(`{member.display_name}`)",
                    inline=True
                )
                if duty_members:
                    duty_list = "\n".join([f"• {m.display_name}" for m in duty_members[:5]])
                    if len(duty_members) > 5:
                        duty_list += f"\n• ...und {len(duty_members) - 5} weitere"
                    embed.add_field(name="🔵 Verfügbare Whitelist-Handler", value=duty_list, inline=True)
                else:
                    embed.add_field(name="🔴 Keine Handler verfügbar", value="Niemand ist gerade im Dienst!", inline=True)
                embed.add_field(name="📍 Channel", value=channel.mention, inline=True)
                embed.set_footer(text="Whitelist Warteraum System • On-Duty aktiv")

                view = discord.ui.View(timeout=None)
                button = discord.ui.Button(
                    label="User zu mir holen",
                    style=discord.ButtonStyle.green,
                    emoji="📋",
                    custom_id=f"fetch_whitelist_user_{member.id}",
                )
                button.callback = self.create_fetch_user_callback(member, channel, whitelist=True)
                view.add_item(button)

                grant_role_id = await self.config.guild(guild).whitelist_grant_role()
                if grant_role_id:
                    grant_button = GrantWhitelistButton(self, guild, member.id)
                    view.add_item(grant_button)

                if duty_role and duty_members:
                    sent_msg = await whitelist_channel.send(
                        content=duty_role.mention, embed=embed, view=view,
                        allowed_mentions=discord.AllowedMentions(roles=[duty_role]),
                    )
                elif base_role:
                    sent_msg = await whitelist_channel.send(
                        content=base_role.mention, embed=embed, view=view,
                        allowed_mentions=discord.AllowedMentions(roles=[base_role]),
                    )
                else:
                    sent_msg = await whitelist_channel.send(embed=embed, view=view)

                # Pending-Request registrieren für Smart-Escalation
                if sent_msg is not None:
                    await self._register_pending_request(guild, sent_msg.id, member.id, whitelist=True)
            else:
                if duty_role and duty_members:
                    message = f"📋 {duty_role.mention} | {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({channel.mention})"
                    await whitelist_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
                elif base_role:
                    message = f"📋 {base_role.mention} | {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({channel.mention}) - Niemand im Duty!"
                    await whitelist_channel.send(message, allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                else:
                    await whitelist_channel.send(f"📋 {user_mention} (`{member.display_name}`) ist im Whitelist-Warteraum ({channel.mention})")
        except discord.Forbidden as e:
            log.warning("Forbidden beim Senden in WL-Channel (Guild %s): %s", guild.id, e)
        except discord.HTTPException:
            log.exception("HTTP-Fehler in WL-Warteraum (Guild %s)", guild.id)
        except Exception:
            log.exception("Unerwarteter Fehler in _notify_whitelist_join (Guild %s)", guild.id)

    async def _notify_whitelist_leave(self, member: discord.Member, channel: discord.VoiceChannel, guild: discord.Guild, use_embed: bool):
        """User hat den Whitelist-Warteraum verlassen."""
        try:
            whitelist_channel = await self.get_whitelist_channel(guild)
            if not whitelist_channel:
                return
            if use_embed:
                embed = discord.Embed(
                    title="📋 Whitelist-Warteraum verlassen",
                    description=f"{member.mention} (`{member.display_name}`) hat den Warteraum verlassen.",
                    color=discord.Color.light_gray(),
                    timestamp=_now(),
                )
                embed.add_field(name="📍 Channel", value=channel.mention, inline=True)
                embed.set_footer(text="Whitelist Warteraum System")
                await whitelist_channel.send(embed=embed)
            else:
                await whitelist_channel.send(f"📋 {member.mention} (`{member.display_name}`) hat den Whitelist-Warteraum verlassen.")
        except discord.Forbidden as e:
            log.warning("Forbidden beim Senden des WL-Leave-Events (Guild %s): %s", guild.id, e)
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Senden des WL-Leave-Events (Guild %s)", guild.id)
        except Exception:
            log.exception("Unerwarteter Fehler in _notify_whitelist_leave (Guild %s)", guild.id)

    # HELPER METHODS - MÜSSEN VOR DEN COMMANDS DEFINIERT WERDEN!
    def _parse_channel_id(self, channel: str) -> Optional[int]:
        """Parse a channel mention or ID to an integer ID.

        Accepts forms like `<#123>`, `123`, or whitespace-padded variants.
        Returns None on parse failure. NOTE: this is a fallback for legacy
        callers; new code should prefer `discord.TextChannel` / `discord.VoiceChannel`
        converters in command signatures.
        """
        if channel is None:
            return None
        channel = channel.strip()
        if channel.startswith("<#") and channel.endswith(">"):
            inner = channel[2:-1]
            # Older clients sometimes produce `<#!123>` (nickname-mention form).
            if inner.startswith("!"):
                inner = inner[1:]
            try:
                return int(inner)
            except ValueError:
                return None
        try:
            return int(channel)
        except ValueError:
            return None

    def _parse_voice_channel_id(self, channel: str) -> Optional[int]:
        """Same parsing logic as _parse_channel_id (kept as a separate name for
        readability of call sites that explicitly expect a voice channel)."""
        return self._parse_channel_id(channel)

    def _parse_role_id(self, role: str) -> Optional[int]:
        """Parse a role mention or ID to an integer ID. Accepts `<@&123>` and `123`."""
        if role is None:
            return None
        role = role.strip()
        if role.startswith("<@&") and role.endswith(">"):
            try:
                return int(role[3:-1])
            except ValueError:
                return None
        try:
            return int(role)
        except ValueError:
            return None

    async def _register_pending_request(self, guild: discord.Guild, message_id: int, user_id: int, *, whitelist: bool):
        """Registriert eine versendete Support/Whitelist-Anfrage für die Smart-Escalation."""
        key = "pending_whitelist_requests" if whitelist else "pending_support_requests"
        pending = await self.config.guild(guild).get_attr(key)() or {}
        pending[str(message_id)] = {
            "user_id": user_id,
            "sent_ts": _now_ts(),
            "escalated": False,
        }
        await self.config.guild(guild).get_attr(key).set(pending)

    async def _unregister_pending_request(self, guild: discord.Guild, message_id: int, *, whitelist: bool):
        """Entfernt eine Anfrage aus der Pending-Liste (z.B. wenn ein Teamler den User geholt hat)."""
        key = "pending_whitelist_requests" if whitelist else "pending_support_requests"
        pending = await self.config.guild(guild).get_attr(key)() or {}
        if str(message_id) in pending:
            pending.pop(str(message_id), None)
            await self.config.guild(guild).get_attr(key).set(pending)
            return True
        return False

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

                # Pending-Request aus der Eskalations-Liste entfernen (Anfrage wurde ja bearbeitet)
                try:
                    await self._unregister_pending_request(guild, interaction.message.id, whitelist=whitelist)
                except Exception:
                    log.exception("Konnte pending_request nicht zu entfernen (msg=%s)", interaction.message.id)

                # cases_handled-Counter für den Teamler hochzählen (für CSV-Export/Leaderboard)
                try:
                    if whitelist:
                        cur = await self.config.member(teamler).whitelist_cases_handled() or 0
                        await self.config.member(teamler).whitelist_cases_handled.set(cur + 1)
                    else:
                        cur = await self.config.member(teamler).cases_handled() or 0
                        await self.config.member(teamler).cases_handled.set(cur + 1)
                except Exception:
                    log.exception("Konnte cases_handled nicht inkrementieren (User %s)", teamler.id)

                # Logge die Aktion
                log_channel = await self.get_log_channel(guild) if not whitelist else await self.get_whitelist_log_channel(guild)
                if log_channel:
                    embed = discord.Embed(
                        title=f"🎧 User geholt ({system_name})",
                        description=f"{teamler.mention} hat {user_to_fetch.mention} aus dem Warteraum geholt.",
                        color=discord.Color.green(),
                        timestamp=_now()
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
        start_time = _now()
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
        async with ctx.typing():
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
            await ctx.send("❌ Kein Status-Display-Channel konfiguriert. Nutze `[p]supportset statusdisplaychannel`.")
            return

        # Erstelle initiales Embed
        embed = discord.Embed(
            title="📊 Live Duty Status Übersicht",
            description="**Aktueller Status aller Teammitglieder im Dienst**\n\n_Lade Status..._",
            color=discord.Color.blue(),
            timestamp=_now()
        )
        embed.add_field(name="🟢 Verfügbar", value="Keine", inline=True)
        embed.add_field(name="🔵 Beschäftigt", value="Keine", inline=True)
        embed.add_field(name="☕ In Pause", value="Keine", inline=True)
        embed.add_field(name="🟡 Abwesend", value="Keine", inline=False)
        embed.add_field(name="📈 Statistik", value="**0** im Duty", inline=False)

        try:
            message = await channel.send(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            await ctx.send("❌ Konnte Status-Display nicht in diesem Channel posten.")
            return
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
            except discord.NotFound:
                # Old message already gone — fine, we create a new one below.
                pass
            except discord.Forbidden:
                log.warning("Fehlende Rechte beim Löschen des alten Panels (Guild %s)", ctx.guild.id)
            except discord.HTTPException:
                log.exception("HTTP-Fehler beim Löschen des alten Panels (Guild %s)", ctx.guild.id)

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

        # Erstelle das Team-Panel (shared logic with [p]teampanel)
        ok = await self.update_team_panel(channel, guild)
        if ok:
            await ctx.send(f"✅ Team-Panel in {channel.mention} erstellt/aktualisiert!")
        else:
            await ctx.send("❌ Konnte Team-Panel nicht erstellen (fehlende Rechte?).")

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
        Gib einem anderen Nutzer die Berechtigung für Whitelist-Aktionen.

        Dies gibt dem Nutzer die whitelist_duty_grant_role, welche ihn berechtigt:
        - Den "Whitelist freischalten" Button zu nutzen
        - Den !wl Befehl zu verwenden
        
        Der Nutzer wird NICHT in den Duty-Modus versetzt! Er erhält nur die Berechtigungsrolle.
        Erfordert die konfigurierte Grant-Rolle (via !whitelistset dutygrantrole eingestellt).
        """
        guild = ctx.guild
        author = ctx.author

        # Prüfe ob Autor die Grant-Rolle hat (die Rolle die via !whitelistset dutygrantrole eingestellt wurde)
        grant_role_id = await self.config.guild(guild).whitelist_duty_grant_role()

        if not grant_role_id:
            await ctx.send("❌ Es wurde keine Whitelist-Duty-Grant-Rolle konfiguriert! Nutze `[p]whitelistset dutygrantrole <Rolle>` um sie festzulegen.")
            return

        has_grant_role = False
        grant_role = guild.get_role(grant_role_id)
        if grant_role and grant_role in author.roles:
            has_grant_role = True

        if not has_grant_role:
            missing_role_msg = f"❌ Du benötigst die {grant_role.mention if grant_role else 'konfigurierte'} Rolle um anderen Nutzern Whitelist-Berechtigungen zu geben!"
            await ctx.send(missing_role_msg)
            return

        # Prüfe ob Target bereits die Always-Allowed Rolle hat
        always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        
        if always_allowed_role and always_allowed_role in target_user.roles:
            await ctx.send(f"ℹ️ {target_user.mention} hat bereits die 'Always Allowed' Rolle und kann immer Whitelist-Aktionen durchführen!")
            return

        # Prüfe ob Target bereits die Grant-Rolle hat
        if grant_role and grant_role in target_user.roles:
            await ctx.send(f"ℹ️ {target_user.mention} hat bereits die Whitelist-Berechtigungsrolle!")
            return

        # Gib dem Target die whitelist_duty_grant_role (das ist die Rolle die bei !whitelistset role eingestellt wurde!)
        # NEIN - warte! Die whitelist_duty_grant_role ist die Rolle die der Author braucht um DEN BEFEHL auszuführen
        # Die Rolle die der TARGET bekommt ist EINE SEPARATE ROLE - nennen wir sie "whitelist_handler_temp_role"
        # ABER laut Anforderung: "die rolle die ich bei !whitelistdutygrant einstelle soll die berechtigung haben leuten die rolle zu geben"
        # Also: 
        # - !whitelistset dutygrantrole setzt die rolle die DEN BEFEHL ausführen darf
        # - !whitelistset role setzt die rolle die für den duty modus notwendig ist (die gepingt wird)
        # - !whitelistdutygrant gibt dem target die rolle (!whitelistset role) - also die whitelist handler role
        
        # Hole die Rolle die via !whitelistset role eingestellt wurde (das ist die Basis-Whitelist-Handler-Rolle)
        whitelist_handler_role_id = await self.config.guild(guild).whitelist_role()
        if not whitelist_handler_role_id:
            await ctx.send("❌ Es wurde keine Whitelist-Handler-Rolle konfiguriert! Nutze `[p]whitelistset role <Rolle>`")
            return
        
        whitelist_handler_role = guild.get_role(whitelist_handler_role_id)
        if not whitelist_handler_role:
            await ctx.send("❌ Die konfigurierte Whitelist-Handler-Rolle existiert nicht mehr!")
            return

        # Gib dem Target die Whitelist-Handler-Rolle (das ermöglicht Button & !wl Nutzung)
        try:
            await target_user.add_roles(whitelist_handler_role, reason=f"Whitelist-Berechtigung von {author.display_name} via !whitelistdutygrant")
        except discord.Forbidden:
            await ctx.send("❌ Ich habe keine Berechtigung diese Rolle zu vergeben!")
            return

        # Log-Nachricht senden
        log_channel = await self.get_whitelist_log_channel(guild)
        start_time = _now()
        if log_channel:
            embed = discord.Embed(
                title="🔵 Whitelist-Berechtigung zugewiesen",
                description=f"{target_user.mention} hat die {whitelist_handler_role.mention} erhalten und kann jetzt Whitelist-Aktionen durchführen!",
                color=discord.Color.blue(),
                timestamp=start_time
            )
            embed.set_thumbnail(url=target_user.display_avatar.url)
            embed.add_field(name="👤 Zugewiesen von", value=f"{author.display_name}", inline=True)
            embed.add_field(name="⚠️ Hinweis", value="Diese Rolle gilt solange bis sie entzogen wird.", inline=True)
            try:
                await log_channel.send(embed=embed)
            except discord.Forbidden:
                pass

        await ctx.send(f"✅ {target_user.mention} hat die {whitelist_handler_role.mention} erhalten und kann jetzt den Button & !wl nutzen!")

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
            timestamp=_now()
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
    
    # ============================================
    # NEUE WHITELIST FEATURES
    # ============================================
    
    @whitelistset.command(name="welcomemessage")
    async def whitelistset_welcomemessage(self, ctx: commands.Context, *, message: str = None):
        """
        Setzt eine individuelle Willkommensnachricht für ge-whitelistete Spieler.
        
        Verwendung: [p]whitelistset welcomemessage <Nachricht>
        Verwende {user} für den Spielernamen und {helper} für den Helper-Namen.
        Ohne Nachricht wird die Standardnachricht verwendet.
        """
        guild = ctx.guild
        
        if message is None:
            await self.config.guild(guild).whitelist_welcome_message.set(None)
            await ctx.send("✅ Die Willkommensnachricht wurde zurückgesetzt. Standardnachricht wird verwendet.")
        else:
            await self.config.guild(guild).whitelist_welcome_message.set(message)
            await ctx.send(f"✅ Die Willkommensnachricht wurde gesetzt:\n\n*{message}*")
    
    @whitelistset.command(name="cooldown")
    async def whitelistset_cooldown(self, ctx: commands.Context, minutes: int = None):
        """
        Setzt den Cooldown für das Whitelisting desselben Spielers.
        
        Verwendung: [p]whitelistset cooldown <Minuten>
        Standard: 5 Minuten
        Setze auf 0 um den Cooldown zu deaktivieren.
        """
        guild = ctx.guild
        
        if minutes is None:
            current = await self.config.guild(guild).whitelist_cooldown_minutes()
            await ctx.send(f"⏱️ Der aktuelle Cooldown beträgt **{current} Minuten**.")
            return
        
        if minutes < 0:
            await ctx.send("❌ Der Cooldown kann nicht negativ sein!")
            return
        
        await self.config.guild(guild).whitelist_cooldown_minutes.set(minutes)
        if minutes == 0:
            await ctx.send("✅ Der Cooldown wurde deaktiviert.")
        else:
            await ctx.send(f"✅ Der Cooldown wurde auf **{minutes} Minuten** gesetzt.")
    
    @commands.command(name="wlstats", aliases=["whitelist_statistics"])
    async def wlstats(self, ctx: commands.Context):
        """
        Zeigt Whitelist-Statistiken an.
        """
        guild = ctx.guild
        approved_role = await self.get_whitelist_approved_role(guild)
        
        if not approved_role:
            await ctx.send("❌ Es wurde keine Whitelist-Approved-Rolle konfiguriert!")
            return
        
        total_whitelisted = len(approved_role.members)
        
        # Hole alle Duty-Zeiten der aktuellen Mitglieder
        total_duty_time = 0
        duty_count = 0
        duty_role = await self.get_or_create_duty_role(guild, whitelist=True)
        
        if duty_role:
            for member in duty_role.members:
                is_duty = await self.config.member(member).whitelist_on_duty()
                if is_duty:
                    duty_count += 1
                total_time = await self.config.member(member).total_whitelist_duty_time()
                total_duty_time += total_time
        
        # Konvertiere zu Stunden
        total_duty_hours = total_duty_time / 3600
        
        embed = discord.Embed(
            title="📊 Whitelist Statistiken",
            description=f"Statistiken für {guild.name}",
            color=discord.Color.green(),
            timestamp=_now()
        )
        
        embed.add_field(name="👥 Whitelisted Spieler", value=f"**{total_whitelisted}**", inline=True)
        embed.add_field(name="🔵 Aktive Handler", value=f"**{duty_count}**", inline=True)
        embed.add_field(name="⏱️ Gesamte Duty-Zeit", value=f"**{total_duty_hours:.1f} Stunden**", inline=True)
        
        # Top 5 Handler nach Duty-Zeit
        duty_times = []
        if duty_role:
            for member in duty_role.members:
                total_time = await self.config.member(member).total_whitelist_duty_time()
                if total_time > 0:
                    duty_times.append((member, total_time))
        
        duty_times.sort(key=lambda x: x[1], reverse=True)
        
        if duty_times:
            top_handlers = ""
            for i, (member, time) in enumerate(duty_times[:5], 1):
                hours = time / 3600
                top_handlers += f"{i}. {member.display_name}: {hours:.1f}h\n"
            embed.add_field(name="🏆 Top Handler", value=top_handlers, inline=False)
        
        embed.set_footer(text="Whitelist Stats • Aktualisiert")
        await ctx.send(embed=embed)
    
    @commands.command(name="wlcheck", aliases=["checkwl_user", "whitelist_check"])
    async def wlcheck(self, ctx: commands.Context, user: discord.Member = None):
        """
        Überprüft den Whitelist-Status eines Spielers.
        
        Verwendung: [p]wlcheck [@User] oder [p]wlcheck <User-ID>
        """
        guild = ctx.guild
        approved_role = await self.get_whitelist_approved_role(guild)
        
        if not approved_role:
            await ctx.send("❌ Es wurde keine Whitelist-Approved-Rolle konfiguriert!")
            return
        
        if user is None:
            user = ctx.author
        
        is_whitelisted = approved_role in user.roles
        
        embed = discord.Embed(
            title="🔍 Whitelist Status",
            color=discord.Color.green() if is_whitelisted else discord.Color.red(),
            timestamp=_now()
        )
        
        embed.set_thumbnail(url=user.display_avatar.url)
        embed.add_field(name="👤 Spieler", value=f"{user.mention}\n*{user.display_name}*", inline=True)
        embed.add_field(name="✅ Status", value="**Whitelisted** ✅" if is_whitelisted else "**Nicht whitelisted** ❌", inline=True)
        
        if is_whitelisted:
            embed.add_field(name="🎮 Rolle", value=f"{approved_role.mention}", inline=True)
        
        # Prüfe auf temporäre Whitelist
        temp_entries = await self.config.guild(guild).whitelist_temp_entries()
        if str(user.id) in temp_entries:
            expiry_ts = temp_entries[str(user.id)]
            try:
                expiry_ts = float(expiry_ts)
            except (TypeError, ValueError):
                expiry_ts = 0
            # Use timezone-aware UTC for the comparison (the sweep loop also uses UTC).
            now_ts = _now_ts()
            if expiry_ts > now_ts:
                remaining_seconds = expiry_ts - now_ts
                hours = remaining_seconds / 3600
                embed.add_field(name="⏳ Temporär", value=f"Läuft ab in **{hours:.1f} Stunden**", inline=False)
            else:
                # Abgelaufen - Rolle entfernen
                if user.get_role(approved_role.id) is not None:
                    try:
                        await user.remove_roles(approved_role, reason="Temporäre Whitelist abgelaufen")
                    except discord.HTTPException:
                        log.warning("Konnte abgelaufene temp. WL-Rolle nicht entfernen (User %s)", user.id)
                temp_entries.pop(str(user.id), None)
                await self.config.guild(guild).whitelist_temp_entries.set(temp_entries)
                embed.add_field(name="⏳ Temporär", value="**Abgelaufen** (Rolle entfernt)", inline=False)
        
        # Zeige Notizen falls vorhanden
        notes = await self.config.guild(guild).whitelist_notes()
        if str(user.id) in notes and notes[str(user.id)]:
            user_notes = notes[str(user.id)]
            embed.add_field(name="📝 Notizen", value=f"**{len(user_notes)}** Notiz(en) vorhanden", inline=False)
        
        embed.set_footer(text=f"Geprüft von {ctx.author.display_name}")
        await ctx.send(embed=embed)
    
    @commands.command(name="wlnote", aliases=["whitelist_note", "wl_notes"])
    async def wlnote(self, ctx: commands.Context, user: discord.Member, *, note: str = None):
        """
        Fügt eine Notiz zu einem Spieler hinzu oder zeigt alle Notizen an.
        
        Verwendung: 
        - [p]wlnote @User <Notiztext> - Fügt Notiz hinzu
        - [p]wlnote @User - Zeigt alle Notizen
        """
        guild = ctx.guild
        
        # Berechtigung prüfen
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in ctx.author.roles:
                has_base_role = True
        
        is_on_duty = await self.config.member(ctx.author).whitelist_on_duty()
        always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in ctx.author.roles
        
        if not has_always_allowed and not has_base_role and not is_on_duty:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle, musst im Duty sein oder die 'Always Allowed' Rolle haben!")
            return
        
        notes = await self.config.guild(guild).whitelist_notes()
        user_id = str(user.id)
        
        if note is None:
            # Zeige Notizen
            if user_id not in notes or not notes[user_id]:
                await ctx.send(f"📝 Keine Notizen zu {user.display_name} vorhanden.")
                return
            
            user_notes = notes[user_id]
            embed = discord.Embed(
                title=f"📝 Notizen zu {user.display_name}",
                color=discord.Color.blue(),
                timestamp=_now()
            )
            
            for i, entry in enumerate(user_notes[-10:], 1):  # Letzte 10 Notizen
                author_id = entry.get("author_id")
                author = guild.get_member(author_id)
                author_name = author.display_name if author else "Unbekannt"
                note_text = entry.get("note", "Kein Text")
                ts = entry.get("timestamp", 0)
                time_str = f"<t:{int(ts)}:R>" if ts else "Unbekannt"
                
                embed.add_field(
                    name=f"Notiz #{i} von {author_name} ({time_str})",
                    value=note_text[:1000],  # Begrenze Länge
                    inline=False
                )
            
            embed.set_footer(text=f"Zeige letzte {min(len(user_notes), 10)} von {len(user_notes)} Notiz(en)")
            await ctx.send(embed=embed)
        else:
            # Füge Notiz hinzu
            if user_id not in notes:
                notes[user_id] = []
            
            new_note = {
                "author_id": ctx.author.id,
                "note": note,
                "timestamp": _now_ts()
            }
            notes[user_id].append(new_note)
            await self.config.guild(guild).whitelist_notes.set(notes)
            
            await ctx.send(f"✅ Notiz zu {user.display_name} hinzugefügt!")
    
    @commands.command(name="wldelnote", aliases=["whitelist_delnote", "wl_removenote", "delwlnote"])
    async def wldelnote(self, ctx: commands.Context, user: discord.Member, note_index: Union[int, str] = None):
        """
        Entfernt eine oder alle Notizen zu einem Spieler.
        
        Verwendung: 
        - [p]wldelnote @User <Nummer> - Entfernt spezifische Notiz (z.B. 1, 2, 3)
        - [p]wldelnote @User all - Entfernt ALLE Notizen zu diesem User
        - [p]wldelnote @User - Zeigt verfügbare Notizen zur Auswahl
        """
        guild = ctx.guild
        
        # Berechtigung prüfen
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in ctx.author.roles:
                has_base_role = True
        
        is_on_duty = await self.config.member(ctx.author).whitelist_on_duty()
        always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in ctx.author.roles
        
        if not has_always_allowed and not has_base_role and not is_on_duty:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle, musst im Duty sein oder die 'Always Allowed' Rolle haben!")
            return
        
        notes = await self.config.guild(guild).whitelist_notes()
        user_id = str(user.id)
        
        if user_id not in notes or not notes[user_id]:
            await ctx.send(f"📝 Keine Notizen zu {user.display_name} vorhanden.")
            return
        
        user_notes = notes[user_id]
        
        # Wenn keine Index angegeben, zeige verfügbare Notizen
        if note_index is None:
            embed = discord.Embed(
                title=f"📝 Verfügbare Notizen zu {user.display_name}",
                color=discord.Color.orange(),
                timestamp=_now()
            )
            
            for i, entry in enumerate(user_notes, 1):
                author_id = entry.get("author_id")
                author = guild.get_member(author_id)
                author_name = author.display_name if author else "Unbekannt"
                note_text = entry.get("note", "Kein Text")[:50] + "..." if len(entry.get("note", "")) > 50 else entry.get("note", "Kein Text")
                ts = entry.get("timestamp", 0)
                time_str = f"<t:{int(ts)}:R>" if ts else "Unbekannt"
                
                embed.add_field(
                    name=f"#{i} von {author_name} ({time_str})",
                    value=note_text,
                    inline=False
                )
            
            embed.set_footer(text="Verwende [p]wldelnote @User <Nummer> zum Löschen oder 'all' für alle")
            await ctx.send(embed=embed)
            return
        
        # Prüfen ob "all" eingegeben wurde
        if isinstance(note_index, str) and note_index.lower() == "all":
            # Alle Notizen löschen
            del notes[user_id]
            await self.config.guild(guild).whitelist_notes.set(notes)
            await ctx.send(f"🗑️ Alle **{len(user_notes)}** Notiz(en) zu {user.display_name} wurden gelöscht!")
            return
        
        # Spezifische Notiz löschen (1-basiert)
        if note_index < 1 or note_index > len(user_notes):
            await ctx.send(f"❌ Ungültige Nummer! Verfügbare Nummern: 1-{len(user_notes)}")
            return
        
        deleted_note = user_notes.pop(note_index - 1)
        
        # Wenn keine Notizen mehr übrig, Eintrag entfernen
        if not user_notes:
            del notes[user_id]
        
        await self.config.guild(guild).whitelist_notes.set(notes)
        
        author_id = deleted_note.get("author_id")
        author = guild.get_member(author_id)
        author_name = author.display_name if author else "Unbekannt"
        
        await ctx.send(f"🗑️ Notiz #{note_index} von {author_name} wurde gelöscht!\nInhalt: `{deleted_note.get('note', 'Kein Text')[:100]}`")
    
    @commands.command(name="wltemp", aliases=["tempwl_user", "whitelist_temp"])
    async def wltemp(self, ctx: commands.Context, user: discord.Member, duration: str):
        """
        Fügt einen Spieler temporär zur Whitelist hinzu.
        
        Verwendung: [p]wltemp @User <Stunden|Tage>
        Beispiele:
        - [p]wltemp @User 2h - 2 Stunden
        - [p]wltemp @User 3d - 3 Tage
        """
        guild = ctx.guild
        member = ctx.author
        
        # Berechtigung prüfen (gleiche wie bei !wl)
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True
        
        is_on_duty = await self.config.member(member).whitelist_on_duty()
        always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in member.roles
        
        if not has_always_allowed and not has_base_role and not is_on_duty:
            await ctx.send("❌ Du benötigst die Whitelist-Handler-Rolle, musst im Duty sein oder die 'Always Allowed' Rolle haben!")
            return
        
        approved_role = await self.get_whitelist_approved_role(guild)
        if not approved_role:
            await ctx.send("❌ Keine Whitelist-Approved-Rolle konfiguriert!")
            return
        
        # Dauer parsen
        duration = duration.lower().strip()
        try:
            if duration.endswith('h'):
                hours = float(duration[:-1])
                minutes = hours * 60
            elif duration.endswith('d'):
                days = float(duration[:-1])
                hours = days * 24
                minutes = hours * 60
            elif duration.endswith('m'):
                minutes = float(duration[:-1])
                hours = minutes / 60
            else:
                # Versuche als Stunden zu interpretieren
                hours = float(duration)
                minutes = hours * 60
        except ValueError:
            await ctx.send("❌ Ungültiges Dauer-Format! Verwende z.B. `2h` für 2 Stunden oder `3d` für 3 Tage.")
            return
        
        if hours <= 0:
            await ctx.send("❌ Die Dauer muss größer als 0 sein!")
            return
        
        # Prüfe ob bereits whitelisted
        if approved_role in user.roles:
            await ctx.send(f"ℹ️ {user.mention} hat bereits die Whitelist-Rolle!")
            return
        
        # Berechne Ablaufzeit (timezone-aware UTC — naive utcnow().timestamp() is host-TZ-dependent)
        expiry_dt = _now() + timedelta(minutes=minutes)
        expiry_ts = int(expiry_dt.timestamp())
        
        # Füge Rolle hinzu
        try:
            await user.add_roles(approved_role, reason=f"Temporäre Whitelist ({hours:.1f}h) von {member.display_name}")
            
            # Speichere temporären Eintrag
            temp_entries = await self.config.guild(guild).whitelist_temp_entries()
            temp_entries[str(user.id)] = expiry_ts
            await self.config.guild(guild).whitelist_temp_entries.set(temp_entries)
            
            embed_success = discord.Embed(
                title="⏳ Temporäre Whitelist",
                description=f"{user.mention} wurde temporär zur Whitelist hinzugefügt!",
                color=discord.Color.gold(),
                timestamp=_now()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention}", inline=True)
            embed_success.add_field(name="⏰ Dauer", value=f"{hours:.1f} Stunden", inline=True)
            embed_success.add_field(name="⏳ Läuft ab", value=f"<t:{int(expiry_ts)}:R>", inline=True)
            
            await ctx.send(embed=embed_success)
            
            # Logge die Aktion
            entries_channel = await self.get_whitelist_entries_channel(guild)
            if entries_channel:
                log_embed = discord.Embed(
                    title="⏳ Temporärer Whitelist Eintrag",
                    description=f"**{user.mention}** wurde temporär whitelisted.",
                    color=discord.Color.gold(),
                    timestamp=_now()
                )
                log_embed.set_thumbnail(url=user.display_avatar.url)
                log_embed.add_field(name="🔹 Von", value=f"{member.mention}", inline=True)
                log_embed.add_field(name="🔹 Dauer", value=f"{hours:.1f} Stunden", inline=True)
                log_embed.add_field(name="🔹 Ablauf", value=f"<t:{int(expiry_ts)}:F>", inline=True)
                log_embed.set_footer(text="Temporärer Whitelist-Eintrag")
                await entries_channel.send(embed=log_embed)
            
        except discord.Forbidden:
            await ctx.send("❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!")
        except Exception as e:
            await ctx.send(f"❌ Fehler: `{str(e)}`")
    
    @commands.command(name="wllog", aliases=["whitelist_logs"])
    async def wllog(self, ctx: commands.Context, limit: int = 10):
        """
        Zeigt die letzten Whitelist-Aktionen an.

        Verwendung: [p]wllog [Anzahl, Standard: 10, Maximum: 25]
        """
        if limit < 1:
            limit = 10
        # Cap to a sane upper bound so a user-supplied 100 doesn't fetch + render 100 embeds.
        limit = min(limit, 25)
        guild = ctx.guild
        entries_channel = await self.get_whitelist_entries_channel(guild)

        if not entries_channel:
            await ctx.send("❌ Kein Whitelist-Einträge-Channel konfiguriert!")
            return

        # Known whitelist-log embed titles — used to filter out unrelated embeds in the channel.
        whitelist_titles = ("Whitelist Eintrag erstellt", "Whitelist Eintrag entfernt", "Temporärer Whitelist")

        # Fetch more than `limit` because we filter; show only `limit`.
        messages = []
        async for msg in entries_channel.history(limit=min(limit * 5, 200)):
            if not msg.embeds:
                continue
            emb = msg.embeds[0]
            if not emb.title:
                continue
            if any(t.lower() in emb.title.lower() for t in whitelist_titles):
                messages.append(msg)
                if len(messages) >= limit:
                    break

        if not messages:
            await ctx.send("📭 Keine Whitelist-Einträge gefunden.")
            return

        embed = discord.Embed(
            title="📋 Letzte Whitelist-Aktionen",
            description=f"Zeige die letzten {len(messages)} Einträge",
            color=discord.Color.blue(),
            timestamp=_now()
        )

        for i, msg in enumerate(messages, 1):
            emb = msg.embeds[0]
            # Embeds may legitimately have description=None — guard against TypeError.
            desc = emb.description or "(keine Beschreibung)"
            embed.add_field(
                name=f"#{i} - {msg.created_at.strftime('%d.%m %H:%M')} • {emb.title}",
                value=desc[:200],
                inline=False
            )

        embed.set_footer(text=f"Quelle: {entries_channel.name}")
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
        
        # Prüfe Berechtigung: Whitelist-Handler-Rolle ODER im Whitelist-Duty ODER Always-Allowed
        # WICHTIG: Duty-Grant-Rolle berechtigt NICHT zum Ausführen von !wl - nur zum Vergeben der Handler-Rolle via !whitelistdutygrant
        role_id = await self.config.guild(guild).whitelist_role()
        has_base_role = False
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role and base_role in member.roles:
                has_base_role = True

        is_on_duty = await self.config.member(member).whitelist_on_duty()
        
        # Prüfe auf Always-Allowed Rolle (die können IMMER ohne Duty)
        always_allowed_role_id = await self.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role and always_allowed_role in member.roles

        # Nutzer mit Always-Allowed Rolle brauchen NICHT im Duty zu sein und können IMMER !wl nutzen
        if has_always_allowed:
            pass  # Immer erlaubt, keine weitere Prüfung nötig
        elif not has_base_role and not is_on_duty:
            error_msg = "❌ Du benötigst die Whitelist-Handler-Rolle oder musst im Whitelist-Duty sein! Die 'Always Allowed' Rolle berechtigt ebenfalls immer."
            await ctx.send(error_msg)
            return
        
        approved_role = await self.get_whitelist_approved_role(guild)
        if not approved_role:
            await ctx.send("❌ Keine Whitelist-Approved-Rolle konfiguriert! Bitte wende dich an einen Admin.")
            return

        # Prüfe ob der User bereits die Rolle hat
        if user.get_role(approved_role.id) is not None:
            await ctx.send(f"ℹ️ {user.mention} hat bereits die Whitelist-Rolle!")
            return

        # Cooldown-Prüfung: verhindert schnelles Re-Whitelisten desselben Users
        cooldown_minutes = await self.config.guild(guild).whitelist_cooldown_minutes() or 0
        if cooldown_minutes > 0:
            # Track per-target user; store last-add timestamp in member-config of the target.
            last_added_ts = await self.config.member(user).whitelist_last_added() or 0
            if last_added_ts and (_now_ts() - int(last_added_ts)) < cooldown_minutes * 60:
                remaining = cooldown_minutes * 60 - (_now_ts() - int(last_added_ts))
                await ctx.send(
                    f"❌ Cooldown aktiv für {user.mention} — bitte warte noch "
                    f"**{remaining // 60}min {remaining % 60}s**."
                )
                return
        
        # Füge die Approved-Rolle hinzu
        try:
            await user.add_roles(approved_role, reason=f"Whitelist genehmigt von {member.display_name}")

            # Update last_added timestamp (for cooldown enforcement)
            await self.config.member(user).whitelist_last_added.set(_now_ts())

            embed_success = discord.Embed(
                title="✅ Whitelist genehmigt",
                description=f"{user.mention} wurde erfolgreich zur Whitelist hinzugefügt!",
                color=discord.Color.green(),
                timestamp=_now()
            )
            embed_success.add_field(name="👤 Genehmigt von", value=f"{member.mention} ({member.display_name})", inline=True)
            embed_success.add_field(name="🎮 Spieler", value=f"{user.display_name}", inline=True)

            await ctx.send(embed=embed_success)

            # Logge die Aktion im Whitelist-Einträge-Channel (getrennt von Duty-Logs!)
            entries_channel = await self.get_whitelist_entries_channel(guild)
            log_ts = _now_ts()
            if entries_channel:
                log_embed = discord.Embed(
                    title="📋 Whitelist Eintrag erstellt",
                    description=f"**{user.mention}** wurde zur Whitelist hinzugefügt.",
                    color=discord.Color.gold(),
                    timestamp=_now()
                )
                log_embed.set_thumbnail(url=user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{user.mention}\n*{user.display_name}* (ID: `{user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{log_ts}:F>\n(<t:{log_ts}:R>)", inline=True)
                log_embed.set_footer(text="Whitelist-Eintrag • Genehmigt von " + member.display_name)

                try:
                    await entries_channel.send(embed=log_embed)
                except discord.HTTPException:
                    log.warning("Konnte WL-Eintrags-Log nicht senden in Guild %s", guild.id)

            # Benachrichtige den Spieler (mit optional konfigurierbarer Welcome-Message)
            try:
                welcome_msg = await self.config.guild(guild).whitelist_welcome_message()
                if welcome_msg:
                    # Support {user} and {helper} placeholders
                    try:
                        welcome_text = welcome_msg.format(user=user.mention, helper=member.mention)
                    except Exception:
                        welcome_text = welcome_msg
                else:
                    welcome_text = f"Du wurdest von **{member.display_name}** zur Whitelist hinzugefügt!"
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=welcome_text,
                    color=discord.Color.green(),
                    timestamp=_now()
                )
                dm_embed.add_field(name="✅ Rolle erhalten", value=f"**{approved_role.name}**", inline=False)
                dm_embed.add_field(name="📝 Hinweis", value="Du kannst jetzt auf unserem Server spielen.", inline=False)
                dm_embed.set_footer(text=f"{guild.name} Whitelist System")
                await user.send(embed=dm_embed)
            except discord.Forbidden:
                pass  # User has DMs closed — not an error.

        except discord.Forbidden:
            await ctx.send("❌ Ich habe keine Berechtigung um diese Rolle zuzuweisen!")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Ein Fehler ist beim Hinzufügen der Rolle aufgetreten: `{e}`")
        except Exception as e:
            log.exception("Unerwarteter Fehler in whitelistuser (Guild %s)", guild.id)
            await ctx.send(f"❌ Unerwarteter Fehler: `{e}`")
    
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
                timestamp=_now()
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
                    timestamp=_now()
                )
                log_embed.set_thumbnail(url=user.display_avatar.url)
                log_embed.add_field(name="🔹 Entfernt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{user.mention}\n*{user.display_name}* (ID: `{user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(_now_ts())}:F>\n(<t:{int(_now_ts())}:R>)", inline=True)
                log_embed.set_footer(text="Whitelist-Entfernung • Durchgeführt von " + member.display_name)
                
                await entries_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="⚠️ Whitelist entfernt",
                    description=f"Deine Whitelist wurde von **{member.display_name}** entfernt.",
                    color=discord.Color.orange(),
                    timestamp=_now()
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
    
    @commands.command(name="checkwhitelist", aliases=["whoadded", "wlcheckinfo"])
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
        if user.get_role(approved_role.id) is None:
            await ctx.send(f"ℹ️ {user.mention} hat die Whitelist-Rolle nicht!")
            return

        # Suche in den Logs nach dem Eintrag
        # WICHTIG: Der Log-Channel für Duty-Logs ist nicht derselbe wie der Entries-Channel.
        # Der ursprüngliche Code hat hier get_whitelist_log_channel() genutzt — das ist der Duty-Log-Channel.
        # Tatsächlich stehen die Eintrag-Logs im Entries-Channel. Wir durchsuchen beide.
        log_channel = await self.get_whitelist_entries_channel(guild)
        if not log_channel:
            await ctx.send("❌ Kein Whitelist-Einträge-Channel konfiguriert!")
            return

        await ctx.send("🔍 Durchsuche Whitelist-Logs...")

        found_entry = None
        approver = None
        grant_time = None

        # Pre-compute all mention forms of the user we want to match.
        user_mention_strs = {f"<@{user.id}>", f"<@!{user.id}>", str(user.id)}

        try:
            # Durchsuche die letzten 1000 Nachrichten im Entries-Channel
            async for message in log_channel.history(limit=1000):
                if not message.embeds:
                    continue

                embed = message.embeds[0]

                # Skip embeds without a title — we only want whitelist-entry embeds.
                if not embed.title:
                    continue
                title_lower = embed.title.lower()
                # Only match "Eintrag erstellt" / "Temporärer Whitelist Eintrag" (not "Entfernt").
                if "eintrag erstellt" not in title_lower and "temporärer whitelist" not in title_lower:
                    continue

                # Determine if this embed is actually about THIS user.
                # (Bug-fix: the original `user.id == int(user.id)` was a tautology.)
                embed_text = str(embed.description or "")
                for field in embed.fields:
                    embed_text += f" {field.value}"
                if not any(s in embed_text for s in user_mention_strs):
                    continue

                # Finde den Genehmiger + Zeitpunkt im Embed
                for field in embed.fields:
                    name_lower = field.name.lower()
                    # Match "Genehmigt von" only — "von" alone also matches "Entfernt von".
                    if "genehmigt von" in name_lower or ("von" in name_lower and "entfernt" not in name_lower):
                        # Robust mention extraction (handles <@123> and <@!123>).
                        m = re.search(r"<@!?(\d+)>", field.value)
                        if m:
                            approver = m.group(0)  # the full mention string
                    if "zeitpunkt" in name_lower or "time" in name_lower:
                        grant_time = field.value

                found_entry = message
                break

            if found_entry and approver:
                # Versuche den Genehmiger als Member zu finden
                approver_member = None
                m = re.search(r"<@!?(\d+)>", approver)
                if m:
                    try:
                        approver_id = int(m.group(1))
                        approver_member = guild.get_member(approver_id) or await guild.fetch_member(approver_id)
                    except (discord.NotFound, discord.HTTPException):
                        pass

                embed_result = discord.Embed(
                    title="📋 Whitelist Information",
                    description=f"Informationen zur Whitelist von {user.mention}",
                    color=discord.Color.blue(),
                    timestamp=_now()
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

        except discord.HTTPException as e:
            await ctx.send(f"❌ HTTP-Fehler beim Durchsuchen der Logs: `{e}`")
        except Exception as e:
            log.exception("Unerwarteter Fehler in checkwhitelist (Guild %s)", guild.id)
            await ctx.send(f"❌ Unerwarteter Fehler: `{e}`")
    
    @commands.command(name="whitelistlogfull", aliases=["fullwllog", "detailed_wllog"])
    async def whitelistlogfull(self, ctx: commands.Context, limit: int = 10):
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
        
        log_channel = await self.get_whitelist_entries_channel(guild)
        if not log_channel:
            await ctx.send("❌ Kein Whitelist-Einträge-Channel konfiguriert!")
            return

        # Begrenze auf max 50 Einträge
        limit = min(limit, 50)

        entries = []
        try:
            async for message in log_channel.history(limit=200):
                if not message.embeds:
                    continue

                embed = message.embeds[0]
                # Skip embeds without a title; only whitelist-entry embeds.
                if not embed.title:
                    continue
                title_lower = embed.title.lower()
                if not any(k in title_lower for k in ("eintrag erstellt", "eintrag entfernt", "temporärer whitelist")):
                    continue
                # Use empty string for None description to avoid NoneType subscript errors later.
                entries.append({
                    "title": embed.title,
                    "description": embed.description or "",
                    "color": embed.color,
                    "timestamp": embed.timestamp if embed.timestamp else message.created_at,
                    "fields": embed.fields[:2] if embed.fields else [],
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
                timestamp=_now()
            )

            for i, entry in enumerate(entries, 1):
                title_lower = entry["title"].lower()
                action_type = "✅" if ("erstellt" in title_lower or "temporärer" in title_lower) else "❌"
                # description is guaranteed to be a string here.
                desc = entry["description"]
                desc_short = (desc[:80] + "...") if len(desc) > 80 else desc
                if entry["timestamp"]:
                    # entry["timestamp"] may be datetime (from embed.timestamp) or message.created_at (datetime).
                    # Both are timezone-aware UTC from discord.py.
                    time_str = f"<t:{int(entry['timestamp'].timestamp())}:R>"
                else:
                    time_str = "Unbekannt"

                embed_list.add_field(
                    name=f"{action_type} Eintrag #{i} • {time_str}",
                    value=desc_short,
                    inline=False
                )

            embed_list.set_footer(text=f"Whitelist-Log • Angezeigt von {member.display_name}")

            await ctx.send(embed=embed_list)

        except discord.HTTPException as e:
            await ctx.send(f"❌ HTTP-Fehler beim Abrufen der Logs: `{e}`")
        except Exception as e:
            log.exception("Unerwarteter Fehler in whitelistlogfull (Guild %s)", guild.id)
            await ctx.send(f"❌ Unerwarteter Fehler: `{e}`")

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
                        start_dt = _from_ts(duty_start)
                        duration = (_now() - start_dt).total_seconds()
                        total_duty_time += duration
                        members_with_duty += 1
        
        avg_duty_hours = (total_duty_time / 3600) / max(members_with_duty, 1)
        
        embed = discord.Embed(
            title="📊 Support Statistiken",
            description="Aktuelle Übersicht des Support-Systems",
            color=discord.Color.blue(),
            timestamp=_now()
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
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def feedbackpanel(self, ctx: commands.Context):
        """
        Erstellt ein Feedback-Panel mit Buttons für positives/negatives Feedback und Vorschläge.
        """
        guild = ctx.guild
        channel = await self.get_feedback_panel_channel(guild)

        # If no feedback panel channel is configured (and no fallbacks), use current channel.
        # Mirrors supportset createfeedbackpanel which falls back to ctx.channel.
        if not channel:
            if not isinstance(ctx.channel, discord.TextChannel):
                await ctx.send("❌ Kein Feedback-Channel konfiguriert und aktueller Channel ist kein Text-Channel!")
                return
            channel = ctx.channel
            await ctx.send(
                "ℹ️ Kein Feedback-Panel-Channel konfiguriert — verwende aktuellen Channel. "
                "Setze einen permanenten Channel mit `[p]supportset feedbackpanelchannel`."
            )
        
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

    async def update_team_panel(self, channel: discord.TextChannel, guild: discord.Guild) -> bool:
        """Erstellt oder aktualisiert das Team-Übersichts-Panel.

        Returns True on success, False on failure.
        Shared between the `teampanel` command and `supportset createteampanel`.
        """
        role_id = await self.config.guild(guild).role()
        # Read-only — never spawn a duty role as a side effect of a panel refresh.
        duty_role = await self.get_duty_role(guild)

        team_members = []
        on_duty_count = 0
        off_duty_count = 0

        if role_id:
            base_role = guild.get_role(role_id)
            if base_role:
                # Single Config.all_members call instead of N per-member reads.
                all_members = await self.config.all_members(guild)
                for m in sorted(base_role.members, key=lambda x: x.display_name.lower()):
                    is_duty = bool(all_members.get(m.id, {}).get("on_duty")) and (
                        duty_role is None or m.get_role(duty_role.id) is not None
                    )
                    status_emoji = "🟢" if is_duty else "🔴"
                    if is_duty:
                        on_duty_count += 1
                    else:
                        off_duty_count += 1
                    team_members.append(f"{status_emoji} {m.display_name}")

        embed = discord.Embed(
            title="👥 Team Übersicht",
            description="**Unser Support-Team**\n\nHier siehst du alle Teammitglieder und deren aktuellen Status.",
            color=discord.Color.blue(),
            timestamp=_now(),
        )

        if team_members:
            embed.add_field(name=f"🟢 Im Dienst ({on_duty_count})", value=f"Insgesamt {len(team_members)} Teammitglieder", inline=False)
            members_text = "\n".join(team_members[:20])
            if len(team_members) > 20:
                members_text += f"\n...und {len(team_members) - 20} weitere"
            embed.add_field(name="📋 Teammitglieder", value=members_text, inline=False)
        else:
            embed.add_field(name="Keine Teammitglieder", value="Es wurden noch keine Teammitglieder mit der Support-Basisrolle ausgestattet.", inline=False)

        embed.set_footer(text=f"On Duty: {on_duty_count} | Off Duty: {off_duty_count}")

        team_panel_message_id = await self.config.guild(guild).team_panel_message_id()

        if team_panel_message_id:
            try:
                message = await channel.fetch_message(team_panel_message_id)
                await message.edit(embed=embed)
                return True
            except discord.NotFound:
                # Stale ID — clear and create new below.
                await self.config.guild(guild).team_panel_message_id.set(None)
            except discord.Forbidden:
                log.warning("Fehlende Rechte beim Editieren des Team-Panels (Guild %s)", guild.id)
                return False
            except discord.HTTPException:
                log.exception("HTTP-Fehler beim Editieren des Team-Panels (Guild %s)", guild.id)
                return False

        try:
            message = await channel.send(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            log.warning("Konnte Team-Panel nicht in Channel %s senden", channel.id)
            return False
        await self.config.guild(guild).team_panel_message_id.set(message.id)
        return True

    @commands.command(name="teampanel", aliases=["teamupdate", "updateteam"])
    @commands.guild_only()
    async def teampanel(self, ctx: commands.Context):
        """
        Aktualisiert oder erstellt das Team-Übersichts-Panel.
        Zeigt alle Teammitglieder und deren Duty-Status.
        """
        guild = ctx.guild
        channel = await self.get_team_channel(guild)

        # If no team channel is configured (and no panel/log channel as fallback),
        # use the channel the command was invoked in. Mirrors supportset createteampanel.
        if not channel:
            if not isinstance(ctx.channel, discord.TextChannel):
                await ctx.send("❌ Kein Team-Channel konfiguriert und aktueller Channel ist kein Text-Channel!")
                return
            channel = ctx.channel
            await ctx.send(
                "ℹ️ Kein Team-Channel konfiguriert — verwende aktuellen Channel. "
                "Setze einen permanenten Channel mit `[p]supportset teamchannel`."
            )

        ok = await self.update_team_panel(channel, guild)
        if ok:
            await ctx.send(f"✅ Team-Panel in {channel.mention} aktualisiert!")
        else:
            await ctx.send("❌ Konnte Team-Panel nicht aktualisieren (fehlende Rechte?).")

    # ============================================
    # TICKET SYSTEM
    # ============================================

    @commands.group(name="ticketset", aliases=["ticketconfig"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def ticketset(self, ctx: commands.Context):
        """Konfiguriere das Ticket-System."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @ticketset.command(name="legacycategory", aliases=["legacycat", "setcategory", "setcat"])
    async def ticketset_legacycategory(self, ctx: commands.Context, *, category: str = None):
        """Setzt die Standard-Kategorie (Legacy), in der Ticket-Channels erstellt werden. 'reset' zum Zurücksetzen.
        Hinweis: Für Multi-Kategorie-System nutze `[p]ticketset category ...`."""
        if category is None or category.lower() == "reset":
            await self.config.guild(ctx.guild).ticket_category.set(None)
            await ctx.send("✅ Ticket-Kategorie zurückgesetzt.")
            return
        cat_id = self._parse_channel_id(category)
        if cat_id is None:
            await ctx.send("❌ Bitte gib eine gültige Kategorie-ID oder Mention ein.")
            return
        cat = ctx.guild.get_channel(cat_id)
        if not cat or not isinstance(cat, discord.CategoryChannel):
            await ctx.send("❌ Das ist keine Kategorie.")
            return
        await self.config.guild(ctx.guild).ticket_category.set(cat_id)
        await ctx.send(f"✅ Ticket-Kategorie auf **{cat.name}** gesetzt.")

    @ticketset.command(name="panelchannel")
    async def ticketset_panelchannel(self, ctx: commands.Context, channel: str = None):
        """Setzt den Channel für das Ticket-Panel. 'reset' zum Zurücksetzen."""
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).ticket_panel_channel.set(None)
            await ctx.send("✅ Ticket-Panel-Channel zurückgesetzt.")
            return
        ch_id = self._parse_channel_id(channel)
        if ch_id is None:
            await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein.")
            return
        ch = ctx.guild.get_channel(ch_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Channel nicht gefunden oder kein Text-Channel!")
            return
        await self.config.guild(ctx.guild).ticket_panel_channel.set(ch_id)
        await ctx.send(f"✅ Ticket-Panel-Channel auf {ch.mention} gesetzt.")

    @ticketset.command(name="supportrole")
    async def ticketset_supportrole(self, ctx: commands.Context, role: str = None):
        """Setzt die Rolle, die Tickets bearbeiten kann. 'reset' zum Zurücksetzen."""
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).ticket_support_role.set(None)
            await ctx.send("✅ Ticket-Support-Rolle zurückgesetzt.")
            return
        role_id = self._parse_role_id(role)
        if role_id is None:
            await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein.")
            return
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
        await self.config.guild(ctx.guild).ticket_support_role.set(role_id)
        await ctx.send(f"✅ Ticket-Support-Rolle auf {r.mention} gesetzt.")

    @ticketset.command(name="logchannel")
    async def ticketset_logchannel(self, ctx: commands.Context, channel: str = None):
        """Setzt den Channel für Ticket-Logs (Erstellung/Schließung). 'reset' zum Zurücksetzen."""
        if channel is None or channel.lower() == "reset":
            await self.config.guild(ctx.guild).ticket_log_channel.set(None)
            await ctx.send("✅ Ticket-Log-Channel zurückgesetzt.")
            return
        ch_id = self._parse_channel_id(channel)
        if ch_id is None:
            await ctx.send("❌ Bitte gib eine gültige Channel-ID oder Mention ein.")
            return
        ch = ctx.guild.get_channel(ch_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            await ctx.send("❌ Channel nicht gefunden oder kein Text-Channel!")
            return
        await self.config.guild(ctx.guild).ticket_log_channel.set(ch_id)
        await ctx.send(f"✅ Ticket-Log-Channel auf {ch.mention} gesetzt.")

    @ticketset.command(name="show")
    async def ticketset_show(self, ctx: commands.Context):
        """Zeigt die aktuelle Ticket-Konfiguration."""
        g = self.config.guild(ctx.guild)
        cat_id = await g.ticket_category()
        panel_ch_id = await g.ticket_panel_channel()
        role_id = await g.ticket_support_role()
        log_ch_id = await g.ticket_log_channel()
        counter = await g.ticket_counter()
        welcome_msg = await g.ticket_welcome_message()
        modal_enabled = await g.ticket_modal_enabled()
        dm_on_close = await g.ticket_dm_on_close()
        auto_close_h = await g.ticket_auto_close_hours()
        transcript = await g.ticket_transcript()
        user_can_close = await g.ticket_user_can_close()
        claim_enabled = await g.ticket_claim_enabled()
        max_open = await g.ticket_max_open()
        panel_color = await g.ticket_panel_color()
        panel_emoji = await g.ticket_panel_emoji()
        panel_title = await g.ticket_panel_title()
        button_text = await g.ticket_panel_button_text()
        blacklist = await g.ticket_blacklist() or []
        active = await g.ticket_active() or {}

        cat = ctx.guild.get_channel(cat_id) if cat_id else None
        panel_ch = ctx.guild.get_channel(panel_ch_id) if panel_ch_id else None
        role = ctx.guild.get_role(role_id) if role_id else None
        log_ch = ctx.guild.get_channel(log_ch_id) if log_ch_id else None

        embed = discord.Embed(
            title="🎫 Ticket-System Konfiguration",
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.add_field(name="Kategorie", value=cat.name if cat else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Panel-Channel", value=panel_ch.mention if panel_ch else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Support-Rolle", value=role.mention if role else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Log-Channel", value=log_ch.mention if log_ch else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Ticket-Counter", value=str(counter), inline=True)
        embed.add_field(name="Offene Tickets", value=str(sum(len(v) for v in active.values())), inline=True)
        embed.add_field(name="Max. offen/User", value=str(max_open), inline=True)
        embed.add_field(name="Panel-Color", value=panel_color, inline=True)
        embed.add_field(name="Panel-Emoji", value=panel_emoji or "Kein", inline=True)
        embed.add_field(name="Modal aktiviert", value="✅" if modal_enabled else "❌", inline=True)
        embed.add_field(name="DM bei Schließen", value="✅" if dm_on_close else "❌", inline=True)
        embed.add_field(name="Auto-Close (h)", value=str(auto_close_h) or "0", inline=True)
        embed.add_field(name="Transcript", value="✅" if transcript else "❌", inline=True)
        embed.add_field(name="User darf schließen", value="✅" if user_can_close else "❌", inline=True)
        embed.add_field(name="Claim-System", value="✅" if claim_enabled else "❌", inline=True)
        embed.add_field(name="Blacklist", value=f"{len(blacklist)} User", inline=True)
        embed.add_field(name="Panel-Titel", value=panel_title, inline=False)
        embed.add_field(name="Button-Text", value=button_text, inline=True)
        embed.add_field(name="Welcome-Message", value=welcome_msg[:200], inline=False)
        embed.set_footer(text=f"{ctx.guild.name}")
        await ctx.send(embed=embed)

    @ticketset.command(name="createpanel")
    async def ticketset_createpanel(self, ctx: commands.Context):
        """Erstellt das Ticket-Panel mit einem 'Ticket erstellen'-Button."""
        guild = ctx.guild
        channel = await self.get_ticket_panel_channel(guild)
        if not channel:
            await ctx.send("❌ Kein Ticket-Panel-Channel konfiguriert. Nutze `[p]ticketset panelchannel`.")
            return
        category = await self.get_ticket_category(guild)
        support_role = await self.get_ticket_support_role(guild)
        if not category:
            await ctx.send("❌ Keine Ticket-Kategorie konfiguriert. Nutze `[p]ticketset category`.")
            return
        if not support_role:
            await ctx.send("❌ Keine Ticket-Support-Rolle konfiguriert. Nutze `[p]ticketset supportrole`.")
            return

        # Konfigurierbares Embed
        title = await self.config.guild(guild).ticket_panel_title()
        description = await self.config.guild(guild).ticket_panel_description()
        color_name = await self.config.guild(guild).ticket_panel_color()
        emoji = await self.config.guild(guild).ticket_panel_emoji()

        color_map = {
            "blurple": discord.Color.blurple(),
            "red": discord.Color.red(),
            "green": discord.Color.green(),
            "grey": discord.Color.greyple(),
            "orange": discord.Color.orange(),
        }
        color = color_map.get(color_name, discord.Color.blurple())

        embed = discord.Embed(
            title=title,
            description=description,
            color=color,
            timestamp=_now(),
        )
        embed.set_footer(text="Ticket-System • Klicke auf den Button")
        view = TicketPanelView(self)
        try:
            message = await channel.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException) as e:
            await ctx.send(f"❌ Konnte das Ticket-Panel nicht posten: `{e}`")
            return
        # Altes Panel löschen falls vorhanden
        old_id = await self.config.guild(guild).ticket_panel_message_id()
        if old_id:
            try:
                old_msg = await channel.fetch_message(old_id)
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await self.config.guild(guild).ticket_panel_message_id.set(message.id)
        await ctx.send(f"✅ Ticket-Panel in {channel.mention} erstellt!")

    # ============================================
    # ERWEITERTE TICKET-SETUP-BEFEHLE
    # ============================================

    @ticketset.command(name="setup")
    async def ticketset_setup(self, ctx: commands.Context):
        """Interaktiver Setup-Wizard für das Ticket-System (mit Buttons)."""
        view = TicketSetupWizardView(self, ctx.guild)
        embed = view.build_embed()
        await ctx.send(embed=embed, view=view)

    @ticketset.command(name="quickstart")
    async def ticketset_quickstart(self, ctx: commands.Context):
        """Ein-Klick-Setup mit empfohlenen Defaults.
        Setzt automatisch: Kategorie (erstellt falls fehlt), Support-Rolle, Panel-Channel,
        Log-Channel und erstellt das Ticket-Panel."""
        guild = ctx.guild
        g = self.config.guild(guild)

        # 1. Support-Rolle: vorhandene mit Manage Channels oder erstellen
        support_role_id = await g.ticket_support_role()
        support_role = guild.get_role(support_role_id) if support_role_id else None
        if not support_role:
            try:
                support_role = await guild.create_role(
                    name="🎫 Ticket Support",
                    reason="Auto-created by ticketset quickstart",
                )
                await g.ticket_support_role.set(support_role.id)
            except discord.Forbidden:
                await ctx.send("❌ Brauche `Manage Roles` um Support-Rolle zu erstellen.")
                return
            except discord.HTTPException as e:
                await ctx.send(f"❌ Konnte Rolle nicht erstellen: `{e}`")
                return

        # 2. Kategorie: vorhandene oder erstellen
        cat_id = await g.ticket_category()
        category = guild.get_channel(cat_id) if cat_id else None
        if not category or not isinstance(category, discord.CategoryChannel):
            try:
                category = await guild.create_category(
                    name="🎫 Tickets",
                    reason="Auto-created by ticketset quickstart",
                    overwrites={
                        guild.default_role: discord.PermissionOverwrite(view_channel=False),
                        guild.me: discord.PermissionOverwrite(
                            view_channel=True, send_messages=True,
                            read_message_history=True, manage_channels=True,
                        ),
                        support_role: discord.PermissionOverwrite(
                            view_channel=True, send_messages=True,
                            read_message_history=True, attach_files=True,
                        ),
                    },
                )
                await g.ticket_category.set(category.id)
            except discord.Forbidden:
                await ctx.send("❌ Brauche `Manage Channels` um Kategorie zu erstellen.")
                return
            except discord.HTTPException as e:
                await ctx.send(f"❌ Konnte Kategorie nicht erstellen: `{e}`")
                return

        # 3. Panel-Channel = aktueller Channel (falls Text-Channel)
        panel_ch_id = await g.ticket_panel_channel()
        panel_ch = guild.get_channel(panel_ch_id) if panel_ch_id else None
        if not panel_ch and isinstance(ctx.channel, discord.TextChannel):
            panel_ch = ctx.channel
            await g.ticket_panel_channel.set(panel_ch.id)
        elif not panel_ch:
            try:
                panel_ch = await guild.create_text_channel(
                    name="ticket-panel",
                    category=None,
                    reason="Auto-created by ticketset quickstart",
                )
                await g.ticket_panel_channel.set(panel_ch.id)
            except (discord.Forbidden, discord.HTTPException) as e:
                await ctx.send(f"❌ Konnte Panel-Channel nicht erstellen: `{e}`")
                return

        # 4. Log-Channel erstellen falls nicht vorhanden
        log_ch_id = await g.ticket_log_channel()
        log_ch = guild.get_channel(log_ch_id) if log_ch_id else None
        if not log_ch:
            try:
                log_ch = await guild.create_text_channel(
                    name="ticket-logs",
                    category=category,
                    reason="Auto-created by ticketset quickstart",
                    overwrites={
                        guild.default_role: discord.PermissionOverwrite(view_channel=False),
                        support_role: discord.PermissionOverwrite(view_channel=True, send_messages=False, read_message_history=True),
                        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
                    },
                )
                await g.ticket_log_channel.set(log_ch.id)
            except (discord.Forbidden, discord.HTTPException):
                pass  # nicht kritisch

        # 5. Defaults für erweiterte Optionen
        await g.ticket_modal_enabled.set(True)
        await g.ticket_dm_on_close.set(False)
        await g.ticket_transcript.set(True)
        await g.ticket_user_can_close.set(True)
        await g.ticket_claim_enabled.set(True)
        await g.ticket_max_open.set(1)

        # 6. Panel erstellen
        embed = discord.Embed(
            title="🎫 Ticket erstellen",
            description=(
                "Brauchst du Hilfe oder möchtest etwas anfragen?\n\n"
                "Klicke auf den Button unten und beschreibe dein Anliegen — "
                "ein privater Ticket-Channel wird für dich erstellt."
            ),
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.set_footer(text="Ticket-System • Klicke auf den Button")
        view = TicketPanelView(self)
        try:
            message = await panel_ch.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException) as e:
            await ctx.send(f"❌ Konnte Panel-Nachricht nicht senden: `{e}`")
            return
        # Altes Panel löschen falls vorhanden
        old_id = await g.ticket_panel_message_id()
        if old_id:
            try:
                old_msg = await panel_ch.fetch_message(old_id)
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await g.ticket_panel_message_id.set(message.id)

        # 7. Zusammenfassung
        result_embed = discord.Embed(
            title="✅ Ticket-System QuickStart fertig!",
            description=(
                f"**Support-Rolle:** {support_role.mention}\n"
                f"**Kategorie:** {category.name}\n"
                f"**Panel-Channel:** {panel_ch.mention}\n"
                f"**Log-Channel:** {log_ch.mention if log_ch else '❌ (nicht erstellt)'}\n"
                f"**Panel-Nachricht:** [Klick]({message.jump_url})\n\n"
                f"**Aktivierte Features:**\n"
                f"• ✅ Modal (User muss Anliegen beschreiben)\n"
                f"• ✅ Transcript bei Schließen\n"
                f"• ✅ User darf eigenes Ticket schließen\n"
                f"• ✅ Claim-System (Team kann Tickets übernehmen)\n"
                f"• ✅ Max. 1 offenes Ticket pro User\n"
                f"• ❌ Auto-Close (deaktiviert — mit `[p]ticketset autoclose 24` aktivieren)\n"
                f"• ❌ DM bei Schließen (mit `[p]ticketset dm True` aktivieren)\n\n"
                f"💡 Mit `[p]ticketset setup` kannst du alles weitere anpassen."
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        await ctx.send(embed=result_embed)

    @ticketset.command(name="welcome")
    async def ticketset_welcome(self, ctx: commands.Context, *, message: str):
        """Setzt die Willkommensnachricht die User im neuen Ticket sehen."""
        if len(message) > 1500:
            await ctx.send("❌ Nachricht zu lang (max 1500 Zeichen).")
            return
        await self.config.guild(ctx.guild).ticket_welcome_message.set(message)
        await ctx.send("✅ Willkommensnachricht gesetzt.")

    @ticketset.command(name="paneltitle")
    async def ticketset_paneltitle(self, ctx: commands.Context, *, title: str):
        """Setzt den Titel des Panel-Embeds."""
        if len(title) > 200:
            await ctx.send("❌ Titel zu lang (max 200 Zeichen).")
            return
        await self.config.guild(ctx.guild).ticket_panel_title.set(title)
        await ctx.send(f"✅ Panel-Titel gesetzt auf: `{title}`.\nMit `[p]ticketset createpanel` Panel aktualisieren.")

    @ticketset.command(name="paneldescription", aliases=["paneldesc"])
    async def ticketset_paneldescription(self, ctx: commands.Context, *, description: str):
        """Setzt die Beschreibung des Panel-Embeds."""
        if len(description) > 1500:
            await ctx.send("❌ Beschreibung zu lang (max 1500 Zeichen).")
            return
        await self.config.guild(ctx.guild).ticket_panel_description.set(description)
        await ctx.send("✅ Panel-Beschreibung gesetzt.\nMit `[p]ticketset createpanel` Panel aktualisieren.")

    @ticketset.command(name="panelcolor")
    async def ticketset_panelcolor(self, ctx: commands.Context, color: str):
        """Setzt die Farbe des Panel-Embeds. Verfügbare Farben: blurple, red, green, grey, orange."""
        color = color.lower()
        if color not in ("blurple", "red", "green", "grey", "orange"):
            await ctx.send("❌ Ungültige Farbe. Verwende: `blurple`, `red`, `green`, `grey`, `orange`.")
            return
        await self.config.guild(ctx.guild).ticket_panel_color.set(color)
        await ctx.send(f"✅ Panel-Farbe gesetzt auf: `{color}`.\nMit `[p]ticketset createpanel` Panel aktualisieren.")

    @ticketset.command(name="panelemoji")
    async def ticketset_panelemoji(self, ctx: commands.Context, emoji: str):
        """Setzt das Emoji auf dem Panel-Button."""
        if len(emoji) > 50:
            await ctx.send("❌ Emoji zu lang.")
            return
        await self.config.guild(ctx.guild).ticket_panel_emoji.set(emoji)
        await ctx.send(f"✅ Panel-Emoji gesetzt auf: {emoji}\nMit `[p]ticketset createpanel` Panel aktualisieren.")

    @ticketset.command(name="buttontext")
    async def ticketset_buttontext(self, ctx: commands.Context, *, text: str):
        """Setzt den Text auf dem Panel-Button."""
        if len(text) > 80:
            await ctx.send("❌ Text zu lang (max 80 Zeichen).")
            return
        await self.config.guild(ctx.guild).ticket_panel_button_text.set(text)
        await ctx.send(f"✅ Button-Text gesetzt auf: `{text}`\nMit `[p]ticketset createpanel` Panel aktualisieren.")

    @ticketset.command(name="modal")
    async def ticketset_modal(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert das Modal (User muss Anliegen beschreiben)."""
        await self.config.guild(ctx.guild).ticket_modal_enabled.set(enabled)
        await ctx.send(f"✅ Modal {'aktiviert' if enabled else 'deaktiviert'}.")

    @ticketset.command(name="modalquestion")
    async def ticketset_modalquestion(self, ctx: commands.Context, *, question: str):
        """Setzt die Frage die im Modal gestellt wird."""
        if len(question) > 45:
            await ctx.send("❌ Frage zu lang (max 45 Zeichen — Discord Modal-Limit).")
            return
        await self.config.guild(ctx.guild).ticket_modal_question.set(question)
        await ctx.send("✅ Modal-Frage gesetzt.")

    @ticketset.command(name="modalplaceholder")
    async def ticketset_modalplaceholder(self, ctx: commands.Context, *, placeholder: str):
        """Setzt den Placeholder-Text im Modal."""
        if len(placeholder) > 100:
            await ctx.send("❌ Placeholder zu lang (max 100 Zeichen).")
            return
        await self.config.guild(ctx.guild).ticket_modal_placeholder.set(placeholder)
        await ctx.send("✅ Modal-Placeholder gesetzt.")

    @ticketset.command(name="dm")
    async def ticketset_dm(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert DM an User bei Ticket-Schließung."""
        await self.config.guild(ctx.guild).ticket_dm_on_close.set(enabled)
        await ctx.send(f"✅ DM bei Schließen {'aktiviert' if enabled else 'deaktiviert'}.")

    @ticketset.command(name="autoclose", aliases=["autoclosehours"])
    async def ticketset_autoclose(self, ctx: commands.Context, hours: int):
        """Setzt Auto-Close für inaktive Tickets in Stunden (0 = deaktiviert)."""
        if hours < 0:
            await ctx.send("❌ Wert muss ≥ 0 sein (0 = deaktiviert).")
            return
        await self.config.guild(ctx.guild).ticket_auto_close_hours.set(hours)
        if hours == 0:
            await ctx.send("✅ Auto-Close deaktiviert.")
        else:
            await ctx.send(f"✅ Auto-Close gesetzt auf {hours} Stunden.")

    @ticketset.command(name="transcript")
    async def ticketset_transcript(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Transcripts bei Schließung."""
        await self.config.guild(ctx.guild).ticket_transcript.set(enabled)
        await ctx.send(f"✅ Transcript {'aktiviert' if enabled else 'deaktiviert'}.")

    @ticketset.command(name="userclose", aliases=["selfclose"])
    async def ticketset_userclose(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert ob User ihr eigenes Ticket schließen dürfen."""
        await self.config.guild(ctx.guild).ticket_user_can_close.set(enabled)
        await ctx.send(f"✅ User-Close {'aktiviert' if enabled else 'deaktiviert'}.")

    @ticketset.command(name="claim")
    async def ticketset_claim(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert das Claim-System."""
        await self.config.guild(ctx.guild).ticket_claim_enabled.set(enabled)
        await ctx.send(f"✅ Claim-System {'aktiviert' if enabled else 'deaktiviert'}.")

    @ticketset.command(name="maxopen", aliases=["maxtickets"])
    async def ticketset_maxopen(self, ctx: commands.Context, count: int):
        """Setzt die maximal gleichzeitig offenen Tickets pro User."""
        if count < 1:
            await ctx.send("❌ Wert muss ≥ 1 sein.")
            return
        await self.config.guild(ctx.guild).ticket_max_open.set(count)
        await ctx.send(f"✅ Max. offene Tickets pro User gesetzt auf {count}.")

    @ticketset.command(name="blacklist")
    async def ticketset_blacklist(self, ctx: commands.Context, action: str, user: discord.User):
        """Fügt User zur Ticket-Blacklist hinzu/entfernt ihn. `add` oder `remove`."""
        bl = await self.config.guild(ctx.guild).ticket_blacklist() or []
        if action.lower() == "add":
            if user.id not in bl:
                bl.append(user.id)
            await self.config.guild(ctx.guild).ticket_blacklist.set(bl)
            await ctx.send(f"✅ {user.mention} kann keine Tickets mehr erstellen.")
        elif action.lower() == "remove":
            if user.id in bl:
                bl.remove(user.id)
            await self.config.guild(ctx.guild).ticket_blacklist.set(bl)
            await ctx.send(f"✅ {user.mention} kann wieder Tickets erstellen.")
        else:
            await ctx.send("❌ Ungültige Aktion. Verwende `add` oder `remove`.")

    # ============================================
    # MULTI-KATEGORIE-SYSTEM
    # ============================================

    @ticketset.group(name="category", aliases=["cat"])
    async def ticketset_cat_group(self, ctx: commands.Context):
        """Verwaltet Ticket-Kategorien (Multi-Kategorie-System)."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @ticketset_cat_group.command(name="add")
    async def ticketset_category_add(self, ctx: commands.Context, key: str, name: str):
        """Fügt eine neue Ticket-Kategorie hinzu.
        Beispiel: `[p]ticketset category add support "Allgemeiner Support"`
        """
        key = key.lower()
        if len(key) > 20 or not key.replace("_", "").isalnum():
            await ctx.send("❌ Key muss alphanumerisch sein (max 20 Zeichen, _ erlaubt).")
            return
        if len(name) > 100:
            await ctx.send("❌ Name zu lang (max 100 Zeichen).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert bereits. Nutze `[p]ticketset category edit {key}` zum Bearbeiten.")
            return
        cats[key] = {
            "name": name,
            "emoji": "🎫",
            "color": "blurple",
            "button_text": name[:80],
            "category_id": None,
            "support_role_id": None,
            "description": "",
            "welcome_message": "Ein Teammitglied wird sich gleich um dein Anliegen kümmern.",
            "modal_question": "Was ist dein Anliegen?",
            "modal_placeholder": "Beschreibe dein Anliegen so detailliert wie möglich...",
        }
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(
            f"✅ Kategorie `{key}` erstellt.\n\n"
            f"**Nächste Schritte:**\n"
            f"• `[p]ticketset category emoji {key} 🎫` — Emoji setzen\n"
            f"• `[p]ticketset category categoryid {key} <category_id>` — Discord-Kategorie setzen\n"
            f"• `[p]ticketset category role {key} @Rolle` — Support-Rolle setzen\n"
            f"• `[p]ticketset category show {key}` — Konfiguration anzeigen"
        )

    @ticketset_cat_group.command(name="remove", aliases=["delete"])
    async def ticketset_category_remove(self, ctx: commands.Context, key: str):
        """Entfernt eine Ticket-Kategorie."""
        key = key.lower()
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        del cats[key]
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Kategorie `{key}` entfernt.")

    @ticketset_cat_group.command(name="list")
    async def ticketset_category_list(self, ctx: commands.Context):
        """Listet alle Ticket-Kategorien auf."""
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if not cats:
            await ctx.send("ℹ️ Keine Kategorien konfiguriert. Nutze `[p]ticketset category add <key> <name>`.")
            return
        embed = discord.Embed(
            title="🎫 Ticket-Kategorien",
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        for key, cat in cats.items():
            status = []
            if cat.get("category_id"):
                status.append("✅ Category")
            else:
                status.append("❌ Category")
            if cat.get("support_role_id"):
                status.append("✅ Rolle")
            else:
                status.append("❌ Rolle")
            cat_obj = ctx.guild.get_channel(cat.get("category_id")) if cat.get("category_id") else None
            role_obj = ctx.guild.get_role(cat.get("support_role_id")) if cat.get("support_role_id") else None
            embed.add_field(
                name=f"{cat.get('emoji', '🎫')} `{key}` — {cat.get('name', '?')}",
                value=(
                    f"• Discord-Kategorie: {cat_obj.name if cat_obj else '❌'}\n"
                    f"• Support-Rolle: {role_obj.mention if role_obj else '❌'}\n"
                    f"• Button-Text: {cat.get('button_text', '?')}\n"
                    f"• Farbe: {cat.get('color', '?')}\n"
                    f"• Status: {' '.join(status)}"
                ),
                inline=False,
            )
        await ctx.send(embed=embed)

    @ticketset_cat_group.command(name="show")
    async def ticketset_category_show(self, ctx: commands.Context, key: str):
        """Zeigt die Konfiguration einer Kategorie."""
        key = key.lower()
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cat = cats[key]
        cat_obj = ctx.guild.get_channel(cat.get("category_id")) if cat.get("category_id") else None
        role_obj = ctx.guild.get_role(cat.get("support_role_id")) if cat.get("support_role_id") else None
        embed = discord.Embed(
            title=f"🎫 Kategorie: {cat.get('name', key)}",
            description=f"Key: `{key}`\nEmoji: {cat.get('emoji', '🎫')}\nFarbe: `{cat.get('color', 'blurple')}`",
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.add_field(name="Name", value=cat.get("name", "?"), inline=True)
        embed.add_field(name="Button-Text", value=cat.get("button_text", "?"), inline=True)
        embed.add_field(name="Discord-Kategorie", value=cat_obj.name if cat_obj else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Support-Rolle", value=role_obj.mention if role_obj else "❌ Nicht gesetzt", inline=True)
        embed.add_field(name="Beschreibung", value=cat.get("description", "Keine") or "Keine", inline=False)
        embed.add_field(name="Welcome-Message", value=(cat.get("welcome_message", "") or "")[:500], inline=False)
        embed.add_field(name="Modal-Frage", value=cat.get("modal_question", "?"), inline=True)
        embed.add_field(name="Modal-Placeholder", value=cat.get("modal_placeholder", "?"), inline=True)
        await ctx.send(embed=embed)

    @ticketset_cat_group.command(name="emoji")
    async def ticketset_category_emoji(self, ctx: commands.Context, key: str, emoji: str):
        """Setzt das Emoji einer Kategorie."""
        key = key.lower()
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        if len(emoji) > 50:
            await ctx.send("❌ Emoji zu lang.")
            return
        cats[key]["emoji"] = emoji
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Emoji für `{key}` gesetzt auf: {emoji}")

    @ticketset_cat_group.command(name="color")
    async def ticketset_category_color(self, ctx: commands.Context, key: str, color: str):
        """Setzt die Farbe einer Kategorie (blurple, red, green, grey, orange)."""
        key = key.lower()
        color = color.lower()
        if color not in ("blurple", "red", "green", "grey", "orange"):
            await ctx.send("❌ Farbe muss sein: blurple, red, green, grey, orange.")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["color"] = color
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Farbe für `{key}` gesetzt auf: `{color}`")

    @ticketset_cat_group.command(name="buttontext")
    async def ticketset_category_buttontext(self, ctx: commands.Context, key: str, *, text: str):
        """Setzt den Button-Text einer Kategorie."""
        key = key.lower()
        if len(text) > 80:
            await ctx.send("❌ Text zu lang (max 80 Zeichen).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["button_text"] = text
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Button-Text für `{key}` gesetzt auf: `{text}`")

    @ticketset_cat_group.command(name="categoryid", aliases=["channel"])
    async def ticketset_category_categoryid(self, ctx: commands.Context, key: str, category: discord.CategoryChannel):
        """Setzt die Discord-Kategorie in der Ticket-Channels erstellt werden."""
        key = key.lower()
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["category_id"] = category.id
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Discord-Kategorie für `{key}` gesetzt auf: {category.name}")

    @ticketset_cat_group.command(name="role", aliases=["supportrole"])
    async def ticketset_category_role(self, ctx: commands.Context, key: str, role: discord.Role):
        """Setzt die Support-Rolle die bei Tickets dieser Kategorie gepingt wird."""
        key = key.lower()
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["support_role_id"] = role.id
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Support-Rolle für `{key}` gesetzt auf: {role.mention}")

    @ticketset_cat_group.command(name="description")
    async def ticketset_category_description(self, ctx: commands.Context, key: str, *, description: str):
        """Setzt die Beschreibung einer Kategorie."""
        key = key.lower()
        if len(description) > 500:
            await ctx.send("❌ Beschreibung zu lang (max 500 Zeichen).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["description"] = description
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Beschreibung für `{key}` gesetzt.")

    @ticketset_cat_group.command(name="welcome")
    async def ticketset_category_welcome(self, ctx: commands.Context, key: str, *, message: str):
        """Setzt die Willkommensnachricht einer Kategorie."""
        key = key.lower()
        if len(message) > 1500:
            await ctx.send("❌ Nachricht zu lang (max 1500 Zeichen).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["welcome_message"] = message
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Willkommensnachricht für `{key}` gesetzt.")

    @ticketset_cat_group.command(name="modalquestion")
    async def ticketset_category_modalquestion(self, ctx: commands.Context, key: str, *, question: str):
        """Setzt die Modal-Frage einer Kategorie."""
        key = key.lower()
        if len(question) > 45:
            await ctx.send("❌ Frage zu lang (max 45 Zeichen — Discord Modal-Limit).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["modal_question"] = question
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Modal-Frage für `{key}` gesetzt.")

    @ticketset_cat_group.command(name="modalplaceholder")
    async def ticketset_category_modalplaceholder(self, ctx: commands.Context, key: str, *, placeholder: str):
        """Setzt den Modal-Placeholder einer Kategorie."""
        key = key.lower()
        if len(placeholder) > 100:
            await ctx.send("❌ Placeholder zu lang (max 100 Zeichen).")
            return
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        if key not in cats:
            await ctx.send(f"❌ Kategorie `{key}` existiert nicht.")
            return
        cats[key]["modal_placeholder"] = placeholder
        await self.config.guild(ctx.guild).ticket_categories.set(cats)
        await ctx.send(f"✅ Modal-Placeholder für `{key}` gesetzt.")

    @ticketset.command(name="createmulti")
    async def ticketset_createmulti(self, ctx: commands.Context):
        """Erstellt ein Multi-Kategorie Panel (mehrere Buttons auf einer Nachricht).
        Voraussetzung: mindestens eine Kategorie ist konfiguriert (category_id + support_role_id)."""
        cats = await self.config.guild(ctx.guild).ticket_categories() or {}
        valid_cats = []
        for key, cat in cats.items():
            if cat.get("category_id") and cat.get("support_role_id"):
                valid_cats.append((key, cat))
        if not valid_cats:
            await ctx.send(
                "❌ Keine gültigen Kategorien gefunden. Jede Kategorie braucht:\n"
                "• `category_id` (Discord-Kategorie) — `[p]ticketset category categoryid <key> <category>`\n"
                "• `support_role_id` (Support-Rolle) — `[p]ticketset category role <key> @Rolle`"
            )
            return
        if len(valid_cats) > 25:
            await ctx.send(f"❌ Zu viele Kategorien ({len(valid_cats)}). Discord erlaubt max 25 Buttons pro Nachricht.")
            return
        # Panel erstellen
        embed = discord.Embed(
            title="🎫 Ticket erstellen",
            description=(
                "Wähle unten aus, welche Art von Ticket du erstellen möchtest.\n\n"
                "**Verfügbare Kategorien:**\n"
                + "\n".join(f"{cat.get('emoji', '🎫')} **{cat.get('name', key)}** — {cat.get('description', '')}" for key, cat in valid_cats)
            ),
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.set_footer(text="Ticket-System • Wähle eine Kategorie")
        view = TicketMultiPanelView(self, valid_cats)
        # Panel-Channel setzen = aktueller Channel falls Text-Channel
        channel = ctx.channel if isinstance(ctx.channel, discord.TextChannel) else None
        if channel is None:
            await ctx.send("❌ Muss in einem Text-Channel ausgeführt werden.")
            return
        try:
            message = await channel.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException) as e:
            await ctx.send(f"❌ Konnte Panel nicht senden: `{e}`")
            return
        # Altes Multi-Panel löschen falls vorhanden
        old_id = await self.config.guild(ctx.guild).ticket_panel_message_id()
        if old_id:
            try:
                old_msg = await channel.fetch_message(old_id)
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await self.config.guild(ctx.guild).ticket_panel_message_id.set(message.id)
        await self.config.guild(ctx.guild).ticket_panel_channel.set(channel.id)
        await self.config.guild(ctx.guild).ticket_panel_multi_enabled.set(True)
        await ctx.send(f"✅ Multi-Kategorie Panel erstellt: {message.jump_url}")

    # ============================================
    # MULTI-KATEGORIE-CORE-LOGIK
    # ============================================

    async def _ticket_create_for_category(self, interaction: discord.Interaction, cat_key: str, subject: str, requester: discord.Member):
        """Erstellt ein Ticket für eine bestimmte Kategorie."""
        guild = interaction.guild
        cats = await self.config.guild(guild).ticket_categories() or {}
        cat = cats.get(cat_key)
        if not cat:
            try:
                await interaction.response.send_message(f"❌ Kategorie `{cat_key}` existiert nicht mehr.", ephemeral=True)
            except discord.HTTPException:
                pass
            return

        category_id = cat.get("category_id")
        support_role_id = cat.get("support_role_id")
        category = guild.get_channel(category_id) if category_id else None
        support_role = guild.get_role(support_role_id) if support_role_id else None

        if not category or not isinstance(category, discord.CategoryChannel):
            try:
                await interaction.response.send_message(
                    "❌ Die Discord-Kategorie für diese Ticket-Kategorie ist nicht (mehr) verfügbar. Bitte informiere einen Admin.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return
        if not support_role:
            try:
                await interaction.response.send_message(
                    "❌ Die Support-Rolle für diese Ticket-Kategorie ist nicht (mehr) verfügbar. Bitte informiere einen Admin.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return

        # Blacklist prüfen
        blacklist = await self.config.guild(guild).ticket_blacklist() or []
        if requester.id in blacklist:
            try:
                await interaction.response.send_message(
                    "❌ Du bist aktuell vom Ticket-System ausgeschlossen.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return

        # Max-Open-Check
        max_open = await self.config.guild(guild).ticket_max_open() or 1
        current_open = await self._ticket_user_open_count(guild, requester.id)
        if current_open >= max_open:
            try:
                await interaction.response.send_message(
                    f"❌ Du hast bereits {current_open} offene Ticket(s). Maximum ist {max_open}.\n"
                    f"Bitte schließe zuerst ein bestehendes Ticket.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        # Ticket-Nummer hochzählen
        counter = await self.config.guild(guild).ticket_counter() or 0
        counter += 1
        await self.config.guild(guild).ticket_counter.set(counter)
        # Ticket-Name: <kategorie>-<nummer>
        ticket_name = f"{cat_key}-{counter:04d}"

        # Berechtigungen
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            requester: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                attach_files=True, embed_links=True,
            ),
            support_role: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                attach_files=True, embed_links=True, manage_messages=True,
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                manage_channels=True, manage_messages=True,
            ),
        }

        try:
            ticket_channel = await guild.create_text_channel(
                name=ticket_name,
                category=category,
                overwrites=overwrites,
                reason=f"Ticket #{counter} ({cat_key}) von {requester.display_name}",
                topic=f"Ticket #{counter} • Category: {cat_key} • User: {requester.id} • Betreff: {subject[:200]} • Created: {_now_ts()}",
            )
        except discord.Forbidden:
            try:
                await interaction.followup.send(
                    "❌ Mir fehlen die Rechte, um einen Ticket-Channel zu erstellen.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return
        except discord.HTTPException as e:
            try:
                await interaction.followup.send(
                    f"❌ Konnte Ticket-Channel nicht erstellen: `{e}`",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return

        # Active-Tracking aktualisieren
        await self._ticket_add_active(guild, requester.id, ticket_channel.id)

        # Welcome-Embed
        welcome_msg = cat.get("welcome_message") or "Ein Teammitglied wird sich gleich um dein Anliegen kümmern."
        claim_enabled = await self.config.guild(guild).ticket_claim_enabled()
        color_name = cat.get("color", "blurple")
        color_map = {
            "blurple": discord.Color.blurple(),
            "red": discord.Color.red(),
            "green": discord.Color.green(),
            "grey": discord.Color.greyple(),
            "orange": discord.Color.orange(),
        }
        embed_color = color_map.get(color_name, discord.Color.blurple())

        embed = discord.Embed(
            title=f"{cat.get('emoji', '🎫')} {cat.get('name', 'Ticket')} #{counter}",
            description=(
                f"Hallo {requester.mention}!\n\n"
                f"**Betreff:** {subject}\n\n"
                f"{welcome_msg}\n\n"
                f"Bitte beschreibe dein Problem so detailliert wie möglich."
            ),
            color=embed_color,
            timestamp=_now(),
        )
        embed.add_field(name="🎫 Ticket-ID", value=f"#{counter}", inline=True)
        embed.add_field(name="📂 Kategorie", value=cat.get("name", cat_key), inline=True)
        embed.add_field(name="👤 Erstellt von", value=f"{requester.mention}\n`{requester.id}`", inline=True)
        embed.add_field(name="📊 Status", value="🟡 Offen", inline=True)
        embed.set_footer(text="Nutze den 'Ticket schließen' Button wenn dein Anliegen geklärt ist.")

        # View mit Close + Claim + Unclaim-Buttons
        view = TicketControlView(self, claim_enabled=claim_enabled)
        try:
            await ticket_channel.send(
                content=f"{cat.get('emoji', '🎫')} {support_role.mention} | {requester.mention}",
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=[support_role], users=[requester]),
            )
        except discord.HTTPException:
            log.warning("Konnte Begrüßungs-Nachricht im Ticket-Channel nicht senden (Guild %s)", guild.id)

        # Welcome-Nachricht pinnen für bessere Übersicht
        try:
            # Letzte Nachricht (unsere) pinnen
            async for msg in ticket_channel.history(limit=1):
                await msg.pin()
                break
        except (discord.Forbidden, discord.HTTPException):
            pass  # nicht kritisch

        # Log-Eintrag
        log_channel_id = await self.config.guild(guild).ticket_log_channel()
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
        if log_channel:
            try:
                log_embed = discord.Embed(
                    title="🎫 Ticket erstellt",
                    description=f"Ticket-Channel: {ticket_channel.mention}",
                    color=discord.Color.green(),
                    timestamp=_now(),
                )
                log_embed.add_field(name="👤 Von", value=f"{requester.mention}\n`{requester.id}`", inline=True)
                log_embed.add_field(name="🎫 ID", value=f"#{counter}", inline=True)
                log_embed.add_field(name="📂 Kategorie", value=cat.get("name", cat_key), inline=True)
                log_embed.add_field(name="📝 Betreff", value=subject[:1024], inline=False)
                log_embed.add_field(name="🔗 Channel", value=ticket_channel.mention, inline=True)
                log_embed.add_field(name="🎭 Support-Rolle", value=support_role.mention, inline=True)
                log_embed.set_footer(text=f"Ticket-System • {guild.name}")
                await log_channel.send(embed=log_embed)
            except discord.HTTPException:
                pass

        try:
            await interaction.followup.send(
                f"✅ Dein Ticket wurde erstellt: {ticket_channel.mention}",
                ephemeral=True,
            )
        except discord.HTTPException:
            pass

    # ============================================
    # ERWEITERTE TICKET-CORE-LOGIK
    # ============================================

    async def _ticket_get_creator(self, channel: discord.TextChannel) -> Optional[int]:
        """Extrahiert die User-ID des Ticket-Erstellers aus dem Channel-Topic."""
        if not channel.topic:
            return None
        m = re.search(r"User: (\d+)", channel.topic)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        return None

    def _ticket_build_html_transcript(
        self,
        channel: discord.TextChannel,
        messages: list,
        *,
        closed_by: Optional[discord.abc.User] = None,
        reason: str = "",
        creator: Optional[discord.abc.User] = None,
        category_name: str = "",
    ) -> str:
        """Baut ein schick formatiertes HTML-Transcript."""
        import html as _html
        ticket_num = "?"
        if channel.topic:
            m = re.search(r"Ticket #(\d+)", channel.topic)
            if m:
                ticket_num = m.group(1)
        # Header
        closed_at_str = _now().strftime("%d.%m.%Y %H:%M:%S UTC")
        created_at_str = ""
        if channel.created_at:
            created_at_str = channel.created_at.strftime("%d.%m.%Y %H:%M:%S UTC")
        creator_name = creator.display_name if creator else "Unbekannt"
        creator_id = creator.id if creator else "?"
        closer_name = closed_by.display_name if closed_by else "Unbekannt"
        closer_id = closed_by.id if closed_by else "?"
        msg_count = len(messages)
        # Messages HTML bauen
        msg_html_parts = []
        for msg in messages:
            ts = msg.created_at.strftime("%H:%M:%S") if msg.created_at else "?"
            date_str = msg.created_at.strftime("%d.%m.%Y") if msg.created_at else ""
            author_name = _html.escape(msg.author.display_name) if msg.author else "Unbekannt"
            author_id = msg.author.id if msg.author else "?"
            author_avatar = msg.author.display_avatar.url if msg.author and hasattr(msg.author, "display_avatar") else ""
            author_color = "#5865F2"
            try:
                if msg.author and hasattr(msg.author, "color") and msg.author.color.value != 0:
                    author_color = f"#{msg.author.color.value:06x}"
            except Exception:
                pass
            content_html = _html.escape(msg.content) if msg.content else ""
            if content_html:
                # Simple markdown: **bold**, *italic*, `code`, line breaks
                content_html = content_html.replace("\n", "<br>")
                import re as _re
                content_html = _re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", content_html)
                content_html = _re.sub(r"\*(.+?)\*", r"<em>\1</em>", content_html)
                content_html = _re.sub(r"`(.+?)`", r"<code>\1</code>", content_html)
            # Attachments
            attachments_html = ""
            if msg.attachments:
                attachments_html = '<div class="attachments">'
                for att in msg.attachments:
                    ext = att.filename.lower().rsplit(".", 1)[-1] if "." in att.filename else ""
                    if ext in ("png", "jpg", "jpeg", "gif", "webp", "bmp"):
                        attachments_html += f'<div class="attachment image"><a href="{_html.escape(att.url)}" target="_blank"><img src="{_html.escape(att.url)}" alt="{_html.escape(att.filename)}"></a></div>'
                    else:
                        attachments_html += f'<div class="attachment file"><a href="{_html.escape(att.url)}" target="_blank">📎 {_html.escape(att.filename)} ({att.size // 1024} KB)</a></div>'
                attachments_html += '</div>'
            # Embeds (vereinfacht)
            embeds_html = ""
            if msg.embeds:
                for emb in msg.embeds:
                    embed_title = _html.escape(emb.title) if emb.title else ""
                    embed_desc = _html.escape(emb.description) if emb.description else ""
                    embed_color = "#5865F2"
                    if emb.color:
                        try:
                            embed_color = f"#{emb.color.value:06x}"
                        except Exception:
                            pass
                    embeds_html += f'<div class="embed" style="border-left-color: {embed_color};"><div class="embed-title">{embed_title}</div><div class="embed-desc">{embed_desc}</div></div>'
            msg_html_parts.append(f"""
            <div class="message">
                <div class="message-avatar"><img src="{_html.escape(str(author_avatar))}" alt=""></div>
                <div class="message-content">
                    <div class="message-header">
                        <span class="author" style="color: {author_color};">{author_name}</span>
                        <span class="timestamp">{date_str} {ts}</span>
                    </div>
                    <div class="message-text">{content_html}</div>
                    {attachments_html}
                    {embeds_html}
                </div>
            </div>""")
        messages_html = "\n".join(msg_html_parts) or '<div class="no-messages">Keine Nachrichten vorhanden</div>'
        # Volles HTML
        html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ticket #{ticket_num} Transcript — {channel.name}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif; background: #313338; color: #dbdee1; padding: 20px; }}
        .container {{ max-width: 900px; margin: 0 auto; background: #2b2d31; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.3); }}
        .header {{ background: #1e1f22; padding: 20px; border-bottom: 3px solid #5865F2; }}
        .header h1 {{ color: #fff; font-size: 22px; margin-bottom: 10px; }}
        .header .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; color: #949ba4; font-size: 13px; }}
        .header .meta-item {{ background: #313338; padding: 8px 12px; border-radius: 4px; }}
        .header .meta-item strong {{ color: #dbdee1; display: block; margin-bottom: 2px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
        .messages {{ padding: 20px; }}
        .message {{ display: flex; gap: 12px; padding: 8px 0; }}
        .message:hover {{ background: rgba(255,255,255,0.02); }}
        .message-avatar {{ flex-shrink: 0; }}
        .message-avatar img {{ width: 40px; height: 40px; border-radius: 50%; }}
        .message-content {{ flex: 1; min-width: 0; }}
        .message-header {{ display: flex; align-items: baseline; gap: 8px; margin-bottom: 2px; }}
        .author {{ font-weight: 600; font-size: 14px; }}
        .timestamp {{ font-size: 11px; color: #949ba4; }}
        .message-text {{ font-size: 14px; line-height: 1.4; word-wrap: break-word; }}
        .message-text code {{ background: #1e1f22; padding: 2px 4px; border-radius: 3px; font-family: 'Consolas', monospace; font-size: 13px; }}
        .attachments {{ margin-top: 8px; display: flex; flex-direction: column; gap: 8px; }}
        .attachment.image img {{ max-width: 400px; max-height: 300px; border-radius: 4px; }}
        .attachment.file a {{ color: #00a8fc; text-decoration: none; font-size: 13px; }}
        .attachment.file a:hover {{ text-decoration: underline; }}
        .embed {{ background: #2b2d31; border-left: 4px solid #5865F2; padding: 8px 12px; margin-top: 8px; border-radius: 0 4px 4px 0; }}
        .embed-title {{ font-weight: 600; color: #fff; font-size: 14px; margin-bottom: 4px; }}
        .embed-desc {{ font-size: 13px; color: #dbdee1; }}
        .no-messages {{ text-align: center; color: #949ba4; padding: 40px; }}
        .footer {{ background: #1e1f22; padding: 12px 20px; text-align: center; color: #949ba4; font-size: 11px; border-top: 1px solid #3f4147; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎫 Ticket #{ticket_num} — Transcript</h1>
            <div class="meta">
                <div class="meta-item"><strong>Channel</strong>{_html.escape(channel.name)}</div>
                <div class="meta-item"><strong>Kategorie</strong>{_html.escape(category_name or "—")}</div>
                <div class="meta-item"><strong>Erstellt am</strong>{created_at_str or "—"}</div>
                <div class="meta-item"><strong>Geschlossen am</strong>{closed_at_str}</div>
                <div class="meta-item"><strong>Ersteller</strong>{_html.escape(creator_name)} ({creator_id})</div>
                <div class="meta-item"><strong>Geschlossen von</strong>{_html.escape(closer_name)} ({closer_id})</div>
                <div class="meta-item"><strong>Nachrichten</strong>{msg_count}</div>
                <div class="meta-item"><strong>Grund</strong>{_html.escape(reason[:200] or "Kein Grund angegeben")}</div>
            </div>
        </div>
        <div class="messages">
            {messages_html}
        </div>
        <div class="footer">
            Generiert von SupportCog • {_now().strftime("%d.%m.%Y %H:%M:%S UTC")} • {msg_count} Nachrichten
        </div>
    </div>
</body>
</html>"""
        return html

    async def _ticket_create_transcript(self, channel: discord.TextChannel, *, closed_by: Optional[discord.abc.User] = None, reason: str = "") -> tuple:
        """Erstellt ein Transcript für einen Ticket-Channel.
        Returns: (html_content, txt_content, creator_user, ticket_num_str)."""
        messages = []
        try:
            async for msg in channel.history(limit=None, oldest_first=True):
                messages.append(msg)
        except (discord.Forbidden, discord.HTTPException):
            pass
        # Creator ermitteln
        creator_id = await self._ticket_get_creator(channel)
        creator_user = None
        if creator_id is not None:
            try:
                creator_user = await self.bot.fetch_user(creator_id)
            except (discord.NotFound, discord.HTTPException):
                pass
        # Kategorie ermitteln
        category_name = ""
        if channel.topic:
            m = re.search(r"Category: (\w+)", channel.topic)
            if m:
                cat_key = m.group(1)
                cats = await self.config.guild(channel.guild).ticket_categories() or {}
                cat = cats.get(cat_key)
                if cat:
                    category_name = cat.get("name", cat_key)
        # HTML Transcript
        html_content = self._ticket_build_html_transcript(
            channel, messages,
            closed_by=closed_by, reason=reason,
            creator=creator_user, category_name=category_name,
        )
        # TXT Transcript (Fallback)
        txt_lines = []
        ticket_num = "?"
        if channel.topic:
            m = re.search(r"Ticket #(\d+)", channel.topic)
            if m:
                ticket_num = m.group(1)
        txt_lines.append(f"Transcript für Ticket #{ticket_num}")
        txt_lines.append(f"Channel: #{channel.name}")
        txt_lines.append(f"Kategorie: {category_name or '—'}")
        txt_lines.append(f"Geschlossen von: {closed_by} ({closed_by.id if closed_by else '?'})")
        txt_lines.append(f"Grund: {reason}")
        txt_lines.append("=" * 60)
        txt_lines.append("")
        for msg in messages:
            ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC") if msg.created_at else "?"
            author = msg.author.display_name if msg.author else "Unbekannt"
            content = msg.content or ""
            if msg.attachments:
                content += " " + " ".join(f"[Attachment: {a.filename}]" for a in msg.attachments)
            if msg.embeds:
                content += f" [{len(msg.embeds)} Embeds]"
            txt_lines.append(f"[{ts}] {author}: {content}")
        txt_content = "\n".join(txt_lines)
        return html_content, txt_content, creator_user, ticket_num

    async def _ticket_get_counter(self, channel: discord.TextChannel) -> Optional[int]:
        """Extrahiert die Ticket-Nummer aus dem Channel-Topic."""
        if not channel.topic:
            return None
        m = re.search(r"Ticket #(\d+)", channel.topic)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
        return None

    async def _ticket_user_open_count(self, guild: discord.Guild, user_id: int) -> int:
        """Gibt die Anzahl aktuell offener Tickets eines Users zurück."""
        active = await self.config.guild(guild).ticket_active() or {}
        return len(active.get(str(user_id), []))

    async def _ticket_add_active(self, guild: discord.Guild, user_id: int, channel_id: int):
        """Registriert ein offenes Ticket."""
        active = await self.config.guild(guild).ticket_active() or {}
        key = str(user_id)
        if key not in active:
            active[key] = []
        if channel_id not in active[key]:
            active[key].append(channel_id)
        await self.config.guild(guild).ticket_active.set(active)

    async def _ticket_remove_active(self, guild: discord.Guild, user_id: int, channel_id: int):
        """Entfernt ein geschlossenes Ticket aus dem Tracking."""
        active = await self.config.guild(guild).ticket_active() or {}
        key = str(user_id)
        if key in active:
            if channel_id in active[key]:
                active[key].remove(channel_id)
            if not active[key]:
                del active[key]
            await self.config.guild(guild).ticket_active.set(active)

    async def _create_ticket(self, interaction: discord.Interaction, subject: str, requester: discord.Member):
        """Erstellt einen Ticket-Channel für einen User."""
        guild = interaction.guild
        category = await self.get_ticket_category(guild)
        support_role = await self.get_ticket_support_role(guild)
        if not category or not support_role:
            await interaction.response.send_message(
                "❌ Ticket-System ist nicht vollständig konfiguriert. Bitte informiere einen Admin.",
                ephemeral=True,
            )
            return

        # Blacklist prüfen
        blacklist = await self.config.guild(guild).ticket_blacklist() or []
        if requester.id in blacklist:
            await interaction.response.send_message(
                "❌ Du bist aktuell vom Ticket-System ausgeschlossen und kannst keine Tickets erstellen.",
                ephemeral=True,
            )
            return

        # Max-Open-Check
        max_open = await self.config.guild(guild).ticket_max_open() or 1
        current_open = await self._ticket_user_open_count(guild, requester.id)
        if current_open >= max_open:
            await interaction.response.send_message(
                f"❌ Du hast bereits {current_open} offene Ticket(s). Maximum ist {max_open}.\n"
                f"Bitte schließe zuerst ein bestehendes Ticket.",
                ephemeral=True,
            )
            return

        # Response bereits gesendet? Sonst deferred response
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        # Ticket-Nummer hochzählen
        counter = await self.config.guild(guild).ticket_counter() or 0
        counter += 1
        await self.config.guild(guild).ticket_counter.set(counter)
        ticket_name = f"ticket-{counter:04d}"

        # Berechtigungen
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            requester: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                attach_files=True, embed_links=True,
            ),
            support_role: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                attach_files=True, embed_links=True, manage_messages=True,
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True,
                manage_channels=True, manage_messages=True,
            ),
        }

        try:
            ticket_channel = await guild.create_text_channel(
                name=ticket_name,
                category=category,
                overwrites=overwrites,
                reason=f"Ticket #{counter} von {requester.display_name}",
                topic=f"Ticket #{counter} • User: {requester.id} • Betreff: {subject[:200]} • Created: {_now_ts()}",
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "❌ Mir fehlen die Rechte, um einen Ticket-Channel zu erstellen.",
                ephemeral=True,
            )
            return
        except discord.HTTPException as e:
            await interaction.followup.send(
                f"❌ Konnte Ticket-Channel nicht erstellen: `{e}`",
                ephemeral=True,
            )
            return

        # Active-Tracking aktualisieren
        await self._ticket_add_active(guild, requester.id, ticket_channel.id)

        # Welcome-Embed
        welcome_msg = await self.config.guild(guild).ticket_welcome_message() or "Willkommen zu deinem Ticket!"
        claim_enabled = await self.config.guild(guild).ticket_claim_enabled()

        embed = discord.Embed(
            title=f"🎫 Ticket #{counter}",
            description=(
                f"Hallo {requester.mention}!\n\n"
                f"**Betreff:** {subject}\n\n"
                f"{welcome_msg}\n\n"
                f"Bitte beschreibe dein Problem so detailliert wie möglich."
            ),
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.add_field(name="🎫 Ticket-ID", value=f"#{counter}", inline=True)
        embed.add_field(name="👤 Erstellt von", value=f"{requester.mention}\n`{requester.id}`", inline=True)
        embed.add_field(name="📊 Status", value="🟡 Offen", inline=True)
        embed.set_footer(text="Nutze den 'Ticket schließen' Button wenn dein Anliegen geklärt ist.")

        # View mit Close + Claim + Add-User-Buttons
        view = TicketControlView(self, claim_enabled=claim_enabled)
        try:
            await ticket_channel.send(
                content=f"🎫 {support_role.mention} | {requester.mention}",
                embed=embed,
                view=view,
                allowed_mentions=discord.AllowedMentions(roles=[support_role], users=[requester]),
            )
        except discord.HTTPException:
            log.warning("Konnte Begrüßungs-Nachricht im Ticket-Channel nicht senden (Guild %s)", guild.id)

        # Log-Eintrag
        log_channel_id = await self.config.guild(guild).ticket_log_channel()
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
        if log_channel:
            try:
                log_embed = discord.Embed(
                    title="🎫 Ticket erstellt",
                    description=f"Ticket-Channel: {ticket_channel.mention}",
                    color=discord.Color.green(),
                    timestamp=_now(),
                )
                log_embed.add_field(name="👤 Von", value=f"{requester.mention}\n`{requester.id}`", inline=True)
                log_embed.add_field(name="🎫 ID", value=f"#{counter}", inline=True)
                log_embed.add_field(name="📝 Betreff", value=subject[:1024], inline=False)
                log_embed.add_field(name="🔗 Channel", value=ticket_channel.mention, inline=True)
                log_embed.set_footer(text=f"Ticket-System • {guild.name}")
                await log_channel.send(embed=log_embed)
            except discord.HTTPException:
                pass

        try:
            await interaction.followup.send(
                f"✅ Dein Ticket wurde erstellt: {ticket_channel.mention}",
                ephemeral=True,
            )
        except discord.HTTPException:
            pass

    async def _close_ticket(self, interaction: discord.Interaction, channel: discord.TextChannel, *, reason: str = "Kein Grund angegeben"):
        """Schließt einen Ticket-Channel (mit Transcript + Log)."""
        guild = interaction.guild
        if not channel.name.startswith("ticket-"):
            try:
                await interaction.response.send_message(
                    "❌ Dieser Channel ist kein Ticket-Channel.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return
        support_role = await self.get_ticket_support_role(guild)
        requester_member = interaction.user
        if not isinstance(requester_member, discord.Member):
            try:
                await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            except discord.HTTPException:
                pass
            return

        # Berechtigungsprüfung
        is_support = support_role is not None and requester_member.get_role(support_role.id) is not None
        is_admin = requester_member.guild_permissions.manage_channels
        creator_id = await self._ticket_get_creator(channel)
        is_creator = creator_id is not None and requester_member.id == creator_id
        user_can_close = await self.config.guild(guild).ticket_user_can_close()

        if is_creator and not user_can_close and not (is_support or is_admin):
            try:
                await interaction.response.send_message(
                    "❌ User dürfen ihre Tickets nicht selbst schließen. Bitte ein Teammitglied bitten.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return
        if not (is_support or is_admin or is_creator):
            try:
                await interaction.response.send_message(
                    "❌ Du bist nicht berechtigt, dieses Ticket zu schließen.",
                    ephemeral=True,
                )
            except discord.HTTPException:
                pass
            return

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception:
            pass

        # Transcript erstellen (vor dem Löschen!)
        transcript_html = ""
        transcript_txt = ""
        transcript_enabled = await self.config.guild(guild).ticket_transcript()
        creator_user = None
        ticket_num_str = "?"
        if transcript_enabled:
            try:
                transcript_html, transcript_txt, creator_user, ticket_num_str = await self._ticket_create_transcript(
                    channel, closed_by=requester_member, reason=reason
                )
            except Exception as e:
                log.exception("Transcript-Erstellung fehlgeschlagen")

        # Creator ermitteln für DM
        creator_mention = "Unbekannt"
        if creator_id is not None:
            creator_mention = f"<@{creator_id}>"
        if creator_user is None and creator_id is not None:
            try:
                creator_user = await self.bot.fetch_user(creator_id)
            except (discord.NotFound, discord.HTTPException):
                creator_user = None

        # Log-Eintrag VOR dem Löschen
        log_channel_id = await self.config.guild(guild).ticket_log_channel()
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
        if log_channel:
            try:
                log_embed = discord.Embed(
                    title="🎫 Ticket geschlossen",
                    description=f"Ticket-Channel: `{channel.name}` (gelöscht)",
                    color=discord.Color.red(),
                    timestamp=_now(),
                )
                log_embed.add_field(name="👤 Geschlossen von", value=f"{requester_member.mention}\n`{requester_member.id}`", inline=True)
                log_embed.add_field(name="🎫 Ersteller", value=creator_mention, inline=True)
                log_embed.add_field(name="📝 Grund", value=reason[:500], inline=False)
                # Transcript als File anhängen (HTML + TXT)
                if transcript_html:
                    try:
                        import io
                        html_file = discord.File(io.StringIO(transcript_html), filename=f"transcript-ticket-{ticket_num_str}.html")
                        txt_file = discord.File(io.StringIO(transcript_txt), filename=f"transcript-ticket-{ticket_num_str}.txt")
                        try:
                            await log_channel.send(embed=log_embed, files=[html_file, txt_file])
                        except discord.HTTPException:
                            # Fallback: nur TXT (HTML evtl. zu groß)
                            await log_channel.send(embed=log_embed, file=txt_file)
                    except (discord.Forbidden, discord.HTTPException):
                        await log_channel.send(embed=log_embed)
                else:
                    await log_channel.send(embed=log_embed)
            except discord.HTTPException:
                pass

        # DM an Ersteller
        dm_enabled = await self.config.guild(guild).ticket_dm_on_close()
        if dm_enabled and creator_user:
            try:
                dm_embed = discord.Embed(
                    title="🎫 Dein Ticket wurde geschlossen",
                    description=(
                        f"Dein Ticket auf **{guild.name}** wurde geschlossen.\n\n"
                        f"**Geschlossen von:** {requester_member.display_name}\n"
                        f"**Grund:** {reason[:500]}"
                    ),
                    color=discord.Color.red(),
                    timestamp=_now(),
                )
                if transcript_html:
                    try:
                        import io
                        html_file = discord.File(io.StringIO(transcript_html), filename=f"transcript-ticket-{ticket_num_str}.html")
                        txt_file = discord.File(io.StringIO(transcript_txt), filename=f"transcript-ticket-{ticket_num_str}.txt")
                        try:
                            await creator_user.send(embed=dm_embed, files=[html_file, txt_file])
                        except discord.HTTPException:
                            await creator_user.send(embed=dm_embed, file=txt_file)
                    except (discord.Forbidden, discord.HTTPException):
                        await creator_user.send(embed=dm_embed)
                else:
                    await creator_user.send(embed=dm_embed)
            except (discord.Forbidden, discord.HTTPException):
                pass

        # Active-Tracking aktualisieren
        if creator_id is not None:
            await self._ticket_remove_active(guild, creator_id, channel.id)

        # Channel löschen
        try:
            await channel.delete(reason=f"Ticket geschlossen von {requester_member.display_name}: {reason[:100]}")
        except discord.Forbidden:
            try:
                await interaction.followup.send("❌ Mir fehlen die Rechte, um den Channel zu löschen.", ephemeral=True)
            except discord.HTTPException:
                pass
        except discord.HTTPException as e:
            try:
                await interaction.followup.send(f"❌ Konnte Channel nicht löschen: `{e}`", ephemeral=True)
            except discord.HTTPException:
                pass

    # ============================================
    # TICKET-STAFF-BEFEHLE
    # ============================================

    @commands.command(name="claim", aliases=["claimticket"])
    @commands.guild_only()
    async def claim_ticket(self, ctx: commands.Context):
        """Übernimmt ein Ticket (Claim-System). Zeigt anderen Teammitgliedern dass du zuständig bist."""
        if not ctx.channel.name.startswith("ticket-") and not self._is_ticket_channel(ctx.channel):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        claim_enabled = await self.config.guild(ctx.guild).ticket_claim_enabled()
        if not claim_enabled:
            await ctx.send("❌ Claim-System ist deaktiviert.")
            return
        # Berechtigungsprüfung: Support-Rolle (legacy oder kategorie-spezifisch) oder Admin
        if not await self._is_ticket_staff(ctx.author, ctx.channel, ctx.guild):
            await ctx.send("❌ Du brauchst eine Support-Rolle oder Admin-Rechte.")
            return
        # Channel-Topic aktualisieren
        topic = ctx.channel.topic or ""
        # Alten Claimed-Eintrag entfernen
        topic_clean = re.sub(r" • Claimed by: \d+", "", topic)
        new_topic = f"{topic_clean} • Claimed by: {ctx.author.id}"
        try:
            await ctx.channel.edit(topic=new_topic, reason=f"Ticket claimed by {ctx.author.display_name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            await ctx.send(f"❌ Konnte Channel-Topic nicht aktualisieren: `{e}`")
            return
        embed = discord.Embed(
            title="✅ Ticket übernommen",
            description=f"{ctx.author.mention} hat dieses Ticket übernommen und ist nun zuständig.",
            color=discord.Color.green(),
            timestamp=_now(),
        )
        await ctx.send(embed=embed)

    @commands.command(name="unclaim", aliases=["unclaimticket", "release"])
    @commands.guild_only()
    async def unclaim_ticket(self, ctx: commands.Context):
        """Gibt ein geclaimtes Ticket wieder ab (Unclaim). Das Ticket ist danach wieder für alle Teammitglieder offen."""
        if not ctx.channel.name.startswith("ticket-") and not self._is_ticket_channel(ctx.channel):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        claim_enabled = await self.config.guild(ctx.guild).ticket_claim_enabled()
        if not claim_enabled:
            await ctx.send("❌ Claim-System ist deaktiviert.")
            return
        # Prüfen ob Ticket überhaupt geclaimt ist
        topic = ctx.channel.topic or ""
        m = re.search(r"Claimed by: (\d+)", topic)
        if not m:
            await ctx.send("ℹ️ Dieses Ticket ist aktuell nicht geclaimt.")
            return
        claimed_by_id = int(m.group(1))
        # Berechtigung: nur der Claimer selbst, Admins oder andere Support-Mitglieder können unclaimen
        is_admin = ctx.author.guild_permissions.manage_channels
        is_claimer = ctx.author.id == claimed_by_id
        is_staff = await self._is_ticket_staff(ctx.author, ctx.channel, ctx.guild)
        if not (is_claimer or is_admin or is_staff):
            await ctx.send("❌ Nur der aktuelle Claimer, Teammitglieder oder Admins können das Ticket freigeben.")
            return
        # Claimed-Eintrag aus Topic entfernen
        topic_clean = re.sub(r" • Claimed by: \d+", "", topic)
        try:
            await ctx.channel.edit(topic=topic_clean, reason=f"Ticket unclaimed by {ctx.author.display_name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            await ctx.send(f"❌ Konnte Channel-Topic nicht aktualisieren: `{e}`")
            return
        embed = discord.Embed(
            title="🔓 Ticket freigegeben",
            description=(
                f"{ctx.author.mention} hat dieses Ticket wieder freigegeben.\n"
                f"Es kann nun von jedem Teammitglied übernommen werden (`[p]claim`)."
            ),
            color=discord.Color.orange(),
            timestamp=_now(),
        )
        await ctx.send(embed=embed)

    def _is_ticket_channel(self, channel) -> bool:
        """Prüft ob ein Channel ein Ticket-Channel ist (anhand des Topics)."""
        if not hasattr(channel, "topic") or not channel.topic:
            return False
        return "Ticket #" in (channel.topic or "")

    async def _is_ticket_staff(self, member: discord.Member, channel, guild: discord.Guild) -> bool:
        """Prüft ob ein Member Support-Staff für diesen Ticket-Channel ist.
        Berücksichtigt sowohl die globale Support-Rolle als auch die kategorie-spezifische Rolle."""
        if member.guild_permissions.manage_channels:
            return True
        # Globale Support-Rolle prüfen
        support_role = await self.get_ticket_support_role(guild)
        if support_role and member.get_role(support_role.id) is not None:
            return True
        # Kategorie-spezifische Rolle prüfen
        if hasattr(channel, "topic") and channel.topic:
            # Kategorie-Key aus Topic extrahieren
            m = re.search(r"Category: (\w+)", channel.topic)
            if m:
                cat_key = m.group(1)
                cats = await self.config.guild(guild).ticket_categories() or {}
                cat = cats.get(cat_key)
                if cat:
                    role_id = cat.get("support_role_id")
                    if role_id:
                        role = guild.get_role(role_id)
                        if role and member.get_role(role_id) is not None:
                            return True
        return False

    @commands.command(name="ticketadd", aliases=["tadd", "adduser"])
    @commands.guild_only()
    async def ticket_add(self, ctx: commands.Context, user: discord.Member):
        """Fügt einen User zum aktuellen Ticket-Channel hinzu."""
        if not ctx.channel.name.startswith("ticket-") and not self._is_ticket_channel(ctx.channel):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        is_staff = await self._is_ticket_staff(ctx.author, ctx.channel, ctx.guild)
        creator_id = await self._ticket_get_creator(ctx.channel)
        is_creator = creator_id == ctx.author.id
        if not (is_staff or is_creator):
            await ctx.send("❌ Du bist nicht berechtigt, User hinzuzufügen.")
            return
        try:
            await ctx.channel.set_permissions(
                user,
                view_channel=True, send_messages=True,
                read_message_history=True, attach_files=True,
                reason=f"Hinzugefügt von {ctx.author.display_name}",
            )
            await ctx.send(f"✅ {user.mention} wurde zum Ticket hinzugefügt.")
        except discord.Forbidden:
            await ctx.send("❌ Mir fehlen die Rechte um Permissions zu setzen.")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Fehler: `{e}`")

    @commands.command(name="ticketremove", aliases=["tremove", "removeuser"])
    @commands.guild_only()
    async def ticket_remove(self, ctx: commands.Context, user: discord.Member):
        """Entfernt einen User aus dem aktuellen Ticket-Channel."""
        if not ctx.channel.name.startswith("ticket-") and not self._is_ticket_channel(ctx.channel):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        is_staff = await self._is_ticket_staff(ctx.author, ctx.channel, ctx.guild)
        creator_id = await self._ticket_get_creator(ctx.channel)
        is_creator = creator_id == ctx.author.id
        if not (is_staff or is_creator):
            await ctx.send("❌ Du bist nicht berechtigt, User zu entfernen.")
            return
        if user.id == creator_id:
            await ctx.send("❌ Du kannst den Ticket-Ersteller nicht entfernen.")
            return
        try:
            await ctx.channel.set_permissions(
                user,
                overwrite=None,
                reason=f"Entfernt von {ctx.author.display_name}",
            )
            await ctx.send(f"✅ {user.mention} wurde aus dem Ticket entfernt.")
        except discord.Forbidden:
            await ctx.send("❌ Mir fehlen die Rechte um Permissions zu setzen.")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Fehler: `{e}`")

    @commands.command(name="ticketrename", aliases=["trename", "renameticket"])
    @commands.guild_only()
    async def ticket_rename(self, ctx: commands.Context, *, new_name: str):
        """Benennt den aktuellen Ticket-Channel um."""
        if not ctx.channel.name.startswith("ticket-") and not self._is_ticket_channel(ctx.channel):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        if len(new_name) > 100:
            await ctx.send("❌ Name zu lang (max 100 Zeichen).")
            return
        is_staff = await self._is_ticket_staff(ctx.author, ctx.channel, ctx.guild)
        creator_id = await self._ticket_get_creator(ctx.channel)
        is_creator = creator_id == ctx.author.id
        if not (is_staff or is_creator):
            await ctx.send("❌ Du bist nicht berechtigt, das Ticket umzubenennen.")
            return
        # Prefix beibehalten (ticket- oder kategorie-key-)
        if not (new_name.startswith("ticket-") or re.match(r"^[a-z_]+-\d", new_name)):
            new_name = f"ticket-{new_name}"
        try:
            await ctx.channel.edit(name=new_name.lower().replace(" ", "-"), reason=f"Umbenannt von {ctx.author.display_name}")
            await ctx.send(f"✅ Ticket umbenannt zu: `{new_name}`")
        except discord.Forbidden:
            await ctx.send("❌ Mir fehlen die Rechte um den Channel umzubenennen.")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Fehler: `{e}`")

    @commands.command(name="ticketclose", aliases=["tclose", "close"])
    @commands.guild_only()
    async def ticket_close_cmd(self, ctx: commands.Context, *, reason: str = "Kein Grund angegeben"):
        """Schließt das aktuelle Ticket (Text-Befehl-Version)."""
        if not ctx.channel.name.startswith("ticket-"):
            await ctx.send("❌ Dieser Befehl funktioniert nur in Ticket-Channels.")
            return
        support_role = await self.get_ticket_support_role(ctx.guild)
        is_support = support_role and ctx.author.get_role(support_role.id) is not None
        is_admin = ctx.author.guild_permissions.manage_channels
        creator_id = await self._ticket_get_creator(ctx.channel)
        is_creator = creator_id == ctx.author.id
        user_can_close = await self.config.guild(ctx.guild).ticket_user_can_close()
        if is_creator and not user_can_close and not (is_support or is_admin):
            await ctx.send("❌ User dürfen ihre Tickets nicht selbst schließen.")
            return
        if not (is_support or is_admin or is_creator):
            await ctx.send("❌ Du bist nicht berechtigt, dieses Ticket zu schließen.")
            return
        # Wir rufen die gleiche Logik auf, aber mit einem Fake-Interaction-Objekt
        # Da _close_ticket interaction.response etc. braucht, bauen wir eine Wrapper-Klasse
        await self._close_ticket_via_command(ctx, reason)

    async def _close_ticket_via_command(self, ctx: commands.Context, reason: str):
        """Schließt ein Ticket via Text-Befehl (ohne Interaction)."""
        guild = ctx.guild
        channel = ctx.channel
        if not isinstance(channel, discord.TextChannel):
            await ctx.send("❌ Nur in Text-Channels verfügbar.")
            return

        # Transcript erstellen (vor dem Löschen!)
        transcript_html = ""
        transcript_txt = ""
        transcript_enabled = await self.config.guild(guild).ticket_transcript()
        creator_user = None
        ticket_num_str = "?"
        if transcript_enabled:
            try:
                transcript_html, transcript_txt, creator_user, ticket_num_str = await self._ticket_create_transcript(
                    channel, closed_by=ctx.author, reason=reason
                )
            except Exception:
                log.exception("Transcript-Erstellung fehlgeschlagen")

        creator_id = await self._ticket_get_creator(channel)
        creator_mention = "Unbekannt"
        if creator_id is not None:
            creator_mention = f"<@{creator_id}>"
        if creator_user is None and creator_id is not None:
            try:
                creator_user = await self.bot.fetch_user(creator_id)
            except (discord.NotFound, discord.HTTPException):
                pass

        # Log
        log_channel_id = await self.config.guild(guild).ticket_log_channel()
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
        if log_channel:
            try:
                log_embed = discord.Embed(
                    title="🎫 Ticket geschlossen",
                    description=f"Ticket-Channel: `{channel.name}` (gelöscht)",
                    color=discord.Color.red(),
                    timestamp=_now(),
                )
                log_embed.add_field(name="👤 Geschlossen von", value=f"{ctx.author.mention}\n`{ctx.author.id}`", inline=True)
                log_embed.add_field(name="🎫 Ersteller", value=creator_mention, inline=True)
                log_embed.add_field(name="📝 Grund", value=reason[:500], inline=False)
                if transcript_html:
                    try:
                        import io
                        html_file = discord.File(io.StringIO(transcript_html), filename=f"transcript-ticket-{ticket_num_str}.html")
                        txt_file = discord.File(io.StringIO(transcript_txt), filename=f"transcript-ticket-{ticket_num_str}.txt")
                        try:
                            await log_channel.send(embed=log_embed, files=[html_file, txt_file])
                        except discord.HTTPException:
                            await log_channel.send(embed=log_embed, file=txt_file)
                    except (discord.Forbidden, discord.HTTPException):
                        await log_channel.send(embed=log_embed)
                else:
                    await log_channel.send(embed=log_embed)
            except discord.HTTPException:
                pass

        # DM
        dm_enabled = await self.config.guild(guild).ticket_dm_on_close()
        if dm_enabled and creator_user:
            try:
                dm_embed = discord.Embed(
                    title="🎫 Dein Ticket wurde geschlossen",
                    description=(
                        f"Dein Ticket auf **{guild.name}** wurde geschlossen.\n\n"
                        f"**Geschlossen von:** {ctx.author.display_name}\n"
                        f"**Grund:** {reason[:500]}"
                    ),
                    color=discord.Color.red(),
                    timestamp=_now(),
                )
                if transcript_html:
                    try:
                        import io
                        html_file = discord.File(io.StringIO(transcript_html), filename=f"transcript-ticket-{ticket_num_str}.html")
                        txt_file = discord.File(io.StringIO(transcript_txt), filename=f"transcript-ticket-{ticket_num_str}.txt")
                        try:
                            await creator_user.send(embed=dm_embed, files=[html_file, txt_file])
                        except discord.HTTPException:
                            await creator_user.send(embed=dm_embed, file=txt_file)
                    except (discord.Forbidden, discord.HTTPException):
                        await creator_user.send(embed=dm_embed)
                else:
                    await creator_user.send(embed=dm_embed)
            except (discord.Forbidden, discord.HTTPException):
                pass

        # Active-Tracking
        if creator_id is not None:
            await self._ticket_remove_active(guild, creator_id, channel.id)

        # Schließen-Nachricht
        try:
            await ctx.send(f"🎫 Ticket wird in 3 Sekunden geschlossen... Grund: {reason[:200]}")
        except discord.HTTPException:
            pass
        await asyncio.sleep(3)
        try:
            await channel.delete(reason=f"Ticket geschlossen von {ctx.author.display_name}: {reason[:100]}")
        except (discord.Forbidden, discord.HTTPException) as e:
            try:
                await ctx.send(f"❌ Konnte Channel nicht löschen: `{e}`")
            except discord.HTTPException:
                pass

    # Auto-Close Loop für inaktive Tickets
    async def _ticket_auto_close_loop(self):
        """Background task: schließt inaktive Tickets automatisch."""
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    try:
                        await self._ticket_auto_close_sweep(guild)
                    except Exception:
                        log.exception("Auto-Close Loop Fehler in Guild %s", getattr(guild, "id", "?"))
            except Exception:
                log.exception("Schwerer Fehler im Ticket-Auto-Close-Loop")
            await asyncio.sleep(1800)  # Alle 30 Minuten

    async def _ticket_auto_close_sweep(self, guild: discord.Guild):
        """Führt den Auto-Close-Sweep für eine Guild aus."""
        hours = await self.config.guild(guild).ticket_auto_close_hours() or 0
        if hours <= 0:
            return
        active = await self.config.guild(guild).ticket_active() or {}
        if not active:
            return
        cutoff_ts = _now_ts() - hours * 3600
        # Kopie zum Iterieren, da wir active verändern
        for user_id_str, channel_ids in list(active.items()):
            for channel_id in list(channel_ids):
                channel = guild.get_channel(channel_id)
                if channel is None:
                    # Channel existiert nicht mehr → cleanup
                    try:
                        channel_ids.remove(channel_id)
                        await self.config.guild(guild).ticket_active.set(active)
                    except Exception:
                        pass
                    continue
                # Erstellungszeit aus Topic extrahieren
                created_ts = None
                if channel.topic:
                    m = re.search(r"Created: (\d+)", channel.topic)
                    if m:
                        try:
                            created_ts = int(m.group(1))
                        except ValueError:
                            pass
                if created_ts is None:
                    continue  # Keine Created-Zeit im Topic → überspringen
                if created_ts >= cutoff_ts:
                    continue  # Ticket ist jünger als cutoff
                # Letzte Nachricht prüfen (nur schließen wenn auch letzte Nachricht alt ist)
                last_msg_ts = None
                try:
                    async for msg in channel.history(limit=1):
                        if msg.created_at:
                            last_msg_ts = msg.created_at.replace(tzinfo=timezone.utc).timestamp() if msg.created_at.tzinfo is None else msg.created_at.timestamp()
                        break
                except (discord.Forbidden, discord.HTTPException):
                    pass
                if last_msg_ts is not None and last_msg_ts >= cutoff_ts:
                    continue  # Letzte Nachricht ist neuer als cutoff
                # Auto-Close durchführen
                try:
                    embed = discord.Embed(
                        title="⏰ Auto-Close",
                        description=f"Dieses Ticket wurde automatisch geschlossen (Inaktivität > {hours}h).",
                        color=discord.Color.orange(),
                        timestamp=_now(),
                    )
                    try:
                        await channel.send(embed=embed)
                    except discord.HTTPException:
                        pass
                    await asyncio.sleep(2)
                    try:
                        await channel.delete(reason=f"Auto-Close: Inaktivität > {hours}h")
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                    # Cleanup
                    try:
                        user_id_int = int(user_id_str)
                        await self._ticket_remove_active(guild, user_id_int, channel_id)
                    except (ValueError, Exception):
                        pass
                except Exception:
                    log.exception("Auto-Close fehlgeschlagen für Channel %s", channel_id)

    # ============================================
    # SUPPORT BLOCKLIST
    # ============================================

    async def _can_manage_blocklist(self, member: discord.Member, guild: discord.Guild) -> bool:
        """Prüft, ob ein Member die Blocklist verwalten darf."""
        if member.guild_permissions.manage_guild:
            return True
        role_id = await self.config.guild(guild).support_blocklist_role()
        if role_id and member.get_role(role_id) is not None:
            return True
        # Support-Basisrolle darf auch (für schnelle Troll-Abwehr)
        base_role_id = await self.config.guild(guild).role()
        if base_role_id and member.get_role(base_role_id) is not None:
            return True
        return False

    @commands.command(name="supportblock", aliases=["sblock", "blocksupport"])
    @commands.guild_only()
    async def support_block(self, ctx: commands.Context, user: discord.Member, *, reason: str = "Kein Grund angegeben"):
        """Blockiert einen User vom Support-System (keine Notifications bei Warteraum-Betritt)."""
        guild = ctx.guild
        if not await self._can_manage_blocklist(ctx.author, guild):
            await ctx.send("❌ Du bist nicht berechtigt, die Blocklist zu verwalten.")
            return
        if user.bot:
            await ctx.send("❌ Bots können nicht blockiert werden.")
            return
        if user == ctx.author:
            await ctx.send("❌ Du kannst dich nicht selbst blockieren.")
            return
        blocklist = await self.config.guild(guild).support_blocklist() or {}
        if str(user.id) in blocklist:
            await ctx.send(f"ℹ️ {user.mention} ist bereits blockiert.")
            return
        blocklist[str(user.id)] = {
            "blocked_by": ctx.author.id,
            "reason": reason[:500],
            "timestamp": _now_ts(),
        }
        await self.config.guild(guild).support_blocklist.set(blocklist)
        await ctx.send(f"✅ {user.mention} wurde vom Support-System blockiert.\n**Grund:** {reason[:500]}")

    @commands.command(name="supportunblock", aliases=["sunblock", "unblocksupport"])
    @commands.guild_only()
    async def support_unblock(self, ctx: commands.Context, user: discord.Member):
        """Hebt die Blockade eines Users auf."""
        guild = ctx.guild
        if not await self._can_manage_blocklist(ctx.author, guild):
            await ctx.send("❌ Du bist nicht berechtigt, die Blocklist zu verwalten.")
            return
        blocklist = await self.config.guild(guild).support_blocklist() or {}
        if str(user.id) not in blocklist:
            await ctx.send(f"ℹ️ {user.mention} ist nicht blockiert.")
            return
        entry = blocklist.pop(str(user.id))
        await self.config.guild(guild).support_blocklist.set(blocklist)
        blocked_by_id = entry.get("blocked_by")
        blocked_by = guild.get_member(blocked_by_id) if blocked_by_id else None
        await ctx.send(
            f"✅ {user.mention} wurde entblockt.\n"
            f"(Ursprünglich blockiert von {blocked_by.display_name if blocked_by else 'Unbekannt'})"
        )

    @commands.command(name="supportblocklist", aliases=["sblocklist", "supblocklist"])
    @commands.guild_only()
    async def support_blocklist(self, ctx: commands.Context):
        """Zeigt alle blockierten User an."""
        guild = ctx.guild
        blocklist = await self.config.guild(guild).support_blocklist() or {}
        if not blocklist:
            await ctx.send("📭 Die Support-Blocklist ist leer.")
            return
        embed = discord.Embed(
            title="🚫 Support-Blocklist",
            description=f"{len(blocklist)} blockierte(r) User",
            color=discord.Color.red(),
            timestamp=_now(),
        )
        for uid_str, info in list(blocklist.items())[:25]:
            try:
                uid = int(uid_str)
            except ValueError:
                continue
            user = guild.get_member(uid)
            name = user.mention if user else f"`{uid}`"
            blocker = guild.get_member(info.get("blocked_by"))
            blocker_name = blocker.display_name if blocker else "Unbekannt"
            reason = info.get("reason", "Kein Grund")[:100]
            ts = info.get("timestamp", 0)
            time_str = f"<t:{int(ts)}:R>" if ts else "Unbekannt"
            embed.add_field(
                name=f"🚫 {name}",
                value=f"**Grund:** {reason}\n**Von:** {blocker_name}\n**Wann:** {time_str}",
                inline=False,
            )
        if len(blocklist) > 25:
            embed.set_footer(text=f"Zeige 25 von {len(blocklist)} Einträgen")
        await ctx.send(embed=embed)

    @commands.group(name="blockset")
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def blockset(self, ctx: commands.Context):
        """Konfiguriere die Support-Blocklist."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @blockset.command(name="role")
    async def blockset_role(self, ctx: commands.Context, role: str = None):
        """Setzt die Rolle, die die Blocklist verwalten darf. 'reset' zum Zurücksetzen."""
        if role is None or role.lower() == "reset":
            await self.config.guild(ctx.guild).support_blocklist_role.set(None)
            await ctx.send("✅ Blocklist-Verwalter-Rolle zurückgesetzt.")
            return
        role_id = self._parse_role_id(role)
        if role_id is None:
            await ctx.send("❌ Bitte gib eine gültige Rollen-ID oder Mention ein.")
            return
        r = ctx.guild.get_role(role_id)
        if not r:
            await ctx.send("❌ Rolle nicht gefunden!")
            return
        await self.config.guild(ctx.guild).support_blocklist_role.set(role_id)
        await ctx.send(f"✅ Blocklist-Verwalter-Rolle auf {r.mention} gesetzt.")

    # ============================================
    # ESCALATION & AWAY-RETURN SETTINGS
    # ============================================

    @supportset.command(name="escalation")
    async def supportset_escalation(self, ctx: commands.Context, minutes: int = None):
        """Konfiguriert die Smart-Escalation: nach X Min ohne Reaktion wird die Basis-Rolle gepingt.

        Verwendung:
        - `[p]supportset escalation 5` — Eskalation nach 5 Minuten
        - `[p]supportset escalation 0` — Eskalation deaktivieren
        - `[p]supportset escalation` — Status anzeigen
        """
        if minutes is None:
            enabled = await self.config.guild(ctx.guild).escalation_enabled()
            cur = await self.config.guild(ctx.guild).escalation_minutes()
            await ctx.send(f"ℹ️ Smart-Escalation ist **{'aktiv' if enabled else 'inaktiv'}** —pingt nach **{cur} Min**.")
            return
        if minutes < 0:
            await ctx.send("❌ Minuten dürfen nicht negativ sein.")
            return
        if minutes == 0:
            await self.config.guild(ctx.guild).escalation_enabled.set(False)
            await ctx.send("✅ Smart-Escalation deaktiviert.")
            return
        await self.config.guild(ctx.guild).escalation_enabled.set(True)
        await self.config.guild(ctx.guild).escalation_minutes.set(minutes)
        await ctx.send(f"✅ Smart-Escalation aktiviert — pingt nach **{minutes} Min** ohne Reaktion die Basis-Rolle.")

    @supportset.command(name="awayautoreturn")
    async def supportset_awayautoreturn(self, ctx: commands.Context, minutes: int = None):
        """Konfiguriert Away-Auto-Return: 'away'-Status wird nach X Min automatisch auf 'available' zurückgesetzt.

        Verwendung:
        - `[p]supportset awayautoreturn 15` — nach 15 Minuten
        - `[p]supportset awayautoreturn 0` — deaktivieren
        - `[p]supportset awayautoreturn` — Status anzeigen
        """
        if minutes is None:
            cur = await self.config.guild(ctx.guild).away_auto_return_minutes()
            await ctx.send(f"ℹ️ Away-Auto-Return: **{cur} Min** ({'aktiv' if cur > 0 else 'inaktiv'}).")
            return
        if minutes < 0:
            await ctx.send("❌ Minuten dürfen nicht negativ sein.")
            return
        await self.config.guild(ctx.guild).away_auto_return_minutes.set(minutes)
        if minutes == 0:
            await ctx.send("✅ Away-Auto-Return deaktiviert.")
        else:
            await ctx.send(f"✅ Away-Auto-Return auf **{minutes} Min** gesetzt.")

    # ============================================
    # CSV STATS EXPORT
    # ============================================

    @commands.command(name="dutyexport", aliases=["dutycsv", "exportduty"])
    @commands.guild_only()
    @commands.mod_or_permissions(manage_guild=True)
    async def duty_export(self, ctx: commands.Context):
        """Exportiert Duty-Statistiken aller Teammitglieder als CSV-Datei."""
        guild = ctx.guild
        role_id = await self.config.guild(guild).role()
        wl_role_id = await self.config.guild(guild).whitelist_role()

        # Alle Member-Daten abrufen
        all_members = await self.config.all_members(guild)

        # Team-Mitglieder sammeln (Support-Basisrolle oder WL-Basisrolle)
        team_ids = set()
        if role_id:
            base_role = guild.get_role(role_id)
            if base_role:
                for m in base_role.members:
                    team_ids.add(m.id)
        if wl_role_id:
            wl_base_role = guild.get_role(wl_role_id)
            if wl_base_role:
                for m in wl_base_role.members:
                    team_ids.add(m.id)
        # Auch Member erfassen, die zwar nicht mehr in der Rolle sind, aber Statistiken haben
        for uid in all_members.keys():
            team_ids.add(int(uid))

        if not team_ids:
            await ctx.send("ℹ️ Keine Teammitglieder oder Statistiken gefunden.")
            return

        # CSV bauen
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        # Header
        writer.writerow([
            "user_id", "display_name", "username",
            "on_duty", "duty_start_ts", "duty_status",
            "total_duty_time_sec", "total_duty_time_h",
            "wl_on_duty", "wl_duty_start_ts",
            "total_wl_duty_time_sec", "total_wl_duty_time_h",
            "cases_handled", "wl_cases_handled",
            "duty_session_count", "last_duty_end_ts",
        ])

        for uid in sorted(team_ids):
            uid_int = int(uid)
            data = all_members.get(uid_int, {})
            member = guild.get_member(uid_int)
            display_name = member.display_name if member else "(ex-Team)"
            username = member.name if member else ""

            total_sup = data.get("total_duty_time", 0) or 0
            total_wl = data.get("total_whitelist_duty_time", 0) or 0
            writer.writerow([
                uid_int,
                display_name,
                username,
                "yes" if data.get("on_duty") else "no",
                data.get("duty_start") or "",
                data.get("duty_status", "available"),
                total_sup,
                f"{total_sup / 3600:.2f}",
                "yes" if data.get("whitelist_on_duty") else "no",
                data.get("whitelist_duty_start") or "",
                total_wl,
                f"{total_wl / 3600:.2f}",
                data.get("cases_handled", 0) or 0,
                data.get("whitelist_cases_handled", 0) or 0,
                data.get("duty_session_count", 0) or 0,
                data.get("last_duty_end") or "",
            ])

        # Datei an Discord senden
        content = output.getvalue().encode("utf-8")
        # BOM für Excel-Kompatibilität
        content = b"\xef\xbb\xbf" + content
        file = discord.File(io.BytesIO(content), filename=f"duty_stats_{guild.id}_{_now().strftime('%Y%m%d_%H%M')}.csv")
        await ctx.send(
            content=f"📊 Duty-Statistik-Export für **{guild.name}** ({len(team_ids)} Teammitglieder)",
            file=file,
        )

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
            timestamp=_now()
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

    async def update_panel_display(self, guild: discord.Guild):
        """Aktualisiert das Support-Duty-Panel mit der aktuellen Liste der Duty-Mitglieder.

        Diese Methode lebt auf der Cog (nicht auf der View!), damit alle
        `duty *` Text-Befehle und die DutyButtonView sie gemeinsam nutzen können.
        Verwendet read-only `get_duty_role`, damit ein Panel-Refresh niemals
        als Side-Effect eine neue Rolle erstellt.
        """
        panel_message_id = await self.config.guild(guild).panel_message_id()
        if not panel_message_id:
            return
        panel_channel = await self.get_panel_channel(guild)
        if not panel_channel:
            return
        try:
            panel_message = await panel_channel.fetch_message(panel_message_id)
        except discord.NotFound:
            await self.config.guild(guild).panel_message_id.set(None)
            return
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Abrufen des Duty-Panels (Guild %s)", guild.id)
            return
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Abrufen des Duty-Panels (Guild %s)", guild.id)
            return

        duty_role = await self.get_duty_role(guild)
        role_id = await self.config.guild(guild).role()

        duty_count = 0
        duty_list = []
        if role_id and duty_role:
            base_role = guild.get_role(role_id)
            if base_role:
                # Single Config.all_members call instead of N per-member reads.
                all_members = await self.config.all_members(guild)
                for m in base_role.members:
                    if m.get_role(duty_role.id) is None:
                        continue
                    member_data = all_members.get(m.id, {})
                    if not member_data.get("on_duty"):
                        continue
                    duty_count += 1
                    status = member_data.get("duty_status", "available")
                    status_emoji = {"available": "🟢", "busy": "🔵", "break": "☕", "away": "🟡"}.get(status, "🟢")
                    duty_list.append(f"{status_emoji} {m.display_name}")

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
            color=discord.Color.blue(),
            timestamp=_now(),
        )
        new_embed.add_field(name=f"🟢 Aktuell im Dienst ({duty_count})", value=duty_text or "Niemand", inline=False)
        new_embed.set_footer(text="Die 🟢 On Duty Rolle wird automatisch zugewiesen/entfernt")

        try:
            await panel_message.edit(embed=new_embed)
        except discord.NotFound:
            await self.config.guild(guild).panel_message_id.set(None)
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Editieren des Duty-Panels (Guild %s)", guild.id)
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Editieren des Duty-Panels (Guild %s)", guild.id)

    async def update_status_display(self, guild: discord.Guild):
        """Aktualisiert das erweiterte Duty-Status-Display mit detaillierten Informationen."""
        status_display_id = await self.config.guild(guild).duty_status_display_message_id()
        if not status_display_id:
            return
        status_channel = await self.get_status_display_channel(guild)
        if not status_channel:
            return
        try:
            status_message = await status_channel.fetch_message(status_display_id)
        except discord.NotFound:
            await self.config.guild(guild).duty_status_display_message_id.set(None)
            return
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Abrufen des Status-Displays (Guild %s)", guild.id)
            return
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Abrufen des Status-Displays (Guild %s)", guild.id)
            return

        duty_role = await self.get_duty_role(guild)
        role_id = await self.config.guild(guild).role()

        available_list: list[str] = []
        busy_list: list[str] = []
        break_list: list[str] = []
        away_list: list[str] = []

        if role_id and duty_role:
            base_role = guild.get_role(role_id)
            if base_role:
                all_members = await self.config.all_members(guild)
                for m in base_role.members:
                    if m.get_role(duty_role.id) is None:
                        continue
                    member_data = all_members.get(m.id, {})
                    if not member_data.get("on_duty"):
                        continue
                    status = member_data.get("duty_status", "available")
                    status_msg = member_data.get("duty_status_message")
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

        embed = discord.Embed(
            title="📊 Live Duty Status Übersicht",
            description="**Aktueller Status aller Teammitglieder im Dienst**",
            color=discord.Color.blue(),
            timestamp=_now(),
        )

        def _truncate(items: list[str]) -> str:
            if not items:
                return "Keine"
            text = "\n".join(items[:10])
            if len(items) > 10:
                text += f"\n_...und {len(items) - 10} weitere_"
            return text

        embed.add_field(name="🟢 Verfügbar", value=_truncate(available_list), inline=True)
        embed.add_field(name="🔵 Beschäftigt", value=_truncate(busy_list), inline=True)
        embed.add_field(name="☕ In Pause", value=_truncate(break_list), inline=True)
        embed.add_field(name="🟡 Abwesend", value=_truncate(away_list), inline=False)

        total = len(available_list) + len(busy_list) + len(break_list) + len(away_list)
        embed.add_field(
            name="📈 Statistik",
            value=(
                f"**{total}** im Duty | 🟢 {len(available_list)} frei | "
                f"🔵 {len(busy_list)} beschäftigt | ☕ {len(break_list)} Pause"
            ),
            inline=False,
        )
        embed.set_footer(text="Zuletzt aktualisiert")

        try:
            await status_message.edit(embed=embed)
        except discord.NotFound:
            await self.config.guild(guild).duty_status_display_message_id.set(None)
        except discord.Forbidden:
            log.warning("Fehlende Rechte zum Editieren des Status-Displays (Guild %s)", guild.id)
        except discord.HTTPException:
            log.exception("HTTP-Fehler beim Editieren des Status-Displays (Guild %s)", guild.id)

    async def _set_duty_status(self, member: discord.Member, status: str, *, message: str = None):
        """Setzt den Duty-Status eines Members und finalisiert ggf. eine Pause.

        Verwendet von: duty pause, duty resume, duty busy, duty away,
        duty setmessage, duty clearmessage und StatusSelectView.

        Wenn der User aktuell im Pause-Status ist und auf einen anderen
        Status wechselt, wird die Pause abgerechnet (current_break_start
        auf duty_total_break_time addiert) und current_break_start
        zurückgesetzt — das verhindert, dass Pausenzeiten verloren gehen.
        """
        guild = member.guild
        mcfg = self.config.member(member)
        cur_status = await mcfg.duty_status()

        # Wenn wir aus einer Pause herauswechseln: Pause abrechnen
        if cur_status == "break" and status != "break":
            break_start = await mcfg.current_break_start()
            if break_start:
                delta = max(0, _now_ts() - int(break_start))
                total_break = await mcfg.duty_total_break_time() or 0
                await mcfg.duty_total_break_time.set(total_break + delta)
            await mcfg.current_break_start.set(None)

        # Wenn wir IN eine Pause wechseln: Startzeit setzen
        if status == "break" and cur_status != "break":
            await mcfg.current_break_start.set(_now_ts())
            # Pausen-Counter erhöhen
            break_count = await mcfg.duty_break_count() or 0
            await mcfg.duty_break_count.set(break_count + 1)

        await mcfg.duty_status.set(status)
        if message is not None:
            await mcfg.duty_status_message.set(message or None)

        # away_since pflegen (für Away-Auto-Return im Background-Sweep)
        if status == "away":
            await mcfg.away_since.set(_now_ts())
        else:
            await mcfg.away_since.set(None)

        # Live-Displays aktualisieren (best-effort, Fehler werden geloggt)
        try:
            await self.update_status_display(guild)
            await self.update_panel_display(guild)
        except Exception:
            log.exception("Panel/Status-Update fehlgeschlagen in _set_duty_status")

    
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

        if member.get_role(role_id) is None:
            await ctx.send(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!")
            return

        # Race-condition-safe duty start: serialize per-(guild, member).
        async with self._lock_for(guild.id, member.id):
            is_on_duty = await self.config.member(member).on_duty()
            if is_on_duty:
                status = await self.config.member(member).duty_status()
                status_emoji = {"available": "🟢", "busy": "🔵", "break": "☕", "away": "⚪", "off_duty": "⚪"}.get(status, "🟢")
                await ctx.send(f"⚠️ Du bist bereits im Duty-Modus! Status: {status_emoji}")
                return

            # Duty aktivieren und Rolle geben
            await self.config.member(member).on_duty.set(True)
            start_ts = _now_ts()
            await self.config.member(member).duty_start.set(start_ts)

            # Duty-Status auf "available" setzen
            await self.config.member(member).duty_status.set("available")
            await self.config.member(member).duty_status_message.set(None)

            # Session-Count erhöhen
            current_sessions = await self.config.member(member).duty_session_count() or 0
            await self.config.member(member).duty_session_count.set(current_sessions + 1)

            # Pausen-Zähler zurücksetzen
            await self.config.member(member).duty_break_count.set(0)
            await self.config.member(member).duty_total_break_time.set(0)
            await self.config.member(member).current_break_start.set(None)

            # Duty-Rolle hinzufügen — roll back Config on failure so panel/stats stay consistent
            ok = await self.update_duty_role(member, True)
            if not ok:
                await self.config.member(member).on_duty.set(False)
                await self.config.member(member).duty_start.set(None)
                await self.config.member(member).duty_status.set("off_duty")
                await self.config.member(member).duty_session_count.set(current_sessions)
                await ctx.send("❌ Konnte die Duty-Rolle nicht zuweisen (fehlende Rechte?). Duty-Start abgebrochen.")
                return

        # Nachricht im Log-Channel senden
        log_channel = await self.get_log_channel(guild)
        start_dt = _from_ts(start_ts)

        # Read the auto-duty-timeout so we can warn the user about it.
        auto_remove = await self.config.guild(guild).auto_remove_duty()
        duty_timeout = await self.config.guild(guild).duty_timeout()

        embed = discord.Embed(
            title="🟢 Duty Gestartet",
            description=f"{member.mention} hat sich für den Support-Dienst angemeldet!",
            color=discord.Color.green(),
            timestamp=start_dt
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        session_count = current_sessions + 1
        embed.add_field(name="📊 Sessions", value=f"Session #{session_count}", inline=True)
        if auto_remove and duty_timeout:
            embed.add_field(name="⏰ Auto-Abmeldung", value=f"Nach **{duty_timeout}h** automatisch", inline=True)

        # Count active duty users (single Config.all_members call).
        duty_count = 0
        duty_role = await self.get_duty_role(guild)
        if duty_role:
            all_members = await self.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Aktive Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_dt.strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte Duty-Start-Log nicht senden (Guild %s)", guild.id)

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
        guild = ctx.guild
        is_on_duty = await self.config.member(member).on_duty()

        if not is_on_duty:
            await ctx.send("ℹ️ Du bist aktuell nicht im Duty-Modus.")
            return

        # Capture duration before _finalize_duty_stop zeroes duty_start.
        start_time_ts = await self.config.member(member).duty_start()
        duration = "Unbekannt"
        duration_seconds = 0
        if start_time_ts:
            duration_seconds = max(0, _now_ts() - int(start_time_ts))
            duration = _fmt_h_m(duration_seconds)

        # Race-condition-safe duty stop.
        async with self._lock_for(guild.id, member.id):
            await self._finalize_duty_stop(member, whitelist=False, reason="Manuell beendet")

        # Nachricht im Log-Channel senden
        log_channel = await self.get_log_channel(guild)

        embed = discord.Embed(
            title="🔴 Duty Beendet",
            description=f"{member.mention} hat sich vom Support-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=_now()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)

        # Pausen-Info
        break_count = await self.config.member(member).duty_break_count() or 0
        total_break_time = await self.config.member(member).duty_total_break_time() or 0
        break_minutes = total_break_time // 60
        if break_count > 0:
            embed.add_field(name="☕ Pausen", value=f"{break_count} Pausen ({break_minutes} min)", inline=True)

        # Count remaining active duty users.
        duty_count = 0
        duty_role = await self.get_duty_role(guild)
        if duty_role:
            all_members = await self.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Verbleibende Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {_now().strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte Duty-Stop-Log nicht senden (Guild %s)", guild.id)

        # _finalize_duty_stop already refreshed the displays, but doing it again
        # is idempotent and ensures we reflect the post-stop count.
        await self.update_panel_display(guild)
        await self.update_status_display(guild)

        await ctx.send(f"✅ Du hast den Duty-Modus verlassen. Gesammelte Zeit: {duration}")

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

        # _set_duty_status handles break-start accounting (current_break_start, break_count).
        await self._set_duty_status(member, "break")
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

        # _set_duty_status finalizes the break (adds to total_break_time, clears current_break_start).
        await self._set_duty_status(member, "available")
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

        # _set_duty_status will finalize any ongoing break before switching to "busy".
        await self._set_duty_status(member, "busy")
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

        await self._set_duty_status(member, "away")
        await ctx.send("🟡 Du bist jetzt als abwesend markiert.")

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
            start_dt = _from_ts(start_time_ts)
            delta = _now() - start_dt
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
                start_dt = _from_ts(start_time_ts)
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

    # ============================================
    # DUTY HANDOVER
    # ============================================

    @duty_group.command(name="handover", aliases=["transfer"])
    async def duty_handover(self, ctx: commands.Context, target: discord.Member):
        """Übergibt deinen aktiven Duty an einen anderen Teamkollegen.

        Du wirst vom Duty abgemeldet, der Ziel-User wird angemeldet.
        Der Ziel-User muss die Support-Basisrolle haben.
        """
        guild = ctx.guild
        member = ctx.author

        if target.id == member.id:
            await ctx.send("❌ Du kannst Duty nicht an dich selbst übergeben.")
            return
        if target.bot:
            await ctx.send("❌ Du kannst Duty nicht an einen Bot übergeben.")
            return

        role_id = await self.config.guild(guild).role()
        if not role_id:
            await ctx.send("❌ Keine Support-Rolle konfiguriert.")
            return
        if target.get_role(role_id) is None:
            await ctx.send(f"❌ {target.mention} hat nicht die Support-Basisrolle.")
            return

        is_on_duty = await self.config.member(member).on_duty()
        if not is_on_duty:
            await ctx.send("❌ Du bist aktuell nicht im Duty.")
            return
        target_on_duty = await self.config.member(target).on_duty()
        if target_on_duty:
            await ctx.send(f"❌ {target.mention} ist bereits im Duty.")
            return

        # Sender abmelden (mit _finalize_duty_stop für saubere Statistik)
        async with self._lock_for(guild.id, member.id):
            await self._finalize_duty_stop(member, whitelist=False, reason=f"Duty übergeben an {target.display_name}")

        # Ziel anmelden
        async with self._lock_for(guild.id, target.id):
            await self.config.member(target).on_duty.set(True)
            start_ts = _now_ts()
            await self.config.member(target).duty_start.set(start_ts)
            await self.config.member(target).duty_status.set("available")
            await self.config.member(target).duty_status_message.set(None)
            current_sessions = await self.config.member(target).duty_session_count() or 0
            await self.config.member(target).duty_session_count.set(current_sessions + 1)
            await self.config.member(target).duty_break_count.set(0)
            await self.config.member(target).duty_total_break_time.set(0)
            await self.config.member(target).current_break_start.set(None)
            ok = await self.update_duty_role(target, True)
            if not ok:
                # Rollback
                await self.config.member(target).on_duty.set(False)
                await self.config.member(target).duty_start.set(None)
                await self.config.member(target).duty_status.set("off_duty")
                await self.config.member(target).duty_session_count.set(current_sessions)
                await ctx.send("❌ Konnte Duty-Rolle für den Ziel-User nicht zuweisen. Übergabe abgebrochen.")
                return

        # Refresh panels
        await self.update_panel_display(guild)
        await self.update_status_display(guild)

        # Log
        log_channel = await self.get_log_channel(guild)
        if log_channel:
            try:
                embed = discord.Embed(
                    title="🔄 Duty übergeben",
                    description=f"{member.mention} hat Duty an {target.mention} übergeben.",
                    color=discord.Color.gold(),
                    timestamp=_now(),
                )
                embed.add_field(name="👤 Von", value=member.display_name, inline=True)
                embed.add_field(name="👤 An", value=target.display_name, inline=True)
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                pass

        await ctx.send(f"✅ Duty von {member.mention} an {target.mention} übergeben.")

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
                                start_time = _from_ts(duty_start)
                                duration = _now() - start_time
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
                                start_time = _from_ts(duty_start)
                                duration = _now() - start_time
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
            timestamp=_now()
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
            timestamp=_now()
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
                start_time = _from_ts(duty_start)
                duration = _now() - start_time
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
            timestamp=_now()
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
            timestamp=_now()
        )
        embed.set_footer(text=f"Duty Ranking • {len(duty_times)} Mitglieder mit Duty-Zeit • {guild.name}")
        
        await ctx.send(embed=embed)

    # HINWEIS: clearwarns, slowmode, purge, lock, unlock, nick, removenick wurden entfernt - verwende offizielle Red-Cogs (mod, admin, roletools)
    # HINWEIS: serverinfo, roleinfo wurden entfernt - verwende offiziellen Red-Cog (info)

    # ENDE DER BEFEHLE - Alle weiteren Befehle (serverinfo, roleinfo) wurden entfernt da sie im offiziellen 'info' Cog enthalten sind

    # ============================================
    # CROSS-SERVER SYNC (BanSync / ModSync)
    # ============================================

    async def _sync_get_log_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        """Log-Channel für Sync-Events."""
        cid = await self.config.guild(guild).sync_log_channel()
        if cid:
            ch = guild.get_channel(cid)
            if ch and isinstance(ch, discord.TextChannel):
                return ch
        return None

    async def _sync_is_excluded(self, guild: discord.Guild) -> bool:
        """Prüft ob eine Guild von der Synchronisation ausgeschlossen ist."""
        excluded = await self.config.guild(guild).sync_excluded_guilds() or []
        return guild.id in excluded

    async def _sync_should_propagate_from(self, guild: discord.Guild) -> bool:
        """Darf diese Guild überhaupt Aktionen an andere senden?"""
        if not await self.config.guild(guild).sync_enabled():
            return False
        if await self._sync_is_excluded(guild):
            return False
        direction = await self.config.guild(guild).sync_direction()
        if direction == "bidirectional":
            return True
        # master_to_all: nur die Master-Guild darf propagieren
        master_id = await self.config.guild(guild).sync_master_guild_id()
        if master_id is None:
            # Keine Master gesetzt → diese Guild ist implizit Master
            return True
        return guild.id == master_id

    async def _sync_should_accept_for(self, guild: discord.Guild) -> bool:
        """Darf diese Guild Aktionen empfangen?"""
        if not await self.config.guild(guild).sync_enabled():
            return False
        if await self._sync_is_excluded(guild):
            return False
        return True

    def _sync_action_key(self, action: str, user_id: int, source_guild_id: int) -> str:
        """Builds a deduplication key for sync actions."""
        return f"{action}:{user_id}:{source_guild_id}"

    async def _sync_mark_recent(self, guild: discord.Guild, key: str, ttl: int = 30):
        """Mark an action as recently-synced to prevent recursion."""
        recent = await self.config.guild(guild).sync_recent_actions() or {}
        recent[key] = _now_ts()
        # Cleanup old entries (>ttl seconds)
        cutoff = _now_ts() - ttl
        recent = {k: v for k, v in recent.items() if v > cutoff}
        await self.config.guild(guild).sync_recent_actions.set(recent)

    async def _sync_was_recent(self, guild: discord.Guild, key: str, ttl: int = 30) -> bool:
        """Check if an action was recently synced (prevents recursion)."""
        recent = await self.config.guild(guild).sync_recent_actions() or {}
        ts = recent.get(key)
        if ts is None:
            return False
        if _now_ts() - ts > ttl:
            return False
        return True

    async def _sync_target_guilds(self, source_guild: discord.Guild) -> list[discord.Guild]:
        """Liste aller Guilds, auf die synchronisiert werden soll."""
        targets = []
        for g in self.bot.guilds:
            if g.id == source_guild.id:
                continue
            if not await self._sync_should_accept_for(g):
                continue
            targets.append(g)
        return targets

    async def _sync_log(self, source_guild: discord.Guild, message: str, *, embed: Optional[discord.Embed] = None):
        """Schreibt einen Sync-Log-Eintrag in den Log-Channel der SOURCE-Guild."""
        log_ch = await self._sync_get_log_channel(source_guild)
        if not log_ch:
            return
        try:
            if embed is not None:
                await log_ch.send(content=message, embed=embed)
            else:
                await log_ch.send(message)
        except discord.HTTPException:
            pass

    async def _sync_propagate_ban(self, source_guild: discord.Guild, user: discord.abc.User, reason: str):
        """Propagiert einen Ban auf alle Ziel-Guilds."""
        if not await self._sync_should_propagate_from(source_guild):
            return
        if not await self.config.guild(source_guild).sync_bans():
            return
        targets = await self._sync_target_guilds(source_guild)
        success, failed = [], []
        for g in targets:
            key = self._sync_action_key("ban", user.id, source_guild.id)
            await self._sync_mark_recent(g, key)
            try:
                # Prüfen ob schon gebannt
                try:
                    ban_entry = await g.fetch_ban(user)
                    if ban_entry:
                        success.append(g.name)
                        continue
                except discord.NotFound:
                    pass  # nicht gebannt → weiter
                # discord.py 2.x (Red 3.5+) verwendet delete_message_seconds,
                # 1.x verwendete delete_message_days. Wir versuchen beides mit Fallback.
                try:
                    await g.ban(user, reason=f"[BanSync von {source_guild.name}] {reason}", delete_message_seconds=86400)
                except TypeError:
                    # Alte discord.py Version ohne delete_message_seconds
                    await g.ban(user, reason=f"[BanSync von {source_guild.name}] {reason}")
                success.append(g.name)
            except discord.Forbidden:
                failed.append((g.name, "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((g.name, str(e)))
        # Log
        embed = discord.Embed(
            title="🔄 BanSync",
            description=f"**{user}** (`{user.id}`) wurde auf `{source_guild.name}` gebannt.",
            color=discord.Color.red(),
            timestamp=_now(),
        )
        embed.add_field(name="Grund", value=reason[:500] or "Kein Grund angegeben", inline=False)
        embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
        if failed:
            embed.add_field(
                name=f"❌ Fehlgeschlagen ({len(failed)})",
                value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                inline=False,
            )
        await self._sync_log(source_guild, f"🔄 Ban für {user} synchronisiert", embed=embed)

    async def _sync_propagate_unban(self, source_guild: discord.Guild, user: discord.abc.User):
        """Propagiert einen Unban auf alle Ziel-Guilds."""
        if not await self._sync_should_propagate_from(source_guild):
            return
        if not await self.config.guild(source_guild).sync_unbans():
            return
        targets = await self._sync_target_guilds(source_guild)
        success, failed = [], []
        for g in targets:
            key = self._sync_action_key("unban", user.id, source_guild.id)
            await self._sync_mark_recent(g, key)
            try:
                await g.unban(user, reason=f"[BanSync von {source_guild.name}] Entbannung")
                success.append(g.name)
            except discord.NotFound:
                # war nicht gebannt → OK, kein Fehler
                success.append(g.name)
            except discord.Forbidden:
                failed.append((g.name, "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((g.name, str(e)))
        embed = discord.Embed(
            title="🔄 UnbanSync",
            description=f"**{user}** (`{user.id}`) wurde auf `{source_guild.name}` entbannt.",
            color=discord.Color.green(),
            timestamp=_now(),
        )
        embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
        if failed:
            embed.add_field(
                name=f"❌ Fehlgeschlagen ({len(failed)})",
                value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                inline=False,
            )
        await self._sync_log(source_guild, f"🔄 Unban für {user} synchronisiert", embed=embed)

    async def _sync_propagate_timeout(
        self, source_guild: discord.Guild, member: discord.Member,
        until: Optional[datetime], reason: str,
    ):
        """Propagiert einen Timeout auf alle Ziel-Guilds."""
        if not await self._sync_should_propagate_from(source_guild):
            return
        if not await self.config.guild(source_guild).sync_timeouts():
            return
        targets = await self._sync_target_guilds(source_guild)
        success, failed = [], []
        for g in targets:
            key = self._sync_action_key("timeout", member.id, source_guild.id)
            await self._sync_mark_recent(g, key)
            try:
                target_member = g.get_member(member.id)
                if target_member is None:
                    # User ist auf dieser Guild nicht vorhanden → skip
                    continue
                await target_member.timeout(until, reason=f"[BanSync von {source_guild.name}] {reason}")
                success.append(g.name)
            except discord.Forbidden:
                failed.append((g.name, "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((g.name, str(e)))
        embed = discord.Embed(
            title="🔄 TimeoutSync",
            description=f"**{member}** (`{member.id}`) hat auf `{source_guild.name}` einen Timeout erhalten.",
            color=discord.Color.orange(),
            timestamp=_now(),
        )
        embed.add_field(name="Grund", value=reason[:500] or "Kein Grund", inline=False)
        embed.add_field(name="Bis", value=until.strftime("%d.%m.%Y %H:%M UTC") if until else "Aufgehoben", inline=True)
        embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
        if failed:
            embed.add_field(
                name=f"❌ Fehlgeschlagen ({len(failed)})",
                value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                inline=False,
            )
        await self._sync_log(source_guild, f"🔄 Timeout für {member} synchronisiert", embed=embed)

    async def _sync_propagate_kick(self, source_guild: discord.Guild, member: discord.Member, reason: str):
        """Propagiert einen Kick auf alle Ziel-Guilds."""
        if not await self._sync_should_propagate_from(source_guild):
            return
        if not await self.config.guild(source_guild).sync_kicks():
            return
        targets = await self._sync_target_guilds(source_guild)
        success, failed = [], []
        for g in targets:
            key = self._sync_action_key("kick", member.id, source_guild.id)
            await self._sync_mark_recent(g, key)
            try:
                target_member = g.get_member(member.id)
                if target_member is None:
                    continue
                await target_member.kick(reason=f"[BanSync von {source_guild.name}] {reason}")
                success.append(g.name)
            except discord.Forbidden:
                failed.append((g.name, "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((g.name, str(e)))
        embed = discord.Embed(
            title="🔄 KickSync",
            description=f"**{member}** (`{member.id}`) wurde auf `{source_guild.name}` gekickt.",
            color=discord.Color.orange(),
            timestamp=_now(),
        )
        embed.add_field(name="Grund", value=reason[:500] or "Kein Grund", inline=False)
        embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
        if failed:
            embed.add_field(
                name=f"❌ Fehlgeschlagen ({len(failed)})",
                value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                inline=False,
            )
        await self._sync_log(source_guild, f"🔄 Kick für {member} synchronisiert", embed=embed)

    async def _sync_propagate_warn(self, source_guild: discord.Guild, user_id: int, user_name: str, reason: str):
        """Propagiert eine Warnung als Log-Eintrag auf alle Ziel-Guilds (keine echte Mod-Aktion,
        nur Log-Eintrag weil Warnungen lokal im Cog gespeichert wären)."""
        if not await self._sync_should_propagate_from(source_guild):
            return
        if not await self.config.guild(source_guild).sync_warns():
            return
        targets = await self._sync_target_guilds(source_guild)
        for g in targets:
            log_ch = await self._sync_get_log_channel(g)
            if not log_ch:
                continue
            try:
                embed = discord.Embed(
                    title="⚠️ Warnung synchronisiert",
                    description=f"**{user_name}** (`{user_id}`) wurde auf `{source_guild.name}` verwarnt.",
                    color=discord.Color.yellow(),
                    timestamp=_now(),
                )
                embed.add_field(name="Grund", value=reason[:500] or "Kein Grund", inline=False)
                await log_ch.send(embed=embed)
            except discord.HTTPException:
                pass

    # --- LISTENERS ---

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        """Wird gefeuert wenn ein User gebannt wird — synchronisiert an andere Guilds."""
        # Anti-Nuke Tracking (immer, wenn enabled)
        await self._antinuke_track(guild, "ban")
        # Sync
        if not await self._sync_should_propagate_from(guild):
            return
        # Vermeide Rekursion: wenn die Aktion vor kurzem durch Sync getriggert wurde, skip
        # Der propagate-Aufruf markiert den Key auf der ZIEL-Guild. Auf der QUELL-Guild gibt
        # es keinen Key (weil die Original-Aktion vom User kam, nicht vom Sync). Hier prüfen
        # wir trotzdem: falls diese Guild selbst vor kurzem Ziel eines Sync-Banns war, skip.
        # Das ist wichtig für bidirektionale Sync-Setups.
        key = self._sync_action_key("ban", user.id, guild.id)
        if await self._sync_was_recent(guild, key):
            return
        # Reason aus Audit Log holen falls aktiviert
        reason = "Kein Grund verfügbar"
        if await self.config.guild(guild).sync_audit_log():
            try:
                async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.ban):
                    if entry.target and entry.target.id == user.id:
                        reason = entry.reason or "Kein Grund angegeben"
                        break
            except (discord.Forbidden, discord.HTTPException):
                pass
        await self._sync_propagate_ban(guild, user, reason)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        """Wird gefeuert wenn ein User entbannt wird — synchronisiert an andere Guilds."""
        # Anti-Nuke: unban nicht limitiert, nur Sync
        if not await self._sync_should_propagate_from(guild):
            return
        # Rekursion verhindern (bidirektionaler Modus)
        key = self._sync_action_key("unban", user.id, guild.id)
        if await self._sync_was_recent(guild, key):
            return
        await self._sync_propagate_unban(guild, user)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Erkennt Timeout-Änderungen und Rollen-Änderungen (für Auto-Role-Sync)."""
        # Timeout-Erkennung
        before_timeout = getattr(before, "timed_out_until", None)
        after_timeout = getattr(after, "timed_out_until", None)
        if before_timeout != after_timeout:
            # Timeout wurde geändert
            if after_timeout is not None and (after_timeout.replace(tzinfo=timezone.utc) if after_timeout.tzinfo is None else after_timeout) > _now():
                # Neuer/verlängerter Timeout
                if await self._sync_should_propagate_from(before.guild):
                    if await self.config.guild(before.guild).sync_timeouts():
                        reason = "Timeout (via Member Update)"
                        # Audit Log für Reason
                        if await self.config.guild(before.guild).sync_audit_log():
                            try:
                                async for entry in before.guild.audit_logs(limit=5, action=discord.AuditLogAction.member_update):
                                    if entry.target and entry.target.id == after.id:
                                        reason = entry.reason or reason
                                        break
                            except (discord.Forbidden, discord.HTTPException):
                                pass
                        await self._sync_propagate_timeout(before.guild, after, after_timeout, reason)
            elif after_timeout is None or (after_timeout.replace(tzinfo=timezone.utc) if after_timeout.tzinfo is None else after_timeout) <= _now():
                # Timeout aufgehoben
                if await self._sync_should_propagate_from(before.guild):
                    if await self.config.guild(before.guild).sync_timeouts():
                        await self._sync_propagate_timeout(before.guild, after, None, "Timeout aufgehoben")

        # Auto-Role-Sync
        if not await self.config.guild(before.guild).sync_role_sync_enabled():
            return
        if not await self._sync_should_propagate_from(before.guild):
            return
        before_roles = set(r.id for r in before.roles)
        after_roles = set(r.id for r in after.roles)
        added = after_roles - before_roles
        removed = before_roles - after_roles
        role_map = await self.config.guild(before.guild).sync_role_map() or {}
        for source_role_id in added:
            if str(source_role_id) in role_map:
                await self._sync_propagate_role_add(before.guild, after, source_role_id)
        for source_role_id in removed:
            if str(source_role_id) in role_map:
                await self._sync_propagate_role_remove(before.guild, after, source_role_id)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Erkennt Kicks (via Audit Log) und synchronisiert + schließt offene Tickets des Users."""
        await self._antinuke_track(member.guild, "kick")
        # Ticket-Auto-Close wenn User die Guild verlässt
        try:
            active = await self.config.guild(member.guild).ticket_active() or {}
            user_tickets = active.get(str(member.id), [])
            for channel_id in list(user_tickets):
                channel = member.guild.get_channel(channel_id)
                if channel is None:
                    continue
                try:
                    # Auto-Close mit Begründung
                    embed = discord.Embed(
                        title="⏰ Auto-Close (User verlassen)",
                        description=f"Dieses Ticket wurde automatisch geschlossen da {member.mention} den Server verlassen hat.",
                        color=discord.Color.orange(),
                        timestamp=_now(),
                    )
                    try:
                        await channel.send(embed=embed)
                    except discord.HTTPException:
                        pass
                    await asyncio.sleep(2)
                    try:
                        await channel.delete(reason=f"Auto-Close: User {member} hat die Guild verlassen")
                    except (discord.Forbidden, discord.HTTPException):
                        pass
                    # Cleanup
                    try:
                        await self._ticket_remove_active(member.guild, member.id, channel_id)
                    except Exception:
                        pass
                except Exception:
                    log.exception("Auto-Close bei Member-Remove fehlgeschlagen")
        except Exception:
            log.exception("Ticket-Auto-Close bei on_member_remove fehlgeschlagen")
        # Sync-Teil
        if not await self._sync_should_propagate_from(member.guild):
            return
        if not await self.config.guild(member.guild).sync_kicks():
            return
        # Prüfe Audit Log ob es ein Kick war (kein Ban, kein Selbst-Leave)
        if not await self.config.guild(member.guild).sync_audit_log():
            return  # ohne Audit Log können wir Kicks nicht zuverlässig erkennen
        try:
            async for entry in member.guild.audit_logs(limit=5, action=discord.AuditLogAction.kick):
                if entry.target and entry.target.id == member.id:
                    # Prüfen ob der Eintrag neu ist (letzte 30 Sekunden)
                    if entry.created_at and (_now() - entry.created_at.replace(tzinfo=timezone.utc)).total_seconds() < 30:
                        reason = entry.reason or "Kein Grund angegeben"
                        await self._sync_propagate_kick(member.guild, member, reason)
                    break
        except (discord.Forbidden, discord.HTTPException):
            pass

    @commands.Cog.listener()
    async def on_audit_log_entry_create(self, entry: discord.AuditLogEntry):
        """Optional: Zusätzliche Audit-Log-Auswertung für manuelle Aktionen.
        Wird nur gefeuert wenn sync_audit_log aktiviert ist. Hauptzweck: Warnungs-Sync
        (da Warnungen keinen eigenen Listener haben)."""
        if not await self.config.guild(entry.guild).sync_audit_log():
            return
        # Warn-Sync via Audit Log ist nicht direkt möglich (Discord hat keinen "warn" AuditLogAction).
        # Warnungen müssen über den [p]warn Befehl des Cogs laufen — falls dieser entfernt wurde,
        # ist Warn-Sync nur über externe Tools möglich. Hier ist nur ein Stub.

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        """Anti-Nuke: Channel-Löschungen tracken."""
        await self._antinuke_track(channel.guild, "channel_delete")

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Anti-Nuke: Rollen-Löschungen tracken."""
        await self._antinuke_track(role.guild, "role_delete")

    # --- AUTO-ROLE-SYNC HELPER ---

    async def _sync_propagate_role_add(self, source_guild: discord.Guild, member: discord.Member, source_role_id: int):
        """Propagiert eine Rollen-Vergabe auf andere Guilds."""
        role_map = await self.config.guild(source_guild).sync_role_map() or {}
        targets = role_map.get(str(source_role_id), [])
        success, failed = [], []
        for entry in targets:
            try:
                target_guild_id = entry.get("guild_id")
                target_role_id = entry.get("role_id")
                if not target_guild_id or not target_role_id:
                    continue
                g = self.bot.get_guild(target_guild_id)
                if g is None:
                    continue
                target_member = g.get_member(member.id)
                if target_member is None:
                    continue
                target_role = g.get_role(target_role_id)
                if target_role is None:
                    continue
                if target_role not in target_member.roles:
                    await target_member.add_roles(target_role, reason=f"[RoleSync von {source_guild.name}]")
                success.append(g.name)
            except discord.Forbidden:
                failed.append((entry.get("guild_id"), "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((entry.get("guild_id"), str(e)))
        if success or failed:
            embed = discord.Embed(
                title="🔄 RoleSync (hinzugefügt)",
                description=f"**{member}** erhielt Rolle `{source_role_id}` auf `{source_guild.name}`.",
                color=discord.Color.green(),
                timestamp=_now(),
            )
            embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
            if failed:
                embed.add_field(
                    name=f"❌ Fehlgeschlagen ({len(failed)})",
                    value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                    inline=False,
                )
            await self._sync_log(source_guild, f"🔄 RoleSync für {member}", embed=embed)

    async def _sync_propagate_role_remove(self, source_guild: discord.Guild, member: discord.Member, source_role_id: int):
        """Propagiert eine Rollen-Entfernung auf andere Guilds."""
        role_map = await self.config.guild(source_guild).sync_role_map() or {}
        targets = role_map.get(str(source_role_id), [])
        success, failed = [], []
        for entry in targets:
            try:
                target_guild_id = entry.get("guild_id")
                target_role_id = entry.get("role_id")
                if not target_guild_id or not target_role_id:
                    continue
                g = self.bot.get_guild(target_guild_id)
                if g is None:
                    continue
                target_member = g.get_member(member.id)
                if target_member is None:
                    continue
                target_role = g.get_role(target_role_id)
                if target_role is None:
                    continue
                if target_role in target_member.roles:
                    await target_member.remove_roles(target_role, reason=f"[RoleSync von {source_guild.name}]")
                success.append(g.name)
            except discord.Forbidden:
                failed.append((entry.get("guild_id"), "Fehlende Rechte"))
            except discord.HTTPException as e:
                failed.append((entry.get("guild_id"), str(e)))
        if success or failed:
            embed = discord.Embed(
                title="🔄 RoleSync (entfernt)",
                description=f"**{member}** verlor Rolle `{source_role_id}` auf `{source_guild.name}`.",
                color=discord.Color.red(),
                timestamp=_now(),
            )
            embed.add_field(name=f"✅ Synchronisiert ({len(success)})", value=", ".join(success) or "Keine", inline=False)
            if failed:
                embed.add_field(
                    name=f"❌ Fehlgeschlagen ({len(failed)})",
                    value="\n".join(f"`{n}`: {r}" for n, r in failed[:10]),
                    inline=False,
                )
            await self._sync_log(source_guild, f"🔄 RoleSync für {member}", embed=embed)

    # ============================================
    # ANTI-NUKE LOGIK
    # ============================================

    async def _antinuke_track(self, guild: discord.Guild, action: str):
        """Trackt eine Aktion für Anti-Nuke. Wenn Schwellwert überschritten → Aktion."""
        if not await self.config.guild(guild).antinuke_enabled():
            return
        # Versuche den Moderator via Audit Log zu identifizieren
        mod_id = None
        audit_action_map = {
            "ban": discord.AuditLogAction.ban,
            "kick": discord.AuditLogAction.kick,
            "channel_delete": discord.AuditLogAction.channel_delete,
            "role_delete": discord.AuditLogAction.role_delete,
        }
        audit_action = audit_action_map.get(action)
        if audit_action is None:
            return  # unban z.B. wird nur geloggt, nicht limitiert
        try:
            async for entry in guild.audit_logs(limit=1, action=audit_action):
                # Prüfen ob der Eintrag neu ist (letzte 5 Sekunden, sonst ist es alte Aktion)
                if entry.created_at:
                    entry_dt = entry.created_at.replace(tzinfo=timezone.utc) if entry.created_at.tzinfo is None else entry.created_at
                    if (_now() - entry_dt).total_seconds() > 5:
                        return
                mod_id = entry.user_id if entry.user else None
                break
        except (discord.Forbidden, discord.HTTPException):
            # Keine Audit-Log-Rechte → kein Tracking möglich
            return
        if mod_id is None or mod_id == self.bot.user.id:
            # Selbst-Action des Bots → nicht tracken
            return
        # Whitelist prüfen
        wl_users = await self.config.guild(guild).antinuke_whitelist_users() or []
        if mod_id in wl_users:
            return
        # Whitelist-Rollen prüfen
        wl_roles = await self.config.guild(guild).antinuke_whitelist_roles() or []
        mod_member = guild.get_member(mod_id)
        if mod_member and wl_roles:
            for r in mod_member.roles:
                if r.id in wl_roles:
                    return
        # Tracker aktualisieren
        tracker = await self.config.guild(guild).antinuke_tracker() or {}
        user_actions = tracker.get(str(mod_id), {})
        now_ts = _now_ts()
        window = await self.config.guild(guild).antinuke_window_seconds() or 60
        cutoff = now_ts - window
        # Cleanup alte Einträge
        actions = [ts for ts in user_actions.get(action, []) if ts > cutoff]
        actions.append(now_ts)
        user_actions[action] = actions
        tracker[str(mod_id)] = user_actions
        await self.config.guild(guild).antinuke_tracker.set(tracker)
        # Schwellwert prüfen
        threshold_key = f"antinuke_{action}_threshold"
        threshold = await self.config.guild(guild).get_attr(threshold_key)()
        if threshold and len(actions) > threshold:
            await self._antinuke_trigger(guild, mod_id, action, len(actions), threshold)

    async def _antinuke_trigger(self, guild: discord.Guild, mod_id: int, action: str, count: int, threshold: int):
        """Wird ausgelöst wenn ein Schwellwert überschritten wurde."""
        action_label = {
            "ban": "Banns",
            "kick": "Kicks",
            "channel_delete": "Channel-Löschungen",
            "role_delete": "Rollen-Löschungen",
        }.get(action, action)
        # Action ausführen
        antinuke_action = await self.config.guild(guild).antinuke_action()
        log_channel_id = await self.config.guild(guild).antinuke_log_channel()
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None
        target_member = guild.get_member(mod_id)
        target_name = target_member.display_name if target_member else f"<@{mod_id}>"

        stripped_roles = []
        if antinuke_action == "strip" and target_member:
            # Entferne alle Rollen mit gefährlichen Rechten
            try:
                dangerous_perms = (
                    discord.Permissions(administrator=True)
                    | discord.Permissions(ban_members=True)
                    | discord.Permissions(kick_members=True)
                    | discord.Permissions(manage_channels=True)
                    | discord.Permissions(manage_roles=True)
                    | discord.Permissions(manage_guild=True)
                    | discord.Permissions(moderate_members=True)
                )
                roles_to_remove = []
                for r in target_member.roles:
                    if r.permissions >= dangerous_perms or any(getattr(r.permissions, p, False) for p in ["administrator", "ban_members", "kick_members", "manage_channels", "manage_roles", "manage_guild", "moderate_members"]):
                        if r.position < guild.me.top_role.position and not r.managed:
                            roles_to_remove.append(r)
                if roles_to_remove:
                    try:
                        await target_member.remove_roles(*roles_to_remove, reason=f"[Anti-Nuke] {action_label}-Schwellwert überschritten")
                        stripped_roles = [r.name for r in roles_to_remove]
                    except discord.Forbidden:
                        antinuke_action = "notify (Strip fehlgeschlagen: Rechte fehlen)"
                    except discord.HTTPException:
                        antinuke_action = "notify (Strip fehlgeschlagen: HTTP-Fehler)"
            except Exception:
                log.exception("Anti-Nuke Strip fehlgeschlagen")

        # Log-Eintrag
        embed = discord.Embed(
            title="🚨 ANTI-NUKE ALARM",
            description=(
                f"**{target_name}** (`{mod_id}`) hat den Schwellwert für **{action_label}** überschritten!\n\n"
                f"**Aktionen:** {count} in letzter Zeit\n"
                f"**Schwellwert:** {threshold}\n"
                f"**Maßnahme:** {antinuke_action}"
            ),
            color=discord.Color.red(),
            timestamp=_now(),
        )
        if stripped_roles:
            embed.add_field(name="🧹 Entfernte Rollen", value=", ".join(stripped_roles[:10]), inline=False)
        if log_channel:
            try:
                await log_channel.send(content="@here ANTI-NUKE ALARM", embed=embed)
            except discord.HTTPException:
                pass
        # Auch ins Sync-Log
        await self._sync_log(guild, f"🚨 Anti-Nuke: {target_name} hat {action_label}-Limit überschritten", embed=embed)

        # Tracker zurücksetzen für diesen User
        tracker = await self.config.guild(guild).antinuke_tracker() or {}
        if str(mod_id) in tracker:
            tracker[str(mod_id)].pop(action, None)
            if not tracker[str(mod_id)]:
                del tracker[str(mod_id)]
            await self.config.guild(guild).antinuke_tracker.set(tracker)

    # ============================================
    # SYNCSET BEFEHLE
    # ============================================

    @commands.group(name="syncset", aliases=["syncconfig", "modsyncset"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def syncset(self, ctx: commands.Context):
        """Konfiguriert das Cross-Server Sync-System (BanSync/ModSync)."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @syncset.command(name="quickstart")
    async def syncset_quickstart(self, ctx: commands.Context):
        """Ein-Klick-Setup mit empfohlenen Defaults für diese Guild.

        Aktiviert:
        - Sync generell
        - Diese Guild als Master (wenn noch keine gesetzt) ODER als Empfänger
        - Ban/Unban/Timeout/Kick-Sync alle AN
        - Audit-Log-Auswertung AN
        - Log-Channel = aktueller Channel

        Für EMPFÄNGER-Guilds stattdessen `[p]syncset slave <master_id>` verwenden.
        """
        g = self.config.guild(ctx.guild)
        # 1. Sync aktivieren
        await g.sync_enabled.set(True)
        # 2. Master-Guild setzen wenn noch keine konfiguriert
        master_id = await g.sync_master_guild_id()
        if master_id is None:
            await g.sync_master_guild_id.set(ctx.guild.id)
            await g.sync_direction.set("master_to_all")
            role_label = "Master (Hauptserver)"
        elif master_id == ctx.guild.id:
            await g.sync_direction.set("master_to_all")
            role_label = "Master (Hauptserver)"
        else:
            # Diese Guild ist nicht Master → als bidirektionaler Empfänger setzen
            await g.sync_direction.set("bidirectional")
            role_label = f"Empfänger (Master: `{master_id}`)"
        # 3. Alle Aktionen aktivieren
        await g.sync_bans.set(True)
        await g.sync_unbans.set(True)
        await g.sync_timeouts.set(True)
        await g.sync_kicks.set(True)
        await g.sync_warns.set(True)
        # 4. Audit-Log aktivieren (für manuelle Aktionen & Kick-Erkennung)
        await g.sync_audit_log.set(True)
        # 5. Log-Channel = aktueller Channel
        if isinstance(ctx.channel, discord.TextChannel):
            await g.sync_log_channel.set(ctx.channel.id)
            log_ch_label = ctx.channel.mention
        else:
            log_ch_label = "❌ (kein Text-Channel)"
        # Zusammenfassung
        embed = discord.Embed(
            title="✅ BanSync QuickStart fertig!",
            description=(
                f"Diese Guild ist konfiguriert als: **{role_label}**\n\n"
                f"**Automatisch synchronisiert werden:**\n"
                f"• 🚫 Banns\n"
                f"• ✅ Entbannungen\n"
                f"• ⏱️ Timeouts/Mutes\n"
                f"• 👢 Kicks\n"
                f"• ⚠️ Warnungen (nur Log)\n\n"
                f"**Audit-Log-Auswertung:** ✅ (erkennt manuelle Aktionen)\n"
                f"**Log-Channel:** {log_ch_label}\n\n"
                f"**WICHTIG für {len(self.bot.guilds) - 1} andere Server:**\n"
                f"Führe dort `[p]syncset slave {ctx.guild.id}` aus, damit sie als Empfänger fungieren."
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        # Falls Master: Hinweis auf SyncNow
        if role_label.startswith("Master"):
            embed.add_field(
                name="💡 Optional: Bestehende Banns übertragen",
                value=f"Führe `[p]syncset syncnow` aus, um alle bereits existierenden Banns dieser Guild auf alle Empfänger zu übertragen (einmalig).",
                inline=False,
            )
        embed.set_footer(text=f"BanSync QuickStart • {ctx.guild.name}")
        await ctx.send(embed=embed)

    @syncset.command(name="slave", aliases=["receiver", "client"])
    async def syncset_slave(self, ctx: commands.Context, master_guild_id: int):
        """Quick-Setup für eine Empfänger-Guild (Nimmt Aktionen vom Master entgegen).

        Beispiel: `[p]syncset slave 123456789012345678`
        """
        # Prüfen ob Bot auf Master-Guild ist
        master_guild = self.bot.get_guild(master_guild_id)
        if master_guild is None:
            await ctx.send(f"❌ Bot ist nicht auf Master-Guild `{master_guild_id}`.")
            return
        g = self.config.guild(ctx.guild)
        await g.sync_enabled.set(True)
        await g.sync_master_guild_id.set(master_guild_id)
        await g.sync_direction.set("master_to_all")  # nur Master propagiert
        await g.sync_bans.set(True)
        await g.sync_unbans.set(True)
        await g.sync_timeouts.set(True)
        await g.sync_kicks.set(True)
        await g.sync_warns.set(True)
        await g.sync_audit_log.set(False)  # als Slave nicht nötig
        # Log-Channel = aktueller Channel
        log_ch_label = "❌ (kein Text-Channel)"
        if isinstance(ctx.channel, discord.TextChannel):
            await g.sync_log_channel.set(ctx.channel.id)
            log_ch_label = ctx.channel.mention
        embed = discord.Embed(
            title="✅ Empfänger-Guild eingerichtet!",
            description=(
                f"Diese Guild (`{ctx.guild.name}`) ist jetzt **Empfänger** für Mod-Aktionen von:\n"
                f"**{master_guild.name}** (`{master_guild_id}`)\n\n"
                f"**Diese Guild wird automatisch empfangen:**\n"
                f"• 🚫 Banns (vom Master gebannt → hier auch gebannt)\n"
                f"• ✅ Entbannungen\n"
                f"• ⏱️ Timeouts/Mutes\n"
                f"• 👢 Kicks (sofern User hier ist)\n"
                f"• ⚠️ Warnungen (nur Log-Eintrag hier)\n\n"
                f"**Log-Channel:** {log_ch_label}\n\n"
                f"💡 **Auf dem Master-Server** muss `[p]syncset quickstart` ausgeführt worden sein."
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        embed.set_footer(text=f"BanSync Slave • {ctx.guild.name}")
        await ctx.send(embed=embed)

    @syncset.command(name="setup")
    async def syncset_setup(self, ctx: commands.Context):
        """Interaktiver Einrichtungsassistent für BanSync (mit Buttons)."""
        view = SyncSetupWizardView(self, ctx.guild)
        embed = view.build_embed()
        await ctx.send(embed=embed, view=view)

    @syncset.command(name="show")
    async def syncset_show(self, ctx: commands.Context):
        """Zeigt die aktuelle Sync-Konfiguration."""
        g = self.config.guild(ctx.guild)
        embed = discord.Embed(title="🔄 Cross-Server Sync Konfiguration", color=discord.Color.blue(), timestamp=_now())
        embed.add_field(name="Aktiviert", value="✅ Ja" if await g.sync_enabled() else "❌ Nein", inline=True)
        embed.add_field(name="Richtung", value=await g.sync_direction(), inline=True)
        master_id = await g.sync_master_guild_id()
        embed.add_field(name="Master-Guild", value=f"`{master_id}`" if master_id else "Aktuelle Guild (implizit)", inline=True)
        embed.add_field(name="Banns", value="✅" if await g.sync_bans() else "❌", inline=True)
        embed.add_field(name="Unbanns", value="✅" if await g.sync_unbans() else "❌", inline=True)
        embed.add_field(name="Timeouts", value="✅" if await g.sync_timeouts() else "❌", inline=True)
        embed.add_field(name="Kicks", value="✅" if await g.sync_kicks() else "❌", inline=True)
        embed.add_field(name="Warnungen", value="✅" if await g.sync_warns() else "❌", inline=True)
        embed.add_field(name="Audit-Log-Auswertung", value="✅" if await g.sync_audit_log() else "❌", inline=True)
        log_ch_id = await g.sync_log_channel()
        log_ch = ctx.guild.get_channel(log_ch_id) if log_ch_id else None
        embed.add_field(name="Log-Channel", value=log_ch.mention if log_ch else "❌ Nicht gesetzt", inline=True)
        excluded = await g.sync_excluded_guilds() or []
        embed.add_field(name="Ausgeschlossene Guilds", value=", ".join(f"`{x}`" for x in excluded) or "Keine", inline=False)
        # Stats
        embed.add_field(name="Bot-Guilds gesamt", value=str(len(self.bot.guilds)), inline=True)
        embed.add_field(name="Mögliche Sync-Targets", value=str(len(self.bot.guilds) - 1), inline=True)
        embed.set_footer(text=f"Sync Config • {ctx.guild.name}")
        await ctx.send(embed=embed)

    @syncset.command(name="toggle")
    async def syncset_toggle(self, ctx: commands.Context):
        """Aktiviert/deaktiviert das Sync-System für diese Guild."""
        current = await self.config.guild(ctx.guild).sync_enabled()
        await self.config.guild(ctx.guild).sync_enabled.set(not current)
        status = "✅ aktiviert" if not current else "❌ deaktiviert"
        await ctx.send(f"🔄 Cross-Server Sync ist jetzt **{status}**.")

        # Warnung: braucht auf allen Ziel-Guilds aktivierten Sync
        if not current:
            target_count = sum(1 for g in self.bot.guilds if g.id != ctx.guild.id)
            await ctx.send(
                f"ℹ️ Hinweis: Auf den {target_count} anderen Guilds muss Sync ebenfalls aktiviert sein "
                f"(mindestens als Empfänger). Konfiguriere mit `[p]syncset toggle` auf jeder Guild separat."
            )

    @syncset.command(name="master")
    async def syncset_master(self, ctx: commands.Context, guild_id: int = None):
        """Setzt die Master-Guild-ID (für master_to_all Richtung).
        Ohne Angabe wird die aktuelle Guild als Master gesetzt."""
        if guild_id is None:
            guild_id = ctx.guild.id
        # Prüfen ob Bot auf dieser Guild ist
        if self.bot.get_guild(guild_id) is None:
            await ctx.send(f"❌ Bot ist nicht auf Guild `{guild_id}`.")
            return
        await self.config.guild(ctx.guild).sync_master_guild_id.set(guild_id)
        await ctx.send(f"✅ Master-Guild gesetzt auf `{guild_id}`.")

    @syncset.command(name="direction")
    async def syncset_direction(self, ctx: commands.Context, direction: str):
        """Setzt die Sync-Richtung. `master_to_all` oder `bidirectional`."""
        direction = direction.lower()
        if direction not in ("master_to_all", "bidirectional"):
            await ctx.send("❌ Ungültige Richtung. Verwende `master_to_all` oder `bidirectional`.")
            return
        await self.config.guild(ctx.guild).sync_direction.set(direction)
        await ctx.send(f"✅ Sync-Richtung gesetzt auf `{direction}`.")

    @syncset.command(name="bans")
    async def syncset_bans(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Sync von Banns."""
        await self.config.guild(ctx.guild).sync_bans.set(enabled)
        await ctx.send(f"✅ Ban-Sync {'aktiviert' if enabled else 'deaktiviert'}.")

    @syncset.command(name="unbans")
    async def syncset_unbans(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Sync von Entbannungen."""
        await self.config.guild(ctx.guild).sync_unbans.set(enabled)
        await ctx.send(f"✅ Unban-Sync {'aktiviert' if enabled else 'deaktiviert'}.")

    @syncset.command(name="timeouts")
    async def syncset_timeouts(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Sync von Timeouts."""
        await self.config.guild(ctx.guild).sync_timeouts.set(enabled)
        await ctx.send(f"✅ Timeout-Sync {'aktiviert' if enabled else 'deaktiviert'}.")

    @syncset.command(name="kicks")
    async def syncset_kicks(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Sync von Kicks."""
        await self.config.guild(ctx.guild).sync_kicks.set(enabled)
        await ctx.send(f"✅ Kick-Sync {'aktiviert' if enabled else 'deaktiviert'}.")

    @syncset.command(name="warns")
    async def syncset_warns(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert Sync von Warnungen."""
        await self.config.guild(ctx.guild).sync_warns.set(enabled)
        await ctx.send(f"✅ Warn-Sync {'aktiviert' if enabled else 'deaktiviert'}.")

    @syncset.command(name="auditlog")
    async def syncset_auditlog(self, ctx: commands.Context, enabled: bool):
        """Aktiviert/deaktiviert die Audit-Log-Auswertung (für manuelle Aktionen & Kicks)."""
        await self.config.guild(ctx.guild).sync_audit_log.set(enabled)
        await ctx.send(
            f"✅ Audit-Log-Auswertung {'aktiviert' if enabled else 'deaktiviert'}.\n"
            f"⚠️ Erfordert `View Audit Log` Berechtigung auf der Guild."
        )

    @syncset.command(name="logchannel")
    async def syncset_logchannel(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Setzt den Log-Channel für Sync-Events. Ohne Angabe zurückgesetzt."""
        if channel is None:
            await self.config.guild(ctx.guild).sync_log_channel.set(None)
            await ctx.send("✅ Sync-Log-Channel zurückgesetzt.")
            return
        await self.config.guild(ctx.guild).sync_log_channel.set(channel.id)
        await ctx.send(f"✅ Sync-Log-Channel gesetzt auf {channel.mention}.")

    @syncset.command(name="exclude")
    async def syncset_exclude(self, ctx: commands.Context, action: str, guild_id: int):
        """Schließt eine Guild von der Synchronisation aus.
        `add` oder `remove` als Aktion."""
        excluded = await self.config.guild(ctx.guild).sync_excluded_guilds() or []
        if action.lower() == "add":
            if guild_id not in excluded:
                excluded.append(guild_id)
            await self.config.guild(ctx.guild).sync_excluded_guilds.set(excluded)
            await ctx.send(f"✅ Guild `{guild_id}` von Sync ausgeschlossen.")
        elif action.lower() == "remove":
            if guild_id in excluded:
                excluded.remove(guild_id)
            await self.config.guild(ctx.guild).sync_excluded_guilds.set(excluded)
            await ctx.send(f"✅ Guild `{guild_id}` wieder für Sync freigegeben.")
        else:
            await ctx.send("❌ Ungültige Aktion. Verwende `add` oder `remove`.")

    @syncset.command(name="excludelist", aliases=["listexcluded"])
    async def syncset_excludelist(self, ctx: commands.Context):
        """Zeigt alle ausgeschlossenen Guilds."""
        excluded = await self.config.guild(ctx.guild).sync_excluded_guilds() or []
        if not excluded:
            await ctx.send("ℹ️ Keine Guilds ausgeschlossen.")
            return
        lines = []
        for gid in excluded:
            g = self.bot.get_guild(gid)
            name = g.name if g else "Unbekannt"
            lines.append(f"• `{gid}` — {name}")
        embed = discord.Embed(
            title="🚫 Ausgeschlossene Guilds",
            description="\n".join(lines),
            color=discord.Color.orange(),
        )
        await ctx.send(embed=embed)

    @syncset.command(name="syncnow")
    async def syncset_syncnow(self, ctx: commands.Context):
        """Synchronisiert ALLE bestehenden Banns der aktuellen Guild auf alle anderen (einmalig)."""
        # ALLES in try/except — jeder Fehler wird direkt im Discord angezeigt
        try:
            if not await self._sync_should_propagate_from(ctx.guild):
                await ctx.send("❌ Sync ist nicht aktiviert oder diese Guild darf nicht propagieren.")
                return
            if not await self.config.guild(ctx.guild).sync_bans():
                await ctx.send("❌ Ban-Sync ist deaktiviert. Aktiviere mit `[p]syncset bans True`.")
                return
            await ctx.send("⏳ Synchronisiere bestehende Banns... das kann einen Moment dauern.")

            # ===== STEP 1: Bans abrufen (ultra-defensiv, alle discord.py-Versionen) =====
            bans_list = []
            try:
                # Variante A: discord.py 1.x — guild.bans() ist eine Coroutine die eine List[BanEntry] zurückgibt
                # Variante B: discord.py 2.x — guild.bans(limit=...) gibt einen AsyncIterator zurück, muss iteriert werden
                bans_obj = ctx.guild.bans()
                # Prüfen ob es awaitable ist
                import inspect
                if inspect.isawaitable(bans_obj):
                    # discord.py 1.x: await coroutine → list
                    try:
                        bans_list = await bans_obj
                    except TypeError:
                        # Manche 2.x Versionen: await gibt async iterator zurück
                        bans_list = [b async for b in (await bans_obj)]
                elif inspect.isasyncgen(bans_obj):
                    # discord.py 2.x ohne await direkt async iterator
                    bans_list = [b async for b in bans_obj]
                else:
                    # Fallback: vielleicht schon eine Liste
                    bans_list = list(bans_obj)
            except discord.Forbidden:
                await ctx.send("❌ Fehlende Rechte um Bans abzufragen (Brauche `Ban Members` Berechtigung).")
                return
            except Exception as e:
                # Letzter Versuch: ohne Argumente
                try:
                    raw = ctx.guild.bans()
                    import inspect as _i
                    if _i.isawaitable(raw):
                        bans_list = await raw
                    elif _i.isasyncgen(raw):
                        bans_list = [b async for b in raw]
                    else:
                        bans_list = list(raw)
                except Exception as e2:
                    await ctx.send(f"❌ Konnte Bans nicht abrufen.\nFehler 1: `{type(e).__name__}: {e}`\nFehler 2: `{type(e2).__name__}: {e2}`")
                    return

            if not bans_list:
                await ctx.send("ℹ️ Keine bestehenden Banns auf dieser Guild gefunden.")
                return

            # ===== STEP 2: Ziel-Guilds abrufen =====
            try:
                targets = await self._sync_target_guilds(ctx.guild)
            except Exception as e:
                await ctx.send(f"❌ Konnte Sync-Targets nicht abrufen: `{type(e).__name__}: {e}`")
                return
            if not targets:
                await ctx.send("ℹ️ Keine Sync-Targets verfügbar. Stelle sicher dass Sync auf anderen Guilds aktiviert ist (`[p]syncset toggle` dort ausführen).")
                return

            total = len(bans_list)
            await ctx.send(f"📋 {total} Banns gefunden. Synchronisiere zu {len(targets)} Guild(s)...")

            # ===== STEP 3: Banns synchronisieren =====
            success_count = 0
            per_guild_success = {}
            per_guild_fail = {}
            per_guild_fail_reasons = {}

            for i, ban_entry in enumerate(bans_list, 1):
                try:
                    # ban_entry könnte BanEntry sein oder User
                    user = getattr(ban_entry, "user", ban_entry)
                    reason = getattr(ban_entry, "reason", None) or "Bestehender Ban (SyncNow)"
                    user_id = getattr(user, "id", None)
                    if user_id is None:
                        per_guild_fail["unknown"] = per_guild_fail.get("unknown", 0) + 1
                        continue
                except Exception:
                    per_guild_fail["unknown"] = per_guild_fail.get("unknown", 0) + 1
                    continue

                for g in targets:
                    try:
                        key = self._sync_action_key("ban", user_id, ctx.guild.id)
                        await self._sync_mark_recent(g, key)
                    except Exception:
                        pass  # nicht kritisch
                    try:
                        # Prüfen ob schon gebannt
                        already_banned = False
                        try:
                            existing = await g.fetch_ban(user)
                            if existing:
                                already_banned = True
                        except discord.NotFound:
                            pass
                        except AttributeError:
                            # Alte discord.py ohne fetch_ban → überspringen
                            pass
                        except Exception:
                            pass
                        if already_banned:
                            per_guild_success[g.name] = per_guild_success.get(g.name, 0) + 1
                            continue
                        # Bannen
                        try:
                            await g.ban(user, reason=f"[BanSync von {ctx.guild.name}] {reason}", delete_message_seconds=86400)
                        except TypeError:
                            # Alte discord.py ohne delete_message_seconds
                            await g.ban(user, reason=f"[BanSync von {ctx.guild.name}] {reason}")
                        per_guild_success[g.name] = per_guild_success.get(g.name, 0) + 1
                    except discord.Forbidden:
                        per_guild_fail[g.name] = per_guild_fail.get(g.name, 0) + 1
                        per_guild_fail_reasons.setdefault(g.name, "Fehlende Rechte")
                    except discord.HTTPException as e:
                        per_guild_fail[g.name] = per_guild_fail.get(g.name, 0) + 1
                        per_guild_fail_reasons.setdefault(g.name, f"HTTP: {e}")
                    except Exception as e:
                        per_guild_fail[g.name] = per_guild_fail.get(g.name, 0) + 1
                        per_guild_fail_reasons.setdefault(g.name, f"{type(e).__name__}: {e}")
                success_count += 1
                # Fortschrittsmeldung alle 25 Banns
                if i % 25 == 0 and i < total:
                    try:
                        await ctx.send(f"⏳ Fortschritt: {i}/{total} Banns verarbeitet...")
                    except Exception:
                        pass

            # ===== STEP 4: Zusammenfassung =====
            try:
                embed = discord.Embed(
                    title="🔄 SyncNow abgeschlossen",
                    description=f"**{success_count}** Banns von `{ctx.guild.name}` synchronisiert zu **{len(targets)}** Guild(s).",
                    color=discord.Color.green(),
                    timestamp=_now(),
                )
                if per_guild_success:
                    success_lines = [f"• `{g}`: {c} Banns" for g, c in sorted(per_guild_success.items())]
                    embed.add_field(name="✅ Erfolgreich", value="\n".join(success_lines[:15]) or "Keine", inline=False)
                if per_guild_fail:
                    fail_lines = [f"• `{g}`: {c} ({per_guild_fail_reasons.get(g, '?')})" for g, c in sorted(per_guild_fail.items())]
                    embed.add_field(name="❌ Fehlgeschlagen", value="\n".join(fail_lines[:15]) or "Keine", inline=False)
                embed.set_footer(text=f"SyncNow • {ctx.guild.name} • {total} Banns gesamt")
                await ctx.send(embed=embed)
                # Auch ins Sync-Log
                try:
                    await self._sync_log(ctx.guild, f"🔄 SyncNow: {success_count} Banns synchronisiert", embed=embed)
                except Exception:
                    pass
            except Exception as e:
                # Fallback wenn Embed-Erstellung scheitert
                await ctx.send(f"✅ SyncNow fertig: {success_count} Banns synchronisiert. (Embed-Fehler: `{e}`)")
        except Exception as e:
            # LETZTER FALL: komplett unbekannter Fehler → direkt im Discord anzeigen
            import traceback
            tb = traceback.format_exception(type(e), e, e.__traceback__)
            tb_str = "".join(tb)
            # Auf 1900 Zeichen begrenzen
            if len(tb_str) > 1900:
                tb_str = tb_str[-1900:]
            try:
                await ctx.send(f"❌ **Unerwarteter Fehler in syncnow:**\n```\n{type(e).__name__}: {e}\n```\nTraceback:\n```py\n{tb_str}\n```")
            except Exception:
                # Wenn selbst das nicht geht
                try:
                    await ctx.send(f"❌ Fehler: `{type(e).__name__}: {e}`")
                except Exception:
                    pass

    @syncset.command(name="test")
    async def syncset_test(self, ctx: commands.Context):
        """Testet welche Guilds als Sync-Targets verfügbar wären."""
        targets = await self._sync_target_guilds(ctx.guild)
        if not targets:
            await ctx.send("ℹ️ Keine Sync-Targets verfügbar. Stelle sicher dass Sync auf anderen Guilds aktiviert ist.")
            return
        lines = []
        for g in targets:
            lines.append(f"• {g.name} (`{g.id}`) — {g.member_count} Mitglieder")
        embed = discord.Embed(
            title=f"🔄 Sync-Targets für {ctx.guild.name}",
            description="\n".join(lines[:20]),
            color=discord.Color.blue(),
            timestamp=_now(),
        )
        if len(targets) > 20:
            embed.set_footer(text=f"Zeige 20 von {len(targets)}")
        await ctx.send(embed=embed)

    # ============================================
    # ROLE SYNC BEFEHLE
    # ============================================

    @commands.group(name="rolesyncset", aliases=["rsyncset"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def rolesyncset(self, ctx: commands.Context):
        """Konfiguriert das Auto-Role-Sync System."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @rolesyncset.command(name="toggle")
    async def rolesyncset_toggle(self, ctx: commands.Context):
        """Aktiviert/deaktiviert Auto-Role-Sync."""
        current = await self.config.guild(ctx.guild).sync_role_sync_enabled()
        await self.config.guild(ctx.guild).sync_role_sync_enabled.set(not current)
        await ctx.send(f"🔄 Auto-Role-Sync ist jetzt **{'✅ aktiviert' if not current else '❌ deaktiviert'}**.")

    @rolesyncset.command(name="add")
    async def rolesyncset_add(self, ctx: commands.Context, source_role: discord.Role, target_guild_id: int, target_role_id: int):
        """Fügt ein Rollen-Mapping hinzu.
        Beispiel: `[p]rolesyncset add @Verified 1234567890 9876543210`"""
        target_guild = self.bot.get_guild(target_guild_id)
        if target_guild is None:
            await ctx.send(f"❌ Bot ist nicht auf Ziel-Guild `{target_guild_id}`.")
            return
        target_role = target_guild.get_role(target_role_id)
        if target_role is None:
            await ctx.send(f"❌ Rolle `{target_role_id}` nicht auf Ziel-Guild `{target_guild.name}` gefunden.")
            return
        role_map = await self.config.guild(ctx.guild).sync_role_map() or {}
        sid = str(source_role.id)
        if sid not in role_map:
            role_map[sid] = []
        # Prüfen ob schon existiert
        for entry in role_map[sid]:
            if entry.get("guild_id") == target_guild_id and entry.get("role_id") == target_role_id:
                await ctx.send("ℹ️ Dieses Mapping existiert bereits.")
                return
        role_map[sid].append({"guild_id": target_guild_id, "role_id": target_role_id})
        await self.config.guild(ctx.guild).sync_role_map.set(role_map)
        await ctx.send(
            f"✅ Mapping hinzugefügt: {source_role.mention} (`{source_role.id}`) "
            f"→ {target_role.name} auf {target_guild.name} (`{target_role_id}`)"
        )

    @rolesyncset.command(name="remove", aliases=["delete"])
    async def rolesyncset_remove(self, ctx: commands.Context, source_role: discord.Role, target_guild_id: int, target_role_id: int):
        """Entfernt ein Rollen-Mapping."""
        role_map = await self.config.guild(ctx.guild).sync_role_map() or {}
        sid = str(source_role.id)
        if sid not in role_map:
            await ctx.send("ℹ️ Keine Mappings für diese Rolle gefunden.")
            return
        before_count = len(role_map[sid])
        role_map[sid] = [
            e for e in role_map[sid]
            if not (e.get("guild_id") == target_guild_id and e.get("role_id") == target_role_id)
        ]
        if not role_map[sid]:
            del role_map[sid]
        await self.config.guild(ctx.guild).sync_role_map.set(role_map)
        await ctx.send(f"✅ Mapping entfernt. Vorher: {before_count}, Jetzt: {len(role_map.get(sid, []))}")

    @rolesyncset.command(name="list")
    async def rolesyncset_list(self, ctx: commands.Context):
        """Zeigt alle Rollen-Mappings."""
        role_map = await self.config.guild(ctx.guild).sync_role_map() or {}
        if not role_map:
            await ctx.send("ℹ️ Keine Rollen-Mappings konfiguriert.")
            return
        lines = []
        for source_role_id, targets in role_map.items():
            src_role = ctx.guild.get_role(int(source_role_id))
            src_name = src_role.name if src_role else f"Unbekannt ({source_role_id})"
            for t in targets:
                g = self.bot.get_guild(t.get("guild_id"))
                gname = g.name if g else f"Unbekannt ({t.get('guild_id')})"
                r = g.get_role(t.get("role_id")) if g else None
                rname = r.name if r else f"Unbekannt ({t.get('role_id')})"
                lines.append(f"• `{src_name}` → `{rname}` auf `{gname}`")
        embed = discord.Embed(
            title="🔄 Rollen-Mappings",
            description="\n".join(lines[:25]) or "Keine",
            color=discord.Color.blue(),
        )
        if len(lines) > 25:
            embed.set_footer(text=f"Zeige 25 von {len(lines)}")
        await ctx.send(embed=embed)

    # ============================================
    # ANTI-NUKE BEFEHLE
    # ============================================

    @commands.group(name="antinukeset", aliases=["antinukeconfig", "nukeset"])
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def antinukeset(self, ctx: commands.Context):
        """Konfiguriert das Anti-Nuke System."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @antinukeset.command(name="show")
    async def antinukeset_show(self, ctx: commands.Context):
        """Zeigt die Anti-Nuke Konfiguration."""
        g = self.config.guild(ctx.guild)
        embed = discord.Embed(title="🚨 Anti-Nuke Konfiguration", color=discord.Color.red(), timestamp=_now())
        embed.add_field(name="Aktiviert", value="✅ Ja" if await g.antinuke_enabled() else "❌ Nein", inline=True)
        embed.add_field(name="Aktion", value=await g.antinuke_action(), inline=True)
        embed.add_field(name="Zeitfenster", value=f"{await g.antinuke_window_seconds()}s", inline=True)
        embed.add_field(name="Ban-Limit", value=await g.antinuke_ban_threshold(), inline=True)
        embed.add_field(name="Kick-Limit", value=await g.antinuke_kick_threshold(), inline=True)
        embed.add_field(name="Channel-Del-Limit", value=await g.antinuke_channel_delete_threshold(), inline=True)
        embed.add_field(name="Role-Del-Limit", value=await g.antinuke_role_delete_threshold(), inline=True)
        log_ch_id = await g.antinuke_log_channel()
        log_ch = ctx.guild.get_channel(log_ch_id) if log_ch_id else None
        embed.add_field(name="Log-Channel", value=log_ch.mention if log_ch else "❌ Nicht gesetzt", inline=True)
        wl_users = await g.antinuke_whitelist_users() or []
        embed.add_field(name="Whitelist Users", value=", ".join(f"<@{u}>" for u in wl_users[:10]) or "Keine", inline=False)
        wl_roles = await g.antinuke_whitelist_roles() or []
        embed.add_field(name="Whitelist Rollen", value=", ".join(f"<@&{r}>" for r in wl_roles[:10]) or "Keine", inline=False)
        embed.set_footer(text=f"Anti-Nuke Config • {ctx.guild.name}")
        await ctx.send(embed=embed)

    @antinukeset.command(name="toggle")
    async def antinukeset_toggle(self, ctx: commands.Context):
        """Aktiviert/deaktiviert Anti-Nuke."""
        current = await self.config.guild(ctx.guild).antinuke_enabled()
        await self.config.guild(ctx.guild).antinuke_enabled.set(not current)
        await ctx.send(f"🚨 Anti-Nuke ist jetzt **{'✅ aktiviert' if not current else '❌ deaktiviert'}**.")

    @antinukeset.command(name="banthreshold")
    async def antinukeset_banthreshold(self, ctx: commands.Context, count: int):
        """Setzt das Limit für Banns im Zeitfenster."""
        if count < 1:
            await ctx.send("❌ Wert muss mindestens 1 sein.")
            return
        await self.config.guild(ctx.guild).antinuke_ban_threshold.set(count)
        await ctx.send(f"✅ Ban-Limit gesetzt auf {count}.")

    @antinukeset.command(name="kickthreshold")
    async def antinukeset_kickthreshold(self, ctx: commands.Context, count: int):
        """Setzt das Limit für Kicks im Zeitfenster."""
        if count < 1:
            await ctx.send("❌ Wert muss mindestens 1 sein.")
            return
        await self.config.guild(ctx.guild).antinuke_kick_threshold.set(count)
        await ctx.send(f"✅ Kick-Limit gesetzt auf {count}.")

    @antinukeset.command(name="channelthreshold", aliases=["channeldeletethreshold"])
    async def antinukeset_channelthreshold(self, ctx: commands.Context, count: int):
        """Setzt das Limit für Channel-Löschungen im Zeitfenster."""
        if count < 1:
            await ctx.send("❌ Wert muss mindestens 1 sein.")
            return
        await self.config.guild(ctx.guild).antinuke_channel_delete_threshold.set(count)
        await ctx.send(f"✅ Channel-Delete-Limit gesetzt auf {count}.")

    @antinukeset.command(name="rolethreshold", aliases=["roledeletethreshold"])
    async def antinukeset_rolethreshold(self, ctx: commands.Context, count: int):
        """Setzt das Limit für Rollen-Löschungen im Zeitfenster."""
        if count < 1:
            await ctx.send("❌ Wert muss mindestens 1 sein.")
            return
        await self.config.guild(ctx.guild).antinuke_role_delete_threshold.set(count)
        await ctx.send(f"✅ Role-Delete-Limit gesetzt auf {count}.")

    @antinukeset.command(name="window")
    async def antinukeset_window(self, ctx: commands.Context, seconds: int):
        """Setzt das Zeitfenster (in Sekunden) für Anti-Nuke."""
        if seconds < 5:
            await ctx.send("❌ Wert muss mindestens 5 Sekunden sein.")
            return
        await self.config.guild(ctx.guild).antinuke_window_seconds.set(seconds)
        await ctx.send(f"✅ Zeitfenster gesetzt auf {seconds} Sekunden.")

    @antinukeset.command(name="action")
    async def antinukeset_action(self, ctx: commands.Context, action: str):
        """Setzt die Aktion bei Überschreitung. `strip` oder `notify`."""
        action = action.lower()
        if action not in ("strip", "notify"):
            await ctx.send("❌ Ungültige Aktion. Verwende `strip` (entfernt Mod-Rechte) oder `notify` (nur Loggen).")
            return
        await self.config.guild(ctx.guild).antinuke_action.set(action)
        await ctx.send(f"✅ Anti-Nuke-Aktion gesetzt auf `{action}`.")

    @antinukeset.command(name="logchannel")
    async def antinukeset_logchannel(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Setzt den Log-Channel für Anti-Nuke Alarme."""
        if channel is None:
            await self.config.guild(ctx.guild).antinuke_log_channel.set(None)
            await ctx.send("✅ Anti-Nuke-Log-Channel zurückgesetzt.")
            return
        await self.config.guild(ctx.guild).antinuke_log_channel.set(channel.id)
        await ctx.send(f"✅ Anti-Nuke-Log-Channel gesetzt auf {channel.mention}.")

    @antinukeset.command(name="whitelistuser", aliases=["wluser"])
    async def antinukeset_whitelistuser(self, ctx: commands.Context, action: str, user: discord.User):
        """Fügt einen User zur Anti-Nuke Whitelist hinzu oder entfernt ihn.
        `add` oder `remove` als Aktion."""
        wl = await self.config.guild(ctx.guild).antinuke_whitelist_users() or []
        if action.lower() == "add":
            if user.id not in wl:
                wl.append(user.id)
            await self.config.guild(ctx.guild).antinuke_whitelist_users.set(wl)
            await ctx.send(f"✅ {user.mention} zur Anti-Nuke Whitelist hinzugefügt.")
        elif action.lower() == "remove":
            if user.id in wl:
                wl.remove(user.id)
            await self.config.guild(ctx.guild).antinuke_whitelist_users.set(wl)
            await ctx.send(f"✅ {user.mention} von Anti-Nuke Whitelist entfernt.")
        else:
            await ctx.send("❌ Ungültige Aktion. Verwende `add` oder `remove`.")

    @antinukeset.command(name="whitelistrole", aliases=["wlrole"])
    async def antinukeset_whitelistrole(self, ctx: commands.Context, action: str, role: discord.Role):
        """Fügt eine Rolle zur Anti-Nuke Whitelist hinzu oder entfernt sie.
        `add` oder `remove` als Aktion."""
        wl = await self.config.guild(ctx.guild).antinuke_whitelist_roles() or []
        if action.lower() == "add":
            if role.id not in wl:
                wl.append(role.id)
            await self.config.guild(ctx.guild).antinuke_whitelist_roles.set(wl)
            await ctx.send(f"✅ Rolle {role.mention} zur Anti-Nuke Whitelist hinzugefügt.")
        elif action.lower() == "remove":
            if role.id in wl:
                wl.remove(role.id)
            await self.config.guild(ctx.guild).antinuke_whitelist_roles.set(wl)
            await ctx.send(f"✅ Rolle {role.mention} von Anti-Nuke Whitelist entfernt.")
        else:
            await ctx.send("❌ Ungültige Aktion. Verwende `add` oder `remove`.")

    @antinukeset.command(name="reset")
    async def antinukeset_reset(self, ctx: commands.Context):
        """Setzt den Anti-Nuke Tracker zurück (löscht alle getrackten Aktionen)."""
        await self.config.guild(ctx.guild).antinuke_tracker.set({})
        await ctx.send("✅ Anti-Nuke Tracker zurückgesetzt.")


class DutyButtonView(discord.ui.View):
    """Button-View für Duty An-/Abmeldung mit erweiterten Funktionen"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Duty Starten", style=discord.ButtonStyle.green, emoji="🟢", custom_id="duty_start")
    async def start_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Startet den Duty-Modus (race-condition-safe via cog._lock_for)."""
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button funktioniert nur auf Servern.", ephemeral=True)
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Duty starten.", ephemeral=True)
            return

        role_id = await self.cog.config.guild(guild).role()

        if not role_id:
            await interaction.response.send_message("❌ Es wurde keine Support-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return

        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Die konfigurierte Support-Rolle existiert nicht mehr!", ephemeral=True)
            return

        if member.get_role(role_id) is None:
            await interaction.response.send_message(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!", ephemeral=True)
            return

        # Serialize per-(guild, member) to prevent double-click races.
        async with self.cog._lock_for(guild.id, member.id):
            is_on_duty = await self.cog.config.member(member).on_duty()
            if is_on_duty:
                await interaction.response.send_message("⚠️ Du bist bereits im Duty-Modus!", ephemeral=True)
                return

            # Duty aktivieren und Rolle geben
            await self.cog.config.member(member).on_duty.set(True)
            start_ts = _now_ts()
            await self.cog.config.member(member).duty_start.set(start_ts)
            await self.cog.config.member(member).duty_status.set("available")
            await self.cog.config.member(member).duty_status_message.set(None)

            current_sessions = await self.cog.config.member(member).duty_session_count() or 0
            await self.cog.config.member(member).duty_session_count.set(current_sessions + 1)
            await self.cog.config.member(member).duty_break_count.set(0)
            await self.cog.config.member(member).duty_total_break_time.set(0)
            await self.cog.config.member(member).current_break_start.set(None)

            # Duty-Rolle hinzufügen — roll back Config on failure.
            ok = await self.cog.update_duty_role(member, True)
            if not ok:
                await self.cog.config.member(member).on_duty.set(False)
                await self.cog.config.member(member).duty_start.set(None)
                await self.cog.config.member(member).duty_status.set("off_duty")
                await self.cog.config.member(member).duty_session_count.set(current_sessions)
                await interaction.response.send_message("❌ Konnte die Duty-Rolle nicht zuweisen (fehlende Rechte?). Duty-Start abgebrochen.", ephemeral=True)
                return

        start_time = _from_ts(start_ts)
        auto_duty = await self.cog.config.guild(guild).auto_remove_duty()
        duty_timeout = await self.cog.config.guild(guild).duty_timeout()

        log_channel = await self.cog.get_log_channel(guild)
        embed = discord.Embed(
            title="🟢 Duty Gestartet",
            description=f"{member.mention} hat sich für den Support-Dienst angemeldet!",
            color=discord.Color.green(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="📊 Sessions", value=f"Session #{current_sessions + 1}", inline=True)
        if auto_duty and duty_timeout:
            end_time = start_time + timedelta(hours=duty_timeout)
            embed.add_field(name="⏰ Automatische Abmeldung", value=f"Nach {duty_timeout} Stunden\n(<t:{int(end_time.timestamp())}:R>)", inline=True)

        # Count active duty users (read-only — never spawn a duty role here).
        duty_count = 0
        duty_role = await self.cog.get_duty_role(guild)
        if duty_role:
            all_members = await self.cog.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Aktive Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_time.strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte Duty-Start-Log nicht senden (Guild %s)", guild.id)

        # Update panels (delegate to cog).
        await self.cog.update_panel_display(guild)
        await self.cog.update_status_display(guild)

        await interaction.response.send_message("✅ Du bist jetzt im Duty-Modus! Du wirst bei neuen Support-Anfragen gepingt.", ephemeral=True)

    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Duty-Modus (race-condition-safe via cog._lock_for + _finalize_duty_stop)."""
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button funktioniert nur auf Servern.", ephemeral=True)
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Duty beenden.", ephemeral=True)
            return

        is_on_duty = await self.cog.config.member(member).on_duty()
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Duty-Modus.", ephemeral=True)
            return

        # Capture duration before _finalize_duty_stop zeroes duty_start.
        start_time_ts = await self.cog.config.member(member).duty_start()
        duration = "Unbekannt"
        if start_time_ts:
            duration_seconds = max(0, _now_ts() - int(start_time_ts))
            duration = _fmt_h_m(duration_seconds)

        # Race-condition-safe stop.
        async with self.cog._lock_for(guild.id, member.id):
            await self.cog._finalize_duty_stop(member, whitelist=False, reason="Manuell beendet (Button)")

        log_channel = await self.cog.get_log_channel(guild)
        embed = discord.Embed(
            title="🔴 Duty Beendet",
            description=f"{member.mention} hat sich vom Support-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=_now()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)

        break_count = await self.cog.config.member(member).duty_break_count() or 0
        total_break_time = await self.cog.config.member(member).duty_total_break_time() or 0
        break_minutes = total_break_time // 60
        if break_count > 0:
            embed.add_field(name="☕ Pausen", value=f"{break_count} Pausen ({break_minutes} min)", inline=True)

        # Count remaining active duty users (read-only).
        duty_count = 0
        duty_role = await self.cog.get_duty_role(guild)
        if duty_role:
            all_members = await self.cog.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Verbleibende Supporter", value=f"🟢 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {_now().strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte Duty-Stop-Log nicht senden (Guild %s)", guild.id)

        # _finalize_duty_stop already refreshed displays, but re-refresh to be safe.
        await self.cog.update_panel_display(guild)
        await self.cog.update_status_display(guild)

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
        """Delegates to the cog-level method.

        Kept for backwards compat with code that calls
        DutyButtonView.update_panel_display. The actual implementation
        lives on SupportCog so all `duty *` text commands and this View
        share the same code path.
        """
        await self.cog.update_panel_display(guild)

    async def update_status_display(self, guild: discord.Guild):
        """Delegates to the cog-level method (see update_panel_display)."""
        await self.cog.update_status_display(guild)


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
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            return
        # cog._set_duty_status handles break-start accounting (current_break_start, break_count).
        await self.cog._set_duty_status(member, "break")
        await interaction.response.send_message("☕ Du bist jetzt in Pause.", ephemeral=True)
    
    @discord.ui.button(label="Abwesend", style=discord.ButtonStyle.secondary, emoji="⚪")
    async def set_away(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._set_status(interaction, "away", "Du bist jetzt abwesend.")
    
    async def _set_status(self, interaction: discord.Interaction, status: str, message: str):
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("\u274c Nur Server-Mitglieder.", ephemeral=True)
            return
        # Delegate to cog._set_duty_status which handles break transitions
        # (start/end break accounting) and refreshes the panels.
        await self.cog._set_duty_status(member, status)
        await interaction.response.edit_message(content=f"\u2705 {message}", view=None)


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
        """Startet den Whitelist-Duty-Modus (race-condition-safe via cog._lock_for)."""
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button funktioniert nur auf Servern.", ephemeral=True)
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Duty starten.", ephemeral=True)
            return

        role_id = await self.cog.config.guild(guild).whitelist_role()

        if not role_id:
            await interaction.response.send_message("❌ Es wurde keine Whitelist-Handler-Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return

        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.response.send_message("❌ Die konfigurierte Whitelist-Rolle existiert nicht mehr!", ephemeral=True)
            return

        if member.get_role(role_id) is None:
            await interaction.response.send_message(f"❌ Du benötigst die {base_role.mention} Rolle um dich auf Duty setzen zu können!", ephemeral=True)
            return

        async with self.cog._lock_for(guild.id, member.id):
            is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
            if is_on_duty:
                await interaction.response.send_message("⚠️ Du bist bereits im Whitelist-Duty-Modus!", ephemeral=True)
                return

            await self.cog.config.member(member).whitelist_on_duty.set(True)
            start_ts = _now_ts()
            await self.cog.config.member(member).whitelist_duty_start.set(start_ts)

            # Duty-Rolle hinzufügen — roll back Config on failure.
            ok = await self.cog.update_duty_role(member, True, whitelist=True)
            if not ok:
                await self.cog.config.member(member).whitelist_on_duty.set(False)
                await self.cog.config.member(member).whitelist_duty_start.set(None)
                await interaction.response.send_message("❌ Konnte die Whitelist-Duty-Rolle nicht zuweisen (fehlende Rechte?).", ephemeral=True)
                return

        start_time = _from_ts(start_ts)
        auto_duty = await self.cog.config.guild(guild).whitelist_auto_remove_duty()
        duty_timeout = await self.cog.config.guild(guild).whitelist_duty_timeout()

        log_channel = await self.cog.get_whitelist_log_channel(guild)
        embed = discord.Embed(
            title="🔵 Whitelist Duty Gestartet",
            description=f"{member.mention} hat sich für den Whitelist-Dienst angemeldet!",
            color=discord.Color.blue(),
            timestamp=start_time
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        if auto_duty and duty_timeout:
            end_time = start_time + timedelta(hours=duty_timeout)
            embed.add_field(name="⏰ Automatische Abmeldung", value=f"Nach {duty_timeout} Stunden\n(<t:{int(end_time.timestamp())}:R>)", inline=True)

        # Read-only count — never spawn a duty role as side effect.
        duty_count = 0
        duty_role = await self.cog.get_duty_role(guild, whitelist=True)
        if duty_role:
            all_members = await self.cog.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("whitelist_on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Aktive Handler", value=f"🔵 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Start • {start_time.strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte WL-Duty-Start-Log nicht senden (Guild %s)", guild.id)

        await self.cog.update_whitelist_panel_display(guild)
        await interaction.response.send_message("✅ Du bist jetzt im Whitelist-Duty-Modus! Du wirst bei neuen Anfragen gepingt.", ephemeral=True)

    @discord.ui.button(label="Duty Beenden", style=discord.ButtonStyle.red, emoji="🔴", custom_id="whitelist_duty_stop")
    async def stop_duty(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Beendet den Whitelist-Duty-Modus (race-condition-safe via cog._lock_for + _finalize_duty_stop)."""
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button funktioniert nur auf Servern.", ephemeral=True)
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Duty beenden.", ephemeral=True)
            return

        is_on_duty = await self.cog.config.member(member).whitelist_on_duty()
        if not is_on_duty:
            await interaction.response.send_message("ℹ️ Du bist aktuell nicht im Whitelist-Duty-Modus.", ephemeral=True)
            return

        # Capture duration before _finalize_duty_stop zeroes the start.
        start_time_ts = await self.cog.config.member(member).whitelist_duty_start()
        duration = "Unbekannt"
        if start_time_ts:
            duration_seconds = max(0, _now_ts() - int(start_time_ts))
            duration = _fmt_h_m(duration_seconds)

        async with self.cog._lock_for(guild.id, member.id):
            await self.cog._finalize_duty_stop(member, whitelist=True, reason="Manuell beendet (Button)")

        log_channel = await self.cog.get_whitelist_log_channel(guild)
        embed = discord.Embed(
            title="🔴 Whitelist Duty Beendet",
            description=f"{member.mention} hat sich vom Whitelist-Dienst abgemeldet.",
            color=discord.Color.red(),
            timestamp=_now()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Mitarbeiter", value=f"{member.display_name}", inline=True)
        embed.add_field(name="⏱️ Dauer", value=duration, inline=True)

        duty_count = 0
        duty_role = await self.cog.get_duty_role(guild, whitelist=True)
        if duty_role:
            all_members = await self.cog.config.all_members(guild)
            for m in duty_role.members:
                if all_members.get(m.id, {}).get("whitelist_on_duty"):
                    duty_count += 1
        embed.add_field(name="📊 Verbleibende Handler", value=f"🔵 {duty_count} Teammitglieder im Dienst", inline=True)
        embed.set_footer(text=f"Duty Ende • {_now().strftime('%d.%m.%Y %H:%M')} UTC")

        if log_channel:
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                log.warning("Konnte WL-Duty-Stop-Log nicht senden (Guild %s)", guild.id)

        await self.cog.update_whitelist_panel_display(guild)
        await interaction.response.send_message("✅ Du hast den Whitelist-Duty-Modus verlassen.", ephemeral=True)

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
                    timestamp=_now()
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
                        timestamp=_now()
                    )
                    log_embed.set_thumbnail(url=target_user.display_avatar.url)
                    log_embed.add_field(name="🔹 Genehmigt von", value=f"{member.mention}\n*{member.display_name}* (ID: `{member.id}`)", inline=True)
                    log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                    log_embed.add_field(name="🔹 Rolle", value=f"{approved_role.mention}", inline=False)
                    log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(_now_ts())}:F>\n(<t:{int(_now_ts())}:R>)", inline=True)
                    log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {member.display_name}")
                    
                    await log_channel.send(embed=log_embed)
                
                # Benachrichtige den Spieler
                try:
                    dm_embed = discord.Embed(
                        title="🎉 Herzlichen Glückwunsch!" if not self.grant_role_mode else "✅ Whitelist-Rolle erhalten",
                        description=f"Du wurdest von **{member.display_name}** {'zur Whitelist hinzugefügt' if not self.grant_role_mode else 'die Whitelist-Rolle zugewiesen'}!",
                        color=discord.Color.green(),
                        timestamp=_now()
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
            except discord.HTTPException:
                pass


class PersistentWhitelistGrantView(discord.ui.View):
    """Persistente View für Whitelist-Rollenvergabe im Panel - OHNE Ziel-User Bindung.

    WICHTIG: Diese View wird in `cog_load` mit `guild=None` registriert, damit
    sie als PERSISTENTE View für ALLE Panel-Nachrichten einer Guild funktioniert
    (auch nach Bot-Neustart). Daher MUSS der Callback `interaction.guild` nutzen
    statt `self.guild` — sonst crasht jeder Klick nach einem Neustart mit
    AttributeError: 'NoneType' object has no attribute 'id'.
    """

    def __init__(self, cog: SupportCog, guild: discord.Guild = None):
        super().__init__(timeout=None)
        self.cog = cog
        # `guild` ist nur ein Hint für nicht-persistente Instanzen; der Callback
        # sollte IMMER interaction.guild nutzen, da persistente Views keinen
        # zuverlässigen Guild-Kontext haben.
        self.guild = guild

    @discord.ui.button(label="Whitelist freischalten", style=discord.ButtonStyle.success, emoji="✅", custom_id="whitelist_grant_role_persistent", row=2)
    async def grant_whitelist(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Öffnet ein Modal zur Eingabe des Spielers der die Rolle erhalten soll"""
        # ALWAYS use interaction.guild — self.guild may be None for persistent views.
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button kann nur auf einem Server verwendet werden.", ephemeral=True)
            return

        # In guild contexts interaction.user is always a Member, but be defensive.
        user = interaction.user
        if not isinstance(user, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können diesen Button nutzen.", ephemeral=True)
            return

        # Prüfen ob User im Whitelist-Duty ist ODER die always_allowed_role hat ODER die whitelist_handler_role
        is_on_duty = await self.cog.config.member(user).whitelist_on_duty()

        # Prüfen auf always_allowed_role
        always_allowed_role_id = await self.cog.config.guild(guild).whitelist_always_allowed_role()
        always_allowed_role = guild.get_role(always_allowed_role_id) if always_allowed_role_id else None
        has_always_allowed = always_allowed_role is not None and user.get_role(always_allowed_role.id) is not None

        # Prüfen auf Whitelist-Handler-Rolle
        whitelist_handler_role_id = await self.cog.config.guild(guild).whitelist_role()
        whitelist_handler_role = guild.get_role(whitelist_handler_role_id) if whitelist_handler_role_id else None
        has_handler_role = whitelist_handler_role is not None and user.get_role(whitelist_handler_role.id) is not None

        # Nutzer mit Always-Allowed Rolle brauchen NICHT im Duty zu sein und können IMMER den Button nutzen
        if has_always_allowed:
            pass
        elif not is_on_duty and not has_handler_role:
            await interaction.response.send_message(
                "❌ Du musst im Whitelist-Duty sein oder die Whitelist-Handler-Rolle haben "
                "um Spieler zur Whitelist hinzuzufügen! Die 'Always Allowed' Rolle berechtigt ebenfalls immer.",
                ephemeral=True,
            )
            return

        # Hole die zu vergebende Rolle
        grant_role_id = await self.cog.config.guild(guild).whitelist_grant_role()
        if not grant_role_id:
            await interaction.response.send_message("❌ Keine 'Whitelist freischalten' Rolle konfiguriert! Bitte wende dich an einen Admin.", ephemeral=True)
            return

        grant_role = guild.get_role(grant_role_id)
        if not grant_role:
            await interaction.response.send_message("❌ Die konfigurierte 'Whitelist freischalten' Rolle existiert nicht mehr!", ephemeral=True)
            return

        # Öffne Modal zur Eingabe des Ziel-Users (ID, Mention oder Name)
        modal = WhitelistGrantRoleModal(self.cog, guild, grant_role)
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
        
        # Versuch 3: Als Username/Nickname suchen.
        # WICHTIG: Zuerst EXAKTEN Match versuchen, danach erst Substring-Match.
        # Sonst vergibt "max" die Rolle an "maxmustermann" statt "Max Power".
        if not target_user:
            query_lower = player_query.lower()
            # 3a: Exakter Match auf name oder display_name
            for member in self.guild.members:
                if member.name.lower() == query_lower or member.display_name.lower() == query_lower:
                    target_user = member
                    break
            # 3b: Substring-Match (nur wenn kein exakter Treffer)
            if not target_user:
                substring_matches = []
                for member in self.guild.members:
                    if (query_lower in member.name.lower() or
                        query_lower in member.display_name.lower()):
                        substring_matches.append(member)
                if len(substring_matches) == 1:
                    target_user = substring_matches[0]
                elif len(substring_matches) > 1:
                    # Mehrdeutig — Liste der Kandidaten anzeigen, damit der Handler
                    # gezielt nochmal mit ID/Mention anfragen kann.
                    candidates = ", ".join(f"{m.mention} (`{m.id}`)" for m in substring_matches[:10])
                    if len(substring_matches) > 10:
                        candidates += f"\n...und {len(substring_matches) - 10} weitere"
                    await interaction.response.send_message(
                        f"⚠️ Mehrere Spieler gefunden für '{player_query}':\n{candidates}\n"
                        f"Bitte wiederhole mit der exakten ID oder Mention.",
                        ephemeral=True,
                    )
                    return
        
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
                timestamp=_now()
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
                    timestamp=_now()
                )
                log_embed.set_thumbnail(url=target_user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{interaction.user.mention}\n*{interaction.user.display_name}* (ID: `{interaction.user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{self.grant_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(_now_ts())}:F>\n(<t:{int(_now_ts())}:R>)", inline=True)
                log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {interaction.user.display_name}")
                
                await log_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=f"Du wurdest von **{interaction.user.display_name}** zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=_now()
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
                timestamp=_now()
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
                    timestamp=_now()
                )
                log_embed.set_thumbnail(url=target_user.display_avatar.url)
                log_embed.add_field(name="🔹 Genehmigt von", value=f"{interaction.user.mention}\n*{interaction.user.display_name}* (ID: `{interaction.user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Spieler", value=f"{target_user.mention}\n*{target_user.display_name}* (ID: `{target_user.id}`)", inline=True)
                log_embed.add_field(name="🔹 Rolle", value=f"{grant_role.mention}", inline=False)
                log_embed.add_field(name="⏰ Zeitpunkt", value=f"<t:{int(_now_ts())}:F>\n(<t:{int(_now_ts())}:R>)", inline=True)
                log_embed.set_footer(text=f"Whitelist-Log • Eintrag von {interaction.user.display_name}")
                
                await log_channel.send(embed=log_embed)
            
            # Benachrichtige den Spieler
            try:
                dm_embed = discord.Embed(
                    title="🎉 Herzlichen Glückwunsch!",
                    description=f"Du wurdest von **{interaction.user.display_name}** zur Whitelist hinzugefügt!",
                    color=discord.Color.green(),
                    timestamp=_now()
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
            timestamp=_now()
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
        """Fehlerbehandlung — robust gegenüber bereits beantworteten Interactions."""
        msg = "❌ Ein Fehler ist aufgetreten. Bitte versuche es später erneut."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass
        log.exception("Fehler im Feedback-Modal", exc_info=error)


class SupportCallView(discord.ui.View):
    """Button-View für Support-Aufruf"""
    
    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
    
    @discord.ui.button(label="Support rufen", style=discord.ButtonStyle.red, emoji="📞", custom_id="support_call")
    async def call_support(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Ruft einen Supporter.

       Dieser Callback macht mehrere Config-Reads + Rollen-Iterationen
        und kann deshalb die 3-Sekunden-Antwort-Grenze von Discord
        überschreiten. Wir defer(ephemeral=True) zuerst, dann folgt die
        eigentliche Arbeit, und am Ende senden wir das Ergebnis via
        interaction.followup.send().
        """
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("❌ Dieser Button funktioniert nur auf Servern.", ephemeral=True)
            return
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Support rufen.", ephemeral=True)
            return

        # Defer FIRST so we have up to 15 min instead of 3 sec to do the work.
        await interaction.response.defer(ephemeral=True)

        # Hole Duty-Rolle und Duty-Mitglieder
        role_id = await self.cog.config.guild(guild).role()
        if not role_id:
            await interaction.followup.send("❌ Keine Support-Rolle konfiguriert!", ephemeral=True)
            return

        base_role = guild.get_role(role_id)
        if not base_role:
            await interaction.followup.send("❌ Support-Rolle nicht gefunden!", ephemeral=True)
            return

        # Read-only lookup — never create a duty role as side effect of a call button.
        duty_role = await self.cog.get_duty_role(guild)

        # Hole Duty-Mitglieder (single Config.all_members call)
        duty_members = []
        if duty_role:
            all_members = await self.cog.config.all_members(guild)
            for m in base_role.members:
                if m.get_role(duty_role.id) is None:
                    continue
                if all_members.get(m.id, {}).get("on_duty"):
                    duty_members.append(m)

        # Channel-mention safe auch wenn interaction.channel None ist
        channel_mention = interaction.channel.mention if interaction.channel else (f"<#{interaction.channel_id}>" if interaction.channel_id else "Unbekannt")

        if not duty_members:
            # Fallback zur Basis-Rolle wenn niemand Duty hat
            call_channel = await self.cog.get_support_call_channel(guild)
            if call_channel:
                embed = discord.Embed(
                    title="📞 Support-Anfrage",
                    description=f"{member.mention} benötigt Support!",
                    color=discord.Color.orange(),
                    timestamp=_now()
                )
                embed.add_field(name="👤 Anfragender", value=f"{member.display_name}\n(`{member.id}`)", inline=True)
                embed.add_field(name="📍 Ursprungs-Channel", value=channel_mention, inline=True)
                embed.set_footer(text="🔴 Niemand im Duty - Basis-Rolle wird gepingt")
                try:
                    await call_channel.send(content=base_role.mention, embed=embed,
                                            allowed_mentions=discord.AllowedMentions(roles=[base_role]))
                except discord.HTTPException:
                    log.warning("Konnte Support-Call-Benachrichtigung nicht senden (Guild %s)", guild.id)
            await interaction.followup.send(
                f"🔴 Aktuell ist kein Supporter im Dienst! Die {base_role.mention} wurde benachrichtigt.",
                ephemeral=True
            )
            return

        # ALLE Duty-Supporter pingen
        call_room = await self.cog.get_call_room(guild)
        call_room_mention = call_room.mention if call_room else "einem Voice-Channel"
        call_channel = await self.cog.get_support_call_channel(guild)

        embed = discord.Embed(
            title="📞 Support-Aufruf",
            description=f"{member.mention} ruft nach Support!",
            color=discord.Color.orange(),
            timestamp=_now()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="👤 Anfragender", value=f"{member.display_name}\n(`{member.id}`)", inline=True)
        embed.add_field(name="📍 Ursprungs-Channel", value=channel_mention, inline=True)
        embed.add_field(name="🎤 Treffpunkt", value=f"Bitte begib dich zu {call_room_mention}", inline=True)
        embed.set_footer(text=f"Support-System • {_now().strftime('%d.%m.%Y %H:%M')} UTC")

        if call_channel and duty_role:
            try:
                await call_channel.send(content=duty_role.mention, embed=embed,
                                        allowed_mentions=discord.AllowedMentions(roles=[duty_role]))
            except discord.HTTPException:
                log.warning("Konnte Support-Call nicht senden (Guild %s)", guild.id)

        supporter_names = ", ".join([m.display_name for m in duty_members[:5]])
        if len(duty_members) > 5:
            supporter_names += f" und {len(duty_members) - 5} weitere"

        await interaction.followup.send(
            f"✅ {supporter_names} wurden gerufen! Bitte warte kurz, die Supporter werden sich bei dir melden.",
            ephemeral=True
        )


class TicketPanelView(discord.ui.View):
    """Persistente View für das Ticket-Panel — ein einziger Button öffnet das Modal."""

    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog
        # Button-Text und Emoji aus Config holen wäre hier synchron nicht möglich,
        # deshalb nutzen wir die Default-Werte. Über `[p]ticketset createpanel` wird
        # die View neu erzeugt und das Panel aktualisiert.

    @discord.ui.button(
        label="Ticket erstellen",
        style=discord.ButtonStyle.primary,
        emoji="🎫",
        custom_id="ticket_create",
    )
    async def create_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Tickets erstellen.", ephemeral=True)
            return
        # Ticket-Blacklist prüfen
        blacklist = await self.cog.config.guild(interaction.guild).ticket_blacklist() or []
        if member.id in blacklist:
            await interaction.response.send_message(
                "❌ Du bist aktuell vom Ticket-System ausgeschlossen und kannst keine Tickets erstellen.",
                ephemeral=True,
            )
            return
        # Support-Blocklist auch prüfen (rückwärtskompatibel)
        blocklist = await self.cog.config.guild(interaction.guild).support_blocklist() or {}
        if str(member.id) in blocklist:
            await interaction.response.send_message(
                "❌ Du bist aktuell vom Support-System blockiert und kannst keine Tickets erstellen.",
                ephemeral=True,
            )
            return
        modal_enabled = await self.cog.config.guild(interaction.guild).ticket_modal_enabled()
        if modal_enabled:
            modal = TicketModal(self.cog)
            await interaction.response.send_modal(modal)
        else:
            # Direkt Ticket erstellen ohne Modal
            await self.cog._create_ticket(interaction, "Kein Betreff angegeben (Modal deaktiviert)", member)


class TicketModal(discord.ui.Modal):
    """Modal zur Eingabe des Ticket-Betreffs (dynamisch aus Config)."""

    def __init__(self, cog: SupportCog):
        # Title kann nicht async geladen werden → wir nutzen Default
        # Die Question/Placeholder werden dynamisch in on_submit behandelt
        super().__init__(title="🎫 Ticket erstellen", timeout=600)
        self.cog = cog
        # Default-Werte (werden bei Bedarf überschrieben)
        self.subject_input = discord.ui.TextInput(
            label="Worum geht es?",
            placeholder="Kurze Beschreibung deines Anliegens...",
            min_length=3,
            max_length=200,
            required=True,
            custom_id="ticket_subject",
        )
        self.add_item(self.subject_input)
        self.detail_input = discord.ui.TextInput(
            label="Details (optional)",
            placeholder="Ausführlichere Beschreibung...",
            required=False,
            max_length=1500,
            style=discord.TextStyle.paragraph,
            custom_id="ticket_details",
        )
        self.add_item(self.detail_input)

    async def on_submit(self, interaction: discord.Interaction):
        subject = self.subject_input.value.strip()
        details = self.detail_input.value.strip() if self.detail_input.value else ""
        full_subject = subject if not details else f"{subject}\n\n{details}"
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            return
        await self.cog._create_ticket(interaction, full_subject, member)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        msg = "❌ Ein Fehler ist beim Erstellen des Tickets aufgetreten."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass
        log.exception("Fehler im TicketModal", exc_info=error)


class TicketControlView(discord.ui.View):
    """Persistente View im Ticket-Channel mit Close/Claim/Unclaim-Buttons."""

    def __init__(self, cog: SupportCog, claim_enabled: bool = True):
        super().__init__(timeout=None)
        self.cog = cog
        self.claim_enabled = claim_enabled

    @discord.ui.button(
        label="Ticket schließen",
        style=discord.ButtonStyle.danger,
        emoji="🔒",
        custom_id="ticket_close",
    )
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.channel is None:
            await interaction.response.send_message("❌ Dieser Button kann nur in einem Channel verwendet werden.", ephemeral=True)
            return
        # Close-Reason Modal öffnen
        modal = TicketCloseReasonModal(self.cog, interaction.channel)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Übernehmen",
        style=discord.ButtonStyle.success,
        emoji="✋",
        custom_id="ticket_claim",
    )
    async def claim_ticket_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.claim_enabled:
            await interaction.response.send_message("❌ Claim-System ist deaktiviert.", ephemeral=True)
            return
        if interaction.channel is None or interaction.guild is None:
            await interaction.response.send_message("❌ Button kann nur in einem Channel verwendet werden.", ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            return
        # Berechtigungsprüfung (inkl. kategorie-spezifischer Rolle)
        is_staff = await self.cog._is_ticket_staff(interaction.user, interaction.channel, interaction.guild)
        if not is_staff:
            await interaction.response.send_message("❌ Nur Teammitglieder können Tickets übernehmen.", ephemeral=True)
            return
        # Channel-Topic aktualisieren
        channel = interaction.channel
        topic = channel.topic or ""
        topic_clean = re.sub(r" • Claimed by: \d+", "", topic)
        new_topic = f"{topic_clean} • Claimed by: {interaction.user.id}"
        try:
            await channel.edit(topic=new_topic, reason=f"Ticket claimed by {interaction.user.display_name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            await interaction.response.send_message(f"❌ Konnte Topic nicht aktualisieren: `{e}`", ephemeral=True)
            return
        embed = discord.Embed(
            title="✅ Ticket übernommen",
            description=f"{interaction.user.mention} hat dieses Ticket übernommen und ist nun zuständig.",
            color=discord.Color.green(),
            timestamp=_now(),
        )
        await interaction.response.send_message(embed=embed)

    @discord.ui.button(
        label="Freigeben",
        style=discord.ButtonStyle.secondary,
        emoji="🔓",
        custom_id="ticket_unclaim",
    )
    async def unclaim_ticket_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.claim_enabled:
            await interaction.response.send_message("❌ Claim-System ist deaktiviert.", ephemeral=True)
            return
        if interaction.channel is None or interaction.guild is None:
            await interaction.response.send_message("❌ Button kann nur in einem Channel verwendet werden.", ephemeral=True)
            return
        if not isinstance(interaction.user, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            return
        channel = interaction.channel
        topic = channel.topic or ""
        m = re.search(r"Claimed by: (\d+)", topic)
        if not m:
            await interaction.response.send_message("ℹ️ Dieses Ticket ist aktuell nicht geclaimt.", ephemeral=True)
            return
        claimed_by_id = int(m.group(1))
        is_claimer = interaction.user.id == claimed_by_id
        is_staff = await self.cog._is_ticket_staff(interaction.user, channel, interaction.guild)
        if not (is_claimer or is_staff):
            await interaction.response.send_message("❌ Nur der aktuelle Claimer oder Teammitglieder können freigeben.", ephemeral=True)
            return
        # Claimed-Eintrag aus Topic entfernen
        topic_clean = re.sub(r" • Claimed by: \d+", "", topic)
        try:
            await channel.edit(topic=topic_clean, reason=f"Ticket unclaimed by {interaction.user.display_name}")
        except (discord.Forbidden, discord.HTTPException) as e:
            await interaction.response.send_message(f"❌ Konnte Topic nicht aktualisieren: `{e}`", ephemeral=True)
            return
        embed = discord.Embed(
            title="🔓 Ticket freigegeben",
            description=f"{interaction.user.mention} hat dieses Ticket wieder freigegeben.\nEs kann nun von jedem Teammitglied übernommen werden.",
            color=discord.Color.orange(),
            timestamp=_now(),
        )
        await interaction.response.send_message(embed=embed)


class TicketMultiPanelView(discord.ui.View):
    """Persistente View für das Multi-Kategorie-Panel — ein Button pro Kategorie."""

    def __init__(self, cog: SupportCog, categories: list):
        """categories: list of (cat_key, cat_config) tuples."""
        super().__init__(timeout=None)
        self.cog = cog
        self.categories = categories
        # Buttons dynamisch hinzufügen
        for cat_key, cat_config in categories:
            button = TicketCategoryButton(self.cog, cat_key, cat_config)
            self.add_item(button)


class TicketCategoryButton(discord.ui.Button):
    """Ein Button für eine Ticket-Kategorie im Multi-Panel."""

    def __init__(self, cog: SupportCog, cat_key: str, cat_config: dict):
        color_map = {
            "blurple": discord.ButtonStyle.primary,
            "red": discord.ButtonStyle.danger,
            "green": discord.ButtonStyle.success,
            "grey": discord.ButtonStyle.secondary,
            "orange": discord.ButtonStyle.secondary,
        }
        style = color_map.get(cat_config.get("color", "blurple"), discord.ButtonStyle.primary)
        emoji = cat_config.get("emoji", "🎫")
        label = cat_config.get("button_text", cat_config.get("name", cat_key))[:80]
        super().__init__(
            label=label,
            style=style,
            emoji=emoji,
            custom_id=f"ticket_cat_{cat_key}",
        )
        self.cog = cog
        self.cat_key = cat_key
        self.cat_config = cat_config

    async def callback(self, interaction: discord.Interaction):
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder können Tickets erstellen.", ephemeral=True)
            return
        # Blacklist prüfen
        blacklist = await self.cog.config.guild(interaction.guild).ticket_blacklist() or []
        if member.id in blacklist:
            await interaction.response.send_message(
                "❌ Du bist aktuell vom Ticket-System ausgeschlossen.",
                ephemeral=True,
            )
            return
        # Modal öffnen
        modal = TicketCategoryModal(self.cog, self.cat_key, self.cat_config)
        await interaction.response.send_modal(modal)


class TicketCategoryModal(discord.ui.Modal):
    """Modal zur Eingabe des Ticket-Betreffs für eine bestimmte Kategorie."""

    def __init__(self, cog: SupportCog, cat_key: str, cat_config: dict):
        title = f"{cat_config.get('emoji', '🎫')} {cat_config.get('name', 'Ticket')} erstellen"
        if len(title) > 45:
            title = title[:45]
        super().__init__(title=title, timeout=600)
        self.cog = cog
        self.cat_key = cat_key
        self.cat_config = cat_config
        question = cat_config.get("modal_question", "Was ist dein Anliegen?")[:45]
        placeholder = cat_config.get("modal_placeholder", "Beschreibe dein Anliegen...")[:100]
        self.subject_input = discord.ui.TextInput(
            label=question,
            placeholder=placeholder,
            min_length=3,
            max_length=200,
            required=True,
            custom_id="ticket_subject",
        )
        self.add_item(self.subject_input)
        self.detail_input = discord.ui.TextInput(
            label="Details (optional)",
            placeholder="Ausführlichere Beschreibung...",
            required=False,
            max_length=1500,
            style=discord.TextStyle.paragraph,
            custom_id="ticket_details",
        )
        self.add_item(self.detail_input)

    async def on_submit(self, interaction: discord.Interaction):
        subject = self.subject_input.value.strip()
        details = self.detail_input.value.strip() if self.detail_input.value else ""
        full_subject = subject if not details else f"{subject}\n\n{details}"
        member = interaction.user
        if not isinstance(member, discord.Member):
            await interaction.response.send_message("❌ Nur Server-Mitglieder.", ephemeral=True)
            return
        await self.cog._ticket_create_for_category(interaction, self.cat_key, full_subject, member)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        msg = "❌ Ein Fehler ist beim Erstellen des Tickets aufgetreten."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass
        log.exception("Fehler im TicketCategoryModal", exc_info=error)


class TicketCloseReasonModal(discord.ui.Modal):
    """Modal zur Eingabe eines Schließ-Grundes."""

    def __init__(self, cog: SupportCog, channel: discord.TextChannel):
        super().__init__(title="🎫 Ticket schließen", timeout=300)
        self.cog = cog
        self.channel = channel
        self.reason_input = discord.ui.TextInput(
            label="Grund für Schließung",
            placeholder="Warum wird dieses Ticket geschlossen?",
            required=False,
            max_length=500,
            style=discord.TextStyle.paragraph,
            custom_id="close_reason",
        )
        self.add_item(self.reason_input)

    async def on_submit(self, interaction: discord.Interaction):
        reason = self.reason_input.value.strip() if self.reason_input.value else "Kein Grund angegeben"
        await self.cog._close_ticket(interaction, self.channel, reason=reason)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"❌ Fehler: {error}", ephemeral=True)
            else:
                await interaction.response.send_message(f"❌ Fehler: {error}", ephemeral=True)
        except discord.HTTPException:
            pass
        log.exception("Fehler im TicketCloseReasonModal", exc_info=error)


class TicketCloseView(discord.ui.View):
    """Legacy persistente View mit 'Ticket schließen'-Button (für Abwärtskompatibilität)."""

    def __init__(self, cog: SupportCog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Ticket schließen",
        style=discord.ButtonStyle.danger,
        emoji="🔒",
        custom_id="ticket_close_legacy",
    )
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.channel is None:
            await interaction.response.send_message("❌ Dieser Button kann nur in einem Channel verwendet werden.", ephemeral=True)
            return
        # Close-Reason Modal öffnen
        modal = TicketCloseReasonModal(self.cog, interaction.channel)
        await interaction.response.send_modal(modal)


class TicketSetupWizardView(discord.ui.View):
    """Interaktiver Setup-Wizard für das Ticket-System."""

    def __init__(self, cog, guild: discord.Guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🎫 Ticket-System Setup Wizard",
            description=(
                "Willkommen beim Einrichtungsassistenten!\n\n"
                "**Schnellstart:**\n"
                "🚀 `QuickStart` — Erstellt alles automatisch (Rolle, Kategorie, Channel, Panel)\n\n"
                "**Manuell konfigurieren:**\n"
                "⚙️ `Toggle` — Aktiviert/deaktiviert das System\n"
                "📋 `Config` — Zeigt die aktuelle Konfiguration\n"
                "🎨 `CreatePanel` — Erstellt das Panel in dem Channel in dem du diesen Befehl ausgeführt hast\n"
                "❌ `Abbrechen` — Bricht den Wizard ab"
            ),
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.set_footer(text="Ticket Setup Wizard • Klicke eine Option")
        return embed

    @discord.ui.button(label="QuickStart", style=discord.ButtonStyle.success, emoji="🚀", custom_id="ticket_wiz_quickstart")
    async def quickstart(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ Du brauchst `Manage Server` Rechte.", ephemeral=True)
            return
        # QuickStart Logik (ähnlich wie ticketset_quickstart, aber via Button)
        guild = self.guild
        g = self.cog.config.guild(guild)
        try:
            # 1. Support-Rolle erstellen falls nicht vorhanden
            support_role_id = await g.ticket_support_role()
            support_role = guild.get_role(support_role_id) if support_role_id else None
            if not support_role:
                try:
                    support_role = await guild.create_role(
                        name="🎫 Ticket Support",
                        reason="Auto-created by ticket setup wizard",
                    )
                    await g.ticket_support_role.set(support_role.id)
                except discord.Forbidden:
                    await interaction.response.send_message("❌ Brauche `Manage Roles` um Support-Rolle zu erstellen.", ephemeral=True)
                    return

            # 2. Kategorie erstellen
            cat_id = await g.ticket_category()
            category = guild.get_channel(cat_id) if cat_id else None
            if not category or not isinstance(category, discord.CategoryChannel):
                try:
                    category = await guild.create_category(
                        name="🎫 Tickets",
                        reason="Auto-created by ticket setup wizard",
                        overwrites={
                            guild.default_role: discord.PermissionOverwrite(view_channel=False),
                            guild.me: discord.PermissionOverwrite(
                                view_channel=True, send_messages=True,
                                read_message_history=True, manage_channels=True,
                            ),
                            support_role: discord.PermissionOverwrite(
                                view_channel=True, send_messages=True,
                                read_message_history=True, attach_files=True,
                            ),
                        },
                    )
                    await g.ticket_category.set(category.id)
                except discord.Forbidden:
                    await interaction.response.send_message("❌ Brauche `Manage Channels` um Kategorie zu erstellen.", ephemeral=True)
                    return

            # 3. Panel-Channel = aktueller Channel
            if isinstance(interaction.channel, discord.TextChannel):
                panel_ch = interaction.channel
                await g.ticket_panel_channel.set(panel_ch.id)
            else:
                try:
                    panel_ch = await guild.create_text_channel(name="ticket-panel", reason="Auto-created")
                    await g.ticket_panel_channel.set(panel_ch.id)
                except (discord.Forbidden, discord.HTTPException):
                    await interaction.response.send_message("❌ Konnte Panel-Channel nicht erstellen.", ephemeral=True)
                    return

            # 4. Log-Channel erstellen
            log_ch_id = await g.ticket_log_channel()
            log_ch = guild.get_channel(log_ch_id) if log_ch_id else None
            log_ch_label = "❌"
            if not log_ch:
                try:
                    log_ch = await guild.create_text_channel(
                        name="ticket-logs",
                        category=category,
                        reason="Auto-created",
                        overwrites={
                            guild.default_role: discord.PermissionOverwrite(view_channel=False),
                            support_role: discord.PermissionOverwrite(view_channel=True, send_messages=False, read_message_history=True),
                            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
                        },
                    )
                    await g.ticket_log_channel.set(log_ch.id)
                    log_ch_label = log_ch.mention
                except (discord.Forbidden, discord.HTTPException):
                    log_ch_label = "❌ (fehlgeschlagen)"
            else:
                log_ch_label = log_ch.mention

            # 5. Defaults
            await g.ticket_modal_enabled.set(True)
            await g.ticket_dm_on_close.set(False)
            await g.ticket_transcript.set(True)
            await g.ticket_user_can_close.set(True)
            await g.ticket_claim_enabled.set(True)
            await g.ticket_max_open.set(1)

            # 6. Panel erstellen
            embed = discord.Embed(
                title="🎫 Ticket erstellen",
                description=(
                    "Brauchst du Hilfe oder möchtest etwas anfragen?\n\n"
                    "Klicke auf den Button unten und beschreibe dein Anliegen — "
                    "ein privater Ticket-Channel wird für dich erstellt."
                ),
                color=discord.Color.blurple(),
                timestamp=_now(),
            )
            embed.set_footer(text="Ticket-System • Klicke auf den Button")
            view = TicketPanelView(self.cog)
            try:
                message = await panel_ch.send(embed=embed, view=view)
            except (discord.Forbidden, discord.HTTPException) as e:
                await interaction.response.send_message(f"❌ Konnte Panel nicht senden: `{e}`", ephemeral=True)
                return
            old_id = await g.ticket_panel_message_id()
            if old_id:
                try:
                    old_msg = await panel_ch.fetch_message(old_id)
                    await old_msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
            await g.ticket_panel_message_id.set(message.id)

            # 7. Zusammenfassung
            result_embed = discord.Embed(
                title="✅ Ticket-System eingerichtet!",
                description=(
                    f"**Support-Rolle:** {support_role.mention}\n"
                    f"**Kategorie:** {category.name}\n"
                    f"**Panel-Channel:** {panel_ch.mention}\n"
                    f"**Log-Channel:** {log_ch_label}\n"
                    f"**Panel-Nachricht:** [Klick]({message.jump_url})\n\n"
                    f"**Aktivierte Features:**\n"
                    f"• ✅ Modal (User muss Anliegen beschreiben)\n"
                    f"• ✅ Transcript bei Schließen\n"
                    f"• ✅ User darf eigenes Ticket schließen\n"
                    f"• ✅ Claim-System\n"
                    f"• ✅ Max. 1 offenes Ticket pro User\n\n"
                    f"💡 Mit `[p]ticketset show` kannst du alles weitere anpassen."
                ),
                color=discord.Color.green(),
                timestamp=_now(),
            )
            await interaction.response.edit_message(embed=result_embed, view=None)
        except Exception as e:
            try:
                await interaction.response.send_message(f"❌ Fehler beim Setup: `{type(e).__name__}: {e}`", ephemeral=True)
            except discord.HTTPException:
                pass

    @discord.ui.button(label="Config anzeigen", style=discord.ButtonStyle.secondary, emoji="📋", custom_id="ticket_wiz_config")
    async def show_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        g = self.cog.config.guild(self.guild)
        embed = discord.Embed(title="🎫 Ticket-System Konfiguration", color=discord.Color.blurple(), timestamp=_now())
        cat_id = await g.ticket_category()
        cat = self.guild.get_channel(cat_id) if cat_id else None
        panel_ch_id = await g.ticket_panel_channel()
        panel_ch = self.guild.get_channel(panel_ch_id) if panel_ch_id else None
        role_id = await g.ticket_support_role()
        role = self.guild.get_role(role_id) if role_id else None
        log_ch_id = await g.ticket_log_channel()
        log_ch = self.guild.get_channel(log_ch_id) if log_ch_id else None
        embed.add_field(name="Kategorie", value=cat.name if cat else "❌", inline=True)
        embed.add_field(name="Panel-Channel", value=panel_ch.mention if panel_ch else "❌", inline=True)
        embed.add_field(name="Support-Rolle", value=role.mention if role else "❌", inline=True)
        embed.add_field(name="Log-Channel", value=log_ch.mention if log_ch else "❌", inline=True)
        embed.add_field(name="Modal", value="✅" if await g.ticket_modal_enabled() else "❌", inline=True)
        embed.add_field(name="DM bei Close", value="✅" if await g.ticket_dm_on_close() else "❌", inline=True)
        embed.add_field(name="Transcript", value="✅" if await g.ticket_transcript() else "❌", inline=True)
        embed.add_field(name="Claim-System", value="✅" if await g.ticket_claim_enabled() else "❌", inline=True)
        embed.add_field(name="User-Close", value="✅" if await g.ticket_user_can_close() else "❌", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="CreatePanel", style=discord.ButtonStyle.primary, emoji="🎨", custom_id="ticket_wiz_createpanel")
    async def create_panel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ Du brauchst `Manage Server` Rechte.", ephemeral=True)
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message("❌ Muss in einem Text-Channel ausgeführt werden.", ephemeral=True)
            return
        g = self.cog.config.guild(self.guild)
        await g.ticket_panel_channel.set(interaction.channel.id)
        # Prüfen dass alles konfiguriert ist
        category = await self.cog.get_ticket_category(self.guild)
        support_role = await self.cog.get_ticket_support_role(self.guild)
        if not category:
            await interaction.response.send_message("❌ Keine Kategorie gesetzt. Nutze `[p]ticketset category` zuerst.", ephemeral=True)
            return
        if not support_role:
            await interaction.response.send_message("❌ Keine Support-Rolle gesetzt. Nutze `[p]ticketset supportrole` zuerst.", ephemeral=True)
            return
        embed = discord.Embed(
            title="🎫 Ticket erstellen",
            description=(
                "Brauchst du Hilfe oder möchtest etwas anfragen?\n\n"
                "Klicke auf den Button unten und beschreibe dein Anliegen — "
                "ein privater Ticket-Channel wird für dich erstellt."
            ),
            color=discord.Color.blurple(),
            timestamp=_now(),
        )
        embed.set_footer(text="Ticket-System • Klicke auf den Button")
        view = TicketPanelView(self.cog)
        try:
            message = await interaction.channel.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException) as e:
            await interaction.response.send_message(f"❌ Konnte Panel nicht posten: `{e}`", ephemeral=True)
            return
        old_id = await g.ticket_panel_message_id()
        if old_id:
            try:
                old_msg = await interaction.channel.fetch_message(old_id)
                await old_msg.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        await g.ticket_panel_message_id.set(message.id)
        await interaction.response.send_message(f"✅ Panel erstellt: {message.jump_url}", ephemeral=True)

    @discord.ui.button(label="Abbrechen", style=discord.ButtonStyle.danger, emoji="✖️", custom_id="ticket_wiz_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="❌ Setup abgebrochen",
                description="Ticket-System wurde nicht verändert.",
                color=discord.Color.red(),
            ),
            view=None,
        )


class SyncSetupWizardView(discord.ui.View):
    """Interaktiver Setup-Wizard für BanSync mit Buttons."""

    def __init__(self, cog, guild: discord.Guild):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild = guild
        self.message: Optional[discord.Message] = None

    def build_embed(self) -> discord.Embed:
        """Baut das Embed mit der aktuellen Konfiguration."""
        # Config live auslesen ist async, deshalb verwenden wir Caches die beim Klick aktualisiert werden.
        # Da wir hier synchron sind, bauen wir nur ein generisches Embed.
        embed = discord.Embed(
            title="🔄 BanSync Setup Wizard",
            description=(
                "Willkommen beim Einrichtungsassistenten!\n\n"
                "**So funktioniert es:**\n"
                "1️⃣ Auf dem **Hauptserver**: Klicke `🚀 QuickStart Master`\n"
                "2️⃣ Auf jedem **anderen Server**: Klicke `📦 QuickStart Empfänger` und gib die Master-ID an\n\n"
                "**Oder manuell konfigurieren:**\n"
                "• `⚙️ Toggle` — Sync an/aus\n"
                "• `🎯 Master setzen` — diese Guild als Master\n"
                "• `📋 Konfiguration anzeigen` — aktuellen Status sehen"
            ),
            color=discord.Color.blue(),
            timestamp=_now(),
        )
        embed.set_footer(text="BanSync Setup Wizard • Klicke eine Option")
        return embed

    @discord.ui.button(label="QuickStart Master", style=discord.ButtonStyle.success, emoji="🚀", custom_id="sync_quickstart_master")
    async def quickstart_master(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Ein-Klick-Setup für den Master-Server."""
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        # Berechtigungsprüfung
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ Du brauchst `Manage Server` Rechte.", ephemeral=True)
            return
        g = self.cog.config.guild(self.guild)
        await g.sync_enabled.set(True)
        await g.sync_master_guild_id.set(self.guild.id)
        await g.sync_direction.set("master_to_all")
        await g.sync_bans.set(True)
        await g.sync_unbans.set(True)
        await g.sync_timeouts.set(True)
        await g.sync_kicks.set(True)
        await g.sync_warns.set(True)
        await g.sync_audit_log.set(True)
        if isinstance(interaction.channel, discord.TextChannel):
            await g.sync_log_channel.set(interaction.channel.id)
            log_label = interaction.channel.mention
        else:
            log_label = "❌"
        embed = discord.Embed(
            title="✅ Master-Server eingerichtet!",
            description=(
                f"**{self.guild.name}** ist jetzt der Master-Server.\n\n"
                f"**Automatisch synchronisiert (an alle Empfänger):**\n"
                f"• 🚫 Banns  • ✅ Unbanns  • ⏱️ Timeouts  • 👢 Kicks  • ⚠️ Warns\n"
                f"**Audit-Log-Auswertung:** ✅\n"
                f"**Log-Channel:** {log_label}\n\n"
                f"**Master-ID für andere Server:** `{self.guild.id}`\n\n"
                f"👉 Gehe jetzt auf jeden anderen Server und führe dort aus:\n"
                f"```\n[p]syncset slave {self.guild.id}\n```\n\n"
                f"💡 Optional: `[p]syncset syncnow` überträgt alle bestehenden Banns einmalig."
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="QuickStart Empfänger", style=discord.ButtonStyle.primary, emoji="📦", custom_id="sync_quickstart_slave")
    async def quickstart_slave(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Slave-Setup — öffnet Modal für Master-ID Eingabe."""
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ Du brauchst `Manage Server` Rechte.", ephemeral=True)
            return
        modal = SyncSlaveModal(self.cog, self.guild)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Toggle Sync", style=discord.ButtonStyle.secondary, emoji="⚙️", custom_id="sync_toggle")
    async def toggle_sync(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Schaltet Sync an/aus."""
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message("❌ Du brauchst `Manage Server` Rechte.", ephemeral=True)
            return
        current = await self.cog.config.guild(self.guild).sync_enabled()
        await self.cog.config.guild(self.guild).sync_enabled.set(not current)
        await interaction.response.send_message(
            f"🔄 BanSync ist jetzt **{'✅ AN' if not current else '❌ AUS'}** für diese Guild.",
            ephemeral=True,
        )

    @discord.ui.button(label="Konfiguration anzeigen", style=discord.ButtonStyle.secondary, emoji="📋", custom_id="sync_show")
    async def show_config(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Zeigt die aktuelle Konfiguration."""
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        g = self.cog.config.guild(self.guild)
        embed = discord.Embed(title="🔄 BanSync Konfiguration", color=discord.Color.blue(), timestamp=_now())
        embed.add_field(name="Aktiviert", value="✅ Ja" if await g.sync_enabled() else "❌ Nein", inline=True)
        embed.add_field(name="Richtung", value=await g.sync_direction(), inline=True)
        master_id = await g.sync_master_guild_id()
        embed.add_field(name="Master-Guild", value=f"`{master_id}`" if master_id else "Aktuelle (implizit)", inline=True)
        embed.add_field(name="Banns", value="✅" if await g.sync_bans() else "❌", inline=True)
        embed.add_field(name="Unbanns", value="✅" if await g.sync_unbans() else "❌", inline=True)
        embed.add_field(name="Timeouts", value="✅" if await g.sync_timeouts() else "❌", inline=True)
        embed.add_field(name="Kicks", value="✅" if await g.sync_kicks() else "❌", inline=True)
        embed.add_field(name="Warns", value="✅" if await g.sync_warns() else "❌", inline=True)
        embed.add_field(name="Audit-Log", value="✅" if await g.sync_audit_log() else "❌", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Abbrechen", style=discord.ButtonStyle.danger, emoji="✖️", custom_id="sync_cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Bricht den Wizard ab."""
        if interaction.guild is None or interaction.guild.id != self.guild.id:
            await interaction.response.send_message("❌ Dieser Button ist für eine andere Guild.", ephemeral=True)
            return
        await interaction.response.edit_message(
            embed=discord.Embed(
                title="❌ Setup abgebrochen",
                description="BanSync wurde nicht verändert.",
                color=discord.Color.red(),
            ),
            view=None,
        )


class SyncSlaveModal(discord.ui.Modal):
    """Modal zur Eingabe der Master-Guild-ID für Slave-Setup."""

    def __init__(self, cog, guild: discord.Guild):
        super().__init__(title="📦 Empfänger-Guild einrichten", timeout=300)
        self.cog = cog
        self.guild = guild
        self.master_id_input = discord.ui.TextInput(
            label="Master-Guild-ID",
            placeholder=f"z.B. 123456789012345678 (die ID des Haupt-Servers)",
            min_length=17,
            max_length=20,
            required=True,
            custom_id="master_guild_id",
        )
        self.add_item(self.master_id_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            master_id = int(self.master_id_input.value.strip())
        except ValueError:
            await interaction.response.send_message("❌ Ungültige Guild-ID. Muss eine Zahl sein.", ephemeral=True)
            return
        master_guild = self.cog.bot.get_guild(master_id)
        if master_guild is None:
            await interaction.response.send_message(f"❌ Bot ist nicht auf Guild `{master_id}`.", ephemeral=True)
            return
        g = self.cog.config.guild(self.guild)
        await g.sync_enabled.set(True)
        await g.sync_master_guild_id.set(master_id)
        await g.sync_direction.set("master_to_all")
        await g.sync_bans.set(True)
        await g.sync_unbans.set(True)
        await g.sync_timeouts.set(True)
        await g.sync_kicks.set(True)
        await g.sync_warns.set(True)
        await g.sync_audit_log.set(False)
        if isinstance(interaction.channel, discord.TextChannel):
            await g.sync_log_channel.set(interaction.channel.id)
            log_label = interaction.channel.mention
        else:
            log_label = "❌"
        embed = discord.Embed(
            title="✅ Empfänger-Guild eingerichtet!",
            description=(
                f"**{self.guild.name}** ist jetzt Empfänger für Mod-Aktionen von:\n"
                f"**{master_guild.name}** (`{master_id}`)\n\n"
                f"**Wird automatisch empfangen:** 🚫 Banns • ✅ Unbanns • ⏱️ Timeouts • 👢 Kicks • ⚠️ Warns\n"
                f"**Log-Channel:** {log_label}"
            ),
            color=discord.Color.green(),
            timestamp=_now(),
        )
        await interaction.response.edit_message(embed=embed, view=None)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(f"❌ Fehler: {error}", ephemeral=True)
            else:
                await interaction.response.send_message(f"❌ Fehler: {error}", ephemeral=True)
        except discord.HTTPException:
            pass
        log.exception("Fehler im SyncSlaveModal", exc_info=error)


async def setup(bot: Red):
    """Lädt den Cog"""
    cog = SupportCog(bot)
    # Die persistent Views werden jetzt in cog_load() registriert
    await bot.add_cog(cog)


async def teardown(bot: Red):
    """Entfernt den Cog"""
    await bot.remove_cog("SupportCog")
