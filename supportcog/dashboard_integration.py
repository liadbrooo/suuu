"""AAA3A Dashboard-Integration für den SupportCog.
Diese Datei macht alle wichtigen Funktionen des supportcog über das
Web-Dashboard von AAA3A (https://github.com/AAA3A-AAA3A/AAA3A-cogs) verfügbar.

Funktionen die im Dashboard verfügbar sind:
- Übersicht / Stats
- Support-Konfiguration
- Mod-Aktionen (Ban/Kick/Timeout/Warn)
- Tickets & Kategorien
- Bewerbungen
- Aufgaben (Tasks)
- Snippets
- Watchlist
- Team-Stats / Leaderboard
- Anti-Link Konfiguration
- BanSync Konfiguration
- Modlog Konfiguration
- Warn-System Konfiguration
"""
import typing
import json
from redbot.core import commands
from redbot.core.bot import Red

# Lokaler dashboard_page Decorator-Stub.
# WICHTIG: Nicht "from dashboard.rpc.third_parties import dashboard_page" verwenden,
# denn das Dashboard-Cog ist möglicherweise nicht installiert!
# Das Dashboard-Cog ersetzt diesen Stub beim Laden durch den echten Decorator.
def dashboard_page(*args, **kwargs):
    def decorator(func):
        func.__dashboard_decorator_params__ = (args, kwargs)
        return func
    return decorator


class DashboardIntegration:
    """Mixin für die AAA3A Dashboard-Integration.
    Der SupportCog erbt von dieser Klasse, damit alle mit @dashboard_page
    markierten Methoden automatisch im Web-Dashboard verfügbar sind."""

    bot: Red

    @commands.Cog.listener()
    async def on_dashboard_cog_add(self, dashboard_cog: commands.Cog) -> None:
        """Wird aufgerufen wenn das Dashboard-Cog geladen wird.
        Registriert diesen Cog als Third-Party-Integration."""
        try:
            # Warten bis der Cog vollständig initialisiert ist
            if hasattr(self, "settings") and hasattr(self.settings, "commands_added"):
                await self.settings.commands_added.wait()
        except Exception:
            pass
        try:
            dashboard_cog.rpc.third_parties_handler.add_third_party(self)
        except Exception as e:
            import logging
            logging.getLogger("red.supportcog.dashboard").exception(
                "Konnte SupportCog nicht beim Dashboard registrieren: %s", e
            )

    # ============================================
    # HELPER
    # ============================================

    def _embed_to_html(self, embed) -> str:
        """Konvertiert ein Discord.Embed in einfaches HTML für das Dashboard."""
        try:
            color = "#5865F2"
            if embed.color:
                color = "#{:06x}".format(embed.color.value)
            html = f'<div style="border-left:4px solid {color};padding:12px;margin:8px 0;background:#2b2d31;border-radius:4px;">'
            if embed.title:
                html += f'<h3 style="margin:0 0 8px 0;color:#fff;">{embed.title}</h3>'
            if embed.description:
                html += f'<div style="color:#dbdee1;margin-bottom:8px;">{embed.description}</div>'
            for field in getattr(embed, "_fields", []):
                html += f'<div style="margin-top:8px;"><strong style="color:#fff;">{field.get("name","")}</strong><br><span style="color:#dbdee1;">{field.get("value","")}</span></div>'
            html += '</div>'
            return html
        except Exception:
            return "<div>Embed-Anzeige fehlgeschlagen</div>"

    def _config_to_form_fields(self, config_dict: dict, prefix: str = "") -> list:
        """Erstellt Formular-Felder aus einem Config-Dictionary."""
        fields = []
        for key, value in config_dict.items():
            field_name = f"{prefix}{key}" if prefix else key
            field_type = "text"
            if isinstance(value, bool):
                field_type = "checkbox"
            elif isinstance(value, int):
                field_type = "number"
            elif isinstance(value, list):
                field_type = "text"
                value = ", ".join(str(v) for v in value)
            fields.append({
                "name": field_name,
                "label": key.replace("_", " ").title(),
                "type": field_type,
                "value": value if not isinstance(value, list) else ", ".join(str(v) for v in value),
            })
        return fields

    def _success(self, message: str, **extra) -> dict:
        return {
            "status": 0,
            "notifications": [{"message": message, "category": "success"}],
            **extra,
        }

    def _error(self, message: str, **extra) -> dict:
        return {
            "status": 1,
            "error_message": message,
            "notifications": [{"message": message, "category": "danger"}],
            **extra,
        }

    def _page(self, title: str, content: str, **extra) -> dict:
        html = f"""
        <div class="container-fluid">
            <h1 class="mb-4">{title}</h1>
            {content}
        </div>
        """
        return {
            "status": 0,
            "web_content": {"source": html, "standalone": False, "fullscreen": False},
            **extra,
        }

    # ============================================
    # 1. ÜBERSICHT / STATS
    # ============================================

    @dashboard_page(name=None, description="SupportCog Übersicht", methods=("GET",))
    async def rpc_overview(self, **kwargs) -> dict:
        """Hauptseite: Übersicht über alle SupportCog-Statistiken."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        try:
            config = self.config.guild(guild)
            # Statistiken sammeln
            tickets = await config.tickets() or {}
            apps = await config.team_applications() or {}
            tasks = await config.team_tasks() or {}
            warns = await config.warn_strikes() or {}
            snippets = await config.snippets() or {}
            watchlist = await config.watchlist() or {}
            activity = await config.team_activity() or {}
            # Zählen
            open_tickets = sum(1 for t in tickets.values() if t.get("status") == "open")
            pending_apps = sum(1 for a in apps.values() if a.get("status") == "pending")
            open_tasks = sum(1 for t in tasks.values() if t.get("status") in ("open", "in_progress"))
            total_warns = sum(len(w) for w in warns.values())
            content = f"""
            <div class="row">
                <div class="col-md-3 mb-3">
                    <div class="card text-white bg-primary">
                        <div class="card-body">
                            <h5 class="card-title">🎫 Tickets</h5>
                            <p class="card-text display-4">{len(tickets)}</p>
                            <small>Offen: {open_tickets}</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 mb-3">
                    <div class="card text-white bg-success">
                        <div class="card-body">
                            <h5 class="card-title">📋 Bewerbungen</h5>
                            <p class="card-text display-4">{len(apps)}</p>
                            <small>Wartend: {pending_apps}</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 mb-3">
                    <div class="card text-white bg-warning">
                        <div class="card-body">
                            <h5 class="card-title">📝 Aufgaben</h5>
                            <p class="card-text display-4">{len(tasks)}</p>
                            <small>Offen: {open_tasks}</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 mb-3">
                    <div class="card text-white bg-danger">
                        <div class="card-body">
                            <h5 class="card-title">⚠️ Verwarnungen</h5>
                            <p class="card-text display-4">{total_warns}</p>
                            <small>{len(warns)} User betroffen</small>
                        </div>
                    </div>
                </div>
            </div>
            <div class="row">
                <div class="col-md-4 mb-3">
                    <div class="card text-white bg-info">
                        <div class="card-body">
                            <h5 class="card-title">💬 Snippets</h5>
                            <p class="card-text display-4">{len(snippets)}</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-4 mb-3">
                    <div class="card text-white bg-secondary">
                        <div class="card-body">
                            <h5 class="card-title">👁️ Watchlist</h5>
                            <p class="card-text display-4">{len(watchlist)}</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-4 mb-3">
                    <div class="card text-white bg-dark">
                        <div class="card-body">
                            <h5 class="card-title">👥 Team-Aktivität</h5>
                            <p class="card-text display-4">{len(activity)}</p>
                            <small>Mitglieder aktiv</small>
                        </div>
                    </div>
                </div>
            </div>
            <div class="mt-4">
                <h3>Verfügbare Seiten</h3>
                <div class="list-group">
                    <a href="/third_parties/supportcog/modactions" class="list-group-item list-group-item-action">🔨 Mod-Aktionen (Ban/Kick/Timeout/Warn)</a>
                    <a href="/third_parties/supportcog/tickets" class="list-group-item list-group-item-action">🎫 Tickets & Kategorien</a>
                    <a href="/third_parties/supportcog/applications" class="list-group-item list-group-item-action">📋 Bewerbungen</a>
                    <a href="/third_parties/supportcog/tasks" class="list-group-item list-group-item-action">📝 Aufgaben</a>
                    <a href="/third_parties/supportcog/snippets" class="list-group-item list-group-item-action">💬 Snippets</a>
                    <a href="/third_parties/supportcog/watchlist" class="list-group-item list-group-item-action">👁️ Watchlist</a>
                    <a href="/third_parties/supportcog/teamstats" class="list-group-item list-group-item-action">📊 Team-Stats</a>
                    <a href="/third_parties/supportcog/warns" class="list-group-item list-group-item-action">⚠️ Warn-System</a>
                    <a href="/third_parties/supportcog/antilink" class="list-group-item list-group-item-action">🔗 Anti-Link</a>
                    <a href="/third_parties/supportcog/bansync" class="list-group-item list-group-item-action">🔄 BanSync</a>
                    <a href="/third_parties/supportcog/modlog" class="list-group-item list-group-item-action">📜 Modlog</a>
                    <a href="/third_parties/supportcog/supportconfig" class="list-group-item list-group-item-action">⚙️ Support-Konfiguration</a>
                </div>
            </div>
            """
            return self._page("SupportCog Übersicht", content)
        except Exception as e:
            return self._error(f"Fehler beim Laden der Übersicht: {e}")

    # ============================================
    # 2. MOD-AKTIONEN (Ban/Kick/Timeout/Warn)
    # ============================================

    @dashboard_page(name="modactions", description="Mod-Aktionen ausführen", methods=("GET", "POST"))
    async def rpc_mod_actions(self, **kwargs) -> dict:
        """Seite für Mod-Aktionen: Ban, Kick, Timeout, Warn."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            action = form_data.get("action", "")
            user_id = form_data.get("user_id", "")
            reason = form_data.get("reason", "Kein Grund angegeben")
            duration = form_data.get("duration", "1h")
            anonymous = form_data.get("anonymous") == "on"
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                return self._error("Ungültige User-ID.")
            try:
                member = guild.get_member(user_id)
                if member is None:
                    member = await guild.fetch_member(user_id)
            except Exception:
                member = None
            if member is None:
                return self._error("User nicht auf diesem Server gefunden.")
            # Aktion ausführen
            try:
                if action == "ban":
                    if anonymous:
                        dm_sent = await self._send_mod_dm(member, guild, "ban", moderator=kwargs.get("member", guild.me), reason=reason, anonymous=True)
                        await member.ban(reason=f"Anonymer Ban via Dashboard: {reason}", delete_message_days=1)
                    else:
                        dm_sent = await self._send_mod_dm(member, guild, "ban", moderator=kwargs.get("member", guild.me), reason=reason)
                        await member.ban(reason=f"Ban via Dashboard: {reason}", delete_message_days=1)
                    await self._punishment_record(guild, member.id, "ban", reason, kwargs.get("member", guild.me))
                    return self._success(f"🔨 {member.display_name} wurde gebannt.")
                elif action == "kick":
                    if anonymous:
                        dm_sent = await self._send_mod_dm(member, guild, "kick", moderator=kwargs.get("member", guild.me), reason=reason, anonymous=True)
                    else:
                        dm_sent = await self._send_mod_dm(member, guild, "kick", moderator=kwargs.get("member", guild.me), reason=reason)
                    await member.kick(reason=f"Kick via Dashboard: {reason}")
                    await self._punishment_record(guild, member.id, "kick", reason, kwargs.get("member", guild.me))
                    return self._success(f"👢 {member.display_name} wurde gekickt.")
                elif action == "timeout":
                    seconds = self._parse_duration(duration)
                    if seconds is None:
                        return self._error("Ungültige Dauer. Verwende z.B. 30s, 5m, 2h, 1d.")
                    if seconds > 28 * 86400:
                        return self._error("Timeout darf maximal 28 Tage dauern.")
                    from datetime import timedelta, datetime as _dt
                    until = _dt.now(tz=member.joined_at.tzinfo if member.joined_at else None) + timedelta(seconds=seconds)
                    if anonymous:
                        await self._send_mod_dm(member, guild, "timeout", moderator=kwargs.get("member", guild.me), reason=reason, duration=duration, anonymous=True)
                    else:
                        await self._send_mod_dm(member, guild, "timeout", moderator=kwargs.get("member", guild.me), reason=reason, duration=duration)
                    await member.timeout(until, reason=f"Timeout via Dashboard: {reason}")
                    await self._punishment_record(guild, member.id, "timeout", f"{reason} ({duration})", kwargs.get("member", guild.me))
                    return self._success(f"⏰ {member.display_name} wurde für {duration} getimeoutet.")
                elif action == "warn":
                    cfg = await self.config.guild(guild).warn_config()
                    counter = await self.config.guild(guild).warn_counter() or 0
                    counter += 1
                    warn_id = str(counter)
                    expires_ts = None
                    expiry_days = cfg.get("warn_expiry_days", 30)
                    if expiry_days > 0:
                        import time as _time
                        expires_ts = int(_time.time()) + expiry_days * 86400
                    strikes = await self.config.guild(guild).warn_strikes() or {}
                    user_strikes = strikes.get(str(member.id)) or []
                    user_strikes.append({
                        "warn_id": warn_id,
                        "reason": f"[{'ANONYM' if anonymous else ''}] {reason}",
                        "moderator_id": kwargs.get("member", guild.me).id if hasattr(kwargs.get("member", guild.me), 'id') else 0,
                        "moderator_name": kwargs.get("member", guild.me).display_name if hasattr(kwargs.get("member", guild.me), 'display_name') else "Dashboard",
                        "ts": int(__import__('time').time()),
                        "expires_ts": expires_ts,
                    })
                    strikes[str(member.id)] = user_strikes
                    await self.config.guild(guild).warn_strikes.set(strikes)
                    await self.config.guild(guild).warn_counter.set(counter)
                    await self._punishment_record(guild, member.id, "warn", reason, kwargs.get("member", guild.me))
                    if anonymous:
                        await self._send_mod_dm(member, guild, "warn", moderator=kwargs.get("member", guild.me), reason=reason, anonymous=True)
                    else:
                        await self._send_mod_dm(member, guild, "warn", moderator=kwargs.get("member", guild.me), reason=reason)
                    return self._success(f"⚠️ {member.display_name} wurde verwarnt ({len(user_strikes)} aktiv).")
                else:
                    return self._error("Unbekannte Aktion.")
            except Exception as e:
                return self._error(f"Aktion fehlgeschlagen: {e}")
        # GET: Formular anzeigen
        content = """
        <form method="POST" class="needs-validation" novalidate>
            <input type="hidden" name="csrf_token" id="csrf_token">
            <div class="mb-3">
                <label for="action" class="form-label">Aktion</label>
                <select class="form-select" id="action" name="action" required>
                    <option value="ban">🔨 Bannen</option>
                    <option value="kick">👢 Kicken</option>
                    <option value="timeout">⏰ Timeout</option>
                    <option value="warn">⚠️ Verwarnen</option>
                </select>
            </div>
            <div class="mb-3">
                <label for="user_id" class="form-label">User-ID</label>
                <input type="number" class="form-control" id="user_id" name="user_id" placeholder="123456789012345678" required>
            </div>
            <div class="mb-3" id="duration_div" style="display:none;">
                <label for="duration" class="form-label">Dauer (nur bei Timeout)</label>
                <input type="text" class="form-control" id="duration" name="duration" placeholder="z.B. 1h, 30m, 2d, 1h30m" value="1h">
                <small class="text-muted">Formate: 30s, 5m, 2h, 1d oder Kombinationen</small>
            </div>
            <div class="mb-3">
                <label for="reason" class="form-label">Grund</label>
                <textarea class="form-control" id="reason" name="reason" rows="3" placeholder="Grund der Aktion..." required></textarea>
            </div>
            <div class="mb-3 form-check">
                <input type="checkbox" class="form-check-input" id="anonymous" name="anonymous">
                <label class="form-check-label" for="anonymous">🕵️ Anonym (User sieht "Server-Team" statt deinem Namen)</label>
            </div>
            <button type="submit" class="btn btn-danger">Aktion ausführen</button>
        </form>
        <script>
        document.getElementById('action').addEventListener('change', function() {
            document.getElementById('duration_div').style.display = this.value === 'timeout' ? 'block' : 'none';
        });
        </script>
        """
        return self._page("🔨 Mod-Aktionen", content)

    # ============================================
    # 3. TICKETS & KATEGORIEN
    # ============================================

    @dashboard_page(name="tickets", description="Tickets & Kategorien", methods=("GET",))
    async def rpc_tickets(self, **kwargs) -> dict:
        """Tickets-Übersicht und Kategorien."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        try:
            tickets = await self.config.guild(guild).tickets() or {}
            categories = await self.config.guild(guild).ticket_categories() or {}
            # Tickets auflisten
            tickets_html = ""
            if tickets:
                tickets_html = '<table class="table table-striped"><thead><tr><th>ID</th><th>User</th><th>Channel</th><th>Status</th><th>Erstellt</th></tr></thead><tbody>'
                for tid, t in list(tickets.items())[-20:]:
                    status = t.get("status", "?")
                    status_badge = f'<span class="badge bg-{"success" if status == "closed" else "warning"}">{status}</span>'
                    tickets_html += f'<tr><td>#{tid}</td><td>{t.get("username", "?")}</td><td>#{t.get("channel_name", "?")}</td><td>{status_badge}</td><td>{t.get("created_at", "?")}</td></tr>'
                tickets_html += '</tbody></table>'
            else:
                tickets_html = '<div class="alert alert-info">Keine Tickets vorhanden.</div>'
            # Kategorien
            cats_html = ""
            if categories:
                cats_html = '<div class="row">'
                for key, cat in categories.items():
                    cats_html += f"""
                    <div class="col-md-4 mb-3">
                        <div class="card">
                            <div class="card-body">
                                <h5 class="card-title">{cat.get('emoji', '🎫')} {cat.get('name', key)}</h5>
                                <p class="card-text">{cat.get('description', '')}</p>
                                <small class="text-muted">Key: {key}</small>
                            </div>
                        </div>
                    </div>
                    """
                cats_html += '</div>'
            else:
                cats_html = '<div class="alert alert-warning">Keine Ticket-Kategorien konfiguriert.</div>'
            content = f"""
            <h3>🎫 Tickets</h3>
            {tickets_html}
            <hr>
            <h3>📂 Kategorien</h3>
            {cats_html}
            """
            return self._page("Tickets & Kategorien", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 4. BEWERBUNGEN
    # ============================================

    @dashboard_page(name="applications", description="Bewerbungen verwalten", methods=("GET", "POST"))
    async def rpc_applications(self, **kwargs) -> dict:
        """Bewerbungen anzeigen und entscheiden."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            app_id = form_data.get("app_id", "")
            decision = form_data.get("decision", "")
            reason = form_data.get("reason", "")
            try:
                apps = await self.config.guild(guild).team_applications() or {}
                if app_id not in apps:
                    return self._error("Bewerbung nicht gefunden.")
                app = apps[app_id]
                if app.get("status") != "pending":
                    return self._error("Bewerbung wurde bereits entschieden.")
                new_status = "accepted" if decision == "accept" else "rejected"
                app["status"] = new_status
                app["decided_ts"] = int(__import__('time').time())
                app["decision_reason"] = reason[:500] if reason else None
                apps[app_id] = app
                await self.config.guild(guild).team_applications.set(apps)
                # Auto-Rolle vergeben
                if new_status == "accepted":
                    accepted_role_id = await self.config.guild(guild).team_applications_accepted_role()
                    if accepted_role_id:
                        try:
                            member = guild.get_member(app.get("user_id"))
                            if member:
                                role = guild.get_role(accepted_role_id)
                                if role:
                                    await member.add_roles(role, reason=f"Bewerbung #{app_id} angenommen via Dashboard")
                        except Exception:
                            pass
                return self._success(f"Bewerbung #{app_id} wurde {new_status}.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        # GET: Liste anzeigen
        try:
            apps = await self.config.guild(guild).team_applications() or {}
            pending = {k: v for k, v in apps.items() if v.get("status") == "pending"}
            decided = {k: v for k, v in apps.items() if v.get("status") != "pending"}
            content = f"""
            <h3>⏳ Offene Bewerbungen ({len(pending)})</h3>
            """
            if pending:
                for app_id, app in list(pending.items())[-15:]:
                    content += f"""
                    <div class="card mb-3">
                        <div class="card-header">
                            Bewerbung #{app_id} — {app.get('position', '?')}
                        </div>
                        <div class="card-body">
                            <p><strong>Bewerber:</strong> {app.get('username', '?')} (<code>{app.get('user_id', '?')}</code>)</p>
                            <p><strong>Bewerbungstext:</strong></p>
                            <div class="bg-light p-2 rounded">{app.get('application_text', 'Kein Text')[:500]}</div>
                            <form method="POST" class="mt-3">
                                <input type="hidden" name="app_id" value="{app_id}">
                                <input type="hidden" name="csrf_token" id="csrf_token">
                                <div class="mb-2">
                                    <input type="text" class="form-control" name="reason" placeholder="Begründung (optional)">
                                </div>
                                <button type="submit" name="decision" value="accept" class="btn btn-success btn-sm">✅ Annehmen</button>
                                <button type="submit" name="decision" value="reject" class="btn btn-danger btn-sm">❌ Ablehnen</button>
                            </form>
                        </div>
                    </div>
                    """
            else:
                content += '<div class="alert alert-info">Keine offenen Bewerbungen.</div>'
            content += f"<hr><h3>📜 Entscheidete Bewerbungen ({len(decided)})</h3>"
            if decided:
                content += '<table class="table table-striped"><thead><tr><th>ID</th><th>Position</th><th>Bewerber</th><th>Status</th></tr></thead><tbody>'
                for app_id, app in list(decided.items())[-20:]:
                    status = app.get("status", "?")
                    badge = "success" if status == "accepted" else "danger"
                    content += f'<tr><td>#{app_id}</td><td>{app.get("position","?")}</td><td>{app.get("username","?")}</td><td><span class="badge bg-{badge}">{status}</span></td></tr>'
                content += '</tbody></table>'
            return self._page("📋 Bewerbungen", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 5. AUFGABEN (TASKS)
    # ============================================

    @dashboard_page(name="tasks", description="Aufgaben verwalten", methods=("GET", "POST"))
    async def rpc_tasks(self, **kwargs) -> dict:
        """Aufgaben anzeigen und Status ändern."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            task_id = form_data.get("task_id", "")
            new_status = form_data.get("status", "")
            try:
                tasks = await self.config.guild(guild).team_tasks() or {}
                if task_id not in tasks:
                    return self._error("Aufgabe nicht gefunden.")
                tasks[task_id]["status"] = new_status
                if new_status == "done":
                    import time as _time
                    tasks[task_id]["completed_ts"] = int(_time.time())
                await self.config.guild(guild).team_tasks.set(tasks)
                return self._success(f"Aufgabe #{task_id} auf '{new_status}' gesetzt.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            tasks = await self.config.guild(guild).team_tasks() or {}
            content = "<h3>📝 Team-Aufgaben</h3>"
            if tasks:
                content += '<table class="table table-striped"><thead><tr><th>ID</th><th>Titel</th><th>Status</th><th>Priorität</th><th>Aktion</th></tr></thead><tbody>'
                for tid, t in tasks.items():
                    status = t.get("status", "open")
                    prio = t.get("priority", "normal")
                    prio_badge = {"urgent": "danger", "high": "warning", "normal": "info", "low": "secondary"}.get(prio, "info")
                    content += f"""
                    <tr>
                        <td>#{tid}</td>
                        <td>{t.get('title', '?')}</td>
                        <td><span class="badge bg-secondary">{status}</span></td>
                        <td><span class="badge bg-{prio_badge}">{prio}</span></td>
                        <td>
                            <form method="POST" style="display:inline-flex;">
                                <input type="hidden" name="task_id" value="{tid}">
                                <input type="hidden" name="csrf_token" id="csrf_token">
                                <select name="status" class="form-select form-select-sm" style="width:auto;" onchange="this.form.submit()">
                                    <option value="open" {'selected' if status=='open' else ''}>Offen</option>
                                    <option value="in_progress" {'selected' if status=='in_progress' else ''}>In Bearbeitung</option>
                                    <option value="done" {'selected' if status=='done' else ''}>Erledigt</option>
                                    <option value="cancelled" {'selected' if status=='cancelled' else ''}>Abgebrochen</option>
                                </select>
                            </form>
                        </td>
                    </tr>
                    """
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-info">Keine Aufgaben vorhanden.</div>'
            return self._page("📝 Aufgaben", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 6. SNIPPETS
    # ============================================

    @dashboard_page(name="snippets", description="Snippets verwalten", methods=("GET", "POST"))
    async def rpc_snippets(self, **kwargs) -> dict:
        """Snippets anzeigen, hinzufügen, bearbeiten, löschen."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            action = form_data.get("snippet_action", "")
            name = form_data.get("name", "").lower().strip()
            content_text = form_data.get("content", "")
            try:
                snippets = await self.config.guild(guild).snippets() or {}
                if action == "add":
                    if not name or not content_text:
                        return self._error("Name und Inhalt erforderlich.")
                    snippets[name] = {
                        "content": content_text,
                        "created_by": 0,
                        "created_by_name": "Dashboard",
                        "created_ts": int(__import__('time').time()),
                        "last_used": None,
                        "uses_count": 0,
                    }
                    await self.config.guild(guild).snippets.set(snippets)
                    return self._success(f"Snippet '{name}' erstellt.")
                elif action == "delete":
                    if name in snippets:
                        del snippets[name]
                        await self.config.guild(guild).snippets.set(snippets)
                        return self._success(f"Snippet '{name}' gelöscht.")
                    return self._error("Snippet nicht gefunden.")
                elif action == "edit":
                    if name in snippets:
                        snippets[name]["content"] = content_text
                        await self.config.guild(guild).snippets.set(snippets)
                        return self._success(f"Snippet '{name}' aktualisiert.")
                    return self._error("Snippet nicht gefunden.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            snippets = await self.config.guild(guild).snippets() or {}
            content = """
            <h3>💬 Neues Snippet</h3>
            <form method="POST" class="mb-4">
                <input type="hidden" name="snippet_action" value="add">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="mb-2">
                    <input type="text" class="form-control" name="name" placeholder="Snippet-Name (z.B. rules)" required>
                </div>
                <div class="mb-2">
                    <textarea class="form-control" name="content" rows="3" placeholder="Snippet-Inhalt..." required></textarea>
                </div>
                <button type="submit" class="btn btn-primary">➕ Hinzufügen</button>
            </form>
            <hr>
            <h3>📋 Vorhandene Snippets</h3>
            """
            if snippets:
                for name, s in snippets.items():
                    content += f"""
                    <div class="card mb-2">
                        <div class="card-body">
                            <h5>💬 {name} <small class="text-muted">({s.get('uses_count', 0)} Verwendungen)</small></h5>
                            <div class="bg-light p-2 rounded mb-2">{s.get('content', '')[:300]}</div>
                            <form method="POST" style="display:inline;">
                                <input type="hidden" name="snippet_action" value="delete">
                                <input type="hidden" name="name" value="{name}">
                                <input type="hidden" name="csrf_token" id="csrf_token">
                                <button type="submit" class="btn btn-danger btn-sm">🗑️ Löschen</button>
                            </form>
                        </div>
                    </div>
                    """
            else:
                content += '<div class="alert alert-info">Keine Snippets vorhanden.</div>'
            return self._page("💬 Snippets", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 7. WATCHLIST
    # ============================================

    @dashboard_page(name="watchlist", description="Watchlist verwalten", methods=("GET", "POST"))
    async def rpc_watchlist(self, **kwargs) -> dict:
        """Watchlist anzeigen und User hinzufügen/entfernen."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            action = form_data.get("wl_action", "")
            try:
                wl = await self.config.guild(guild).watchlist() or {}
                if action == "add":
                    user_id = form_data.get("user_id", "")
                    reason = form_data.get("reason", "")
                    try:
                        user_id = int(user_id)
                    except (ValueError, TypeError):
                        return self._error("Ungültige User-ID.")
                    member = guild.get_member(user_id)
                    if not member:
                        return self._error("User nicht auf diesem Server.")
                    wl[str(user_id)] = {
                        "added_by": 0,
                        "added_by_name": "Dashboard",
                        "added_ts": int(__import__('time').time()),
                        "reason": reason,
                        "notify_on_message": True,
                        "notify_on_voice": True,
                        "notify_on_rejoin": True,
                        "username": member.display_name,
                    }
                    await self.config.guild(guild).watchlist.set(wl)
                    return self._success(f"{member.display_name} zur Watchlist hinzugefügt.")
                elif action == "remove":
                    user_id = form_data.get("user_id", "")
                    if user_id in wl:
                        del wl[user_id]
                        await self.config.guild(guild).watchlist.set(wl)
                        return self._success("User von Watchlist entfernt.")
                    return self._error("User nicht auf Watchlist.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            wl = await self.config.guild(guild).watchlist() or {}
            content = """
            <h3>👁️ User hinzufügen</h3>
            <form method="POST" class="mb-4">
                <input type="hidden" name="wl_action" value="add">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="mb-2">
                    <input type="number" class="form-control" name="user_id" placeholder="User-ID" required>
                </div>
                <div class="mb-2">
                    <input type="text" class="form-control" name="reason" placeholder="Grund der Beobachtung" required>
                </div>
                <button type="submit" class="btn btn-warning">👁️ Hinzufügen</button>
            </form>
            <hr>
            <h3>📋 Watchlist</h3>
            """
            if wl:
                content += '<table class="table table-striped"><thead><tr><th>User</th><th>Grund</th><th>Hinzugefügt</th><th>Aktion</th></tr></thead><tbody>'
                for uid, data in wl.items():
                    content += f"""
                    <tr>
                        <td>{data.get('username', '?')} (<code>{uid}</code>)</td>
                        <td>{data.get('reason', '?')[:100]}</td>
                        <td>{data.get('added_ts', '?')}</td>
                        <td>
                            <form method="POST" style="display:inline;">
                                <input type="hidden" name="wl_action" value="remove">
                                <input type="hidden" name="user_id" value="{uid}">
                                <input type="hidden" name="csrf_token" id="csrf_token">
                                <button type="submit" class="btn btn-danger btn-sm">❌ Entfernen</button>
                            </form>
                        </td>
                    </tr>
                    """
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-info">Watchlist ist leer.</div>'
            return self._page("👁️ Watchlist", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 8. TEAM-STATS / LEADERBOARD
    # ============================================

    @dashboard_page(name="teamstats", description="Team-Statistiken", methods=("GET",))
    async def rpc_team_stats(self, **kwargs) -> dict:
        """Team-Statistiken und Leaderboard."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        try:
            activity = await self.config.guild(guild).team_activity() or {}
            if not activity:
                return self._page("📊 Team-Stats", '<div class="alert alert-info">Noch keine Team-Aktivität erfasst.</div>')
            # Score berechnen und sortieren
            def _score(d):
                return (
                    d.get("tickets_closed", 0) * 5
                    + d.get("warns_issued", 0) * 2
                    + d.get("tasks_completed", 0) * 3
                    + min(d.get("messages_sent", 0), 1000) / 100
                )
            sorted_act = sorted(activity.items(), key=lambda x: _score(x[1]), reverse=True)
            content = '<table class="table table-striped"><thead><tr><th>Rang</th><th>Mitglied</th><th>Tickets</th><th>Warns</th><th>Aufgaben</th><th>Nachrichten</th><th>Score</th></tr></thead><tbody>'
            medals = ["🥇", "🥈", "🥉"]
            for i, (uid, data) in enumerate(sorted_act[:25]):
                member = guild.get_member(int(uid))
                name = member.display_name if member else data.get("username", f"User {uid}")
                rank = medals[i] if i < 3 else f"#{i+1}"
                score = _score(data)
                content += f"""
                <tr>
                    <td>{rank}</td>
                    <td>{name}</td>
                    <td>{data.get('tickets_closed', 0)}</td>
                    <td>{data.get('warns_issued', 0)}</td>
                    <td>{data.get('tasks_completed', 0)}</td>
                    <td>{data.get('messages_sent', 0)}</td>
                    <td><strong>{score:.1f}</strong></td>
                </tr>
                """
            content += '</tbody></table>'
            return self._page("📊 Team-Stats / Leaderboard", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 9. WARN-SYSTEM KONFIGURATION
    # ============================================

    @dashboard_page(name="warns", description="Warn-System", methods=("GET", "POST"))
    async def rpc_warns(self, **kwargs) -> dict:
        """Warn-System Konfiguration und Liste."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                cfg = await self.config.guild(guild).warn_config()
                threshold = form_data.get("threshold")
                action_type = form_data.get("action_type")
                expiry = form_data.get("expiry")
                if threshold:
                    cfg["auto_action_threshold"] = int(threshold)
                if action_type:
                    cfg["auto_action_type"] = action_type
                if expiry:
                    cfg["warn_expiry_days"] = int(expiry)
                cfg["notify_user_dm"] = form_data.get("notify_dm") == "on"
                await self.config.guild(guild).warn_config.set(cfg)
                return self._success("Warn-Konfiguration aktualisiert.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            cfg = await self.config.guild(guild).warn_config()
            strikes = await self.config.guild(guild).warn_strikes() or {}
            content = f"""
            <h3>⚙️ Warn-Konfiguration</h3>
            <form method="POST" class="mb-4">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="mb-2">
                    <label>Auto-Action Threshold (ab wie vielen Warns)</label>
                    <input type="number" class="form-control" name="threshold" value="{cfg.get('auto_action_threshold', 3)}">
                </div>
                <div class="mb-2">
                    <label>Auto-Action Typ</label>
                    <select class="form-select" name="action_type">
                        <option value="timeout" {'selected' if cfg.get('auto_action_type')=='timeout' else ''}>Timeout</option>
                        <option value="kick" {'selected' if cfg.get('auto_action_type')=='kick' else ''}>Kick</option>
                        <option value="ban" {'selected' if cfg.get('auto_action_type')=='ban' else ''}>Ban</option>
                        <option value="none" {'selected' if cfg.get('auto_action_type')=='none' else ''}>Deaktiviert</option>
                    </select>
                </div>
                <div class="mb-2">
                    <label>Verfall nach Tagen (0 = nie)</label>
                    <input type="number" class="form-control" name="expiry" value="{cfg.get('warn_expiry_days', 30)}">
                </div>
                <div class="mb-2 form-check">
                    <input type="checkbox" class="form-check-input" name="notify_dm" id="notify_dm" {'checked' if cfg.get('notify_user_dm', True) else ''}>
                    <label class="form-check-label" for="notify_dm">DM an User senden</label>
                </div>
                <button type="submit" class="btn btn-primary">💾 Speichern</button>
            </form>
            <hr>
            <h3>⚠️ Verwarnungen ({sum(len(w) for w in strikes.values())} gesamt, {len(strikes)} User)</h3>
            """
            if strikes:
                content += '<table class="table table-striped"><thead><tr><th>User-ID</th><th>Anzahl</th><th>Letzter Grund</th></tr></thead><tbody>'
                for uid, user_strikes in strikes.items():
                    last = user_strikes[-1] if user_strikes else {}
                    content += f'<tr><td><code>{uid}</code></td><td>{len(user_strikes)}</td><td>{last.get("reason", "?")[:100]}</td></tr>'
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-info">Keine Verwarnungen vergeben.</div>'
            return self._page("⚠️ Warn-System", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 10. ANTI-LINK KONFIGURATION
    # ============================================

    @dashboard_page(name="antilink", description="Anti-Link Konfiguration", methods=("GET", "POST"))
    async def rpc_antilink(self, **kwargs) -> dict:
        """Anti-Link System konfigurieren."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                await self.config.guild(guild).antilink_enabled.set(form_data.get("enabled") == "on")
                mode = form_data.get("mode", "all")
                if mode in ("all", "discord", "off"):
                    await self.config.guild(guild).antilink_mode.set(mode)
                action = form_data.get("action", "delete")
                if action in ("delete", "warn", "timeout"):
                    await self.config.guild(guild).antilink_action.set(action)
                warn_msg = form_data.get("warn_message", "")
                if warn_msg:
                    await self.config.guild(guild).antilink_warning_message.set(warn_msg[:500])
                return self._success("Anti-Link Konfiguration aktualisiert.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            enabled = await self.config.guild(guild).antilink_enabled()
            mode = await self.config.guild(guild).antilink_mode()
            action = await self.config.guild(guild).antilink_action()
            warn_msg = await self.config.guild(guild).antilink_warning_message()
            content = f"""
            <form method="POST">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="mb-3 form-check">
                    <input type="checkbox" class="form-check-input" name="enabled" id="enabled" {'checked' if enabled else ''}>
                    <label class="form-check-label" for="enabled">Anti-Link aktiviert</label>
                </div>
                <div class="mb-3">
                    <label>Modus</label>
                    <select class="form-select" name="mode">
                        <option value="all" {'selected' if mode=='all' else ''}>Alle Links blockieren</option>
                        <option value="discord" {'selected' if mode=='discord' else ''}>Nur Discord-Invites blockieren</option>
                        <option value="off" {'selected' if mode=='off' else ''}>Deaktiviert</option>
                    </select>
                </div>
                <div class="mb-3">
                    <label>Aktion</label>
                    <select class="form-select" name="action">
                        <option value="delete" {'selected' if action=='delete' else ''}>Nur löschen</option>
                        <option value="warn" {'selected' if action=='warn' else ''}>Löschen + Warn</option>
                        <option value="timeout" {'selected' if action=='timeout' else ''}>Löschen + Timeout</option>
                    </select>
                </div>
                <div class="mb-3">
                    <label>Warnungs-Nachricht</label>
                    <textarea class="form-control" name="warn_message" rows="2">{warn_msg or ''}</textarea>
                </div>
                <button type="submit" class="btn btn-primary">💾 Speichern</button>
            </form>
            """
            return self._page("🔗 Anti-Link Konfiguration", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 11. BANSYNC KONFIGURATION
    # ============================================

    @dashboard_page(name="bansync", description="BanSync Konfiguration", methods=("GET", "POST"))
    async def rpc_bansync(self, **kwargs) -> dict:
        """BanSync System konfigurieren."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                await self.config.guild(guild).sync_bans.set(form_data.get("sync_bans") == "on")
                await self.config.guild(guild).sync_unbans.set(form_data.get("sync_unbans") == "on")
                await self.config.guild(guild).sync_timeouts.set(form_data.get("sync_timeouts") == "on")
                await self.config.guild(guild).sync_kicks.set(form_data.get("sync_kicks") == "on")
                await self.config.guild(guild).sync_warns.set(form_data.get("sync_warns") == "on")
                await self.config.guild(guild).sync_roles.set(form_data.get("sync_roles") == "on")
                return self._success("BanSync Konfiguration aktualisiert.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            sync_bans = await self.config.guild(guild).sync_bans()
            sync_unbans = await self.config.guild(guild).sync_unbans()
            sync_timeouts = await self.config.guild(guild).sync_timeouts()
            sync_kicks = await self.config.guild(guild).sync_kicks()
            sync_warns = await self.config.guild(guild).sync_warns()
            sync_roles = await self.config.guild(guild).sync_roles()
            content = f"""
            <form method="POST">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_bans" id="sync_bans" {'checked' if sync_bans else ''}>
                    <label class="form-check-label" for="sync_bans">🔨 Bans synchronisieren</label>
                </div>
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_unbans" id="sync_unbans" {'checked' if sync_unbans else ''}>
                    <label class="form-check-label" for="sync_unbans">✅ Unbans synchronisieren</label>
                </div>
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_timeouts" id="sync_timeouts" {'checked' if sync_timeouts else ''}>
                    <label class="form-check-label" for="sync_timeouts">⏰ Timeouts synchronisieren</label>
                </div>
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_kicks" id="sync_kicks" {'checked' if sync_kicks else ''}>
                    <label class="form-check-label" for="sync_kicks">👢 Kicks synchronisieren</label>
                </div>
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_warns" id="sync_warns" {'checked' if sync_warns else ''}>
                    <label class="form-check-label" for="sync_warns">⚠️ Warns synchronisieren</label>
                </div>
                <div class="form-check mb-2">
                    <input type="checkbox" class="form-check-input" name="sync_roles" id="sync_roles" {'checked' if sync_roles else ''}>
                    <label class="form-check-label" for="sync_roles">👥 Rollen synchronisieren</label>
                </div>
                <button type="submit" class="btn btn-primary">💾 Speichern</button>
            </form>
            """
            return self._page("🔄 BanSync Konfiguration", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 12. MODLOG KONFIGURATION
    # ============================================

    @dashboard_page(name="modlog", description="Modlog Konfiguration", methods=("GET",))
    async def rpc_modlog(self, **kwargs) -> dict:
        """Modlog-Übersicht."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        try:
            modlog_channels = await self.config.guild(guild).modlog_channels() or {}
            ignored_channels = await self.config.guild(guild).modlog_ignored_channels() or []
            content = f"""
            <h3>📜 Modlog-Kanäle</h3>
            """
            if modlog_channels:
                content += '<table class="table table-striped"><thead><tr><th>Event-Typ</th><th>Channel-ID</th></tr></thead><tbody>'
                for event_type, ch_id in modlog_channels.items():
                    ch = guild.get_channel(ch_id) if ch_id else None
                    ch_name = ch.mention if ch else f"Channel {ch_id} (gelöscht)"
                    content += f'<tr><td>{event_type}</td><td>{ch_name}</td></tr>'
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-info">Keine Modlog-Kanäle konfiguriert. Verwende <code>[p]extmodlog channel</code> im Discord.</div>'
            content += f"""
            <h3>🚫 Ignorierte Channels ({len(ignored_channels)})</h3>
            """
            if ignored_channels:
                content += '<ul>'
                for ch_id in ignored_channels:
                    ch = guild.get_channel(ch_id)
                    content += f'<li>{ch.mention if ch else f"Channel {ch_id} (gelöscht)"}</li>'
                content += '</ul>'
            else:
                content += '<div class="alert alert-info">Keine Channels ignoriert.</div>'
            content += '<div class="alert alert-info">Konfiguriere Modlog-Kanäle mit <code>[p]extmodlog channel &lt;event&gt; #channel</code> in Discord.</div>'
            return self._page("📜 Modlog Konfiguration", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")

    # ============================================
    # 13. SUPPORT-KONFIGURATION
    # ============================================

    @dashboard_page(name="supportconfig", description="Support-Konfiguration", methods=("GET", "POST"))
    async def rpc_support_config(self, **kwargs) -> dict:
        """Support-System Konfiguration."""
        guild = kwargs.get("guild")
        if guild is None:
            return self._error("Keine Guild angegeben.")
        method = kwargs.get("method", "GET")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                enabled = form_data.get("enabled") == "on"
                await self.config.guild(guild).enabled.set(enabled)
                return self._success("Support-Konfiguration aktualisiert.")
            except Exception as e:
                return self._error(f"Fehler: {e}")
        try:
            enabled = await self.config.guild(guild).enabled()
            content = f"""
            <form method="POST">
                <input type="hidden" name="csrf_token" id="csrf_token">
                <div class="form-check mb-3">
                    <input type="checkbox" class="form-check-input" name="enabled" id="enabled" {'checked' if enabled else ''}>
                    <label class="form-check-label" for="enabled">Support-System aktiviert</label>
                </div>
                <button type="submit" class="btn btn-primary">💾 Speichern</button>
            </form>
            <hr>
            <div class="alert alert-info">
                <h5>📋 Verfügbare Discord-Befehle</h5>
                <p>Die meisten SupportCog-Einstellungen werden über Discord-Befehle konfiguriert:</p>
                <ul>
                    <li><code>[p]supportset</code> — Support-Konfiguration</li>
                    <li><code>[p]ticketset</code> — Ticket-System</li>
                    <li><code>[p]teamapp</code> — Bewerbungs-System</li>
                    <li><code>[p]swarnset</code> — Warn-Konfiguration</li>
                    <li><code>[p]extmodlog</code> — Modlog</li>
                    <li><code>[p]syncset</code> — BanSync</li>
                    <li><code>[p]antilink</code> — Anti-Link</li>
                    <li><code>[p]moddm</code> — Mod-DM Templates</li>
                </ul>
            </div>
            """
            return self._page("⚙️ Support-Konfiguration", content)
        except Exception as e:
            return self._error(f"Fehler: {e}")
