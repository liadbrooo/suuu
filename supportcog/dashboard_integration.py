"""AAA3A Dashboard-Integration für den SupportCog.

WICHTIG:
- 'guild' und 'member' müssen explizit als Funktionsparameter stehen (nicht via kwargs.get)
- Bei GET: web_content mit source zurückgeben
- Bei POST: redirect_url zurückgeben (PRG-Pattern) + notifications als Flash-Message
- Nie hardcoded <a href> Links ins Dashboard verwenden
- Keine manuellen CSRF-Tokens (Dashboard kümmert sich selbst)
- Top-Level 'data' key NICHT verwenden (würde Rendering bypassen!)
"""
import typing
import time as _time
import discord
from redbot.core import commands
from redbot.core.bot import Red

# Lokaler dashboard_page Decorator-Stub.
# Das Dashboard-Cog ersetzt diesen Stub beim Laden durch den echten Decorator.
def dashboard_page(*args, **kwargs):
    def decorator(func):
        func.__dashboard_decorator_params__ = (args, kwargs)
        return func
    return decorator


class DashboardIntegration:
    """Mixin für die AAA3A Dashboard-Integration."""

    bot: Red

    @commands.Cog.listener()
    async def on_dashboard_cog_add(self, dashboard_cog: commands.Cog) -> None:
        """Wird aufgerufen wenn das Dashboard-Cog geladen wird."""
        import logging
        logger = logging.getLogger("red.supportcog.dashboard")
        logger.info("on_dashboard_cog_add empfangen! Versuche Registration...")
        try:
            if hasattr(self, "settings") and hasattr(self.settings, "commands_added"):
                await self.settings.commands_added.wait()
        except Exception:
            pass
        try:
            # Prüfen ob das Dashboard-Cog die erwartete Struktur hat
            if not hasattr(dashboard_cog, "rpc"):
                logger.error("Dashboard-Cog hat kein 'rpc' Attribut! Attribute: %s", [a for a in dir(dashboard_cog) if not a.startswith('_')])
                return
            if not hasattr(dashboard_cog.rpc, "third_parties_handler"):
                logger.error("Dashboard rpc hat kein 'third_parties_handler'! Attribute: %s", [a for a in dir(dashboard_cog.rpc) if not a.startswith('_')])
                return
            # Anzahl der @dashboard_page Methoden zählen
            page_count = sum(1 for name in dir(self) if hasattr(getattr(self, name), '__dashboard_decorator_params__'))
            logger.info("Registriere SupportCog mit %d Dashboard-Pages...", page_count)
            dashboard_cog.rpc.third_parties_handler.add_third_party(self)
            logger.info("✅ SupportCog erfolgreich beim Dashboard registriert!")
        except Exception as e:
            logger.exception("❌ Konnte SupportCog nicht beim Dashboard registrieren: %s", e)

    # ============================================
    # HELPER (wichtig: korrekte Response-Schema!)
    # ============================================

    def _page(self, title, content):
        """Für GET-Seiten: rendert HTML im Dashboard-Layout."""
        html = '<div class="container-fluid"><h1 class="mb-4">' + title + '</h1>' + content + '</div>'
        return {
            "status": 0,
            "web_content": {
                "source": html,
            },
        }

    def _success_post(self, message, request_url):
        """Für POST-Erfolg: redirect zur GET-URL + Flash-Message."""
        return {
            "status": 0,
            "notifications": [{"message": message, "category": "success"}],
            "redirect_url": request_url,
        }

    def _error_post(self, message, request_url):
        """Für POST-Fehler: redirect zur GET-URL + Flash-Message (danger)."""
        return {
            "status": 1,
            "notifications": [{"message": message, "category": "danger"}],
            "redirect_url": request_url,
        }

    def _error_page(self, message):
        """Für GET-Fehler: rendert Fehlermeldung im Dashboard-Layout."""
        html = '<div class="container-fluid"><div class="alert alert-danger"><h4>Fehler</h4><p>' + message + '</p></div></div>'
        return {
            "status": 1,
            "web_content": {
                "source": html,
            },
        }

    def _fmt_ts(self, ts):
        """Formatiert einen Timestamp lesbar."""
        try:
            if not ts:
                return "—"
            from datetime import datetime
            dt = datetime.fromtimestamp(int(ts))
            return dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            return str(ts) if ts else "—"

    # ============================================
    # 1. ÜBERSICHT / STATS
    # ============================================

    @dashboard_page(name=None, description="SupportCog Übersicht", methods=("GET",))
    async def rpc_overview(self, guild, **kwargs) -> dict:
        """Hauptseite: Übersicht über alle SupportCog-Statistiken."""
        try:
            config = self.config.guild(guild)
            tickets = await config.tickets() or {}
            apps = await config.team_applications() or {}
            tasks = await config.team_tasks() or {}
            warns = await config.warn_strikes() or {}
            snippets = await config.snippets() or {}
            watchlist = await config.watchlist() or {}
            activity = await config.team_activity() or {}
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
            <div class="alert alert-info mt-4">
                <h5>📖 Verfügbare Seiten</h5>
                <p>Verwende die Seitenleiste um zu navigieren. Verfügbare Seiten:</p>
                <ul class="mb-0">
                    <li>🔨 Mod-Aktionen — Ban/Kick/Timeout/Warn mit Anonym-Option</li>
                    <li>🎫 Tickets & Kategorien — Übersicht</li>
                    <li>📋 Bewerbungen — Annehmen/Ablehnen</li>
                    <li>📝 Aufgaben — Status ändern</li>
                    <li>💬 Snippets — Text-Vorlagen</li>
                    <li>👁️ Watchlist — User beobachten</li>
                    <li>📊 Team-Stats — Leaderboard</li>
                    <li>⚠️ Warn-System — Konfiguration</li>
                    <li>🔗 Anti-Link — Konfiguration</li>
                    <li>🔄 BanSync — Cross-Server Sync</li>
                    <li>📜 Modlog — Übersicht</li>
                    <li>⚙️ Support-Konfig — Einstellungen</li>
                    <li>🔍 Member-Suche — User-Info mit Historie</li>
                    <li>📊 Server-Info — Statistiken</li>
                    <li>📝 Embed Builder — Embeds erstellen</li>
                    <li>📜 Modlog-Viewer — Bestrafungs-Historie</li>
                    <li>⏱️ Slowmode & Lock — Channel-Kontrolle</li>
                    <li>📦 Massen-Aktionen — Bulk Ban/Kick/Warn</li>
                </ul>
            </div>
            """
            return self._page("SupportCog Übersicht", content)
        except Exception as e:
            return self._error_page(f"Fehler beim Laden der Übersicht: {e}")

    # ============================================
    # 2. MOD-AKTIONEN
    # ============================================

    @dashboard_page(name="modactions", description="Mod-Aktionen ausführen", methods=("GET", "POST"))
    async def rpc_mod_actions(self, guild, member, **kwargs) -> dict:
        """Seite für Mod-Aktionen: Ban, Kick, Timeout, Warn."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                return self._error_post("Ungültige User-ID.", request_url)
            try:
                target = guild.get_member(user_id)
                if target is None:
                    target = await guild.fetch_member(user_id)
            except Exception:
                target = None
            if target is None:
                return self._error_post("User nicht auf diesem Server gefunden.", request_url)
            try:
                if action == "ban":
                    if anonymous:
                        await self._send_mod_dm(target, guild, "ban", moderator=member, reason=reason, anonymous=True)
                    else:
                        await self._send_mod_dm(target, guild, "ban", moderator=member, reason=reason)
                    await target.ban(reason=f"Ban via Dashboard von {member}: {reason}", delete_message_days=1)
                    await self._punishment_record(guild, target.id, "ban", reason, member)
                    return self._success_post(f"🔨 {target.display_name} wurde gebannt.", request_url)
                elif action == "kick":
                    if anonymous:
                        await self._send_mod_dm(target, guild, "kick", moderator=member, reason=reason, anonymous=True)
                    else:
                        await self._send_mod_dm(target, guild, "kick", moderator=member, reason=reason)
                    await target.kick(reason=f"Kick via Dashboard von {member}: {reason}")
                    await self._punishment_record(guild, target.id, "kick", reason, member)
                    return self._success_post(f"👢 {target.display_name} wurde gekickt.", request_url)
                elif action == "timeout":
                    seconds = self._parse_duration(duration)
                    if seconds is None:
                        return self._error_post("Ungültige Dauer. Verwende z.B. 30s, 5m, 2h, 1d.", request_url)
                    if seconds > 28 * 86400:
                        return self._error_post("Timeout darf maximal 28 Tage dauern.", request_url)
                    from datetime import timedelta, datetime as _dt, timezone as _tz
                    until = _dt.now(tz=_tz.utc) + timedelta(seconds=seconds)
                    if anonymous:
                        await self._send_mod_dm(target, guild, "timeout", moderator=member, reason=reason, duration=duration, anonymous=True)
                    else:
                        await self._send_mod_dm(target, guild, "timeout", moderator=member, reason=reason, duration=duration)
                    await target.timeout(until, reason=f"Timeout via Dashboard von {member}: {reason}")
                    await self._punishment_record(guild, target.id, "timeout", f"{reason} ({duration})", member)
                    return self._success_post(f"⏰ {target.display_name} wurde für {duration} getimeoutet.", request_url)
                elif action == "warn":
                    cfg = await self.config.guild(guild).warn_config()
                    counter = await self.config.guild(guild).warn_counter() or 0
                    counter += 1
                    warn_id = str(counter)
                    expires_ts = None
                    expiry_days = cfg.get("warn_expiry_days", 30)
                    if expiry_days > 0:
                        expires_ts = int(_time.time()) + expiry_days * 86400
                    strikes = await self.config.guild(guild).warn_strikes() or {}
                    user_strikes = strikes.get(str(target.id)) or []
                    user_strikes.append({
                        "warn_id": warn_id,
                        "reason": f"[{'ANONYM' if anonymous else ''}] {reason}",
                        "moderator_id": member.id,
                        "moderator_name": f"{member.display_name}{' (anonym)' if anonymous else ''}",
                        "ts": int(_time.time()),
                        "expires_ts": expires_ts,
                    })
                    strikes[str(target.id)] = user_strikes
                    await self.config.guild(guild).warn_strikes.set(strikes)
                    await self.config.guild(guild).warn_counter.set(counter)
                    await self._punishment_record(guild, target.id, "warn", reason, member)
                    if anonymous:
                        await self._send_mod_dm(target, guild, "warn", moderator=member, reason=reason, anonymous=True)
                    else:
                        await self._send_mod_dm(target, guild, "warn", moderator=member, reason=reason)
                    return self._success_post(f"⚠️ {target.display_name} wurde verwarnt ({len(user_strikes)} aktiv).", request_url)
                else:
                    return self._error_post("Unbekannte Aktion.", request_url)
            except Exception as e:
                return self._error_post(f"Aktion fehlgeschlagen: {e}", request_url)
        content = """
        <form method="POST" class="needs-validation" novalidate>
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
            <div class="mb-3" id="duration_div">
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
        """
        return self._page("🔨 Mod-Aktionen", content)

    # ============================================
    # 3. TICKETS
    # ============================================

    @dashboard_page(name="tickets", description="Tickets & Kategorien", methods=("GET",))
    async def rpc_tickets(self, guild, **kwargs) -> dict:
        """Tickets-Übersicht und Kategorien."""
        try:
            tickets = await self.config.guild(guild).tickets() or {}
            categories = await self.config.guild(guild).ticket_categories() or {}
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 4. BEWERBUNGEN
    # ============================================

    @dashboard_page(name="applications", description="Bewerbungen verwalten", methods=("GET", "POST"))
    async def rpc_applications(self, guild, member, **kwargs) -> dict:
        """Bewerbungen anzeigen und entscheiden."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            app_id = form_data.get("app_id", "")
            decision = form_data.get("decision", "")
            reason = form_data.get("reason", "")
            try:
                apps = await self.config.guild(guild).team_applications() or {}
                if app_id not in apps:
                    return self._error_post("Bewerbung nicht gefunden.", request_url)
                app = apps[app_id]
                if app.get("status") != "pending":
                    return self._error_post("Bewerbung wurde bereits entschieden.", request_url)
                new_status = "accepted" if decision == "accept" else "rejected"
                app["status"] = new_status
                app["decided_by"] = member.id
                app["decided_ts"] = int(_time.time())
                app["decision_reason"] = reason[:500] if reason else None
                apps[app_id] = app
                await self.config.guild(guild).team_applications.set(apps)
                if new_status == "accepted":
                    accepted_role_id = await self.config.guild(guild).team_applications_accepted_role()
                    if accepted_role_id:
                        try:
                            target = guild.get_member(app.get("user_id"))
                            if target:
                                role = guild.get_role(accepted_role_id)
                                if role:
                                    await target.add_roles(role, reason=f"Bewerbung #{app_id} via Dashboard von {member}")
                        except Exception:
                            pass
                return self._success_post(f"Bewerbung #{app_id} wurde {new_status}.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            apps = await self.config.guild(guild).team_applications() or {}
            pending = {k: v for k, v in apps.items() if v.get("status") == "pending"}
            decided = {k: v for k, v in apps.items() if v.get("status") != "pending"}
            content = f"<h3>⏳ Offene Bewerbungen ({len(pending)})</h3>"
            if pending:
                for app_id, app in list(pending.items())[-15:]:
                    content += f"""
                    <div class="card mb-3">
                        <div class="card-header">Bewerbung #{app_id} — {app.get('position', '?')}</div>
                        <div class="card-body">
                            <p><strong>Bewerber:</strong> {app.get('username', '?')} (<code>{app.get('user_id', '?')}</code>)</p>
                            <p><strong>Bewerbungstext:</strong></p>
                            <div class="bg-light p-2 rounded">{app.get('application_text', 'Kein Text')[:500]}</div>
                            <form method="POST" class="mt-3">
                                <input type="hidden" name="app_id" value="{app_id}">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 5. AUFGABEN
    # ============================================

    @dashboard_page(name="tasks", description="Aufgaben verwalten", methods=("GET", "POST"))
    async def rpc_tasks(self, guild, member, **kwargs) -> dict:
        """Aufgaben anzeigen und Status ändern."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            task_id = form_data.get("task_id", "")
            new_status = form_data.get("status", "")
            try:
                tasks = await self.config.guild(guild).team_tasks() or {}
                if task_id not in tasks:
                    return self._error_post("Aufgabe nicht gefunden.", request_url)
                tasks[task_id]["status"] = new_status
                if new_status == "done":
                    tasks[task_id]["completed_by"] = member.id
                    tasks[task_id]["completed_ts"] = int(_time.time())
                await self.config.guild(guild).team_tasks.set(tasks)
                return self._success_post(f"Aufgabe #{task_id} auf '{new_status}' gesetzt.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 6. SNIPPETS
    # ============================================

    @dashboard_page(name="snippets", description="Snippets verwalten", methods=("GET", "POST"))
    async def rpc_snippets(self, guild, member, **kwargs) -> dict:
        """Snippets anzeigen, hinzufügen, löschen."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                        return self._error_post("Name und Inhalt erforderlich.", request_url)
                    snippets[name] = {
                        "content": content_text,
                        "created_by": member.id,
                        "created_by_name": member.display_name,
                        "created_ts": int(_time.time()),
                        "last_used": None,
                        "uses_count": 0,
                    }
                    await self.config.guild(guild).snippets.set(snippets)
                    return self._success_post(f"Snippet '{name}' erstellt.", request_url)
                elif action == "delete":
                    if name in snippets:
                        del snippets[name]
                        await self.config.guild(guild).snippets.set(snippets)
                        return self._success_post(f"Snippet '{name}' gelöscht.", request_url)
                    return self._error_post("Snippet nicht gefunden.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            snippets = await self.config.guild(guild).snippets() or {}
            content = """
            <h3>💬 Neues Snippet</h3>
            <form method="POST" class="mb-4">
                <input type="hidden" name="snippet_action" value="add">
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
                                <button type="submit" class="btn btn-danger btn-sm">🗑️ Löschen</button>
                            </form>
                        </div>
                    </div>
                    """
            else:
                content += '<div class="alert alert-info">Keine Snippets vorhanden.</div>'
            return self._page("💬 Snippets", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 7. WATCHLIST
    # ============================================

    @dashboard_page(name="watchlist", description="Watchlist verwalten", methods=("GET", "POST"))
    async def rpc_watchlist(self, guild, member, **kwargs) -> dict:
        """Watchlist anzeigen und User hinzufügen/entfernen."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                        return self._error_post("Ungültige User-ID.", request_url)
                    target = guild.get_member(user_id)
                    if not target:
                        return self._error_post("User nicht auf diesem Server.", request_url)
                    wl[str(user_id)] = {
                        "added_by": member.id,
                        "added_by_name": member.display_name,
                        "added_ts": int(_time.time()),
                        "reason": reason,
                        "notify_on_message": True,
                        "notify_on_voice": True,
                        "notify_on_rejoin": True,
                        "username": target.display_name,
                    }
                    await self.config.guild(guild).watchlist.set(wl)
                    return self._success_post(f"{target.display_name} zur Watchlist hinzugefügt.", request_url)
                elif action == "remove":
                    user_id = form_data.get("user_id", "")
                    if user_id in wl:
                        del wl[user_id]
                        await self.config.guild(guild).watchlist.set(wl)
                        return self._success_post("User von Watchlist entfernt.", request_url)
                    return self._error_post("User nicht auf Watchlist.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            wl = await self.config.guild(guild).watchlist() or {}
            content = """
            <h3>👁️ User hinzufügen</h3>
            <form method="POST" class="mb-4">
                <input type="hidden" name="wl_action" value="add">
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
                        <td>{self._fmt_ts(data.get('added_ts'))}</td>
                        <td>
                            <form method="POST" style="display:inline;">
                                <input type="hidden" name="wl_action" value="remove">
                                <input type="hidden" name="user_id" value="{uid}">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 8. TEAM-STATS
    # ============================================

    @dashboard_page(name="teamstats", description="Team-Statistiken", methods=("GET",))
    async def rpc_team_stats(self, guild, **kwargs) -> dict:
        """Team-Statistiken und Leaderboard."""
        try:
            activity = await self.config.guild(guild).team_activity() or {}
            if not activity:
                return self._page("📊 Team-Stats", '<div class="alert alert-info">Noch keine Team-Aktivität erfasst.</div>')
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
                m = guild.get_member(int(uid))
                name = m.display_name if m else data.get("username", f"User {uid}")
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 9. WARN-SYSTEM
    # ============================================

    @dashboard_page(name="warns", description="Warn-System", methods=("GET", "POST"))
    async def rpc_warns(self, guild, member, **kwargs) -> dict:
        """Warn-System Konfiguration und Liste."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                return self._success_post("Warn-Konfiguration aktualisiert.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            cfg = await self.config.guild(guild).warn_config()
            strikes = await self.config.guild(guild).warn_strikes() or {}
            content = f"""
            <h3>⚙️ Warn-Konfiguration</h3>
            <form method="POST" class="mb-4">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 10. ANTI-LINK
    # ============================================

    @dashboard_page(name="antilink", description="Anti-Link Konfiguration", methods=("GET", "POST"))
    async def rpc_antilink(self, guild, member, **kwargs) -> dict:
        """Anti-Link System konfigurieren."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                return self._success_post("Anti-Link Konfiguration aktualisiert.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            enabled = await self.config.guild(guild).antilink_enabled()
            mode = await self.config.guild(guild).antilink_mode()
            action = await self.config.guild(guild).antilink_action()
            warn_msg = await self.config.guild(guild).antilink_warning_message()
            content = f"""
            <form method="POST">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 11. BANSYNC
    # ============================================

    @dashboard_page(name="bansync", description="BanSync Konfiguration", methods=("GET", "POST"))
    async def rpc_bansync(self, guild, member, **kwargs) -> dict:
        """BanSync System konfigurieren."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
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
                return self._success_post("BanSync Konfiguration aktualisiert.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            sync_bans = await self.config.guild(guild).sync_bans()
            sync_unbans = await self.config.guild(guild).sync_unbans()
            sync_timeouts = await self.config.guild(guild).sync_timeouts()
            sync_kicks = await self.config.guild(guild).sync_kicks()
            sync_warns = await self.config.guild(guild).sync_warns()
            sync_roles = await self.config.guild(guild).sync_roles()
            content = f"""
            <form method="POST">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 12. MODLOG
    # ============================================

    @dashboard_page(name="modlog", description="Modlog Konfiguration", methods=("GET",))
    async def rpc_modlog(self, guild, **kwargs) -> dict:
        """Modlog-Übersicht."""
        try:
            modlog_channels = await self.config.guild(guild).modlog_channels() or {}
            ignored_channels = await self.config.guild(guild).modlog_ignored_channels() or []
            content = "<h3>📜 Modlog-Kanäle</h3>"
            if modlog_channels:
                content += '<table class="table table-striped"><thead><tr><th>Event-Typ</th><th>Channel</th></tr></thead><tbody>'
                for event_type, ch_id in modlog_channels.items():
                    ch = guild.get_channel(ch_id) if ch_id else None
                    ch_name = f"#{ch.name}" if ch else f"Channel {ch_id} (gelöscht)"
                    content += f'<tr><td>{event_type}</td><td>{ch_name}</td></tr>'
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-info">Keine Modlog-Kanäle konfiguriert. Verwende <code>[p]extmodlog channel</code> im Discord.</div>'
            content += f"<h3>🚫 Ignorierte Channels ({len(ignored_channels)})</h3>"
            if ignored_channels:
                content += '<ul>'
                for ch_id in ignored_channels:
                    ch = guild.get_channel(ch_id)
                    ch_name = f"#{ch.name}" if ch else f"Channel {ch_id} (gelöscht)"
                    content += f'<li>{ch_name}</li>'
                content += '</ul>'
            else:
                content += '<div class="alert alert-info">Keine Channels ignoriert.</div>'
            content += '<div class="alert alert-info">Konfiguriere Modlog-Kanäle mit <code>[p]extmodlog channel &lt;event&gt; #channel</code> in Discord.</div>'
            return self._page("📜 Modlog Konfiguration", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 13. SUPPORT-KONFIG
    # ============================================

    @dashboard_page(name="supportconfig", description="Support-Konfiguration", methods=("GET", "POST"))
    async def rpc_support_config(self, guild, member, **kwargs) -> dict:
        """Support-System Konfiguration."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                enabled = form_data.get("enabled") == "on"
                await self.config.guild(guild).enabled.set(enabled)
                return self._success_post("Support-Konfiguration aktualisiert.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            enabled = await self.config.guild(guild).enabled()
            content = f"""
            <form method="POST">
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
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 14. MEMBER-SUCHE
    # ============================================

    @dashboard_page(name="membersearch", description="Member-Suche mit Bestrafungs-Historie", methods=("GET", "POST"))
    async def rpc_member_search(self, guild, member, **kwargs) -> dict:
        """Member-Info mit Bestrafungs-Historie anzeigen."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        search_query = form_data.get("query", "") if method == "POST" else ""
        if not search_query:
            content = """
            <h3>🔍 Member suchen</h3>
            <form method="POST" class="mb-4">
                <div class="mb-2">
                    <input type="text" class="form-control" name="query" placeholder="User-ID, Name oder Mention" required>
                </div>
                <button type="submit" class="btn btn-primary">🔍 Suchen</button>
            </form>
            """
            return self._page("🔍 Member-Suche", content)
        target = None
        try:
            uid = int(search_query.replace("<@", "").replace(">", "").replace("!", ""))
            target = guild.get_member(uid)
            if target is None:
                target = await guild.fetch_member(uid)
        except (ValueError, TypeError):
            pass
        if target is None:
            for m in guild.members:
                if search_query.lower() in m.display_name.lower() or search_query.lower() in m.name.lower():
                    target = m
                    break
        if target is None:
            return self._error_post(f"Kein Member gefunden für '{search_query}'.", request_url)
        try:
            strikes = await self.config.guild(guild).warn_strikes() or {}
            user_strikes = strikes.get(str(target.id)) or []
            active_warns = [s for s in user_strikes if not s.get("expires_ts") or s["expires_ts"] > _time.time()]
            punishment_history = await self.config.guild(guild).punishment_history() or {}
            user_punishments = punishment_history.get(str(target.id)) or []
            watchlist = await self.config.guild(guild).watchlist() or {}
            on_watchlist = str(target.id) in watchlist
            from datetime import datetime, timezone
            account_age = (datetime.now(timezone.utc) - target.created_at).days
            join_age = (datetime.now(timezone.utc) - target.joined_at).days if target.joined_at else 0
            content = f"""
            <div class="card mb-3">
                <div class="card-header d-flex align-items-center">
                    <img src="{target.display_avatar.url}" class="rounded-circle me-3" style="width:48px;height:48px;" alt="Avatar">
                    <div>
                        <h5 class="mb-0">{target.display_name}</h5>
                        <small class="text-muted">{target.name}#{target.discriminator} • ID: {target.id}</small>
                    </div>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <p><strong>📊 Account-Alter:</strong> {account_age} Tage</p>
                            <p><strong>🏠 Auf Server seit:</strong> {join_age} Tagen</p>
                            <p><strong>👥 Rollen:</strong> {len(target.roles) - 1}</p>
                            <p><strong>⏰ Getimeoutet:</strong> {'Ja' if target.is_timed_out() else 'Nein'}</p>
                        </div>
                        <div class="col-md-6">
                            <p><strong>⚠️ Aktive Verwarnungen:</strong> {len(active_warns)}</p>
                            <p><strong>📋 Bestrafungs-Historie:</strong> {len(user_punishments)} Einträge</p>
                            <p><strong>👁️ Auf Watchlist:</strong> {'Ja ⚠️' if on_watchlist else 'Nein'}</p>
                            <p><strong>🤖 Bot:</strong> {'Ja' if target.bot else 'Nein'}</p>
                        </div>
                    </div>
                </div>
            </div>
            <h3>⚠️ Verwarnungen</h3>
            """
            if active_warns:
                content += '<table class="table table-sm"><thead><tr><th>ID</th><th>Grund</th><th>Moderator</th><th>Datum</th></tr></thead><tbody>'
                for s in active_warns[-10:]:
                    content += f'<tr><td>#{s.get("warn_id","?")}</td><td>{s.get("reason","?")[:100]}</td><td>{s.get("moderator_name","?")}</td><td>{self._fmt_ts(s.get("ts"))}</td></tr>'
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-success">Keine aktiven Verwarnungen.</div>'
            content += "<h3>📜 Bestrafungs-Historie</h3>"
            if user_punishments:
                content += '<table class="table table-sm"><thead><tr><th>Typ</th><th>Grund</th><th>Moderator</th><th>Datum</th></tr></thead><tbody>'
                for p in user_punishments[-15:]:
                    content += f'<tr><td><span class="badge bg-{ "danger" if p.get("type") in ("ban","kick") else "warning" if p.get("type")=="warn" else "info"}">{p.get("type","?")}</span></td><td>{p.get("reason","?")[:100]}</td><td>{p.get("moderator_name","?")}</td><td>{self._fmt_ts(p.get("ts"))}</td></tr>'
                content += '</tbody></table>'
            else:
                content += '<div class="alert alert-success">Keine Bestrafungs-Historie.</div>'
            if on_watchlist:
                wl_data = watchlist[str(target.id)]
                content += f"""
                <h3>👁️ Watchlist-Eintrag</h3>
                <div class="alert alert-warning">
                    <p><strong>Grund:</strong> {wl_data.get('reason', '?')}</p>
                    <p><strong>Hinzugefügt von:</strong> {wl_data.get('added_by_name', '?')} am {self._fmt_ts(wl_data.get('added_ts'))}</p>
                </div>
                """
            content += """
            <hr>
            <h3>🔍 Neue Suche</h3>
            <form method="POST">
                <div class="mb-2">
                    <input type="text" class="form-control" name="query" placeholder="User-ID, Name oder Mention" required>
                </div>
                <button type="submit" class="btn btn-primary">🔍 Suchen</button>
            </form>
            """
            return self._page(f"🔍 Member: {target.display_name}", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 15. SERVER-INFO
    # ============================================

    @dashboard_page(name="serverinfo", description="Server-Informationen", methods=("GET",))
    async def rpc_server_info(self, guild, **kwargs) -> dict:
        """Server-Statistiken anzeigen."""
        try:
            member_count = guild.member_count
            channel_count = len(guild.channels)
            text_channels = len(guild.text_channels)
            voice_channels = len(guild.voice_channels)
            role_count = len(guild.roles)
            emoji_count = len(guild.emojis)
            boost_level = guild.premium_tier
            boost_count = guild.premium_subscription_count
            from datetime import datetime, timezone
            created_days = (datetime.now(timezone.utc) - guild.created_at).days
            tickets = await self.config.guild(guild).tickets() or {}
            apps = await self.config.guild(guild).team_applications() or {}
            strikes = await self.config.guild(guild).warn_strikes() or {}
            content = f"""
            <div class="row">
                <div class="col-md-6">
                    <div class="card mb-3">
                        <div class="card-header">📊 Server-Statistiken</div>
                        <div class="card-body">
                            <p><strong>👥 Mitglieder:</strong> {member_count}</p>
                            <p><strong>📺 Channels:</strong> {channel_count} ({text_channels} Text, {voice_channels} Voice)</p>
                            <p><strong>🎭 Rollen:</strong> {role_count}</p>
                            <p><strong>😀 Emojis:</strong> {emoji_count}</p>
                            <p><strong>💎 Boost-Level:</strong> {boost_level} ({boost_count} Boosts)</p>
                            <p><strong>📅 Erstellt vor:</strong> {created_days} Tagen</p>
                            <p><strong>🆔 Server-ID:</strong> <code>{guild.id}</code></p>
                            <p><strong>👑 Besitzer:</strong> <@{guild.owner_id}></p>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card mb-3">
                        <div class="card-header">🎫 SupportCog-Statistiken</div>
                        <div class="card-body">
                            <p><strong>🎫 Tickets gesamt:</strong> {len(tickets)}</p>
                            <p><strong>📋 Bewerbungen:</strong> {len(apps)}</p>
                            <p><strong>⚠️ User mit Verwarnungen:</strong> {len(strikes)}</p>
                            <p><strong>📌 Bot-Nickname:</strong> {guild.me.display_name}</p>
                            <p><strong>👀 Bot-Rollen:</strong> {len(guild.me.roles) - 1}</p>
                        </div>
                    </div>
                </div>
            </div>
            """
            return self._page(f"📊 Server-Info: {guild.name}", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 16. EMBED-BUILDER
    # ============================================

    @dashboard_page(name="embedbuilder", description="Embed Builder", methods=("GET", "POST"))
    async def rpc_embed_builder(self, guild, member, **kwargs) -> dict:
        """Embeds erstellen und senden."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                channel_id = form_data.get("channel_id", "")
                title = form_data.get("title", "")
                description = form_data.get("description", "")
                color = form_data.get("color", "0x5865F2")
                footer = form_data.get("footer", "")
                image_url = form_data.get("image_url", "")
                thumbnail_url = form_data.get("thumbnail_url", "")
                try:
                    channel_id = int(channel_id)
                except (ValueError, TypeError):
                    return self._error_post("Ungültige Channel-ID.", request_url)
                channel = guild.get_channel(channel_id)
                if not channel:
                    return self._error_post("Channel nicht gefunden.", request_url)
                try:
                    if color.startswith("0x"):
                        color_int = int(color, 16)
                    elif color.startswith("#"):
                        color_int = int(color[1:], 16)
                    else:
                        color_int = int(color)
                except (ValueError, TypeError):
                    color_int = 0x5865F2
                embed = discord.Embed(
                    title=title[:256] if title else None,
                    description=description[:4096] if description else None,
                    color=color_int,
                )
                if footer:
                    embed.set_footer(text=footer[:2048])
                if image_url:
                    embed.set_image(url=image_url)
                if thumbnail_url:
                    embed.set_thumbnail(url=thumbnail_url)
                await channel.send(embed=embed)
                return self._success_post(f"Embed an #{channel.name} gesendet.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            channels_html = ""
            for ch in guild.text_channels[:50]:
                channels_html += f'<option value="{ch.id}">#{ch.name}</option>'
            content = f"""
            <form method="POST">
                <div class="mb-3">
                    <label>Channel</label>
                    <select class="form-select" name="channel_id" required>
                        <option value="">-- Channel wählen --</option>
                        {channels_html}
                    </select>
                </div>
                <div class="mb-3">
                    <label>Titel</label>
                    <input type="text" class="form-control" name="title" placeholder="Embed-Titel" maxlength="256">
                </div>
                <div class="mb-3">
                    <label>Beschreibung</label>
                    <textarea class="form-control" name="description" rows="4" placeholder="Embed-Beschreibung..." maxlength="4096"></textarea>
                </div>
                <div class="mb-3">
                    <label>Farbe (Hex)</label>
                    <input type="text" class="form-control" name="color" placeholder="0x5865F2 oder #5865F2" value="0x5865F2">
                </div>
                <div class="mb-3">
                    <label>Footer</label>
                    <input type="text" class="form-control" name="footer" placeholder="Footer-Text" maxlength="2048">
                </div>
                <div class="mb-3">
                    <label>Thumbnail URL</label>
                    <input type="url" class="form-control" name="thumbnail_url" placeholder="https://...">
                </div>
                <div class="mb-3">
                    <label>Image URL</label>
                    <input type="url" class="form-control" name="image_url" placeholder="https://...">
                </div>
                <button type="submit" class="btn btn-primary">📤 Embed senden</button>
            </form>
            """
            return self._page("📝 Embed Builder", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 17. MODLOG-VIEWER
    # ============================================

    @dashboard_page(name="modlogviewer", description="Bestrafungs-Historie durchsuchen", methods=("GET",))
    async def rpc_modlog_viewer(self, guild, **kwargs) -> dict:
        """Bestrafungs-Historie aller User durchsuchen."""
        try:
            punishment_history = await self.config.guild(guild).punishment_history() or {}
            all_punishments = []
            for uid, plist in punishment_history.items():
                for p in plist:
                    p["user_id"] = uid
                    all_punishments.append(p)
            all_punishments.sort(key=lambda x: x.get("ts", 0), reverse=True)
            content = f"<h3>📜 Bestrafungs-Historie ({len(all_punishments)} Einträge)</h3>"
            if all_punishments:
                content += '<table class="table table-striped"><thead><tr><th>Datum</th><th>User-ID</th><th>Typ</th><th>Grund</th><th>Moderator</th></tr></thead><tbody>'
                for p in all_punishments[:50]:
                    ptype = p.get("type", "?")
                    badge_class = "danger" if ptype in ("ban", "kick") else "warning" if ptype == "warn" else "info"
                    content += f"""
                    <tr>
                        <td>{self._fmt_ts(p.get('ts'))}</td>
                        <td><code>{p.get('user_id', '?')}</code></td>
                        <td><span class="badge bg-{badge_class}">{ptype}</span></td>
                        <td>{p.get('reason', '?')[:100]}</td>
                        <td>{p.get('moderator_name', '?')}</td>
                    </tr>
                    """
                content += '</tbody></table>'
                if len(all_punishments) > 50:
                    content += f'<div class="alert alert-info">Zeige die letzten 50 von {len(all_punishments)} Einträgen.</div>'
            else:
                content += '<div class="alert alert-info">Noch keine Bestrafungen protokolliert.</div>'
            return self._page("📜 Modlog-Viewer", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 18. SLOWMODE / LOCK
    # ============================================

    @dashboard_page(name="slowmode", description="Slowmode & Channel-Lock", methods=("GET", "POST"))
    async def rpc_slowmode(self, guild, member, **kwargs) -> dict:
        """Slowmode und Channel-Lock verwalten."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                action = form_data.get("sm_action", "")
                channel_id = form_data.get("channel_id", "")
                try:
                    channel_id = int(channel_id)
                except (ValueError, TypeError):
                    return self._error_post("Ungültige Channel-ID.", request_url)
                channel = guild.get_channel(channel_id)
                if not channel:
                    return self._error_post("Channel nicht gefunden.", request_url)
                if action == "slowmode":
                    seconds = int(form_data.get("seconds", "0"))
                    seconds = max(0, min(seconds, 21600))
                    await channel.edit(slowmode_delay=seconds, reason=f"Slowmode via Dashboard von {member}")
                    return self._success_post(f"Slowmode für #{channel.name} auf {seconds}s gesetzt.", request_url)
                elif action == "lock":
                    overwrite = channel.overwrites_for(guild.default_role)
                    overwrite.send_messages = False
                    await channel.set_permissions(guild.default_role, overwrite=overwrite, reason=f"Lock via Dashboard von {member}")
                    return self._success_post(f"Channel #{channel.name} wurde gesperrt. 🔒", request_url)
                elif action == "unlock":
                    overwrite = channel.overwrites_for(guild.default_role)
                    overwrite.send_messages = None
                    await channel.set_permissions(guild.default_role, overwrite=overwrite, reason=f"Unlock via Dashboard von {member}")
                    return self._success_post(f"Channel #{channel.name} wurde entsperrt. 🔓", request_url)
                else:
                    return self._error_post("Unbekannte Aktion.", request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        try:
            channels_html = ""
            for ch in guild.text_channels[:50]:
                current_slow = ch.slowmode_delay
                channels_html += f'<option value="{ch.id}">#{ch.name} (Slow: {current_slow}s)</option>'
            content = f"""
            <form method="POST" class="mb-4">
                <input type="hidden" name="sm_action" value="slowmode">
                <div class="mb-3">
                    <label>Channel</label>
                    <select class="form-select" name="channel_id" required>
                        <option value="">-- Channel wählen --</option>
                        {channels_html}
                    </select>
                </div>
                <div class="mb-3">
                    <label>Slowmode (Sekunden, 0 = aus, max 21600)</label>
                    <input type="number" class="form-control" name="seconds" value="0" min="0" max="21600">
                </div>
                <button type="submit" class="btn btn-warning">⏱️ Slowmode setzen</button>
            </form>
            <hr>
            <h3>🔒 Channel sperren/entsperren</h3>
            <form method="POST" class="mb-3">
                <input type="hidden" name="sm_action" value="lock">
                <div class="mb-3">
                    <label>Channel sperren</label>
                    <select class="form-select" name="channel_id" required>
                        <option value="">-- Channel wählen --</option>
                        {channels_html}
                    </select>
                </div>
                <button type="submit" class="btn btn-danger">🔒 Channel sperren</button>
            </form>
            <form method="POST">
                <input type="hidden" name="sm_action" value="unlock">
                <div class="mb-3">
                    <label>Channel entsperren</label>
                    <select class="form-select" name="channel_id" required>
                        <option value="">-- Channel wählen --</option>
                        {channels_html}
                    </select>
                </div>
                <button type="submit" class="btn btn-success">🔓 Channel entsperren</button>
            </form>
            """
            return self._page("⏱️ Slowmode & Channel-Lock", content)
        except Exception as e:
            return self._error_page(f"Fehler: {e}")

    # ============================================
    # 19. BULK-ACTIONS
    # ============================================

    @dashboard_page(name="bulkactions", description="Massen-Aktionen", methods=("GET", "POST"))
    async def rpc_bulk_actions(self, guild, member, **kwargs) -> dict:
        """Mehrere User gleichzeitig bannen/kicken/warnen."""
        method = kwargs.get("method", "GET")
        request_url = kwargs.get("request_url", "")
        data = kwargs.get("data", {})
        form_data = data.get("form", {}) if isinstance(data, dict) else {}
        if method == "POST":
            try:
                action = form_data.get("bulk_action", "")
                user_ids_raw = form_data.get("user_ids", "")
                reason = form_data.get("reason", "Massen-Aktion via Dashboard")
                import re as _re
                id_strings = _re.findall(r'\d{17,20}', user_ids_raw)
                if not id_strings:
                    return self._error_post("Keine gültigen User-IDs gefunden.", request_url)
                success = 0
                failed = 0
                failed_ids = []
                for uid_str in id_strings[:50]:
                    try:
                        uid = int(uid_str)
                        if action == "ban":
                            try:
                                await guild.ban(discord.Object(id=uid), reason=f"Bulk-Ban via Dashboard von {member}: {reason}", delete_message_days=1)
                                await self._punishment_record(guild, uid, "ban", reason, member)
                                success += 1
                            except Exception:
                                failed += 1
                                failed_ids.append(uid_str)
                        elif action == "kick":
                            target = guild.get_member(uid)
                            if target:
                                await target.kick(reason=f"Bulk-Kick via Dashboard von {member}: {reason}")
                                await self._punishment_record(guild, uid, "kick", reason, member)
                                success += 1
                            else:
                                failed += 1
                                failed_ids.append(uid_str)
                        elif action == "warn":
                            target = guild.get_member(uid)
                            if target:
                                counter = await self.config.guild(guild).warn_counter() or 0
                                counter += 1
                                strikes = await self.config.guild(guild).warn_strikes() or {}
                                user_strikes = strikes.get(str(uid)) or []
                                user_strikes.append({
                                    "warn_id": str(counter),
                                    "reason": reason,
                                    "moderator_id": member.id,
                                    "moderator_name": member.display_name,
                                    "ts": int(_time.time()),
                                    "expires_ts": None,
                                })
                                strikes[str(uid)] = user_strikes
                                await self.config.guild(guild).warn_strikes.set(strikes)
                                await self.config.guild(guild).warn_counter.set(counter)
                                await self._punishment_record(guild, uid, "warn", reason, member)
                                success += 1
                            else:
                                failed += 1
                                failed_ids.append(uid_str)
                    except Exception:
                        failed += 1
                        failed_ids.append(uid_str)
                msg = f"✅ {success} User {action}ed."
                if failed:
                    msg += f" ❌ {failed} fehlgeschlagen: {', '.join(failed_ids[:5])}"
                return self._success_post(msg, request_url)
            except Exception as e:
                return self._error_post(f"Fehler: {e}", request_url)
        content = """
        <div class="alert alert-warning">
            <strong>⚠️ Warnung:</strong> Massen-Aktionen betreffen sofort alle angegebenen User. Mit Bedacht verwenden!
        </div>
        <form method="POST">
            <div class="mb-3">
                <label>Aktion</label>
                <select class="form-select" name="bulk_action" required>
                    <option value="ban">🔨 Alle bannen</option>
                    <option value="kick">👢 Alle kicken</option>
                    <option value="warn">⚠️ Alle verwarnen</option>
                </select>
            </div>
            <div class="mb-3">
                <label>User-IDs (eine pro Zeile oder kommasepariert, max 50)</label>
                <textarea class="form-control" name="user_ids" rows="6" placeholder="123456789012345678&#10;234567890123456789&#10;..." required></textarea>
            </div>
            <div class="mb-3">
                <label>Grund</label>
                <input type="text" class="form-control" name="reason" placeholder="Grund der Massen-Aktion" required>
            </div>
            <button type="submit" class="btn btn-danger" onclick="return confirm('Wirklich alle User diese Aktion ausführen?')">⚠️ Massen-Aktion ausführen</button>
        </form>
        """
        return self._page("📦 Massen-Aktionen", content)
