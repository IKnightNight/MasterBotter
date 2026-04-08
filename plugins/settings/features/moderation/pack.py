diff --git a/plugins/settings/features/moderation/pack.py b/plugins/settings/features/moderation/pack.py
index ab90df76d97dc15c5f7f383e949c86fb9cd2c780..6252c2976164356807e49fd984097eb0e8949220 100644
--- a/plugins/settings/features/moderation/pack.py
+++ b/plugins/settings/features/moderation/pack.py
@@ -1,96 +1,171 @@
 from __future__ import annotations
 
+# Touched on 2026-04-07 UTC for interactive tutorial workflow updates.
+
 import json
 import logging
 import re
 from datetime import datetime, timezone, timedelta
 from zoneinfo import ZoneInfo
 from pathlib import Path
 from typing import Any, Dict, Optional
 
 import discord
 from discord import app_commands
 from discord.ext import commands
 from discord import ui
 
 from plugins.settings.ops_forum import get_or_create_ops_forum, post_status
 from plugins.settings.registry import FeatureAction, SettingFeature, SettingsRegistry
 
 PACK_META = {
     "id": "moderation",
     "name": "Moderation",
     "version": "L.14-RESTORED",
     "description": "Civilian reports and staff strike intake routed into offender threads.",
     "category": "Moderation",
     "category_description": "Reports, strikes, offender investigation intake, and moderation tracking.",
 }
 
 STATE_PATH = Path(__file__).resolve().parent / "_data" / "state.json"
 THREAD_INVESTIGATION = "Offenders - Investigation"
 THREAD_ACTIVE = "Offenders - Active"
 THREAD_ARCHIVE = "Offenders - Archive"
 MAX_REASON_LEN = 300
 MAX_OPTIONS = 25
 ENABLED_TAG = "ENABLED"
 DISABLED_TAG = "DISABLED"
 BAN_RECOVERY_INVITE_DAYS = 7
+TUTORIAL_DURATION_HOURS = 2
 
 DEFAULT_MODERATION_SETTINGS = {
     "auto_threshold_actions": True,
     "threshold_1_action": "timeout",
     "threshold_1_duration_minutes": 5,
     "threshold_2_action": "timeout",
     "threshold_2_duration_days": 7,
     "threshold_3_action": "timeout",
     "threshold_3_duration_days": 30,
     "threshold_4_action": "ban",
 }
 
 log = logging.getLogger("bot.settings.moderation")
 debug_log = logging.getLogger("debug.moderation")
 
 
 def _now_iso() -> str:
     return datetime.now(timezone.utc).isoformat()
 
 
 def _default_state() -> Dict[str, Any]:
     return {"guilds": {}, "next_ids": {"investigation": 1, "active": 1, "archive": 1, "alert": 1, "recovery": 1}}
 
 
 def _default_moderation_settings() -> Dict[str, Any]:
     return dict(DEFAULT_MODERATION_SETTINGS)
 
 
 def _guild_settings(cfg: Dict[str, Any]) -> Dict[str, Any]:
     settings = cfg.setdefault("settings", {})
     for key, value in DEFAULT_MODERATION_SETTINGS.items():
         settings.setdefault(key, value)
     return settings
 
 
+def _tutorial_sessions(cfg: Dict[str, Any]) -> Dict[str, Any]:
+    sessions = cfg.setdefault("tutorial_sessions", {})
+    if not isinstance(sessions, dict):
+        sessions = {}
+        cfg["tutorial_sessions"] = sessions
+    return sessions
+
+
+def _parse_iso_utc(raw: Any) -> Optional[datetime]:
+    if raw is None:
+        return None
+    text = str(raw).strip()
+    if not text:
+        return None
+    try:
+        parsed = datetime.fromisoformat(text)
+    except Exception:
+        return None
+    if parsed.tzinfo is None:
+        parsed = parsed.replace(tzinfo=timezone.utc)
+    return parsed.astimezone(timezone.utc)
+
+
+def _cleanup_expired_tutorial_sessions(cfg: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
+    sessions = _tutorial_sessions(cfg)
+    now_dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
+    for key, rec in list(sessions.items()):
+        if not isinstance(rec, dict):
+            sessions.pop(key, None)
+            continue
+        status = str(rec.get("status") or "active").strip().lower()
+        if status != "active":
+            sessions.pop(key, None)
+            continue
+        expires_at = _parse_iso_utc(rec.get("expires_at"))
+        if expires_at is None or expires_at <= now_dt:
+            sessions.pop(key, None)
+
+
+def _find_active_tutorial_session(cfg: Dict[str, Any], owner_user_id: int, tutorial_type: str) -> Optional[Dict[str, Any]]:
+    _cleanup_expired_tutorial_sessions(cfg)
+    owner = int(owner_user_id)
+    expected_type = str(tutorial_type).strip().lower()
+    for rec in _tutorial_sessions(cfg).values():
+        if not isinstance(rec, dict):
+            continue
+        if str(rec.get("status") or "active").strip().lower() != "active":
+            continue
+        if int(rec.get("owner_user_id") or 0) != owner:
+            continue
+        if str(rec.get("tutorial_type") or "").strip().lower() != expected_type:
+            continue
+        return rec
+    return None
+
+
+def _format_remaining_tutorial_time(expires_at_raw: Any) -> str:
+    expires_at = _parse_iso_utc(expires_at_raw)
+    if expires_at is None:
+        return "expired"
+    seconds = int((expires_at - datetime.now(timezone.utc)).total_seconds())
+    if seconds <= 0:
+        return "expired"
+    hours, remainder = divmod(seconds, 3600)
+    minutes, _ = divmod(remainder, 60)
+    if hours > 0:
+        return f"{hours}h {minutes}m"
+    if minutes > 0:
+        return f"{minutes}m"
+    return "<1m"
+
+
 def _threshold_action_key(strike_count: int) -> Optional[str]:
     if strike_count <= 0:
         return None
     return f"threshold_{min(int(strike_count), 4)}"
 
 
 def _threshold_config(settings: Dict[str, Any], strike_count: int) -> Dict[str, Any]:
     key = _threshold_action_key(strike_count)
     if not key:
         return {"key": None, "type": "none", "duration_minutes": None, "label": "No action"}
     action_type = str(settings.get(f"{key}_action", "none") or "none").strip().lower()
     duration_minutes = None
     label = "No action"
     if action_type == "timeout":
         if strike_count == 1:
             duration_minutes = int(settings.get("threshold_1_duration_minutes", 5) or 5)
         elif strike_count == 2:
             duration_minutes = int(settings.get("threshold_2_duration_days", 7) or 7) * 24 * 60
         elif strike_count == 3:
             duration_minutes = int(settings.get("threshold_3_duration_days", 30) or 30) * 24 * 60
         if duration_minutes == 5:
             label = "Timed out for 5 minutes"
         elif duration_minutes and duration_minutes % (24 * 60) == 0:
             days = duration_minutes // (24 * 60)
             label = f"Timed out for {days} day" + ("s" if days != 1 else "")
@@ -216,101 +291,104 @@ def _append_strike_entry(case: Dict[str, Any], *, reason: str, moderator_id: int
             "label": None,
             "applied": False,
             "failed": False,
             "error": None,
         }
     )
     return entry
 
 
 def _normalize_state(raw: Dict[str, Any]) -> Dict[str, Any]:
     raw.setdefault("guilds", {})
     raw.setdefault("next_ids", {})
     for key in ("investigation", "active", "archive", "alert", "recovery"):
         raw["next_ids"].setdefault(key, 1)
     for cfg in raw.get("guilds", {}).values():
         if isinstance(cfg, dict):
             cfg.setdefault("enabled", True)
             cfg.setdefault("investigation_thread_id", None)
             cfg.setdefault("active_thread_id", None)
             cfg.setdefault("archive_thread_id", None)
             cfg.setdefault("investigation_cases", {})
             cfg.setdefault("active_cases", {})
             cfg.setdefault("archive_cases", {})
             cfg.setdefault("archive_index_by_user", {})
             cfg.setdefault("pending_ban_recoveries", {})
+            cfg.setdefault("tutorial_sessions", {})
             _normalize_archive_storage(cfg)
             _guild_settings(cfg)
             for case_map_name in ("active_cases", "archive_cases"):
                 for case in (cfg.get(case_map_name) or {}).values():
                     if isinstance(case, dict):
                         _ensure_case_strike_entries(case)
                         case.setdefault("last_applied_threshold", 0)
     return raw
 
 
 def _load_state() -> Dict[str, Any]:
     try:
         if STATE_PATH.exists():
             raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
             if isinstance(raw, dict):
                 return _normalize_state(raw)
     except Exception:
         log.exception("moderation: failed to load state")
     return _default_state()
 
 
 def _save_state(state: Dict[str, Any]) -> None:
     try:
         STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
         STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
     except Exception:
         log.exception("moderation: failed to save state")
 
 
 def _guild_cfg(state: Dict[str, Any], guild_id: int) -> Dict[str, Any]:
     guilds = state.setdefault("guilds", {})
     cfg = guilds.setdefault(
         str(guild_id),
         {
             "enabled": True,
             "investigation_thread_id": None,
             "active_thread_id": None,
             "archive_thread_id": None,
             "investigation_cases": {},
             "active_cases": {},
             "archive_cases": {},
             "archive_index_by_user": {},
             "pending_ban_recoveries": {},
+            "tutorial_sessions": {},
             "settings": _default_moderation_settings(),
         },
     )
     cfg.setdefault("investigation_cases", {})
     cfg.setdefault("active_cases", {})
     cfg.setdefault("archive_cases", {})
     cfg.setdefault("archive_index_by_user", {})
     cfg.setdefault("pending_ban_recoveries", {})
+    cfg.setdefault("tutorial_sessions", {})
     _normalize_archive_storage(cfg)
     _guild_settings(cfg)
     return cfg
 
 
 def _next_case_id(state: Dict[str, Any], kind: str) -> str:
     next_ids = state.setdefault("next_ids", {})
     current = int(next_ids.get(kind, 1) or 1)
     next_ids[kind] = current + 1
     prefix = {"investigation": "inv", "active": "act", "archive": "arc", "alert": "alr", "recovery": "rcv"}.get(kind, kind[:3])
     return f"{prefix}-{current}"
 
 
 
 
 def _normalize_archive_storage(cfg: Dict[str, Any]) -> Dict[str, Any]:
     archive_cases = cfg.setdefault("archive_cases", {})
     archive_index = cfg.setdefault("archive_index_by_user", {})
     if not isinstance(archive_cases, dict):
         archive_cases = {}
         cfg["archive_cases"] = archive_cases
     if not isinstance(archive_index, dict):
         archive_index = {}
         cfg["archive_index_by_user"] = archive_index
     if archive_cases and all(isinstance(v, dict) and v.get("case_id") for v in archive_cases.values()):
@@ -1431,56 +1509,121 @@ class PromoteReasonHistoryButton(ui.Button):
         super().__init__(label="Full History", style=discord.ButtonStyle.secondary, custom_id=f"moderation:reason:history:{int(user_id)}")
         self._cog = cog
         self._user_id = int(user_id)
 
     async def callback(self, interaction: discord.Interaction):
         await self._cog.mgr.send_investigation_history(interaction, self._user_id)
 
 
 class PromoteReasonChoiceView(ui.View):
     def __init__(self, cog: "ModerationCog", *, user_id: int, mode: str, options: list[discord.SelectOption], initial_text: str = ""):
         super().__init__(timeout=300)
         self._cog = cog
         self.add_item(ChooseExistingReasonSelect(cog, user_id=user_id, mode=mode, options=options))
         self.add_item(SummaryReasonButton(cog, user_id=user_id, mode=mode, initial_text=initial_text))
         self.add_item(PromoteReasonHistoryButton(cog, user_id=user_id))
         self.add_item(CancelButton(custom_id=f"moderation:reason:cancel:{mode}:{user_id}"))
 
     async def interaction_check(self, interaction: discord.Interaction) -> bool:
         member = interaction.user if isinstance(interaction.user, discord.Member) else None
         if not _is_staff(member):
             await interaction.response.send_message("Mods/admins only.", ephemeral=True)
             return False
         return True
 
 
-class TutorialCompleteView(ui.View):
-    def __init__(self, cog: "ModerationCog", guild_id: int, session_id: str):
+class TutorialInteractiveView(ui.View):
+    def __init__(
+        self,
+        cog: "ModerationCog",
+        guild_id: int,
+        session_id: str,
+        tutorial_type: str,
+        owner: discord.Member,
+        steps: list[Dict[str, Any]],
+    ):
         super().__init__(timeout=3600)
         self._cog = cog
         self._guild_id = int(guild_id)
         self._session_id = str(session_id)
+        self._tutorial_type = str(tutorial_type).strip().lower()
+        self._owner = owner
+        self._steps = list(steps)
+        self._index = 0
+        self._sync_buttons()
+
+    def _sync_buttons(self) -> None:
+        total = max(len(self._steps), 1)
+        self.step_indicator.label = f"Step {self._index + 1}/{total}"
+        self.prev_step.disabled = self._index <= 0
+        self.next_step.disabled = self._index >= total - 1
+
+    async def _validate_owner_and_session(self, interaction: discord.Interaction) -> bool:
+        if interaction.guild is None or int(interaction.guild.id) != self._guild_id:
+            await _send_ephemeral_response(interaction, "This tutorial button can only be used in its original server.")
+            return False
+        state = _load_state()
+        cfg = _guild_cfg(state, self._guild_id)
+        _cleanup_expired_tutorial_sessions(cfg)
+        rec = _tutorial_sessions(cfg).get(self._session_id)
+        if not isinstance(rec, dict):
+            await _send_ephemeral_response(interaction, "This tutorial session is already closed or expired.")
+            return False
+        if int(rec.get("owner_user_id") or 0) != int(interaction.user.id):
+            await _send_ephemeral_response(interaction, "Only the tutorial owner can use this tutorial session.")
+            return False
+        _save_state(state)
+        return True
+
+    async def _redraw(self, interaction: discord.Interaction) -> None:
+        embed = self._cog.mgr.build_tutorial_step_embed(
+            owner=self._owner,
+            tutorial_type=self._tutorial_type,
+            session_id=self._session_id,
+            steps=self._steps,
+            step_index=self._index,
+        )
+        self._sync_buttons()
+        await interaction.response.edit_message(embed=embed, view=self)
+
+    @ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
+    async def prev_step(self, interaction: discord.Interaction, button: ui.Button):
+        if not await self._validate_owner_and_session(interaction):
+            return
+        self._index = max(self._index - 1, 0)
+        await self._redraw(interaction)
+
+    @ui.button(label="Step", style=discord.ButtonStyle.secondary, disabled=True)
+    async def step_indicator(self, interaction: discord.Interaction, button: ui.Button):
+        await _send_ephemeral_response(interaction, "Use Previous/Next to move through the tutorial.")
+
+    @ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
+    async def next_step(self, interaction: discord.Interaction, button: ui.Button):
+        if not await self._validate_owner_and_session(interaction):
+            return
+        self._index = min(self._index + 1, max(len(self._steps) - 1, 0))
+        await self._redraw(interaction)
 
     @ui.button(label="Mark Tutorial Complete", style=discord.ButtonStyle.success)
     async def complete(self, interaction: discord.Interaction, button: ui.Button):
         if interaction.guild is None or int(interaction.guild.id) != self._guild_id:
             await _send_ephemeral_response(interaction, "This tutorial button can only be used in its original server.")
             return
         state = _load_state()
         cfg = _guild_cfg(state, self._guild_id)
         rec = _tutorial_sessions(cfg).get(self._session_id)
         if not isinstance(rec, dict):
             await _send_ephemeral_response(interaction, "This tutorial session is already closed or expired.")
             return
         if int(rec.get("owner_user_id") or 0) != int(interaction.user.id):
             await _send_ephemeral_response(interaction, "Only the tutorial owner can complete this session.")
             return
         rec["status"] = "completed"
         rec["completed_at"] = _now_iso()
         _tutorial_sessions(cfg).pop(self._session_id, None)
         _save_state(state)
         for child in self.children:
             child.disabled = True
         try:
             if interaction.response.is_done():
                 await interaction.edit_original_response(view=self)
             else:
@@ -2260,92 +2403,170 @@ class ModerationManager:
                     msg = await th.fetch_message(notice_message_id)
                     await msg.delete()
                 except Exception:
                     pass
 
         if isinstance(archive_case, dict):
             await self._refresh_archive_case_message(guild, archive_case, state=state, ensured=ensured)
         _save_state(state)
         pending_map.pop(str(pending_key), None)
         _save_state(state)
         try:
             await self._post_status_embed(
                 guild,
                 title="Moderation • Ban Recovery Completed",
                 user=member,
                 case_id=str(active_case.get("case_id") or active_case_id or "UnknownCase"),
                 details=[
                     ("Recovery ID", str((pending or {}).get("recovery_id") or "Unknown"), True),
                     ("Restored Roles", str(len(restored_role_ids)), True),
                     ("Source Archive", archive_case_id or "Unknown", True),
                 ],
             )
         except Exception:
             pass
 
-    def _build_tutorial_payload(self, guild: discord.Guild, owner: discord.Member, tutorial_type: str, session_id: str) -> tuple[discord.Embed, ui.View]:
-        fake_user = owner.display_name
+    def _tutorial_steps(self, tutorial_type: str) -> list[Dict[str, Any]]:
         tutorial_type = str(tutorial_type).strip().lower()
-        title = "Moderator Tutorial" if tutorial_type == "moderator" else "Admin Tutorial"
         if tutorial_type == "moderator":
-            lines = [
-                f"Current Summary: {owner.mention}",
-                f"• Case ID: **tut-mod-1**",
-                f"• Stage: **Investigation**",
-                f"• Status: **ACTIVE**",
-                "",
-                "Reports: **1**",
-                '↦ 1: @reporter: "Training report reason"',
-                "",
-                "Use this tutorial to practice the normal moderation flow:",
-                "↦ Read the case card",
-                "↦ Use Current Summary for current state",
-                "↦ Use Full History for the timeline",
-                "↦ Practice Promote to Strike and Remove Report on real cases only",
+            return [
+                {
+                    "name": "Orientation",
+                    "lines": [
+                        "This is a safe walkthrough for the day-to-day moderator flow.",
+                        "Nothing in this tutorial issues actions against a real member.",
+                        "Goal: learn which case controls to use and when.",
+                    ],
+                },
+                {
+                    "name": "Read Investigation Card",
+                    "lines": [
+                        "Start in **Offenders - Investigation**.",
+                        "Use **Current Summary** to review the latest case state.",
+                        "Use **Full History** to inspect timeline events before taking action.",
+                    ],
+                },
+                {
+                    "name": "Promote and Strike Flow",
+                    "lines": [
+                        "When evidence is sufficient, use **Promote to Strike**.",
+                        "Pick or write a clear summary reason for staff auditability.",
+                        "Always verify strike count and threshold action before confirming.",
+                    ],
+                },
+                {
+                    "name": "Report Hygiene",
+                    "lines": [
+                        "Use **Remove Report** only for duplicate/spam/invalid reports.",
+                        "Keep at least one valid report when escalation is still active.",
+                        "If uncertain, leave notes in the timeline instead of deleting context.",
+                    ],
+                },
             ]
-        else:
-            lines = [
-                f"Current Summary: {owner.mention}",
-                f"• Case ID: **tut-admin-1**",
-                f"• Stage: **Archive**",
-                f"• Status: **FINALIZED**",
-                "",
-                "Reports: **1**",
-                '↦ 1: @reporter: "Training report reason"',
-                "",
-                "Strikes: **2**",
-                '↦ 1: @moderator: "Training strike 1"',
-                '↦ 2: @moderator: "Training strike 2"',
-                "",
-                "Final Action",
-                f"↦ Approved by: {owner.mention}",
-                "↦ Closed",
+        return [
+            {
+                "name": "Admin Scope",
+                "lines": [
+                    "Admin tutorial covers archive and recovery controls.",
+                    "Use this when reviewing finalized strike histories or ban reversals.",
+                    "Preserve staff traceability in every action reason.",
+                ],
+            },
+            {
+                "name": "Archive Validation",
+                "lines": [
+                    "Start in **Offenders - Archive** for finalized records.",
+                    "Confirm strike timeline, final action, and approving moderator.",
+                    "Never rewrite history; append corrective context in follow-up actions.",
+                ],
+            },
+            {
+                "name": "Recovery Review",
+                "lines": [
+                    "Use recovery tools only after policy approval for reversal.",
+                    "Check source archive case ID and restored-role list before executing.",
+                    "Post status notes so moderation staff sees completion outcomes.",
+                ],
+            },
+            {
+                "name": "Closeout Checklist",
+                "lines": [
+                    "Validate pending items are cleared from active/recovery queues.",
+                    "Ensure any recovery notice has a completion status update.",
+                    "Mark tutorial complete once you can explain this flow unaided.",
+                ],
+            },
+        ]
+
+    def build_tutorial_step_embed(
+        self,
+        *,
+        owner: discord.Member,
+        tutorial_type: str,
+        session_id: str,
+        steps: list[Dict[str, Any]],
+        step_index: int,
+    ) -> discord.Embed:
+        tutorial_type = str(tutorial_type).strip().lower()
+        title = "Moderator Tutorial" if tutorial_type == "moderator" else "Admin Tutorial"
+        idx = max(0, min(step_index, max(len(steps) - 1, 0)))
+        total = max(len(steps), 1)
+        step = steps[idx] if steps else {"name": "Overview", "lines": ["No tutorial steps are available."]}
+        lines = [
+            f"Current Summary: {owner.mention}",
+            f"• Case ID: **{session_id}**",
+            f"• Tutorial Type: **{title}**",
+            f"• Progress: **Step {idx + 1}/{total} — {step.get('name', 'Step')}**",
+            "",
+        ]
+        for entry in list(step.get("lines") or []):
+            lines.append(f"↦ {entry}")
+        lines.extend(
+            [
                 "",
-                "Use this tutorial to review archive/admin-only moderation concepts, including finalization and recovery review.",
+                "Controls",
+                "↦ Use **Previous** and **Next** to move through each module.",
+                "↦ Use **Mark Tutorial Complete** to clear your tutorial lock.",
             ]
-        embed = _build_case_embed(f"{title} • {session_id}", "\n".join(lines), footer=f"Tutorial lock active for {owner.mention} • expires in {TUTORIAL_DURATION_HOURS} hour(s)")
-        view = TutorialCompleteView(self.bot.get_cog("ModerationCog"), guild.id, session_id)
+        )
+        return _build_case_embed(
+            f"{title} • {session_id}",
+            "\n".join(lines),
+            footer=f"Tutorial lock active for {owner.mention} • expires in {TUTORIAL_DURATION_HOURS} hour(s)",
+        )
+
+    def _build_tutorial_payload(self, guild: discord.Guild, owner: discord.Member, tutorial_type: str, session_id: str) -> tuple[discord.Embed, ui.View]:
+        tutorial_type = str(tutorial_type).strip().lower()
+        steps = self._tutorial_steps(tutorial_type)
+        embed = self.build_tutorial_step_embed(
+            owner=owner,
+            tutorial_type=tutorial_type,
+            session_id=session_id,
+            steps=steps,
+            step_index=0,
+        )
+        view = TutorialInteractiveView(self.bot.get_cog("ModerationCog"), guild.id, session_id, tutorial_type, owner, steps)
         return embed, view
 
     def _build_recovery_pending_embed(self, guild: discord.Guild, active_case: Dict[str, Any], recovery: Dict[str, Any]) -> discord.Embed:
         user_id = int((active_case or {}).get("user_id") or 0)
         display_name = str((active_case or {}).get("display_name") or (active_case or {}).get("user_name") or user_id)
         strike_count = int((active_case or {}).get("strike_count") or 0)
         stage = "Recovery" if strike_count == 0 else "Active"
         status = "PENDING RETURN" if strike_count == 0 else "RECOVERY PENDING"
         case_label = "RECOVERY CASE" if strike_count == 0 else "STRIKE"
         approved_by = int((recovery or {}).get("approved_by_user_id") or 0)
         reason = str((recovery or {}).get("recovery_reason") or (active_case or {}).get("reason") or "No reason provided")
         lines = [
             f"• Case ID: **{str((active_case or {}).get('case_id') or str((recovery or {}).get('recovery_id') or 'UnknownCase'))}**",
             f"• Stage: **{stage}**",
             f"• Status: **{status}**",
             "",
             f"Recovery",
             f"↦ Approved by: <@{approved_by}>" if approved_by else "↦ Approved by: Unknown",
             f"↦ Reason: {reason}",
             "↦ Invite Available" if str((recovery or {}).get('invite_url') or '') else "↦ Invite Pending",
             "",
             "Recovered From",
             f"↦ Archive Case: **{str((recovery or {}).get('source_archive_case_id') or (active_case or {}).get('restored_from_archive_case_id') or 'UnknownCase')}**",
             "↦ Final Action Reversed: **Ban**",
             "",
