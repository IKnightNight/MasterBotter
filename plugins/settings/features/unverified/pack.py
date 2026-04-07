from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import traceback

import discord
from discord.ext import commands, tasks

from plugins.settings.registry import SettingsRegistry, SettingFeature, FeatureAction
from plugins.settings.ops_forum import debug_enabled, post_debug, post_status, get_or_create_ops_forum
from core.log_formatter import build_log, event_color, ansi_wrap, guild_value, user_value, channel_value, thread_value, message_value, log_id_mode

PACK_META = {
    "id": "unverified",
    "name": "unverified",
    "version": "J.21",
    "description": "Forum-based unverified verification workflow.",
    "category": "Moderation",
    "category_description": "Moderation queues, verification, and cleanup.",
}

CATEGORY = "Moderation"
CATEGORY_DESCRIPTION = "Moderation queues, verification, and cleanup."

ENABLED_TAG = "ENABLED"
DISABLED_TAG = "DISABLED"
THREAD_UNVERIFIED = "Verify - UNVERIFIED"
THREAD_DENIED = "Verify - DENIED"
THREAD_COMPLETED = "Verify - COMPLETED"
CASE_OPEN = {"UNVERIFIED", "DENIED"}
CASE_FINAL = {"APPROVED", "KICKED", "BANNED", "EXPIRED"}

DEFAULT_GUILD_STATE: Dict[str, Any] = {
    "enabled": False,
    "thread_unverified_id": None,
    "thread_denied_id": None,
    "thread_completed_id": None,
    "unverified_role_id": None,
    "verified_role_id": None,
    "verifier_role_ids": [],
    "verifier_role_id": None,
    "warn_after_days": 3,
    "kick_after_days": 7,
    "dm_expiry_minutes": 2,
    "manual_override_grace_seconds": 15,
    "cleanup_invite_channel_id": None,
    "cleanup_strike_limit": 3,
    "verifier_dm_embed_enabled": True,
    "verifier_dm_title": "Verification Request",
    "verifier_dm_description": "An unverified member is in voice with you, a verifier.",
}

DEFAULT_STATE: Dict[str, Any] = {
    "guilds": {},
    "cases": {},
    "invite_cache": {},
    "invite_temp": {},
    "case_seq": 0,
}


ANSI_RESET = "\033[0m"
ANSI_LIGHT_BLUE = "\033[94m"
ANSI_RED = "\033[91m"
ANSI_GREEN = "\033[92m"
ANSI_ORANGE = "\033[93m"
ANSI_CYAN = "\033[96m"


def _log_id_mode() -> bool:
    return os.getenv("LOG_ID_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _fromiso(raw: str | None) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _data_file() -> Path:
    data_dir = Path(__file__).parent / "_data"
    data_dir.mkdir(exist_ok=True)
    return data_dir / "state.json"


def load_state() -> Dict[str, Any]:
    path = _data_file()
    if not path.exists():
        return json.loads(json.dumps(DEFAULT_STATE))
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}
    merged = json.loads(json.dumps(DEFAULT_STATE))
    merged.update(data)
    merged.setdefault("guilds", {})
    merged.setdefault("cases", {})
    merged.setdefault("invite_cache", {})
    merged.setdefault("invite_temp", {})
    merged.setdefault("case_seq", 0)
    return merged


def save_state(state: Dict[str, Any]) -> None:
    _data_file().write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _guild_cfg(state: Dict[str, Any], guild_id: int, *, create: bool = True) -> Optional[Dict[str, Any]]:
    gs = state.setdefault("guilds", {})
    key = str(guild_id)
    if key not in gs:
        if not create:
            return None
        gs[key] = json.loads(json.dumps(DEFAULT_GUILD_STATE))
    cfg = gs[key]
    for k, v in DEFAULT_GUILD_STATE.items():
        cfg.setdefault(k, v)
    return cfg


def _next_case_id(state: Dict[str, Any]) -> str:
    state["case_seq"] = int(state.get("case_seq", 0)) + 1
    return f"case-{state['case_seq']}"


def _display_user(member: discord.abc.User) -> str:
    name = getattr(member, "display_name", None) or getattr(member, "name", None) or str(getattr(member, "id", "user"))
    return f"{name} ({member.id})"


def _fmt_ts(raw: str | None) -> str:
    dt = _fromiso(raw)
    if dt is None:
        return "Unknown"
    unix = int(dt.timestamp())
    return f"<t:{unix}:F>"


def _status_line(case: Dict[str, Any]) -> str:
    return str(case.get("status") or "UNVERIFIED")


def _case_key(guild_id: int, user_id: int) -> str:
    return f"{guild_id}:{user_id}"


def _sync_pending_dms(case: Dict[str, Any]) -> None:
    dm_requests = case.get("dm_requests") or {}
    pending: List[Dict[str, Any]] = []
    if isinstance(dm_requests, dict):
        for value in dm_requests.values():
            if not isinstance(value, dict):
                continue
            if str(value.get("status") or "pending").lower() == "pending":
                pending.append(dict(value))
    case["pending_dms"] = pending


def _get_dm_requests(case: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = case.get("dm_requests")
    requests: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not isinstance(value, dict):
                continue
            try:
                verifier_id = int(value.get("verifier_id") or key)
            except Exception:
                continue
            item = dict(value)
            item["verifier_id"] = verifier_id
            item.setdefault("status", "pending")
            requests[str(verifier_id)] = item
    legacy = case.get("pending_dms") or []
    if isinstance(legacy, list):
        for value in legacy:
            if not isinstance(value, dict):
                continue
            try:
                verifier_id = int(value.get("verifier_id"))
            except Exception:
                continue
            if str(verifier_id) in requests:
                continue
            item = dict(value)
            item["verifier_id"] = verifier_id
            item.setdefault("status", "pending")
            requests[str(verifier_id)] = item
    case["dm_requests"] = requests
    _sync_pending_dms(case)
    return requests


def _set_dm_requests(case: Dict[str, Any], requests: Dict[str, Dict[str, Any]]) -> None:
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in (requests or {}).items():
        if not isinstance(value, dict):
            continue
        try:
            verifier_id = int(value.get("verifier_id") or key)
        except Exception:
            continue
        item = dict(value)
        item["verifier_id"] = verifier_id
        item.setdefault("status", "pending")
        normalized[str(verifier_id)] = item
    case["dm_requests"] = normalized
    _sync_pending_dms(case)


def _ensure_case_defaults(case: Dict[str, Any]) -> Dict[str, Any]:
    case.setdefault("status", "UNVERIFIED")
    case.setdefault("warned", False)
    case.setdefault("cleanup_strikes", 0)
    case.setdefault("pending_dms", [])
    case.setdefault("dm_requests", {})
    case.setdefault("voice_context", [])
    case.setdefault("history", [])
    case.setdefault("closed", False)
    _get_dm_requests(case)
    return case


def _is_admin(member: discord.Member) -> bool:
    try:
        return member.guild_permissions.administrator
    except Exception:
        return False


def _member_has_role(member: discord.Member, role_id: int | None) -> bool:
    if not role_id:
        return False
    return any(r.id == int(role_id) for r in member.roles)


def _member_has_any_role(member: discord.Member, role_ids: Any) -> bool:
    if role_ids is None:
        return False
    if isinstance(role_ids, (list, tuple, set)):
        ids = []
        for x in role_ids:
            try:
                ids.append(int(x))
            except Exception:
                pass
        return any(r.id in ids for r in member.roles)
    try:
        rid = int(role_ids)
    except Exception:
        return False
    return any(r.id == rid for r in member.roles)


def _normalize_role_name(name: str) -> str:
    return "".join(ch for ch in (name or "").lower() if ch.isalnum())


def _autodetect_role_id(guild: discord.Guild, target_name: str) -> Optional[int]:
    want = _normalize_role_name(target_name)
    for role in guild.roles:
        if _normalize_role_name(role.name) == want:
            return role.id
    return None


def _coerce_role_id_list(raw: str) -> List[int]:
    out: List[int] = []
    for part in str(raw or "").replace("\n", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out

ACTIVE_UNVERIFIED_MANAGER: Optional["UnverifiedManager"] = None

class PersistentAdminActionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="unverified:admin:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        await mgr.dlog("button approve clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None))
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await mgr.admin_approve(interaction, case_key)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.secondary, custom_id="unverified:admin:deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        if not await mgr.ensure_admin(interaction):
            return
        await mgr.dlog("button deny clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None))
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await interaction.response.send_modal(DenyReasonModal(mgr, case_key, source="forum"))

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger, custom_id="unverified:admin:kick")
    async def kick(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        if not await mgr.ensure_admin(interaction):
            return
        await mgr.dlog("button kick clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None))
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await interaction.response.send_modal(KickReasonModal(mgr, case_key, source="forum"))

class PersistentDeniedActionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="unverified:admin:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await mgr.admin_approve(interaction, case_key)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger, custom_id="unverified:admin:kick")
    async def kick(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        if not await mgr.ensure_admin(interaction):
            return
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await interaction.response.send_modal(KickReasonModal(mgr, case_key, source="forum"))

    @discord.ui.button(label="Forgive", style=discord.ButtonStyle.primary, custom_id="unverified:admin:forgive")
    async def forgive(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        if not await mgr.ensure_admin(interaction):
            return
        state = mgr.state()
        case_key = mgr.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message) or "0:0"
        await mgr.admin_forgive(interaction, case_key)

class PersistentDMActionView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="unverified:dm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        state = mgr.state()
        case_key = None
        for case in state.setdefault("cases", {}).values():
            for item in _get_dm_requests(case).values():
                try:
                    if int(item.get("message_id")) == int(interaction.message.id):
                        case_key = f"{int(case.get('guild_id'))}:{int(case.get('user_id'))}"
                        break
                except Exception:
                    pass
            if case_key:
                break
        await mgr.dm_approve(interaction, case_key or "0:0")

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, custom_id="unverified:dm:deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        mgr = ACTIVE_UNVERIFIED_MANAGER
        if mgr is None:
            await interaction.response.send_message("Verification system is not ready.", ephemeral=True)
            return
        state = mgr.state()
        case_key = None
        for case in state.setdefault("cases", {}).values():
            for item in _get_dm_requests(case).values():
                try:
                    if int(item.get("message_id")) == int(interaction.message.id):
                        case_key = f"{int(case.get('guild_id'))}:{int(case.get('user_id'))}"
                        break
                except Exception:
                    pass
            if case_key:
                break
        await mgr.dm_deny(interaction, case_key or "0:0")


class ConfigureModal(discord.ui.Modal):
    def __init__(self, guild_id: int):
        super().__init__(title="Configure Unverified", timeout=300)
        state = load_state()
        cfg = _guild_cfg(state, guild_id) or json.loads(json.dumps(DEFAULT_GUILD_STATE))
        self.guild_id = guild_id
        self.unverified_role = discord.ui.TextInput(
            label="Unverified Role ID",
            style=discord.TextStyle.short,
            default=str(cfg.get("unverified_role_id") or ""),
            required=False,
            max_length=30,
        )
        self.verified_role = discord.ui.TextInput(
            label="Verified Role ID",
            style=discord.TextStyle.short,
            default=str(cfg.get("verified_role_id") or ""),
            required=False,
            max_length=30,
        )
        self.verifier_role = discord.ui.TextInput(
            label="Verifier Role IDs (comma-separated)",
            style=discord.TextStyle.short,
            default=",".join(str(x) for x in (cfg.get("verifier_role_ids") or ([cfg.get("verifier_role_id")] if cfg.get("verifier_role_id") else []))),
            required=False,
            max_length=30,
        )
        self.warn_days = discord.ui.TextInput(
            label="Warn Days",
            style=discord.TextStyle.short,
            default=str(cfg.get("warn_after_days", 3)),
            required=False,
            max_length=10,
        )
        self.kick_days = discord.ui.TextInput(
            label="Kick Days",
            style=discord.TextStyle.short,
            default=str(cfg.get("kick_after_days", 7)),
            required=False,
            max_length=10,
        )
        self.add_item(self.unverified_role)
        self.add_item(self.verified_role)
        self.add_item(self.verifier_role)
        self.add_item(self.warn_days)
        self.add_item(self.kick_days)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        state = load_state()
        cfg = _guild_cfg(state, self.guild_id)
        assert cfg is not None

        def as_int(v: str) -> Optional[int]:
            v = (v or "").strip()
            return int(v) if v.isdigit() else None

        cfg["unverified_role_id"] = as_int(str(self.unverified_role.value or ""))
        cfg["verified_role_id"] = as_int(str(self.verified_role.value or ""))
        cfg["verifier_role_ids"] = _coerce_role_id_list(str(self.verifier_role.value or ""))
        cfg["verifier_role_id"] = cfg["verifier_role_ids"][0] if cfg["verifier_role_ids"] else None

        try:
            cfg["warn_after_days"] = max(0, int(str(self.warn_days.value or cfg.get("warn_after_days", 3)).strip()))
            cfg["kick_after_days"] = max(1, int(str(self.kick_days.value or cfg.get("kick_after_days", 7)).strip()))
        except Exception:
            await interaction.response.send_message("Warn Days and Kick Days must be numbers.", ephemeral=True)
            return

        save_state(state)
        await interaction.response.send_message("Unverified configuration saved.", ephemeral=True)


class DenyReasonModal(discord.ui.Modal):
    def __init__(self, manager: "UnverifiedManager", case_key: str, source: str):
        super().__init__(title="Deny Verification", timeout=300)
        self.manager = manager
        self.case_key = case_key
        self.source = source
        self.reason = discord.ui.TextInput(label="Reason", style=discord.TextStyle.paragraph, required=True, max_length=1000)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            if self.source == "dm":
                if not interaction.response.is_done():
                    await interaction.response.defer()
            else:
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True)
        except Exception:
            pass
        await self.manager.resolve_case(interaction, self.case_key, outcome="DENIED", reason=str(self.reason.value), source=self.source)


class KickReasonModal(discord.ui.Modal):
    def __init__(self, manager: "UnverifiedManager", case_key: str, source: str):
        super().__init__(title="Kick User", timeout=300)
        self.manager = manager
        self.case_key = case_key
        self.source = source
        self.reason = discord.ui.TextInput(
            label="Kick reason",
            placeholder="Shown to the user before removal",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=1000,
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await self.manager.dlog(
                "kick modal submitted",
                guild=interaction.guild,
                user=interaction.user,
                case=self.case_key,
            )
            await self.manager.resolve_case(interaction, self.case_key, outcome="KICKED", reason=str(self.reason.value), source=self.source)
        except Exception as exc:
            await self.manager.dlog(
                "kick modal failed",
                guild=interaction.guild,
                user=interaction.user,
                case=self.case_key,
                error=f"{type(exc).__name__}: {exc}",
            )
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(f"Kick failed: {type(exc).__name__}: {exc}", ephemeral=True)
                else:
                    await interaction.response.send_message(f"Kick failed: {type(exc).__name__}: {exc}", ephemeral=True)
            except Exception:
                pass


class DMActionView(discord.ui.View):
    def __init__(self, manager: "UnverifiedManager", case_key: str):
        super().__init__(timeout=None)
        self.manager = manager
        self.case_key = case_key

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="unverified:dm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.manager.dm_approve(interaction, self.case_key)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, custom_id="unverified:dm:deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.manager.dm_deny(interaction, self.case_key)


class AdminActionView(discord.ui.View):
    def __init__(self, manager: "UnverifiedManager", case_key: str, status: str):
        super().__init__(timeout=None)
        self.manager = manager
        self.case_key = case_key
        if status == "DENIED":
            self.clear_items()
            self.add_item(AdminApproveButton(manager, case_key, status))
            self.add_item(AdminKickButton(manager, case_key, status))
            self.add_item(AdminForgiveButton(manager, case_key))
        elif status in CASE_FINAL:
            self.clear_items()
        else:
            self.clear_items()
            self.add_item(AdminApproveButton(manager, case_key, status))
            self.add_item(AdminDenyButton(manager, case_key, status))
            self.add_item(AdminKickButton(manager, case_key, status))


class AdminApproveButton(discord.ui.Button):
    def __init__(self, manager: "UnverifiedManager", case_key: str, status: str):
        super().__init__(label="Approve", style=discord.ButtonStyle.success, custom_id=f"unverified:admin:approve:{case_key}")
        self.manager = manager
        self.case_key = case_key


    async def callback(self, interaction: discord.Interaction):
        try:
            await self.manager.dlog("button approve clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None), case=self.case_key)
            await self.manager.admin_approve(interaction, self.case_key)
        except Exception:
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("Approve failed.", ephemeral=True)
                else:
                    await interaction.response.send_message("Approve failed.", ephemeral=True)
            except Exception:
                pass


class AdminDenyButton(discord.ui.Button):
    def __init__(self, manager: "UnverifiedManager", case_key: str, status: str):
        super().__init__(label="Deny", style=discord.ButtonStyle.secondary, custom_id=f"unverified:admin:deny:{case_key}")
        self.manager = manager
        self.case_key = case_key


    async def callback(self, interaction: discord.Interaction):
        try:
            if not await self.manager.ensure_admin(interaction):
                return
            await self.manager.dlog("button deny clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None), case=self.case_key)
            state = self.manager.state()
            resolved = self.manager.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message)
            await interaction.response.send_modal(DenyReasonModal(self.manager, resolved or self.case_key, source="forum"))
        except Exception:
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("Deny failed.", ephemeral=True)
                else:
                    await interaction.response.send_message("Deny failed.", ephemeral=True)
            except Exception:
                pass


class AdminKickButton(discord.ui.Button):
    def __init__(self, manager: "UnverifiedManager", case_key: str, status: str):
        super().__init__(label="Kick", style=discord.ButtonStyle.danger, custom_id=f"unverified:admin:kick:{case_key}")
        self.manager = manager
        self.case_key = case_key


    async def callback(self, interaction: discord.Interaction):
        try:
            if not await self.manager.ensure_admin(interaction):
                return
            await self.manager.dlog("button kick clicked", guild=interaction.guild, message=getattr(interaction.message, "id", None), case=self.case_key)
            state = self.manager.state()
            resolved = self.manager.get_case_key_from_message(state, interaction.guild.id if interaction.guild else 0, interaction.message)
            await interaction.response.send_modal(KickReasonModal(self.manager, resolved or self.case_key, source="forum"))
        except Exception as exc:
            await self.manager.dlog("button kick failed", guild=interaction.guild, message=getattr(interaction.message, "id", None), case=self.case_key, error=f"{type(exc).__name__}: {exc}")
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(f"Kick failed: {type(exc).__name__}: {exc}", ephemeral=True)
                else:
                    await interaction.response.send_message(f"Kick failed: {type(exc).__name__}: {exc}", ephemeral=True)
            except Exception:
                pass


class AdminForgiveButton(discord.ui.Button):
    def __init__(self, manager: "UnverifiedManager", case_key: str):
        super().__init__(label="Forgive", style=discord.ButtonStyle.primary, custom_id=f"unverified:admin:forgive:{case_key}")
        self.manager = manager
        self.case_key = case_key


    async def callback(self, interaction: discord.Interaction):
        try:
            await self.manager.admin_forgive(interaction, self.case_key)
        except Exception:
            try:
                if interaction.response.is_done():
                    await interaction.followup.send("Forgive failed.", ephemeral=True)
                else:
                    await interaction.response.send_message("Forgive failed.", ephemeral=True)
            except Exception:
                pass


class UnverifiedManager:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._reconcile_tasks: Dict[str, asyncio.Task] = {}
        self._dm_expiry_tasks: Dict[str, asyncio.Task] = {}
        self._supersede_attempted: set[int] = set()
        self._guild_boot_locks: Dict[int, asyncio.Lock] = {}
        self._startup_complete_guilds: set[int] = set()
        self._boot_owned_messages: Dict[int, Dict[int, int]] = {}
        self._case_locks: Dict[str, asyncio.Lock] = {}

    def _ansi(self, value: str, color: str) -> str:
        return ansi_wrap(value, color)

    def _event_color(self, message: str) -> Optional[str]:
        return event_color(message)

    def _guild_label(self, guild_id: Any, guild: Optional[discord.Guild] = None) -> str:
        g = guild
        if g is None:
            try:
                g = self.bot.get_guild(int(guild_id))
            except Exception:
                g = None
        return guild_value(g)

    def _user_label(self, user_id: Any, guild: Optional[discord.Guild] = None) -> str:
        return user_value(user_id, guild=guild, bot=self.bot)

    def _channel_label(self, channel_id: Any, guild: Optional[discord.Guild] = None) -> str:
        return channel_value(channel_id, guild=guild, bot=self.bot)

    def _message_label(self, message_id: Any) -> str:
        return message_value(message_id)

    def _console_debug(self, event: str, guild: Optional[discord.Guild] = None, **fields: Any) -> None:
        parts: List[str] = []
        if guild is not None:
            parts.append(f"guild={self._guild_label(guild.id, guild)}")
        for key, value in fields.items():
            rendered = value
            try:
                if key == "user":
                    rendered = self._user_label(value, guild=guild)
                elif key in {"channel", "thread", "old_channel", "new_channel"}:
                    rendered = self._channel_label(value, guild=guild)
                elif key in {"message", "old_message", "new_message", "previous"}:
                    rendered = self._message_label(value)
            except Exception:
                rendered = value
            parts.append(f"{key}={rendered}")
        suffix = (" " + " ".join(parts)) if parts else ""
        print(f"[DEBUG][UNVERIFIED] {event}{suffix}")

    async def dlog(self, event: str, guild: Optional[discord.Guild] = None, **fields: Any) -> None:
        if not debug_enabled():
            return
        try:
            self._console_debug(str(event), guild=guild, **fields)
        except Exception:
            pass
        try:
            content = build_log(str(event), guild=guild, bot=self.bot, colorize=True, **fields)
            await post_debug(self.bot, content=content, guild=guild)
        except Exception:
            pass


    async def slog(self, guild: discord.Guild, message: str) -> None:
        try:
            await post_status(self.bot, content=message, guild=guild)
        except Exception:
            pass

    def state(self) -> Dict[str, Any]:
        return load_state()

    def save(self, state: Dict[str, Any]) -> None:
        save_state(state)

    def _cleanup_action_key(self, guild_id: int, user_id: int, action: str, dt: datetime) -> str:
        day = dt.astimezone(timezone.utc).date().isoformat()
        return f"cleanup-action:{int(guild_id)}:{int(user_id)}:{action}:{day}"

    def _cleanup_action_done_today(self, state: Dict[str, Any], guild_id: int, user_id: int, action: str, dt: datetime) -> bool:
        return bool(state.setdefault("invite_temp", {}).get(self._cleanup_action_key(guild_id, user_id, action, dt)))

    def _mark_cleanup_action_today(self, state: Dict[str, Any], guild_id: int, user_id: int, action: str, dt: datetime) -> None:
        state.setdefault("invite_temp", {})[self._cleanup_action_key(guild_id, user_id, action, dt)] = _iso(dt)

    def get_cfg(self, guild_id: int, *, create: bool = True) -> Optional[Dict[str, Any]]:
        state = self.state()
        return _guild_cfg(state, guild_id, create=create)


    def _boot_lock_for(self, guild_id: int) -> asyncio.Lock:
        guild_id = int(guild_id)
        lock = self._guild_boot_locks.get(guild_id)
        if lock is None:
            lock = asyncio.Lock()
            self._guild_boot_locks[guild_id] = lock
        return lock

    def _case_lock_for(self, case_key: str) -> asyncio.Lock:
        key = str(case_key)
        lock = self._case_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._case_locks[key] = lock
        return lock

    def mark_startup_complete(self, guild_id: int) -> None:
        self._startup_complete_guilds.add(int(guild_id))

    def clear_startup_complete(self, guild_id: int) -> None:
        gid = int(guild_id)
        self._startup_complete_guilds.discard(gid)
        self._boot_owned_messages[gid] = {}

    def _get_boot_owned_message(self, guild_id: int, user_id: int) -> Optional[int]:
        return self._boot_owned_messages.get(int(guild_id), {}).get(int(user_id))

    def _set_boot_owned_message(self, guild_id: int, user_id: int, message_id: int) -> None:
        self._boot_owned_messages.setdefault(int(guild_id), {})[int(user_id)] = int(message_id)

    def is_startup_complete(self, guild_id: int) -> bool:
        return int(guild_id) in self._startup_complete_guilds

    def ensure_core_roles(self, guild: discord.Guild, cfg: Dict[str, Any]) -> bool:
        changed = False
        if not cfg.get("unverified_role_id"):
            rid = _autodetect_role_id(guild, "unverified")
            if rid:
                cfg["unverified_role_id"] = rid
                changed = True
        if not cfg.get("verified_role_id"):
            rid = _autodetect_role_id(guild, "verified")
            if rid:
                cfg["verified_role_id"] = rid
                changed = True
        return changed

    def find_case_by_message(self, state: Dict[str, Any], guild_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        for case in state.setdefault("cases", {}).values():
            try:
                if int(case.get("guild_id")) != int(guild_id):
                    continue
            except Exception:
                continue
            mids = [case.get("current_message_id"), case.get("archive_message_id")]
            if any(m and int(m) == int(message_id) for m in mids):
                return _ensure_case_defaults(case)
            for item in _get_dm_requests(case).values():
                try:
                    if int(item.get("message_id")) == int(message_id):
                        return _ensure_case_defaults(case)
                except Exception:
                    pass
        return None


    def find_case_by_embed_user(self, state: Dict[str, Any], guild_id: int, message: discord.Message) -> Optional[Dict[str, Any]]:
        try:
            embeds = list(message.embeds or [])
        except Exception:
            embeds = []
        if not embeds:
            return None

        user_id = None
        for emb in embeds:
            for field in emb.fields:
                if field.name == "User":
                    m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                    if m:
                        user_id = int(m.group(1))
                        break
            if user_id:
                break

        if user_id is None:
            return None
        return self.get_case(state, guild_id, user_id)


    def _extract_user_id_from_case_message(self, message: Optional[discord.Message]) -> Optional[int]:
        if message is None:
            return None
        try:
            embeds = list(message.embeds or [])
        except Exception:
            embeds = []
        for emb in embeds:
            for field in emb.fields:
                if field.name != "User":
                    continue
                match = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                if match:
                    try:
                        return int(match.group(1))
                    except Exception:
                        return None
        return None

    def _extract_status_from_case_message(self, message: Optional[discord.Message], *, fallback: str = "UNVERIFIED") -> str:
        if message is None:
            return fallback
        try:
            embeds = list(message.embeds or [])
        except Exception:
            embeds = []
        if not embeds:
            return fallback
        title = str(embeds[0].title or "").strip()
        if not title:
            return fallback
        head = title.split("•", 1)[0].strip().upper()
        return head or fallback

    async def _find_existing_operational_message(self, guild: discord.Guild, user_id: int) -> Optional[tuple[str, discord.Thread, discord.Message]]:
        threads = await self.get_configured_threads(guild)
        for status in ("UNVERIFIED", "DENIED", "COMPLETED"):
            thread = threads.get(status)
            if not isinstance(thread, discord.Thread):
                continue
            try:
                async for msg in thread.history(limit=200):
                    if msg.author.id != self.bot.user.id or not msg.embeds:
                        continue
                    if self._extract_user_id_from_case_message(msg) != int(user_id):
                        continue
                    return status, thread, msg
            except Exception:
                continue
        return None

    def _rebuild_case_from_operational_message(
        self,
        state: Dict[str, Any],
        guild: discord.Guild,
        status: str,
        thread: discord.Thread,
        msg: discord.Message,
        *,
        member: Optional[discord.Member] = None,
        existing_case: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        user_id = self._extract_user_id_from_case_message(msg)
        if user_id is None:
            raise ValueError("case message does not contain a user id")
        status = str(status or self._extract_status_from_case_message(msg)).upper()
        if status == "COMPLETED":
            status = self._extract_status_from_case_message(msg, fallback="APPROVED")
        now = _utcnow()
        created_at = msg.created_at.astimezone(timezone.utc) if getattr(msg, 'created_at', None) else now
        base = dict(existing_case or {})
        case = _ensure_case_defaults(base)
        case.setdefault("case_id", _next_case_id(state))
        case["guild_id"] = guild.id
        case["user_id"] = int(user_id)
        case["user_name"] = str(member) if member is not None else str(case.get("user_name") or user_id)
        case.setdefault("created_at", _iso(created_at))
        case.setdefault("entered_unverified_at", case.get("created_at") or _iso(created_at))
        case.setdefault("server_join_time", case.get("entered_unverified_at") or _iso(created_at))
        case.setdefault("invite_creator", case.get("invite_creator") or "Unknown")
        case.setdefault("invite_creator_id", case.get("invite_creator_id"))
        if member is not None:
            self._snapshot_case_member(guild, case, member)
        if status in CASE_OPEN:
            case["status"] = status
            case["closed"] = False
            case["current_channel_id"] = thread.id
            case["current_message_id"] = msg.id
            if case.get("archive_channel_id") == thread.id and case.get("archive_message_id") == msg.id:
                case.pop("archive_channel_id", None)
                case.pop("archive_message_id", None)
        else:
            case["status"] = status if status in CASE_FINAL else str(case.get("status") or "APPROVED")
            case["closed"] = True
            case["archive_channel_id"] = thread.id
            case["archive_message_id"] = msg.id
            case.setdefault("handled_at", _iso(created_at))
            case.setdefault("handled_by", "System Reconcile")
            case.setdefault("reason", case.get("reason") or "Recovered from existing completed verification message.")
        history = list(case.get("history") or [])
        event_key = f"recovered:{thread.id}:{msg.id}"
        if not any(str(item.get("event") or "") == event_key for item in history if isinstance(item, dict)):
            history.append({"at": _iso(now), "event": event_key, "by": "startup_reconcile"})
        case["history"] = history
        self.set_case(state, case)
        return case

    def get_case_key_from_message(self, state: Dict[str, Any], guild_id: int, message: Optional[discord.Message]) -> Optional[str]:
        if message is None:
            return None
        case = self.find_case_by_message(state, guild_id, message.id)
        if case is None:
            case = self.find_case_by_embed_user(state, guild_id, message)
        if not case:
            return None
        return _case_key(int(case["guild_id"]), int(case["user_id"]))



    async def cleanup_thread_noise(self, guild: discord.Guild, thread: discord.Thread, thread_name: str) -> None:
        init_messages = []
        case_messages_by_user: Dict[int, list[discord.Message]] = {}
        try:
            async for msg in thread.history(limit=100):
                if msg.author.id != self.bot.user.id:
                    continue
                if msg.embeds:
                    emb = msg.embeds[0]
                    if emb.title == "Status initialized.":
                        init_messages.append(msg)
                        continue
                    uid = None
                    for field in emb.fields:
                        if field.name == "User":
                            m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                            if m:
                                uid = int(m.group(1))
                                break
                    if uid is not None:
                        case_messages_by_user.setdefault(uid, []).append(msg)
        except Exception:
            return

        for old in init_messages[1:]:
            try:
                await old.delete()
            except Exception:
                pass

        for uid, msgs in case_messages_by_user.items():
            if len(msgs) <= 1:
                continue
            msgs = sorted(msgs, key=lambda m: m.created_at or m.edited_at)
            for old in msgs[:-1]:
                try:
                    await old.delete()
                except Exception:
                    pass


    async def _flag_drift(self, guild: discord.Guild, member: Optional[discord.Member], case: Optional[Dict[str, Any]], reason: str) -> None:
        user_id = None
        if member is not None:
            user_id = member.id
        elif case is not None:
            try:
                user_id = int(case.get("user_id"))
            except Exception:
                user_id = None
        state = self.state()
        flag_key = f"drift:{guild.id}:{user_id or 'unknown'}:{reason}"
        temp = state.setdefault("invite_temp", {})
        now = time.time()
        last = float(temp.get(flag_key, 0) or 0)
        if now - last < 6 * 60 * 60:
            return
        temp[flag_key] = now
        self.save(state)
        await self.dlog("flagged drift", guild=guild, user=user_id or "unknown", reason=reason)
        member_label = "unknown"
        if member is not None:
            member_label = f"{member.display_name} ({member.id})"
        elif user_id is not None:
            member_label = str(user_id)
        case_status = str((case or {}).get("status") or "unknown").upper()
        detail = f"unverified drift flagged • user={member_label} • status={case_status} • reason={reason}"
        try:
            await post_status(
                self.bot,
                content=f"@here {detail}",
                guild=guild,
                allowed_mentions=discord.AllowedMentions(everyone=True, roles=False, users=False),
            )
        except Exception:
            await self.dlog("drift status post failed", guild=guild, user=user_id or "unknown", reason=reason)

    def _role_state(self, member: Optional[discord.Member], cfg: Dict[str, Any]) -> str:
        if member is None:
            return "missing_member"
        has_u = _member_has_role(member, cfg.get("unverified_role_id"))
        has_v = _member_has_role(member, cfg.get("verified_role_id"))
        if has_u and has_v:
            return "conflicting"
        if has_u:
            return "unverified"
        if has_v:
            return "verified"
        return "none"

    async def _case_message_exists(self, guild: discord.Guild, case: Dict[str, Any]) -> bool:
        cid = case.get("current_channel_id")
        mid = case.get("current_message_id")
        if not cid or not mid:
            return False
        channel = guild.get_channel(int(cid))
        if not isinstance(channel, discord.Thread):
            channel = await self._resolve_thread_by_id(guild, int(cid))
        if not isinstance(channel, discord.Thread):
            return False
        try:
            await channel.fetch_message(int(mid))
            return True
        except Exception:
            return False

    async def _repair_active_case_alignment(self, guild: discord.Guild, case: Dict[str, Any]) -> bool:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        expected = await self._thread_for_status(guild, cfg, str(case.get("status") or "UNVERIFIED"))
        expected_id = expected.id if isinstance(expected, discord.Thread) else None
        current_id = case.get("current_channel_id")
        message_ok = await self._case_message_exists(guild, case)

        boot_mid = self._get_boot_owned_message(guild.id, int(case.get("user_id")))
        if boot_mid and expected_id:
            if not (case.get("current_message_id") == boot_mid and case.get("current_channel_id") == expected_id):
                case["current_channel_id"] = expected_id
                case["current_message_id"] = boot_mid
                self.set_case(state, case)
                self.save(state)
            try:
                await self._edit_case_message(guild, case)
            except Exception:
                pass
            return False

        if expected_id and current_id == expected_id and message_ok:
            try:
                await self._edit_case_message(guild, case)
            except Exception:
                pass
            return False
        old_channel = case.get("current_channel_id")
        old_message = case.get("current_message_id")
        if old_message or old_channel:
            await self._repost_active_case_message(guild, case)
        else:
            await self._post_case_message(guild, case)
            self.save(state)
        await self.dlog("case realigned", guild=guild, user=case.get("user_id"), status=case.get("status"), old_channel=old_channel, new_channel=case.get("current_channel_id"), old_message=old_message, new_message=case.get("current_message_id"))
        return True

    async def _purge_duplicate_operational_messages(self, guild: discord.Guild) -> None:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        threads = []
        for st in ("UNVERIFIED", "DENIED"):
            th = await self._thread_for_status(guild, cfg, st)
            if isinstance(th, discord.Thread):
                threads.append(th)

        for thread in threads:
            latest_by_user: Dict[int, discord.Message] = {}
            dups: List[discord.Message] = []
            try:
                async for msg in thread.history(limit=200):
                    if msg.author.id != self.bot.user.id or not msg.embeds:
                        continue
                    emb = msg.embeds[0]
                    if emb.title == "Status initialized.":
                        continue
                    uid = None
                    for field in emb.fields:
                        if field.name == "User":
                            m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                            if m:
                                uid = int(m.group(1))
                                break
                    if uid is None:
                        continue
                    prev = latest_by_user.get(uid)
                    if prev is None:
                        latest_by_user[uid] = msg
                    else:
                        prev_time = prev.created_at or prev.edited_at
                        msg_time = msg.created_at or msg.edited_at
                        if msg_time and prev_time and msg_time > prev_time:
                            latest_by_user[uid] = msg
                            dups.append(prev)
                        else:
                            dups.append(msg)
            except Exception:
                continue

            for uid, msg in latest_by_user.items():
                case = self.get_case(state, guild.id, uid)
                if case and (case.get("current_message_id") != msg.id or case.get("current_channel_id") != thread.id):
                    case["current_message_id"] = msg.id
                    case["current_channel_id"] = thread.id
                    self.set_case(state, case)
            self.save(state)
            for dup in dups:
                try:
                    await dup.delete()
                except Exception:
                    pass

    async def _cleanup_stale_operational_messages(self, guild: discord.Guild) -> None:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        changed = False
        for status in ("UNVERIFIED", "DENIED"):
            thread = await self._thread_for_status(guild, cfg, status)
            if not isinstance(thread, discord.Thread):
                continue
            try:
                async for msg in thread.history(limit=200):
                    if msg.author.id != self.bot.user.id or not msg.embeds:
                        continue
                    emb = msg.embeds[0]
                    if emb.title == "Status initialized.":
                        continue
                    uid = None
                    for field in emb.fields:
                        if field.name == "User":
                            m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                            if m:
                                uid = int(m.group(1))
                                break
                    if uid is None:
                        continue

                    case = self.get_case(state, guild.id, uid)
                    keep_live = bool(
                        case
                        and not case.get("closed")
                        and str(case.get("status") or "") == status
                        and int(case.get("current_channel_id") or 0) == int(thread.id)
                        and int(case.get("current_message_id") or 0) == int(msg.id)
                    )
                    if keep_live:
                        continue

                    if case and case.get("closed") and (case.get("current_message_id") or case.get("current_channel_id")):
                        case.pop("current_message_id", None)
                        case.pop("current_channel_id", None)
                        self.set_case(state, case)
                        changed = True

                    try:
                        await msg.delete()
                        await self.dlog("stale operational message removed", guild=guild, user=uid, thread=_thread_name_for_status(status), message=msg.id)
                    except Exception:
                        try:
                            await self._mark_message_superseded(msg, "This message is stale and no longer actionable.")
                        except Exception:
                            pass
            except Exception:
                continue

        if changed:
            self.save(state)

    async def validate_case_for_guild(self, guild: discord.Guild, member: discord.Member, case: Optional[Dict[str, Any]], *, source: str) -> str:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        role_state = self._role_state(member, cfg)

        if role_state == "conflicting":
            await self._flag_drift(guild, member, case, "conflicting_roles")
            return "flagged"

        if role_state == "none":
            if case and (not case.get("closed") or case.get("status") in CASE_OPEN):
                await self._flag_drift(guild, member, case, "no_resolution_roles")
                return "flagged"
            return "skip"

        if role_state == "verified":
            if case and (not case.get("closed") or case.get("status") in CASE_OPEN):
                case["handled_by"] = "System Validate"
                case["handled_by_id"] = self.bot.user.id if self.bot.user else None
                case["handled_at"] = _iso(_utcnow())
                case["reason"] = f"Validated from live Discord state ({source})"
                await self._finalize_case(state, guild, case, final_status="APPROVED", notify_text="This verification case has already been handled by staff. No further action can be taken from this message.")
                self.save(state)
                await self.dlog("case auto-finalized", guild=guild, user=member, status="APPROVED", source=source)
                return "finalized"
            if case and case.get("closed") and case.get("status") not in {"APPROVED"}:
                await self._flag_drift(guild, member, case, f"verified_conflicts_with_{str(case.get('status') or 'unknown').lower()}")
                return "flagged"
            return "ok"

        # unverified state
        live_case = case
        if live_case is None:
            recovered = await self._find_existing_operational_message(guild, member.id)
            if recovered is not None:
                recovered_status, recovered_thread, recovered_msg = recovered
                state = self.state()
                live_case = self._rebuild_case_from_operational_message(
                    state,
                    guild,
                    recovered_status,
                    recovered_thread,
                    recovered_msg,
                    member=member,
                )
                self.save(state)
            else:
                live_case = await self.create_or_get_case(member, source="reconcile")
        if str(live_case.get("status") or "UNVERIFIED") not in CASE_OPEN:
            await self._flag_drift(guild, member, live_case, "unexpected_non_open_active_case")
            return "flagged"
        await self._repair_active_case_alignment(guild, live_case)
        return "active"

    async def validate_cases_for_guild(self, guild: discord.Guild, *, source: str) -> Dict[str, int]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return {"validated": 0, "active": 0, "finalized": 0, "flagged": 0}

        validated = active = finalized = flagged = 0
        for member in guild.members:
            if member.bot:
                continue
            case = self.get_case(state, guild.id, member.id)
            role_state = self._role_state(member, cfg)
            if role_state == "none" and case is None:
                continue
            result = await self.validate_case_for_guild(guild, member, case, source=source)
            validated += 1
            if result == "active":
                active += 1
            elif result == "finalized":
                finalized += 1
            elif result == "flagged":
                flagged += 1
            state = self.state()
        await self.dlog("validation pass", guild=guild, source=source, validated=validated, active=active, finalized=finalized, flagged=flagged)
        return {"validated": validated, "active": active, "finalized": finalized, "flagged": flagged}

    async def _mark_message_superseded(self, msg: discord.Message, note: str) -> None:
        try:
            if msg.id in self._supersede_attempted:
                return
            if not msg.embeds:
                return
            emb = msg.embeds[0].copy()
            title = emb.title or "Verification case"
            names = [f.name for f in emb.fields]
            current_note = None
            if "Superseded" in names:
                idx = names.index("Superseded")
                current_note = emb.fields[idx].value
            already_superseded = title.startswith("SUPERSEDED • ")
            already_archived = emb.color == discord.Color.orange()
            if already_superseded and already_archived and current_note == note:
                self._supersede_attempted.add(msg.id)
                return
            if not already_superseded:
                emb.title = f"SUPERSEDED • {title}"
            emb.color = discord.Color.orange()
            if "Superseded" in names:
                idx = names.index("Superseded")
                emb.set_field_at(idx, name="Superseded", value=note, inline=False)
            else:
                emb.add_field(name="Superseded", value=note, inline=False)
            self._supersede_attempted.add(msg.id)
            await msg.edit(embed=emb, view=None)
        except Exception:
            pass

    async def reconcile_cases_for_guild(self, guild: discord.Guild) -> None:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if not cfg:
            return

        urole_id = cfg.get("unverified_role_id")
        threads = await self.get_configured_threads(guild)

        for status, thread in threads.items():
            if not isinstance(thread, discord.Thread):
                continue

            latest_by_user: Dict[int, discord.Message] = {}
            duplicates: List[discord.Message] = []

            try:
                async for msg in thread.history(limit=200):
                    if msg.author.id != self.bot.user.id or not msg.embeds:
                        continue
                    emb = msg.embeds[0]
                    if emb.title == "Status initialized.":
                        continue

                    uid = None
                    for field in emb.fields:
                        if field.name == "User":
                            m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                            if m:
                                uid = int(m.group(1))
                                break
                    if uid is None:
                        continue

                    prev = latest_by_user.get(uid)
                    if prev is None:
                        latest_by_user[uid] = msg
                    else:
                        prev_time = prev.created_at or prev.edited_at
                        msg_time = msg.created_at or msg.edited_at
                        if msg_time and prev_time and msg_time > prev_time:
                            latest_by_user[uid] = msg
                            duplicates.append(prev)
                        else:
                            duplicates.append(msg)
            except Exception:
                continue

            changed = False
            for uid, msg in latest_by_user.items():
                member = guild.get_member(uid)
                case = self.get_case(state, guild.id, uid)
                member_is_unverified = member is not None and _member_has_role(member, urole_id)

                if status == "COMPLETED":
                    if member_is_unverified:
                        await self._mark_message_superseded(msg, "User re-entered active verification workflow.")
                    if case and (case.get("archive_message_id") != msg.id or case.get("archive_channel_id") != thread.id):
                        case["archive_message_id"] = msg.id
                        case["archive_channel_id"] = thread.id
                        self.set_case(state, case)
                        changed = True
                    continue

                if status == "DENIED" and member_is_unverified and case and str(case.get("status") or "") == "UNVERIFIED" and not case.get("closed"):
                    await self._mark_message_superseded(msg, "User re-entered active verification workflow.")
                    if case.get("archive_message_id") != msg.id or case.get("archive_channel_id") != thread.id:
                        case["archive_message_id"] = msg.id
                        case["archive_channel_id"] = thread.id
                        self.set_case(state, case)
                        changed = True
                    continue

                if not case:
                    continue

                if case.get("closed"):
                    await self._mark_message_superseded(msg, "This message belongs to a case that has already been completed.")
                    archive_changed = False
                    if case.get("archive_message_id") != msg.id or case.get("archive_channel_id") != thread.id:
                        case["archive_message_id"] = msg.id
                        case["archive_channel_id"] = thread.id
                        archive_changed = True
                    if archive_changed:
                        self.set_case(state, case)
                        changed = True
                    continue

                owns_different_active = bool(case.get("current_message_id") and case.get("current_channel_id") and (case.get("current_message_id") != msg.id or case.get("current_channel_id") != thread.id))

                if owns_different_active:
                    # Historical message for the same user; never let thread scans steal ownership from a known active case.
                    if case.get("archive_message_id") != msg.id or case.get("archive_channel_id") != thread.id:
                        case["archive_message_id"] = msg.id
                        case["archive_channel_id"] = thread.id
                        self.set_case(state, case)
                        changed = True
                    continue

                if case.get("current_message_id") != msg.id or case.get("current_channel_id") != thread.id:
                    case["current_message_id"] = msg.id
                    case["current_channel_id"] = thread.id
                    changed = True
                if case.get("status") != status or case.get("closed"):
                    case["status"] = status
                    case["closed"] = False
                    case.pop("handled_by", None)
                    case.pop("handled_at", None)
                    case.pop("reason", None)
                    changed = True
                if changed:
                    self.set_case(state, case)

            if changed:
                self.save(state)

            for dup in duplicates:
                try:
                    await dup.delete()
                except Exception:
                    pass
    def get_case(self, state: Dict[str, Any], guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        return _ensure_case_defaults(state.setdefault("cases", {}).get(_case_key(guild_id, user_id), {})) if _case_key(guild_id, user_id) in state.setdefault("cases", {}) else None

    def set_case(self, state: Dict[str, Any], case: Dict[str, Any]) -> str:
        key = _case_key(int(case["guild_id"]), int(case["user_id"]))
        state.setdefault("cases", {})[key] = case
        return key

    async def ensure_admin(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None or not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
            if interaction.response.is_done():
                await interaction.followup.send("Admins only.", ephemeral=True)
            else:
                await interaction.response.send_message("Admins only.", ephemeral=True)
            return False
        return True

    async def ensure_forum_board(self, guild: discord.Guild) -> Dict[str, Any]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id)
        assert cfg is not None
        if self.ensure_core_roles(guild, cfg):
            self.save(state)

        try:
            forum = await get_or_create_ops_forum(guild)
        except Exception:
            forum = None
        if not isinstance(forum, discord.ForumChannel):
            return cfg

        tags = list(forum.available_tags)
        by_name = {t.name: t for t in tags}
        changed = False
        for name in (ENABLED_TAG, DISABLED_TAG):
            if name not in by_name:
                tags.append(discord.ForumTag(name=name, emoji=None, moderated=False))
                changed = True
        if changed:
            await forum.edit(available_tags=tags)
            refreshed = guild.get_channel(forum.id)
            if isinstance(refreshed, discord.ForumChannel):
                forum = refreshed

        tags_by_name = {t.name: t for t in forum.available_tags}
        status_tag = tags_by_name[ENABLED_TAG if cfg.get("enabled") else DISABLED_TAG]

        async def ensure_thread(name: str, key: str) -> tuple[int, bool]:
            tid = cfg.get(key)
            thread = await self._resolve_thread_by_id(guild, tid)
            if isinstance(thread, discord.Thread):
                try:
                    await thread.edit(applied_tags=[status_tag])
                except Exception:
                    pass
                return thread.id, False

            existing_threads = list(getattr(forum, "threads", [])) + [
                t for t in guild.threads if getattr(t, "parent_id", None) == forum.id
            ]
            for th in existing_threads:
                if isinstance(th, discord.Thread) and th.name == name:
                    cfg[key] = th.id
                    try:
                        await th.edit(applied_tags=[status_tag])
                    except Exception:
                        pass
                    return th.id, False

            created = await forum.create_thread(
                name=name,
                content=f"{name} queue for verification cases.",
                applied_tags=[status_tag],
            )
            thread = created.thread if hasattr(created, "thread") else created[0] if isinstance(created, tuple) else created
            cfg[key] = thread.id
            return thread.id, True

        ids = {
            "UNVERIFIED": await ensure_thread(THREAD_UNVERIFIED, "thread_unverified_id"),
            "DENIED": await ensure_thread(THREAD_DENIED, "thread_denied_id"),
            "COMPLETED": await ensure_thread(THREAD_COMPLETED, "thread_completed_id"),
        }
        self.save(state)

        for name, (tid, created_now) in ids.items():
            th = await self._resolve_thread_by_id(guild, tid)
            if isinstance(th, discord.Thread):
                await self.cleanup_thread_noise(guild, th, name)
                if created_now:
                    try:
                        await th.send(
                            embed=discord.Embed(
                                title="Status initialized.",
                                description=f"I found and connected this {name} thread.",
                                color=discord.Color.green(),
                            ),
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                    except Exception:
                        pass

        return cfg

    async def _resolve_thread_by_id(self, guild: discord.Guild, thread_id: Optional[int]) -> Optional[discord.Thread]:
        if not thread_id:
            return None
        tid = int(thread_id)

        th = guild.get_thread(tid)
        if isinstance(th, discord.Thread):
            return th

        ch = guild.get_channel(tid)
        if isinstance(ch, discord.Thread):
            return ch

        try:
            fetched = await guild.fetch_channel(tid)
        except Exception:
            fetched = None
        return fetched if isinstance(fetched, discord.Thread) else None

    async def get_configured_threads(self, guild: discord.Guild) -> Dict[str, Optional[discord.Thread]]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        return {
            "UNVERIFIED": await self._resolve_thread_by_id(guild, cfg.get("thread_unverified_id")),
            "DENIED": await self._resolve_thread_by_id(guild, cfg.get("thread_denied_id")),
            "COMPLETED": await self._resolve_thread_by_id(guild, cfg.get("thread_completed_id")),
        }

    async def _thread_for_status(self, guild: discord.Guild, cfg: Dict[str, Any], status: str) -> Optional[discord.Thread]:
        key = {
            "UNVERIFIED": "thread_unverified_id",
            "DENIED": "thread_denied_id",
        }.get(status, "thread_completed_id")
        return await self._resolve_thread_by_id(guild, cfg.get(key))

    def _extract_identity_id(self, raw: Any) -> Optional[int]:
        if raw is None:
            return None
        if isinstance(raw, int):
            return int(raw)
        if isinstance(raw, dict):
            for key in ("id", "user_id", "member_id", "inviter_id", "handled_by_id"):
                value = raw.get(key)
                if value is None:
                    continue
                try:
                    return int(value)
                except Exception:
                    continue
            raw = raw.get("name") or raw.get("display") or raw.get("value")
        text = str(raw).strip()
        if not text:
            return None
        if text.isdigit():
            try:
                return int(text)
            except Exception:
                return None
        for pattern in (r"^<@!?(\d{15,22})>$", r"\((\d{15,22})\)$"):
            match = re.search(pattern, text)
            if match:
                try:
                    return int(match.group(1))
                except Exception:
                    return None
        return None


    def _extract_identity_name(self, raw: Any) -> Optional[str]:
        if raw is None:
            return None
        if isinstance(raw, dict):
            for key in ("name", "display", "value", "user_name", "display_name"):
                value = raw.get(key)
                if value:
                    return str(value)
        text = str(raw).strip()
        return text or None

    def _render_identity_value(self, guild: discord.Guild, raw: Any, *, fallback: str = "Unknown", bot_for_system: bool = False) -> str:
        system_labels = {"System Validate", "System Cleanup", "System Reconcile"}
        if bot_for_system and str(raw) in system_labels and self.bot.user is not None:
            return self.bot.user.mention
        user_id = self._extract_identity_id(raw)
        if user_id:
            return f"<@{user_id}>"
        if isinstance(raw, dict):
            text = str(raw.get("name") or raw.get("display") or raw.get("value") or "").strip()
        else:
            text = str(raw or "").strip()
        return text or fallback


    def _case_target_label(self, guild: discord.Guild, case: Dict[str, Any]) -> str:
        member = guild.get_member(int(case.get("user_id", 0))) if guild is not None and case.get("user_id") else None
        if member is not None:
            return member.display_name
        raw = str(case.get("snapshot_display_name") or case.get("user_name") or case.get("display_name") or case.get("username") or case.get("user_id") or "user").strip()
        return raw

    def _render_voice_context(self, guild: discord.Guild, lines: List[Any]) -> str:
        rendered: List[str] = []
        for line in list(lines or [])[-10:]:
            user_id = self._extract_identity_id(line)
            if user_id:
                rendered.append(f"<@{user_id}>")
            else:
                text = str(line).strip()
                if text:
                    rendered.append(text)
        return "\n".join(rendered)[:1024] if rendered else "Unknown"

    def _snapshot_case_member(self, guild: discord.Guild, case: Dict[str, Any], member: Optional[discord.Member] = None) -> None:
        if member is None:
            try:
                member = guild.get_member(int(case.get("user_id", 0)))
            except Exception:
                member = None
        if member is None:
            return
        frozen_name = getattr(member, "nick", None) or getattr(member, "display_name", None) or getattr(member, "name", None) or str(member)
        case["snapshot_display_name"] = str(frozen_name)
        case["snapshot_nick"] = str(getattr(member, "nick", None) or "")
        case["snapshot_user_name"] = str(member)
        case["snapshot_avatar_url"] = str(member.display_avatar.url)
        case["snapshot_mention"] = member.mention

    def _build_dm_pending_embed(self, guild: discord.Guild, member: Optional[discord.Member], case: Dict[str, Any], channel_name: str) -> discord.Embed:
        name_value = getattr(member, "nick", None) or getattr(member, "display_name", None) or case.get("snapshot_display_name") or case.get("user_name") or "Unknown"
        invite_value = self._render_identity_value(guild, {"id": case.get("invite_creator_id"), "name": case.get("invite_creator")}, fallback="Unknown")
        embed = discord.Embed(
            title="Verification Request",
            description="An unverified member is in voice with you, a verifier.",
            color=discord.Color(0xFFD700),
        )
        try:
            if member is not None:
                embed.set_thumbnail(url=member.display_avatar.url)
            elif case.get("snapshot_avatar_url"):
                embed.set_thumbnail(url=str(case.get("snapshot_avatar_url")))
        except Exception:
            pass
        embed.add_field(name="Name", value=str(name_value)[:1024] or "Unknown", inline=False)
        embed.add_field(name="Server Join Time", value=_fmt_ts(case.get("server_join_time")), inline=False)
        embed.add_field(name="Invite Creator", value=str(invite_value)[:1024] if invite_value is not None else "Unknown", inline=False)
        embed.set_footer(text=f"Channel: {channel_name}"[:2048])
        return embed

    def _build_dm_resolution_embed(self, kind: str) -> discord.Embed:
        kind = str(kind or "handled").lower()
        if kind == "approved":
            return discord.Embed(
                title="Verification Approved",
                description=(
                    "You approved this verification request.\n\n"
                    "No action is need from you, thank you."
                ),
                color=discord.Color.green(),
            )
        if kind == "denied":
            return discord.Embed(
                title="Verification Denied",
                description=(
                    "You denied this verification request.\n\n"
                    "They remain unverified, and the case has been updated."
                ),
                color=discord.Color.red(),
            )
        if kind == "expired":
            return discord.Embed(
                title="Verification Request Expired",
                description=(
                    "This request timed out before anyone acted on it.\n\n"
                    "They can rejoin voice and try again later."
                ),
                color=discord.Color.orange(),
            )
        return discord.Embed(
            title="Verification Handled",
            description=(
                "Another verifier already handled this request.\n\n"
                "No action is need from you, thank you."
            ),
            color=discord.Color.light_grey(),
        )

    async def _edit_dm_interaction_message(self, interaction: discord.Interaction, embed: discord.Embed) -> None:
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(content=None, embed=embed, view=None)
                return
        except Exception:
            pass
        try:
            if interaction.message is not None:
                await interaction.message.edit(content=None, embed=embed, view=None)
        except Exception:
            pass

    async def _apply_dm_resolution(self, state: Dict[str, Any], guild_id: int, user_id: int, *, outcome: Optional[str] = None, actor_id: Optional[int] = None, interaction: Optional[discord.Interaction] = None, expired: bool = False) -> None:
        case = self.get_case(state, guild_id, user_id)
        if not case:
            return
        requests = _get_dm_requests(case)
        for key, item in requests.items():
            try:
                verifier_id = int(item.get("verifier_id") or key)
            except Exception:
                continue
            if expired:
                embed = self._build_dm_resolution_embed("expired")
                item["status"] = "expired"
            elif actor_id is not None and verifier_id == int(actor_id) and outcome:
                embed = self._build_dm_resolution_embed(outcome)
                item["status"] = "handled"
            else:
                embed = self._build_dm_resolution_embed("handled")
                item["status"] = "handled"
            item["resolved_at"] = _iso(_utcnow())
            try:
                if interaction is not None and interaction.message is not None and int(item.get("message_id")) == int(interaction.message.id) and verifier_id == int(actor_id or 0):
                    await self._edit_dm_interaction_message(interaction, embed)
                    continue
            except Exception:
                pass
            try:
                chan = await self.bot.fetch_channel(int(item["channel_id"]))
                if isinstance(chan, discord.DMChannel):
                    msg = await chan.fetch_message(int(item["message_id"]))
                    await msg.edit(content=None, embed=embed, view=None)
            except Exception:
                continue
        _set_dm_requests(case, requests)
        self.set_case(state, case)
        self.save(state)

    async def _build_embed(self, guild: discord.Guild, case: Dict[str, Any]) -> discord.Embed:
        member = guild.get_member(int(case["user_id"]))
        if member is not None:
            self._snapshot_case_member(guild, case, member)
        title_name = str(case.get("snapshot_display_name") or getattr(member, "nick", None) or getattr(member, "display_name", None) or case.get("user_name") or case.get("user_id") or "user")
        title = f"{_status_line(case)} • {title_name} ({case['user_id']})"
        desc = "Verification case record."
        embed = discord.Embed(title=title[:256], description=desc, color=self._color_for_status(case.get("status", "UNVERIFIED"), warned=bool(case.get("warned"))))
        try:
            if member is not None:
                embed.set_thumbnail(url=member.display_avatar.url)
            elif case.get("snapshot_avatar_url"):
                embed.set_thumbnail(url=str(case.get("snapshot_avatar_url")))
        except Exception:
            pass
        embed.add_field(name="User", value=f"<@{case['user_id']}>\nID: `{case['user_id']}`", inline=False)
        embed.add_field(name="Server Join Time", value=_fmt_ts(case.get("server_join_time")), inline=False)
        inviter = case.get("invite_creator") or "Unknown"
        embed.add_field(name="Invite Creator", value=self._render_identity_value(guild, inviter, fallback="Unknown"), inline=False)
        embed.add_field(name="Warned", value="Yes" if case.get("warned") else "No", inline=True)
        embed.add_field(name="Cleanup Strikes", value=str(case.get("cleanup_strikes", 0)), inline=True)
        if case.get("voice_context"):
            embed.add_field(name="Recent Voice Context", value=self._render_voice_context(guild, case.get("voice_context", [])), inline=False)
        if case.get("reason"):
            label = "Reason"
            if case.get("status") == "DENIED":
                label = "Reason of Denied"
            embed.add_field(name=label, value=str(case.get("reason"))[:1024], inline=False)
        if case.get("handled_by"):
            embed.add_field(name="Handled By", value=self._render_identity_value(guild, {"id": case.get("handled_by_id"), "name": case.get("handled_by")}, fallback=str(case.get("handled_by")), bot_for_system=True), inline=False)
        if case.get("handled_at"):
            embed.add_field(name="Handled At", value=_fmt_ts(case.get("handled_at")), inline=False)
        if case.get("dm_notice"):
            embed.add_field(name="User Notified", value=str(case.get("dm_notice")), inline=True)
        if case.get("cleanup_invite"):
            embed.add_field(name="Rejoin Invite Sent", value="Yes", inline=True)
        footer_parts = []
        if case.get("closed"):
            footer_parts.append("Closed")
        footer_parts.append(f"Case ID: {case.get('case_id')}")
        embed.set_footer(text=" • ".join(footer_parts))
        return embed

    def _color_for_status(self, status: str, warned: bool = False) -> discord.Color:
        if status == "APPROVED":
            return discord.Color.green()
        if status == "DENIED":
            return discord.Color.red()
        if status in {"KICKED", "BANNED"}:
            return discord.Color.dark_red()
        if status == "EXPIRED" or warned:
            return discord.Color.orange()
        return discord.Color.blurple()


    async def _delete_case_message(self, guild: discord.Guild, case: Dict[str, Any]) -> bool:
        cid = case.get("current_channel_id")
        mid = case.get("current_message_id")
        if not cid or not mid:
            return False
        channel = await self._resolve_thread_by_id(guild, int(cid))
        if not isinstance(channel, discord.Thread):
            return False
        try:
            msg = await channel.fetch_message(int(mid))
        except Exception:
            return False
        try:
            await msg.edit(embed=await self._build_embed(guild, case), view=None)
        except Exception:
            pass
        try:
            await msg.delete()
            return True
        except Exception:
            return False

    async def _repost_active_case_message(self, guild: discord.Guild, case: Dict[str, Any]) -> None:
        old_snapshot = dict(case)
        case.pop("current_channel_id", None)
        case.pop("current_message_id", None)
        await self._post_case_message(guild, case)
        state = self.state()
        self.set_case(state, case)
        self.save(state)
        await self._delete_case_message(guild, old_snapshot)

    async def _edit_case_message(self, guild: discord.Guild, case: Dict[str, Any]) -> None:
        cid = case.get("current_channel_id")
        mid = case.get("current_message_id")
        if not cid or not mid:
            return
        channel = guild.get_channel(int(cid))
        if not isinstance(channel, discord.Thread):
            return
        try:
            msg = await channel.fetch_message(int(mid))
        except Exception:
            return
        view: discord.ui.View | None = None
        if not case.get("closed"):
            view = AdminActionView(self, _case_key(guild.id, int(case["user_id"])), str(case.get("status") or "UNVERIFIED"))
        try:
            await msg.edit(embed=await self._build_embed(guild, case), view=view)
        except Exception:
            pass

    async def _post_case_message(self, guild: discord.Guild, case: Dict[str, Any]) -> None:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if cfg is None:
            raise RuntimeError("Unverified config missing")
        status = str(case.get("status") or "UNVERIFIED")
        thread = await self._thread_for_status(guild, cfg, status)
        if thread is None:
            cfg = await self.ensure_forum_board(guild)
            state = self.state()
            cfg = _guild_cfg(state, guild.id, create=False) or cfg
            thread = await self._thread_for_status(guild, cfg, status)
        if thread is None:
            raise RuntimeError("Verification thread missing")
        view: discord.ui.View | None = None
        if not case.get("closed"):
            view = AdminActionView(self, _case_key(guild.id, int(case["user_id"])), str(case.get("status") or "UNVERIFIED"))
        msg = await thread.send(embed=await self._build_embed(guild, case), view=view, allowed_mentions=discord.AllowedMentions.none())
        case["current_channel_id"] = thread.id
        case["current_message_id"] = msg.id
        if not self.is_startup_complete(guild.id):
            try:
                self._set_boot_owned_message(guild.id, int(case.get("user_id")), int(msg.id))
            except Exception:
                pass
        self.set_case(state, case)
        self.save(state)

    async def create_or_get_case(self, member: discord.Member, source: str = "live_join") -> Dict[str, Any]:
        state = self.state()
        cfg = _guild_cfg(state, member.guild.id)
        assert cfg is not None
        key = _case_key(member.guild.id, member.id)
        existing = state.setdefault("cases", {}).get(key)
        entered_now = _utcnow()
        entered_unverified_at = (
            member.joined_at.astimezone(timezone.utc)
            if source == "live_join" and member.joined_at
            else entered_now
        )
        if existing:
            case = _ensure_case_defaults(existing)
            changed = False
            if case.get("user_name") != str(member):
                case["user_name"] = str(member)
                changed = True

            if case.get("closed") and case.get("status") not in CASE_OPEN:
                note = "User re-entered active verification workflow."
                archive_channel_id = existing.get("archive_channel_id") or existing.get("current_channel_id")
                archive_message_id = existing.get("archive_message_id") or existing.get("current_message_id")
                if archive_channel_id and archive_message_id:
                    try:
                        th = await self._resolve_thread_by_id(member.guild, int(archive_channel_id))
                        if isinstance(th, discord.Thread):
                            old_msg = await th.fetch_message(int(archive_message_id))
                            await self._mark_message_superseded(old_msg, note)
                    except Exception:
                        pass
                invite_meta = self._consume_invite_temp(state, member.guild.id, member.id) or "Unknown"
                invite_creator = invite_meta.get("name") if isinstance(invite_meta, dict) else invite_meta
                invite_creator_id = invite_meta.get("id") if isinstance(invite_meta, dict) else None
                case = _ensure_case_defaults({
                    "case_id": _next_case_id(state),
                    "guild_id": member.guild.id,
                    "user_id": member.id,
                    "user_name": str(member),
                    "status": "UNVERIFIED",
                    "created_at": _iso(entered_now),
                    "entered_unverified_at": _iso(entered_unverified_at),
                    "server_join_time": _iso(member.joined_at.astimezone(timezone.utc)) if member.joined_at else _iso(entered_now),
                    "invite_creator": invite_creator or "Unknown",
                    "invite_creator_id": invite_creator_id,
                    "warned": False,
                    "cleanup_strikes": int(state.setdefault("invite_temp", {}).get(f"cleanup-strikes:{member.guild.id}:{member.id}", 0) or 0),
                    "pending_dms": [],
                    "dm_requests": {},
                    "voice_context": [],
                    "history": list(existing.get("history", [])) + [{"at": _iso(entered_now), "event": "re-entered", "by": source}],
                    "closed": False,
                })
                self.set_case(state, case)
                self.save(state)
                await self.dlog("case created", guild=member.guild, user=member)
                await self._post_case_message(member.guild, case)
                self.save(state)
                return case

            if not case.get("entered_unverified_at"):
                case["entered_unverified_at"] = _iso(entered_unverified_at)
                changed = True

            if changed:
                self.set_case(state, case)
                self.save(state)
            if case.get("current_message_id") and case.get("current_channel_id"):
                return case
            return case

        recovered = await self._find_existing_operational_message(member.guild, member.id)
        if recovered is not None:
            recovered_status, recovered_thread, recovered_msg = recovered
            case = self._rebuild_case_from_operational_message(
                state,
                member.guild,
                recovered_status,
                recovered_thread,
                recovered_msg,
                member=member,
            )
            self.save(state)
            return case

        invite_meta = self._consume_invite_temp(state, member.guild.id, member.id) or "Unknown"
        invite_creator = invite_meta.get("name") if isinstance(invite_meta, dict) else invite_meta
        invite_creator_id = invite_meta.get("id") if isinstance(invite_meta, dict) else None
        case = _ensure_case_defaults({
            "case_id": _next_case_id(state),
            "guild_id": member.guild.id,
            "user_id": member.id,
            "user_name": str(member),
            "status": "UNVERIFIED",
            "created_at": _iso(entered_now),
            "entered_unverified_at": _iso(entered_unverified_at),
            "server_join_time": _iso(member.joined_at.astimezone(timezone.utc)) if member.joined_at else _iso(entered_now),
            "invite_creator": invite_creator or "Unknown",
            "invite_creator_id": invite_creator_id,
            "warned": False,
            "cleanup_strikes": int(state.setdefault("invite_temp", {}).get(f"cleanup-strikes:{member.guild.id}:{member.id}", 0) or 0),
            "pending_dms": [],
            "dm_requests": {},
            "voice_context": [],
            "history": [],
            "closed": False,
        })
        self.set_case(state, case)
        self.save(state)
        await self.dlog("case created", guild=member.guild, user=member)
        await self._post_case_message(member.guild, case)
        self.save(state)
        return case

    def _consume_invite_temp(self, state: Dict[str, Any], guild_id: int, user_id: int) -> Optional[Any]:
        temp = state.setdefault("invite_temp", {})
        data = temp.pop(f"invite:{guild_id}:{user_id}", None)
        return data

    def _voice_context_from_channel(self, subject_user_id: int, channel: discord.VoiceChannel | discord.StageChannel) -> List[Dict[str, Any]]:
        context: List[Dict[str, Any]] = []
        seen: set[int] = set()
        for m in list(getattr(channel, "members", []) or []):
            try:
                mid = int(m.id)
            except Exception:
                continue
            if getattr(m, "bot", False) or mid == int(subject_user_id) or mid in seen:
                continue
            seen.add(mid)
            context.append({"name": getattr(m, "display_name", None) or str(m), "id": mid})
        return context[:10]

    async def update_voice_context(self, guild: discord.Guild, user_id: int, lines: List[Any]) -> None:
        state = self.state()
        case = self.get_case(state, guild.id, user_id)
        if not case:
            return
        merged: List[Any] = []
        seen: set[int] = set()
        for item in list(lines or []) + list(case.get("voice_context") or []):
            user_id_value = self._extract_identity_id(item)
            if user_id_value is not None:
                if user_id_value in seen or int(user_id_value) == int(user_id):
                    continue
                seen.add(int(user_id_value))
                merged.append({"id": int(user_id_value), "name": self._extract_identity_name(item) or str(user_id_value)})
            else:
                text_value = str(item).strip()
                if text_value and text_value not in merged:
                    merged.append(text_value)
        case["voice_context"] = merged[:10]
        self.set_case(state, case)
        self.save(state)
        await self._edit_case_message(guild, case)

    async def send_verifier_dms(self, member: discord.Member, channel: discord.VoiceChannel | discord.StageChannel) -> None:
        state = self.state()
        cfg = _guild_cfg(state, member.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        verifier_role_ids = cfg.get("verifier_role_ids") or ([cfg.get("verifier_role_id")] if cfg.get("verifier_role_id") else [])
        if not verifier_role_ids:
            return
        case = await self.create_or_get_case(member)
        if case.get("status") != "UNVERIFIED" or case.get("closed"):
            return
        verifiers = [m for m in channel.members if not m.bot and _member_has_any_role(m, verifier_role_ids)]
        if not verifiers:
            return
        self._snapshot_case_member(member.guild, case, member)
        context = [{"name": m.display_name, "id": m.id} for m in channel.members if not m.bot]
        case["voice_context"] = context[:10]
        case["last_voice_request_at"] = _iso(_utcnow())
        requests = _get_dm_requests(case)
        self.set_case(state, case)
        self.save(state)
        await self._edit_case_message(member.guild, case)
        expiry = _utcnow() + timedelta(minutes=int(cfg.get("dm_expiry_minutes", 2)))
        sent = 0
        for verifier in verifiers:
            if str(verifier.id) in requests:
                continue
            try:
                dm = verifier.dm_channel or await verifier.create_dm()
                embed = self._build_dm_pending_embed(member.guild, member, case, channel.name)
                msg = await dm.send(
                    content=None,
                    embed=embed,
                    view=DMActionView(self, _case_key(member.guild.id, member.id)),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                requests[str(verifier.id)] = {
                    "verifier_id": verifier.id,
                    "channel_id": dm.id,
                    "message_id": msg.id,
                    "expires_at": _iso(expiry),
                    "status": "pending",
                }
                sent += 1
            except Exception:
                continue
        _set_dm_requests(case, requests)
        case["voice_context"] = context[:10]
        self.set_case(state, case)
        self.save(state)
        await self.dlog("dm requests sent", guild=member.guild, user=member, count=sent)
        if case.get("pending_dms"):
            task_key = _case_key(member.guild.id, member.id)
            old = self._dm_expiry_tasks.pop(task_key, None)
            if old:
                old.cancel()
            self._dm_expiry_tasks[task_key] = asyncio.create_task(self._expire_dms_later(member.guild.id, member.id, int(cfg.get("dm_expiry_minutes", 2))))

    async def _expire_dms_later(self, guild_id: int, user_id: int, minutes: int) -> None:
        try:
            await asyncio.sleep(max(1, minutes) * 60)
            state = self.state()
            case = self.get_case(state, guild_id, user_id)
            if not case or case.get("closed") or not case.get("pending_dms"):
                return
            guild = self.bot.get_guild(guild_id)
            member = guild.get_member(user_id) if guild else None
            still_unverified = bool(member and _member_has_role(member, (_guild_cfg(state, guild_id, create=False) or {}).get("unverified_role_id")))
            if still_unverified:
                case["status"] = "EXPIRED"
            await self._apply_dm_resolution(state, guild_id, user_id, expired=True)
            if case.get("status") == "EXPIRED":
                case["status"] = "UNVERIFIED"
            self.set_case(state, case)
            self.save(state)
            await self.dlog("dm expired", guild=self.bot.get_guild(int(guild_id)) if str(guild_id).isdigit() else None, user=user_id)
        except asyncio.CancelledError:
            return

    async def _close_dm_requests(self, state: Dict[str, Any], guild_id: int, user_id: int, text: str, expired: bool = False) -> None:
        await self._apply_dm_resolution(state, guild_id, user_id, expired=expired)

    async def dm_approve(self, interaction: discord.Interaction, case_key: str) -> None:
        await self.resolve_case(interaction, case_key, outcome="APPROVED", reason=None, source="dm")

    async def dm_deny(self, interaction: discord.Interaction, case_key: str) -> None:
        state = self.state()
        try:
            guild_id, user_id = [int(x) for x in case_key.split(":", 1)]
        except Exception:
            if interaction.response.is_done():
                await interaction.followup.send("Invalid case.", ephemeral=True)
            else:
                await interaction.response.send_message("Invalid case.", ephemeral=True)
            return
        case = self.get_case(state, guild_id, user_id)
        if case is None and interaction.message is not None:
            case = self.find_case_by_message(state, guild_id, interaction.message.id)
        if case is None and interaction.message is not None:
            case = self.find_case_by_embed_user(state, guild_id, interaction.message)
        if not case or case.get("closed"):
            msg = "This verification case has already been handled by another moderator. No further action can be taken from this message."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return
        await interaction.response.send_modal(DenyReasonModal(self, case_key, source="dm"))

    async def admin_approve(self, interaction: discord.Interaction, case_key: str) -> None:
        await self.resolve_case(interaction, case_key, outcome="APPROVED", reason=None, source="forum")

    async def admin_forgive(self, interaction: discord.Interaction, case_key: str) -> None:
        if not await self.ensure_admin(interaction):
            return
        async with self._case_lock_for(case_key):
            await self._transition_case(interaction, case_key, new_status="UNVERIFIED", reason="Forgiven", outcome_label="Forgiven")

    async def resolve_case(self, interaction: discord.Interaction, case_key: str, outcome: str, reason: Optional[str], source: str) -> None:
        try:
            guild_id, user_id = [int(x) for x in case_key.split(":", 1)]
        except Exception:
            if interaction.response.is_done():
                await interaction.followup.send("Invalid case.", ephemeral=True)
            else:
                await interaction.response.send_message("Invalid case.", ephemeral=True)
            return

        async with self._case_lock_for(case_key):
            state = self.state()
            guild = self.bot.get_guild(guild_id)
            member = guild.get_member(user_id) if guild else None
            await self.dlog("resolve case", guild=guild, message=getattr(interaction.message, "id", None), case=case_key, outcome=outcome, source=source)
            case = self.get_case(state, guild_id, user_id)
            if case is None and interaction.message is not None:
                case = self.find_case_by_message(state, guild_id, interaction.message.id)
            if case is None and interaction.message is not None:
                case = self.find_case_by_embed_user(state, guild_id, interaction.message)
            if guild is None or case is None:
                if interaction.response.is_done():
                    await interaction.followup.send("Case no longer exists.", ephemeral=True)
                else:
                    await interaction.response.send_message("Case no longer exists.", ephemeral=True)
                return
            if case.get("closed"):
                if source == "dm":
                    await self._edit_dm_interaction_message(interaction, self._build_dm_resolution_embed("handled"))
                else:
                    msg = "This verification case has already been handled by another moderator. No further action can be taken from this message."
                    if interaction.response.is_done():
                        await interaction.followup.send(msg, ephemeral=True)
                    else:
                        await interaction.response.send_message(msg, ephemeral=True)
                return
            actor = str(interaction.user)
            actor_id = interaction.user.id
            if source == "forum" and not await self.ensure_admin(interaction):
                return
            if source == "dm":
                cfg = _guild_cfg(state, guild_id, create=False) or {}
                verifier_role_ids = cfg.get("verifier_role_ids") or ([cfg.get("verifier_role_id")] if cfg.get("verifier_role_id") else [])
                verifier_member = guild.get_member(actor_id)
                if verifier_member is None or not _member_has_any_role(verifier_member, verifier_role_ids):
                    if interaction.response.is_done():
                        await interaction.followup.send("You are not allowed to act on this verification request.", ephemeral=True)
                    else:
                        await interaction.response.send_message("You are not allowed to act on this verification request.", ephemeral=True)
                    return
            case["handled_by"] = actor
            case["handled_by_id"] = actor_id
            case["handled_at"] = _iso(_utcnow())
            self._snapshot_case_member(guild, case, member)
            if reason:
                case["reason"] = reason
            cfg = _guild_cfg(state, guild_id, create=False) or {}
            unverified_role_id = cfg.get("unverified_role_id")
            verified_role_id = cfg.get("verified_role_id")
            if outcome == "APPROVED":
                if member is not None:
                    try:
                        if unverified_role_id and _member_has_role(member, int(unverified_role_id)):
                            role = guild.get_role(int(unverified_role_id))
                            if role:
                                await member.remove_roles(role, reason="Verification approved")
                        if verified_role_id and not _member_has_role(member, int(verified_role_id)):
                            role = guild.get_role(int(verified_role_id))
                            if role:
                                await member.add_roles(role, reason="Verification approved")
                    except Exception:
                        pass
                await self._finalize_case(
                    state,
                    guild,
                    case,
                    final_status="APPROVED",
                    notify_text="This verification case has already been handled by another moderator. No further action can be taken from this message.",
                    dm_outcome="approved" if source == "dm" else None,
                    dm_actor_id=actor_id if source == "dm" else None,
                    dm_interaction=interaction if source == "dm" else None,
                )
                target_label = self._case_target_label(guild, case)
                if source != "dm":
                    if interaction.response.is_done():
                        await interaction.followup.send(f"Approved {target_label}.", ephemeral=True)
                    else:
                        await interaction.response.send_message(f"Approved {target_label}.", ephemeral=True)
                return
            if outcome == "DENIED":
                await self._transition_case(interaction, case_key, new_status="DENIED", reason=reason or "Denied", outcome_label="Denied", dm_outcome="denied" if source == "dm" else None, dm_actor_id=actor_id if source == "dm" else None)
                return
            if outcome == "KICKED":
                note_ok = False
                if member is not None:
                    try:
                        await member.send(
                            f"You were removed because of a verification moderation action.\nReason: {reason or 'No reason provided.'}",
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
                        note_ok = True
                    except Exception:
                        note_ok = False
                case["dm_notice"] = "Yes" if note_ok else "No"
                if member is None:
                    await self.dlog("kick failed", guild=guild, user=user_id, case=case_key, error="member_missing")
                    if interaction.response.is_done():
                        await interaction.followup.send("Kick failed: member is no longer in the server.", ephemeral=True)
                    else:
                        await interaction.response.send_message("Kick failed: member is no longer in the server.", ephemeral=True)
                    return
                kick_error = None
                self._snapshot_case_member(guild, case, member)
                try:
                    await member.kick(reason=reason or "Verification moderation action")
                    await self.dlog("kick completed", guild=guild, user=user_id, case=case_key)
                except discord.Forbidden as exc:
                    kick_error = f"missing permission or role hierarchy prevents kicking this member ({type(exc).__name__})."
                except discord.HTTPException as exc:
                    kick_error = f"Discord rejected the kick ({type(exc).__name__}: {exc})."
                except Exception as exc:
                    kick_error = f"{type(exc).__name__}: {exc}"
                if kick_error:
                    await self.dlog("kick failed", guild=guild, user=user_id, case=case_key, error=kick_error)
                    if interaction.response.is_done():
                        await interaction.followup.send(f"Kick failed: {kick_error}", ephemeral=True)
                    else:
                        await interaction.response.send_message(f"Kick failed: {kick_error}", ephemeral=True)
                    return
                await self._finalize_case(state, guild, case, final_status="KICKED", notify_text="This verification case has already been handled by another moderator. No further action can be taken from this message.")
                target_label = self._case_target_label(guild, case)
                if interaction.response.is_done():
                    await interaction.followup.send(f"Kicked {target_label}.", ephemeral=True)
                else:
                    await interaction.response.send_message(f"Kicked {target_label}.", ephemeral=True)
                return
    async def _transition_case(self, interaction: discord.Interaction, case_key: str, new_status: str, reason: str, outcome_label: str, dm_outcome: Optional[str] = None, dm_actor_id: Optional[int] = None) -> None:
        state = self.state()
        guild_id, user_id = [int(x) for x in case_key.split(":", 1)]
        guild = self.bot.get_guild(guild_id)
        await self.dlog("transition case", guild=guild, message=getattr(interaction.message, "id", None), case=case_key, new_status=new_status)
        case = self.get_case(state, guild_id, user_id)
        if case is None and interaction.message is not None:
            case = self.find_case_by_message(state, guild_id, interaction.message.id)
        if case is None and interaction.message is not None:
            case = self.find_case_by_embed_user(state, guild_id, interaction.message)
        if guild is None or case is None:
            if interaction.response.is_done():
                await interaction.followup.send("Case missing.", ephemeral=True)
            else:
                await interaction.response.send_message("Case missing.", ephemeral=True)
            return
        if case.get("closed") and case.get("status") not in CASE_OPEN:
            msg = "This verification case has already been handled by another moderator. No further action can be taken from this message."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
            return
        case["handled_by"] = str(interaction.user)
        case["handled_by_id"] = interaction.user.id
        case["handled_at"] = _iso(_utcnow())
        case["reason"] = reason
        old_case_snapshot = dict(case)
        old_case_snapshot["closed"] = True
        old_case_snapshot["reason"] = reason
        old_case_snapshot["handled_by"] = str(interaction.user)
        old_case_snapshot["handled_by_id"] = interaction.user.id
        old_case_snapshot["handled_at"] = _iso(_utcnow())
        self.set_case(state, old_case_snapshot)
        self.save(state)
        await self._delete_case_message(guild, old_case_snapshot)
        await self._apply_dm_resolution(state, guild_id, user_id, outcome=dm_outcome, actor_id=dm_actor_id, interaction=interaction if dm_outcome else None)
        case["closed"] = False
        case["status"] = new_status
        case["history"] = list(case.get("history", [])) + [{"at": _iso(_utcnow()), "event": outcome_label, "by": str(interaction.user), "reason": reason}]
        case.pop("archive_channel_id", None)
        case.pop("archive_message_id", None)
        case.pop("current_channel_id", None)
        case.pop("current_message_id", None)
        self.set_case(state, case)
        self.save(state)
        await self._post_case_message(guild, case)
        self.save(state)
        target_label = self._case_target_label(guild, case)
        if dm_outcome is None:
            if interaction.response.is_done():
                await interaction.followup.send(f"{outcome_label} {target_label}.", ephemeral=True)
            else:
                await interaction.response.send_message(f"{outcome_label} {target_label}.", ephemeral=True)

    async def _delete_operational_messages_for_user(self, guild: discord.Guild, user_id: int, *, keep_message_id: Optional[int] = None) -> None:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        for status in ("UNVERIFIED", "DENIED"):
            thread = await self._thread_for_status(guild, cfg, status)
            if not isinstance(thread, discord.Thread):
                continue
            try:
                async for msg in thread.history(limit=200):
                    if msg.author.id != self.bot.user.id or not msg.embeds:
                        continue
                    if keep_message_id and int(msg.id) == int(keep_message_id):
                        continue
                    emb = msg.embeds[0]
                    uid = None
                    for field in emb.fields:
                        if field.name == "User":
                            m = re.search(r'ID:\s*`?(\d{15,25})`?', field.value or "")
                            if m:
                                uid = int(m.group(1))
                                break
                    if uid != int(user_id):
                        continue
                    try:
                        await msg.delete()
                    except Exception:
                        try:
                            await self._mark_message_superseded(msg, "This message is no longer actionable.")
                        except Exception:
                            pass
            except Exception:
                continue

    async def _finalize_case(self, state: Dict[str, Any], guild: discord.Guild, case: Dict[str, Any], final_status: str, notify_text: str, dm_outcome: Optional[str] = None, dm_actor_id: Optional[int] = None, dm_interaction: Optional[discord.Interaction] = None) -> None:
        await self._apply_dm_resolution(state, guild.id, int(case["user_id"]), outcome=dm_outcome, actor_id=dm_actor_id, interaction=dm_interaction)
        delete_case = dict(case)
        delete_case["closed"] = True
        delete_case["status"] = final_status
        self.set_case(state, delete_case)
        self.save(state)
        await self._delete_case_message(guild, delete_case)
        await self._delete_operational_messages_for_user(guild, int(case["user_id"]))

        case["closed"] = True
        case["status"] = final_status
        case.pop("current_channel_id", None)
        case.pop("current_message_id", None)
        self.set_case(state, case)
        self.save(state)

        record = dict(case)
        record["closed"] = True
        record.pop("current_channel_id", None)
        record.pop("current_message_id", None)
        await self._post_case_message(guild, record)

        case["archive_channel_id"] = record.get("current_channel_id")
        case["archive_message_id"] = record.get("current_message_id")
        case.pop("current_channel_id", None)
        case.pop("current_message_id", None)
        self.set_case(state, case)
        self.save(state)

    async def baseline_scan(self, guild: discord.Guild) -> Dict[str, int]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False) or {}
        unverified_role_id = cfg.get("unverified_role_id")
        verified_role_id = cfg.get("verified_role_id")
        scanned = created = skipped = 0
        for member in guild.members:
            if member.bot:
                continue
            scanned += 1
            has_u = _member_has_role(member, unverified_role_id)
            has_v = _member_has_role(member, verified_role_id)
            existing = self.get_case(state, guild.id, member.id)
            if has_v and has_u:
                await self._flag_drift(guild, member, existing, "conflicting_roles")
                skipped += 1
                continue
            if not has_u:
                skipped += 1
                continue
            if existing and not existing.get("closed") and existing.get("status") in CASE_OPEN:
                skipped += 1
                continue
            await self.create_or_get_case(member, source="baseline")
            created += 1
        self.save(state)
        return {"scanned": scanned, "created": created, "skipped": skipped}

    async def rebuild_active_case_cards(self, guild: discord.Guild, *, source: str, announce_status: bool = False) -> Dict[str, int]:
        if announce_status:
            await self.slog(guild, "startup repair started")
        validation = await self.validate_cases_for_guild(guild, source=source)
        pruned = reposted = failed = 0
        state = self.state()
        cases = list(state.setdefault("cases", {}).values())
        for case in cases:
            try:
                if int(case.get("guild_id")) != int(guild.id):
                    continue
            except Exception:
                continue
            if case.get("closed") or case.get("status") not in CASE_OPEN:
                continue
            old_channel = case.get("current_channel_id")
            old_message = case.get("current_message_id")
            try:
                was_pruned = False
                if old_channel or old_message:
                    was_pruned = await self._delete_case_message(guild, dict(case))
                    if was_pruned:
                        pruned += 1
                case.pop("current_channel_id", None)
                case.pop("current_message_id", None)
                await self._post_case_message(guild, case)
                state = self.state()
                self.set_case(state, case)
                self.save(state)
                reposted += 1
                await self.dlog(
                    "rebuild case reposted",
                    guild=guild,
                    user=case.get("user_id"),
                    old_channel=old_channel,
                    new_channel=case.get("current_channel_id"),
                    old_message=old_message,
                    new_message=case.get("current_message_id"),
                    pruned=was_pruned,
                    source=source,
                )
                await self.dlog(
                    "rebuild pacing",
                    guild=guild,
                    user=case.get("user_id"),
                    source=source,
                    sleep_seconds=0.75,
                )
                await asyncio.sleep(0.75)
            except Exception:
                failed += 1
                await self.dlog(
                    "rebuild case failed",
                    guild=guild,
                    user=case.get("user_id"),
                    old_channel=old_channel,
                    old_message=old_message,
                    source=source,
                    detail=traceback.format_exc().replace("\n", " | "),
                )
        try:
            await self._cleanup_stale_operational_messages(guild)
            await self._purge_duplicate_operational_messages(guild)
        except Exception:
            await self.dlog("rebuild cleanup failed", guild=guild, source=source, detail=traceback.format_exc().replace("\n", " | "))
        await self.dlog(
            "rebuild complete",
            guild=guild,
            source=source,
            validated=validation.get("validated", 0),
            active=validation.get("active", 0),
            finalized=validation.get("finalized", 0),
            flagged=validation.get("flagged", 0),
            pruned=pruned,
            reposted=reposted,
            failed=failed,
        )
        if announce_status:
            await self.slog(guild, "startup repair complete")
        return {
            "validated": int(validation.get("validated", 0)),
            "active": int(validation.get("active", 0)),
            "finalized": int(validation.get("finalized", 0)),
            "flagged": int(validation.get("flagged", 0)),
            "pruned": pruned,
            "reposted": reposted,
            "failed": failed,
        }

    async def manual_reconcile_later(self, guild_id: int, user_id: int, delay_seconds: int) -> None:
        key = _case_key(guild_id, user_id)
        old = self._reconcile_tasks.pop(key, None)
        if old:
            old.cancel()
        self._reconcile_tasks[key] = asyncio.create_task(self._manual_reconcile_task(guild_id, user_id, delay_seconds))

    async def _manual_reconcile_task(self, guild_id: int, user_id: int, delay_seconds: int) -> None:
        try:
            await asyncio.sleep(max(5, delay_seconds))
            state = self.state()
            case = self.get_case(state, guild_id, user_id)
            if not case or case.get("closed"):
                return
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                return
            member = guild.get_member(user_id)
            if member is None:
                return
            cfg = _guild_cfg(state, guild_id, create=False) or {}
            u = _member_has_role(member, cfg.get("unverified_role_id"))
            v = _member_has_role(member, cfg.get("verified_role_id"))
            if not u and v:
                case["handled_by"] = "Manual role change"
                case["handled_by_id"] = self.bot.user.id if self.bot.user else None
                case["handled_at"] = _iso(_utcnow())
                await self._finalize_case(state, guild, case, final_status="APPROVED", notify_text="This verification case has already been handled by staff. No further action can be taken from this message.")
                self.save(state)
                return
            if not u and not v:
                await self._flag_drift(guild, member, case, "no_resolution_roles")
                return
            if u and v:
                await self._flag_drift(guild, member, case, "conflicting_roles")
                return
        except asyncio.CancelledError:
            return

    async def record_invite_join(self, member: discord.Member) -> None:
        state = self.state()
        temp = state.setdefault("invite_temp", {})
        inviter = temp.get(f"invite:{member.guild.id}:{member.id}")
        if inviter:
            return
        cache = state.setdefault("invite_cache", {}).get(str(member.guild.id), {})
        try:
            invites = await member.guild.invites()
        except Exception:
            return
        used = None
        prev = {code: int(meta.get("uses", 0)) for code, meta in cache.items()}
        new_cache = {}
        for inv in invites:
            new_cache[inv.code] = {
                "uses": inv.uses or 0,
                "inviter": str(inv.inviter) if inv.inviter else "Unknown",
                "inviter_id": int(inv.inviter.id) if inv.inviter else None,
            }
            if (inv.uses or 0) > prev.get(inv.code, 0):
                used = {
                    "name": str(inv.inviter) if inv.inviter else "Unknown",
                    "id": int(inv.inviter.id) if inv.inviter else None,
                }
        state.setdefault("invite_cache", {})[str(member.guild.id)] = new_cache
        if used:
            temp[f"invite:{member.guild.id}:{member.id}"] = used
        self.save(state)

    async def snapshot_invites(self, guild: discord.Guild) -> None:
        state = self.state()
        try:
            invites = await guild.invites()
        except Exception:
            return
        state.setdefault("invite_cache", {})[str(guild.id)] = {
            inv.code: {
                "uses": inv.uses or 0,
                "inviter": str(inv.inviter) if inv.inviter else "Unknown",
                "inviter_id": int(inv.inviter.id) if inv.inviter else None,
            }
            for inv in invites
        }
        self.save(state)

    async def cleanup_scan_once(self) -> None:
        state = self.state()
        for guild in self.bot.guilds:
            cfg = _guild_cfg(state, guild.id, create=False)
            if not cfg or not cfg.get("enabled"):
                continue
            if not self.is_startup_complete(guild.id):
                continue
            urole_id = cfg.get("unverified_role_id")
            if not urole_id:
                continue
            async with self._boot_lock_for(guild.id):
                if not self.is_startup_complete(guild.id):
                    continue
                await self.validate_cases_for_guild(guild, source="cleanup")
                await self._purge_duplicate_operational_messages(guild)
                now = _utcnow()
                verified_role_id = cfg.get("verified_role_id")
                warn_after = int(cfg.get("warn_after_days", 3))
                kick_after = int(cfg.get("kick_after_days", 7))
                strike_limit = int(cfg.get("cleanup_strike_limit", 3))

                for member in guild.members:
                    if member.bot:
                        continue
                    has_u = _member_has_role(member, int(urole_id))
                    has_v = _member_has_role(member, int(verified_role_id)) if verified_role_id else False
                    existing = self.get_case(state, guild.id, member.id)

                    if has_u and has_v:
                        await self._flag_drift(guild, member, existing, "conflicting_roles")
                        continue

                    if has_v and not has_u:
                        if existing and (not existing.get("closed") or existing.get("status") in CASE_OPEN):
                            existing["handled_by"] = "System Reconcile"
                            existing["handled_by_id"] = self.bot.user.id if self.bot.user else None
                            existing["handled_at"] = _iso(now)
                            await self._finalize_case(state, guild, existing, final_status="APPROVED", notify_text="This verification case has already been handled by staff. No further action can be taken from this message.")
                            self.save(state)
                        continue

                    if not has_u and not has_v:
                        if existing and (not existing.get("closed") or existing.get("status") in CASE_OPEN):
                            await self._flag_drift(guild, member, existing, "no_resolution_roles")
                        continue

                    case = await self.create_or_get_case(member, source="reconcile")
                    entered = _fromiso(case.get("entered_unverified_at")) or now
                    age_days = (now - entered).days

                    if not case.get("warned") and age_days >= warn_after:
                        if self._cleanup_action_done_today(state, guild.id, member.id, "warn", now):
                            await self.dlog("cleanup warn skipped", guild=guild, user=member, reason="already_warned_today")
                        else:
                            case["warned"] = True
                            case["warning_at"] = _iso(now)
                            case["reason"] = "Unverified too long"
                            self.set_case(state, case)
                            self._mark_cleanup_action_today(state, guild.id, member.id, "warn", now)
                            self.save(state)
                            await self._edit_case_message(guild, case)
                            await self.dlog("warning issued", guild=guild, user=member)

                    if age_days < kick_after:
                        continue

                    next_action = "kick" if int(case.get("cleanup_strikes", 0)) + 1 < strike_limit else "ban"
                    if self._cleanup_action_done_today(state, guild.id, member.id, next_action, now):
                        await self.dlog(f"cleanup {next_action} skipped", guild=guild, user=member, reason="already_processed_today")
                        continue

                    strikes = int(case.get("cleanup_strikes", 0)) + 1
                    case["cleanup_strikes"] = strikes
                    dm_ok = False

                    if strikes < strike_limit:
                        invite_url = await self._make_cleanup_invite(guild, cfg)
                        text_msg = (
                            "You were removed because you stayed unverified for too long.\n\n"
                            "You can return using this invite link:\n"
                            f"{invite_url or 'Invite unavailable.'}\n\n"
                            "This invite is valid for 1 week.\n\n"
                            "If it expires, you can search for the server again and rejoin when you are ready to complete verification."
                        )
                        try:
                            await member.send(text_msg, allowed_mentions=discord.AllowedMentions.none())
                            dm_ok = True
                        except Exception:
                            dm_ok = False

                        kick_ok = False
                        try:
                            await member.kick(reason="System Cleanup: Unverified too long")
                            kick_ok = True
                        except Exception:
                            kick_ok = False

                        if kick_ok:
                            case["dm_notice"] = "Yes" if dm_ok else "No"
                            case["cleanup_invite"] = invite_url or ""
                            case["handled_by"] = "System Cleanup"
                            case["handled_by_id"] = self.bot.user.id if self.bot.user else None
                            case["handled_at"] = _iso(now)
                            case["reason"] = "Unverified too long"
                            await self._finalize_case(state, guild, case, final_status="KICKED", notify_text="This verification request is no longer active. No further action can be taken from this message.")
                            state.setdefault("invite_temp", {})[f"cleanup-strikes:{guild.id}:{member.id}"] = strikes
                            self._mark_cleanup_action_today(state, guild.id, member.id, "kick", now)
                            self.save(state)
                            await self.dlog("cleanup kick", guild=guild, user=member, strike=strikes)
                        else:
                            self.set_case(state, case)
                            self.save(state)
                            await self.dlog("cleanup kick failed", guild=guild, user=member, strike=strikes)
                    else:
                        ban_ok = False
                        try:
                            await member.ban(reason="System Cleanup: repeated failure to verify")
                            ban_ok = True
                        except Exception:
                            ban_ok = False

                        if ban_ok:
                            case["dm_notice"] = "No"
                            case["handled_by"] = "System Cleanup"
                            case["handled_at"] = _iso(now)
                            case["reason"] = "Repeated failure to verify"
                            await self._finalize_case(state, guild, case, final_status="BANNED", notify_text="This verification request is no longer active. No further action can be taken from this message.")
                            self._mark_cleanup_action_today(state, guild.id, member.id, "ban", now)
                            self.save(state)
                            await self.dlog("cleanup ban", guild=guild, user=member, strike=strikes)
                        else:
                            self.set_case(state, case)
                            self.save(state)
                            await self.dlog("cleanup ban failed", guild=guild, user=member, strike=strikes)
        self.save(state)

    async def _make_cleanup_invite(self, guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[str]:
        ch = None
        cid = cfg.get("cleanup_invite_channel_id")
        if cid:
            ch = guild.get_channel(int(cid))
        if ch is None:
            for candidate in guild.text_channels:
                perms = candidate.permissions_for(guild.me)
                if perms.create_instant_invite:
                    ch = candidate
                    break
        if ch is None or not isinstance(ch, discord.abc.GuildChannel):
            return None
        try:
            invite = await ch.create_invite(max_age=7 * 24 * 60 * 60, max_uses=1, unique=True, reason="Cleanup rejoin invite")
            return invite.url
        except Exception:
            return None


    async def rebind_active_views(self, guild: discord.Guild) -> None:
        await self.rebuild_active_case_cards(guild, source="startup", announce_status=True)


class UnverifiedCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.mgr = UnverifiedManager(bot)
        global ACTIVE_UNVERIFIED_MANAGER
        ACTIVE_UNVERIFIED_MANAGER = self.mgr
        try:
            bot.add_view(PersistentAdminActionView())
            bot.add_view(PersistentDeniedActionView())
            bot.add_view(PersistentDMActionView())
        except Exception:
            pass
        self.cleanup_loop.start()

    def cog_unload(self):
        self.cleanup_loop.cancel()

    @tasks.loop(hours=6)
    async def cleanup_loop(self):
        await self.mgr.cleanup_scan_once()

    @cleanup_loop.before_loop
    async def before_cleanup(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            self.mgr.clear_startup_complete(guild.id)
            try:
                await self.mgr.snapshot_invites(guild)
            except Exception:
                pass
            try:
                state = load_state()
                cfg = _guild_cfg(state, guild.id, create=False)
                if cfg and cfg.get("enabled"):
                    async with self.mgr._boot_lock_for(guild.id):
                        await self.mgr.dlog("startup repair using configured threads only", guild=guild)
                        try:
                            await self.mgr.reconcile_cases_for_guild(guild)
                        except Exception:
                            pass
                        try:
                            await self.mgr._cleanup_stale_operational_messages(guild)
                            await self.mgr._purge_duplicate_operational_messages(guild)
                        except Exception:
                            pass
                        try:
                            await self.mgr.rebuild_active_case_cards(guild, source="startup", announce_status=True)
                        except Exception:
                            pass
            except Exception:
                pass
            finally:
                self.mgr.mark_startup_complete(guild.id)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.mgr.record_invite_join(member)
        state = load_state()
        cfg = _guild_cfg(state, member.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        if cfg.get("unverified_role_id") and _member_has_role(member, int(cfg.get("unverified_role_id"))):
            try:
                await self.mgr.ensure_forum_board(member.guild)
            except Exception:
                pass
            await self.mgr.create_or_get_case(member, source="live_join")

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        state = load_state()
        cfg = _guild_cfg(state, after.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        role_ids_before = {r.id for r in before.roles}
        role_ids_after = {r.id for r in after.roles}
        relevant = {int(x) for x in [cfg.get("unverified_role_id") or 0, cfg.get("verified_role_id") or 0] if x}
        if not relevant:
            return
        if role_ids_before == role_ids_after:
            return
        if not (relevant & (role_ids_before | role_ids_after)):
            return
        await self.mgr.manual_reconcile_later(after.guild.id, after.id, int(cfg.get("manual_override_grace_seconds", 15)))

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if member.bot:
            return
        state = load_state()
        cfg = _guild_cfg(state, member.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        if before.channel == after.channel:
            return
        if after.channel is None:
            return

        unverified_role_id = cfg.get("unverified_role_id")
        verifier_role_ids = cfg.get("verifier_role_ids") or ([cfg.get("verifier_role_id")] if cfg.get("verifier_role_id") else [])

        if _member_has_role(member, unverified_role_id):
            await self.mgr.send_verifier_dms(member, after.channel)
            return

        if verifier_role_ids and _member_has_any_role(member, verifier_role_ids):
            for other in after.channel.members:
                if other.bot or other.id == member.id:
                    continue
                if _member_has_role(other, unverified_role_id):
                    await self.mgr.send_verifier_dms(other, after.channel)


async def setup(bot: commands.Bot, registry: SettingsRegistry) -> None:
    existing = bot.get_cog("UnverifiedCog")
    if existing is not None:
        await bot.remove_cog("UnverifiedCog")
    await bot.add_cog(UnverifiedCog(bot))
    manager = bot.get_cog("UnverifiedCog").mgr  # type: ignore[attr-defined]

    def status() -> str:
        # Shows aggregate quick line.
        state = load_state()
        enabled_guilds = sum(1 for cfg in state.get("guilds", {}).values() if cfg.get("enabled"))
        return f"✅ Enabled in {enabled_guilds} guild(s)" if enabled_guilds else "❌ Disabled"

    async def handler(interaction: discord.Interaction, ctx: Dict[str, Any]) -> Optional[dict]:
        if interaction.guild is None:
            return {"op": "respond", "payload": {"content": "Server only.", "ephemeral": True}}
        if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
            return {"op": "respond", "payload": {"content": "Admins only.", "ephemeral": True}}
        action = str(ctx.get("action") or "toggle").strip().lower()
        state = load_state()
        cfg = _guild_cfg(state, interaction.guild.id)
        assert cfg is not None

        if action == "toggle":
            cfg["enabled"] = not bool(cfg.get("enabled"))
            save_state(state)
            try:
                await manager.ensure_forum_board(interaction.guild)
            except Exception:
                pass
            return None

        if action == "configure":
            return {"op": "modal", "modal": ConfigureModal(interaction.guild.id)}

        if action == "preview":
            preview_case = _ensure_case_defaults({
                "case_id": "preview-1",
                "guild_id": interaction.guild.id,
                "user_id": interaction.user.id,
                "user_name": str(interaction.user),
                "status": "UNVERIFIED",
                "server_join_time": _iso(_utcnow() - timedelta(days=2)),
                "invite_creator": "ExampleMod#0001",
                "warned": True,
                "cleanup_strikes": 1,
                "voice_context": ["VerifierOne (1)", "UserTwo (2)", "UserThree (3)"],
                "reason": "",
                "handled_by": "PreviewOnly",
                "handled_at": _iso(_utcnow()),
            })
            unverified = await manager._build_embed(interaction.guild, preview_case)

            denied_case = dict(preview_case)
            denied_case["status"] = "DENIED"
            denied_case["reason"] = "Reason of denied example"
            denied_case["handled_by"] = "ModeratorExample"
            denied = await manager._build_embed(interaction.guild, denied_case)

            completed_case = dict(preview_case)
            completed_case["status"] = "APPROVED"
            completed_case["closed"] = True
            completed_case["handled_by"] = "VerifierExample"
            completed = await manager._build_embed(interaction.guild, completed_case)

            cleanup_kick_case = dict(preview_case)
            cleanup_kick_case["status"] = "KICKED"
            cleanup_kick_case["closed"] = True
            cleanup_kick_case["handled_by"] = "System Cleanup"
            cleanup_kick_case["reason"] = "Unverified too long"
            cleanup_kick = await manager._build_embed(interaction.guild, cleanup_kick_case)

            cleanup_ban_case = dict(preview_case)
            cleanup_ban_case["status"] = "BANNED"
            cleanup_ban_case["closed"] = True
            cleanup_ban_case["handled_by"] = "System Cleanup"
            cleanup_ban_case["cleanup_strikes"] = 3
            cleanup_ban_case["reason"] = "Repeated failure to verify"
            cleanup_ban = await manager._build_embed(interaction.guild, cleanup_ban_case)

            await interaction.response.send_message(
                content="Preview set: Verify - UNVERIFIED / Verify - DENIED / Verify - COMPLETED / CLEANUP KICK / CLEANUP BAN",
                embeds=[unverified, denied, completed, cleanup_kick, cleanup_ban],
                ephemeral=True,
            )
            return None

        if action in {"rebuild", "repair", "baseline", "rescan"}:
            await manager.ensure_forum_board(interaction.guild)
            try:
                await interaction.response.defer(ephemeral=True)
            except Exception:
                pass
            result = await manager.rebuild_active_case_cards(interaction.guild, source="manual_rebuild", announce_status=False)
            try:
                await interaction.followup.send(
                    content=(
                        f"Verification case rebuild complete. "
                        f"validated={result['validated']} active={result['active']} flagged={result['flagged']} "
                        f"pruned={result['pruned']} reposted={result['reposted']} failed={result['failed']}"
                    ),
                    ephemeral=True,
                )
            except Exception:
                pass
            return None

        return None

    registry.register(
        SettingFeature(
            feature_id="unverified",
            label="Unverified System",
            description="Forum-based verification board, verifier DMs, cleanup, preview, and verification case rebuild.",
            category=CATEGORY,
            category_description=CATEGORY_DESCRIPTION,
            handler=handler,
            status=status,
            actions=[
                FeatureAction("configure", "Configure", "Set unverified, verified, verifier role IDs and timing values for the unverified workflow.", row=1),
                FeatureAction("preview", "Preview", "Show actual embed previews for Verify - UNVERIFIED, Verify - DENIED, Verify - COMPLETED, cleanup kick, and cleanup ban states.", row=2),
                FeatureAction("rebuild", "Rebuild Verification Cases", "Prune existing active verification cards, repost fresh cards, and rebind active verification views.", row=2),
            ],
        )
    )


async def teardown(bot: commands.Bot, registry: SettingsRegistry) -> None:
    cog = bot.get_cog("UnverifiedCog")
    if cog is not None:
        await bot.remove_cog("UnverifiedCog")
    registry.unregister("unverified")
