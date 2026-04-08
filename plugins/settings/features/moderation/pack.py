from __future__ import annotations

# Touched on 2026-04-07 UTC for interactive tutorial workflow updates (repo sync update).

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
TUTORIAL_DURATION_HOURS = 2

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
    return {"guilds": {}, "next_ids": {"investigation": 1, "active": 1, "archive": 1, "alert": 1, "recovery": 1, "tutorial": 1}}


def _default_moderation_settings() -> Dict[str, Any]:
    return dict(DEFAULT_MODERATION_SETTINGS)


def _guild_settings(cfg: Dict[str, Any]) -> Dict[str, Any]:
    settings = cfg.setdefault("settings", {})
    for key, value in DEFAULT_MODERATION_SETTINGS.items():
        settings.setdefault(key, value)
    return settings


def _tutorial_sessions(cfg: Dict[str, Any]) -> Dict[str, Any]:
    sessions = cfg.setdefault("tutorial_sessions", {})
    if not isinstance(sessions, dict):
        sessions = {}
        cfg["tutorial_sessions"] = sessions
    return sessions


def _parse_iso_utc(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _cleanup_expired_tutorial_sessions(cfg: Dict[str, Any], *, now: Optional[datetime] = None) -> None:
    sessions = _tutorial_sessions(cfg)
    now_dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    for key, rec in list(sessions.items()):
        if not isinstance(rec, dict):
            sessions.pop(key, None)
            continue
        status = str(rec.get("status") or "active").strip().lower()
        if status != "active":
            sessions.pop(key, None)
            continue
        expires_at = _parse_iso_utc(rec.get("expires_at"))
        if expires_at is None or expires_at <= now_dt:
            sessions.pop(key, None)


def _find_active_tutorial_session(cfg: Dict[str, Any], owner_user_id: int, tutorial_type: str) -> Optional[Dict[str, Any]]:
    _cleanup_expired_tutorial_sessions(cfg)
    owner = int(owner_user_id)
    expected_type = str(tutorial_type).strip().lower()
    for rec in _tutorial_sessions(cfg).values():
        if not isinstance(rec, dict):
            continue
        if str(rec.get("status") or "active").strip().lower() != "active":
            continue
        if int(rec.get("owner_user_id") or 0) != owner:
            continue
        if str(rec.get("tutorial_type") or "").strip().lower() != expected_type:
            continue
        return rec
    return None


def _format_remaining_tutorial_time(expires_at_raw: Any) -> str:
    expires_at = _parse_iso_utc(expires_at_raw)
    if expires_at is None:
        return "expired"
    seconds = int((expires_at - datetime.now(timezone.utc)).total_seconds())
    if seconds <= 0:
        return "expired"
    hours, remainder = divmod(seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m"
    return "<1m"


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
        elif duration_minutes:
            label = f"Timed out for {duration_minutes} minute" + ("s" if duration_minutes != 1 else "")
    elif action_type == "ban":
        label = "Banned from the server"
    return {"key": key, "type": action_type, "duration_minutes": duration_minutes, "label": label}




def _current_action_label(settings: Dict[str, Any], strike_count: int) -> str:
    cfg = _threshold_config(settings, int(strike_count or 0))
    action_type = str(cfg.get("type") or "none").lower()
    if action_type == "timeout":
        duration_minutes = int(cfg.get("duration_minutes") or 0)
        if duration_minutes == 5:
            return "5m Timeout"
        if duration_minutes and duration_minutes % (24 * 60) == 0:
            days = duration_minutes // (24 * 60)
            return f"{days}d Timeout"
        if duration_minutes:
            return f"{duration_minutes}m Timeout"
        return "Timeout"
    if action_type == "ban":
        return "Ban"
    return "None"
def _next_action_text(settings: Dict[str, Any], strike_count: int) -> str:
    next_count = int(strike_count) + 1
    if next_count > 4:
        return ""
    cfg = _threshold_config(settings, next_count)
    if cfg["type"] == "timeout":
        return f"If another strike is issued, you will be {cfg['label'].lower()}."
    if cfg["type"] == "ban":
        return "If another strike is issued, you will be banned from the server."
    return "If another strike is issued, staff will review further action."


def _ensure_case_strike_entries(case: Dict[str, Any]) -> list[Dict[str, Any]]:
    entries = case.get("strikes")
    if isinstance(entries, list):
        normalized: list[Dict[str, Any]] = []
        for idx, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                continue
            normalized.append({
                "strike_id": str(entry.get("strike_id") or f"stk-{idx}"),
                "reason": str(entry.get("reason") or case.get("latest_reason") or "No reason provided"),
                "issued_by": entry.get("issued_by"),
                "issued_at": entry.get("issued_at") or entry.get("timestamp") or _now_iso(),
                "threshold_level": int(entry.get("threshold_level") or idx),
                "action_applied": entry.get("action_applied"),
            })
        case["strikes"] = normalized
        case["strike_count"] = len(normalized)
        return normalized
    strikes: list[Dict[str, Any]] = []
    for entry in list(case.get("history") or []):
        if str(entry.get("type") or "") != "STRIKE_INCREMENT":
            continue
        idx = len(strikes) + 1
        strikes.append({
            "strike_id": f"legacy-{idx}",
            "reason": str(entry.get("reason") or case.get("latest_reason") or "No reason provided"),
            "issued_by": entry.get("moderator_id"),
            "issued_at": entry.get("timestamp") or _now_iso(),
            "threshold_level": idx,
            "action_applied": None,
        })
    case["strikes"] = strikes
    case["strike_count"] = len(strikes)
    return strikes




def _find_triggering_ban_strike_id(case: Dict[str, Any]) -> Optional[str]:
    """Compatibility shim for older strike-finalization paths.

    Returns the most recent strike_id from the case when available. This keeps
    any latent legacy callsites from crashing while preserving the current
    L.12-based strike behavior.
    """
    strike_entries = _ensure_case_strike_entries(case)
    if strike_entries:
        strike_id = strike_entries[-1].get("strike_id")
        if strike_id is not None:
            return str(strike_id)
    history = list(case.get("history") or [])
    for entry in reversed(history):
        if str(entry.get("type") or "") not in {"STRIKE_APPLIED", "STRIKE_INCREMENT"}:
            continue
        strike_id = entry.get("strike_id")
        if strike_id is not None:
            return str(strike_id)
    return None

def _append_strike_entry(case: Dict[str, Any], *, reason: str, moderator_id: int, source: str) -> Dict[str, Any]:
    strikes = _ensure_case_strike_entries(case)
    strike_number = len(strikes) + 1
    entry = {
        "strike_id": f"stk-{strike_number}",
        "reason": reason,
        "issued_by": int(moderator_id),
        "issued_at": _now_iso(),
        "threshold_level": strike_number,
        "action_applied": None,
    }
    strikes.append(entry)
    case["strike_count"] = len(strikes)
    case.setdefault("history", []).append(
        {
            "timestamp": entry["issued_at"],
            "type": "STRIKE_APPLIED",
            "source": source,
            "moderator_id": int(moderator_id),
            "reason": reason,
            "strike_id": entry["strike_id"],
            "strike_count": strike_number,
            "action": None,
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
    for key in ("investigation", "active", "archive", "alert", "recovery", "tutorial"):
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
            cfg.setdefault("tutorial_sessions", {})
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
            "tutorial_sessions": {},
            "settings": _default_moderation_settings(),
        },
    )
    cfg.setdefault("investigation_cases", {})
    cfg.setdefault("active_cases", {})
    cfg.setdefault("archive_cases", {})
    cfg.setdefault("archive_index_by_user", {})
    cfg.setdefault("pending_ban_recoveries", {})
    cfg.setdefault("tutorial_sessions", {})
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
        if any(str(k) != str(v.get("case_id")) for k, v in archive_cases.items()):
            rebuilt_cases: Dict[str, Dict[str, Any]] = {}
            rebuilt_index: Dict[str, list[str]] = {}
            for case in archive_cases.values():
                case_id = str(case.get("case_id") or "")
                if not case_id:
                    continue
                rebuilt_cases[case_id] = case
                uid = str(int(case.get("user_id") or 0))
                if uid != "0":
                    rebuilt_index.setdefault(uid, []).append(case_id)
            archive_cases = rebuilt_cases
            archive_index = rebuilt_index
            cfg["archive_cases"] = archive_cases
            cfg["archive_index_by_user"] = archive_index
    for uid, case_ids in list(archive_index.items()):
        seen = set()
        cleaned: list[str] = []
        for case_id in case_ids or []:
            cid = str(case_id)
            if cid in archive_cases and cid not in seen:
                cleaned.append(cid)
                seen.add(cid)
        cleaned.sort(key=lambda cid: str((archive_cases.get(cid) or {}).get("archived_at") or ""), reverse=True)
        archive_index[uid] = cleaned
    return cfg


def _get_archived_case_ids_for_user(cfg: Dict[str, Any], user_id: int) -> list[str]:
    _normalize_archive_storage(cfg)
    return list(cfg.setdefault("archive_index_by_user", {}).get(str(int(user_id)), []) or [])


def _latest_archived_case_for_user(cfg: Dict[str, Any], user_id: int) -> Optional[Dict[str, Any]]:
    archive_cases = _normalize_archive_storage(cfg).setdefault("archive_cases", {})
    case_ids = _get_archived_case_ids_for_user(cfg, int(user_id))
    if not case_ids:
        return None
    return archive_cases.get(str(case_ids[0]))


def _recovery_prepared(case: Dict[str, Any]) -> bool:
    recovery = case.get("ban_recovery") or {}
    return bool(recovery.get("status") == "prepared")


def _recovery_completed(case: Dict[str, Any]) -> bool:
    recovery = case.get("ban_recovery") or {}
    return bool(recovery.get("status") == "completed")


def _eligible_ban_recovery(case: Dict[str, Any]) -> bool:
    if str(case.get("final_action") or "").lower() != "ban":
        return False
    if _recovery_prepared(case) or _recovery_completed(case):
        return False
    return True


def _major_history_event(entry: Dict[str, Any]) -> bool:
    return str(entry.get("type") or "").upper() in {"STRIKE_INCREMENT","STRIKE_APPLIED","STRIKE_FORGIVEN","THRESHOLD_ACTION","TIMEOUT_APPLIED","TIMEOUT_REMOVED","BAN","KICK","CASE_CLOSED"}


def _history_event_summary(entry: Dict[str, Any]) -> str:
    etype = str(entry.get("type") or "").upper()
    strike_count = entry.get("strike_count")
    ts = _format_central_timestamp(str(entry.get("timestamp") or ""))
    reason = str(entry.get("reason") or "").strip()
    if etype in {"STRIKE_INCREMENT", "STRIKE_APPLIED"}:
        label = f"Strike {int(strike_count)} applied" if strike_count else "Strike applied"
    elif etype == "STRIKE_FORGIVEN":
        label = "Strike forgiven"
    elif etype == "TIMEOUT_REMOVED":
        label = "Timeout removed"
    elif etype == "BAN":
        label = "User banned"
    elif etype == "KICK":
        label = "User kicked"
    elif etype == "CASE_CLOSED":
        label = str(entry.get("final_action") or "Case closed").replace("_", " ").title()
    else:
        label = str(entry.get("label") or entry.get("action_label") or etype.replace("_", " ").title() or "History event")
    if reason:
        return f"• {label} — {reason} ({ts})"
    return f"• {label} ({ts})"


def _major_history_lines(history: list[Dict[str, Any]], *, limit: int = 3) -> list[str]:
    lines = [_history_event_summary(entry) for entry in history if _major_history_event(entry)]
    return lines[-max(1, int(limit)):] if lines else ["• No major events recorded"]


def _final_action_title(final_action: str) -> str:
    mapping = {"ban": "Ban", "kick": "Kick", "cleared": "Closed", "forgiven": "Forgiven", "auto_finalized": "Auto-Finalized"}
    return mapping.get(str(final_action).lower(), str(final_action).replace("_", " ").title() or "Finalized")


def _final_action_explanation(final_action: str, finalization_source: str) -> str:
    action = str(final_action).lower()
    source = str(finalization_source).lower()
    if action == "ban" and source == "manual_ban":
        return "manual admin ban"
    if action == "ban":
        return "strike threshold reached"
    if action == "kick":
        return "manual removal"
    if action == "cleared" and source == "forgive_to_zero":
        return "reduced to zero"
    if action == "cleared":
        return "cleared by moderator"
    if source == "auto_cleanup":
        return "system cleanup finalized"
    return "finalized moderation outcome"


def _finalization_source_label(source: str) -> str:
    mapping = {"threshold_ban": "Threshold Ban", "manual_ban": "Manual Ban", "moderator_clear": "Moderator Clear Case", "forgive_to_zero": "Forgive to Zero", "manual_kick": "Manual Kick", "auto_cleanup": "System Auto-Finalized Cleanup"}
    return mapping.get(str(source).lower(), str(source).replace("_", " ").title() or "Unknown")
def _format_central_timestamp(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    central = dt.astimezone(ZoneInfo("America/Chicago"))
    return central.strftime("%m/%d/%Y  %I:%M %p")


def _actor_display(actor_id: Any = None, actor_name: str = "Unknown") -> str:
    try:
        if actor_id:
            return f"<@{int(actor_id)}>"
    except Exception:
        pass
    return str(actor_name or "Unknown")


def _display_handle(guild: Optional[discord.Guild], user_id: Any, fallback: str = "User") -> str:
    try:
        uid = int(user_id)
    except Exception:
        return f"@{str(fallback or 'User')}"
    member = guild.get_member(uid) if guild is not None else None
    if member is not None:
        return f"@{getattr(member, 'display_name', getattr(member, 'name', uid))}"
    return f"@{str(fallback or uid)}"


async def _send_ephemeral_response(interaction: discord.Interaction, content: Optional[str] = None, *, embed: Optional[discord.Embed] = None, view: Optional[ui.View] = None):
    kwargs = {"ephemeral": True}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    if view is not None:
        kwargs["view"] = view
    try:
        if interaction.response.is_done():
            return await interaction.followup.send(**kwargs)
        return await interaction.response.send_message(**kwargs)
    except discord.NotFound:
        try:
            return await interaction.followup.send(**kwargs)
        except Exception:
            return None


async def _edit_or_followup(interaction: discord.Interaction, content: Optional[str] = None, *, embed: Optional[discord.Embed] = None, view: Optional[ui.View] = None):
    try:
        if interaction.response.is_done():
            return await interaction.edit_original_response(content=content, embed=embed, view=view)
        return await interaction.response.edit_message(content=content, embed=embed, view=view)
    except discord.NotFound:
        return await _send_ephemeral_response(interaction, content=content, embed=embed, view=view)


def _quoted_reason(reason: str) -> str:
    cleaned = _sanitize_reason(str(reason or ""))
    return f'"{cleaned}"' if cleaned else '"No reason provided"'


def _report_summary_lines(history: list[Dict[str, Any]]) -> tuple[int, list[str]]:
    lines: list[str] = []
    count = 0
    for entry in history:
        if entry.get("removed"):
            continue
        if not entry.get("reporter_id") and not entry.get("reporter_name"):
            continue
        count += 1
        actor = _actor_display(entry.get("reporter_id"), str(entry.get("reporter_name") or "UnknownReporter"))
        lines.append(f"↦ {count}: {actor}: {_quoted_reason(str(entry.get('reason') or ''))}")
    return count, lines


def _strike_summary_lines(history: list[Dict[str, Any]]) -> tuple[int, list[str]]:
    lines: list[str] = []
    seen: set[tuple[Any, ...]] = set()
    out_idx = 0
    for entry in history:
        etype = str(entry.get("type") or "").upper()
        if etype not in {"STRIKE_INCREMENT", "STRIKE_APPLIED"} or entry.get("removed"):
            continue
        key = (entry.get("moderator_id"), str(entry.get("timestamp") or ""), str(entry.get("reason") or ""), entry.get("strike_count"))
        if key in seen:
            continue
        seen.add(key)
        out_idx += 1
        actor = _actor_display(entry.get("moderator_id"), str(entry.get("moderator_name") or "UnknownModerator"))
        lines.append(f"↦ {out_idx}: {actor}: {_quoted_reason(str(entry.get('reason') or ''))}")
    return len(lines), lines


def _current_summary_lines(*, user_mention: str, case_id: str, stage: str, status: str, history: list[Dict[str, Any]], final_action: str = "", final_by: Any = None, final_by_name: str = "", recovery_status: str = "") -> list[str]:
    lines = [
        f"Current Summary: {user_mention}",
        f"Case ID: {case_id}",
        f"Stage: {stage}",
        f"Status: {status}",
    ]
    report_count, report_lines = _report_summary_lines(history)
    lines.extend(["", f"Reports: {report_count}"])
    lines.extend(report_lines or ["↦ None"])
    strike_count, strike_lines = _strike_summary_lines(history)
    if stage.lower() != "investigation" or strike_count > 0:
        lines.extend(["", f"Strikes: {strike_count}"])
        lines.extend(strike_lines or ["↦ None"])
    if final_action:
        lines.extend(["", f"Final Action: {_final_action_title(str(final_action))}"])
    if recovery_status:
        lines.extend(["", f"Recovery: {str(recovery_status).title()}"])
    return lines


def _history_title_for_entry(entry: Dict[str, Any]) -> str:
    etype = str(entry.get("type") or "").upper()
    if etype == "INVESTIGATION_REPORT":
        return "Investigation Report"
    if etype == "POST_STRIKE_REPORT":
        return "Post-Strike Report"
    if etype in {"STRIKE_INCREMENT", "STRIKE_APPLIED"}:
        return "Strike Issued"
    if etype in {"PROMOTION", "PROMOTED", "PROMOTE"}:
        return "Promotion"
    if etype == "TIMEOUT_APPLIED":
        return "Timeout Applied"
    if etype == "TIMEOUT_REMOVED":
        return "Timeout Removed"
    if etype == "BAN":
        return "User Banned"
    if etype == "KICK":
        return "User Kicked"
    if etype == "STRIKE_FORGIVEN":
        return "Strike Forgiven"
    if etype == "CASE_CLOSED":
        fa = str(entry.get("final_action") or "").lower()
        if fa == "cleared":
            return "Case Cleared"
        if fa == "forgiven":
            return "Case Forgiven"
        if fa == "auto_finalized":
            return "Auto-Finalized"
        return "Case Closed"
    return str(entry.get("label") or entry.get("action_label") or etype.replace("_", " ").title() or "History Event")


def _history_detail_lines(entry: Dict[str, Any]) -> list[str]:
    etype = str(entry.get("type") or "").upper()
    details: list[str] = []
    reason = str(entry.get("reason") or "").strip()
    if etype in {"INVESTIGATION_REPORT", "POST_STRIKE_REPORT"}:
        details.append(f"↦ Made by: {_actor_display(entry.get('reporter_id'), str(entry.get('reporter_name') or 'UnknownReporter'))}")
        details.append(f"↦ Reason: {_quoted_reason(reason)}")
        return details
    if etype in {"PROMOTION", "PROMOTED", "PROMOTE"}:
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        return details
    if etype in {"STRIKE_INCREMENT", "STRIKE_APPLIED"}:
        details.append(f"↦ Made by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        label = str(entry.get("label") or entry.get("action_label") or "").strip()
        if label:
            details.append(f"↦ Action: {label}")
        return details
    if etype == "TIMEOUT_APPLIED":
        label = str(entry.get("label") or entry.get("action_label") or "Timeout Applied").strip()
        details.append(f"↦ Action: {label}")
        return details
    if etype == "TIMEOUT_REMOVED":
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        details.append("↦ Action: Timeout Removed")
        return details
    if etype == "BAN":
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        details.append("↦ Action: Ban Applied")
        return details
    if etype == "KICK":
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        details.append("↦ Action: Kick Applied")
        return details
    if etype == "STRIKE_FORGIVEN":
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        details.append("↦ Action: Strike Removed")
        return details
    if etype == "CASE_CLOSED":
        details.append(f"↦ Approved by: {_actor_display(entry.get('moderator_id') or entry.get('closed_by'), str(entry.get('moderator_name') or 'UnknownModerator'))}")
        if reason:
            details.append(f"↦ Reason: {_quoted_reason(reason)}")
        final_action = str(entry.get('final_action') or '').strip()
        if final_action:
            details.append(f"↦ Action: {_final_action_title(final_action)}")
        return details
    if reason:
        details.append(f"↦ Reason: {_quoted_reason(reason)}")
    return details


def _render_full_history_lines(history: list[Dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    pending_promotion: Optional[Dict[str, Any]] = None
    pending_strike: Optional[Dict[str, Any]] = None

    def append_event(title: str, details: list[str]):
        if lines:
            lines.append("")
        lines.append(title)
        lines.extend(details)

    def flush_pending():
        nonlocal pending_promotion, pending_strike
        if pending_promotion is not None and pending_strike is not None:
            promo = pending_promotion
            strike = pending_strike
            promo_ts = _format_central_timestamp(str(promo.get("timestamp") or ""))
            details = _history_detail_lines(promo)
            for det in _history_detail_lines(strike):
                if det.startswith("↦ Action:"):
                    details.append(det)
            append_event(f"Promotion ({promo_ts})", details)
            pending_promotion = None
            pending_strike = None
            return
        if pending_strike is not None:
            title = _history_title_for_entry(pending_strike)
            ts = _format_central_timestamp(str(pending_strike.get("timestamp") or ""))
            append_event(f"{title} ({ts})", _history_detail_lines(pending_strike))
            pending_strike = None
        if pending_promotion is not None:
            title = _history_title_for_entry(pending_promotion)
            ts = _format_central_timestamp(str(pending_promotion.get("timestamp") or ""))
            append_event(f"{title} ({ts})", _history_detail_lines(pending_promotion))
            pending_promotion = None

    for entry in history:
        etype = str(entry.get("type") or "").upper()
        if etype in {"CASE_FINALIZED", "FINALIZED", "STATUS_CHANGED", "CASE_UPDATED"}:
            continue
        if etype in {"PROMOTION", "PROMOTED", "PROMOTE"}:
            flush_pending()
            pending_promotion = dict(entry)
            continue
        if etype in {"THRESHOLD_ACTION"}:
            if pending_strike is not None:
                pending_strike["label"] = entry.get("label")
                pending_strike["action_label"] = entry.get("action_label")
                pending_strike["action"] = entry.get("action")
                continue
        if etype in {"STRIKE_INCREMENT", "STRIKE_APPLIED"}:
            if pending_promotion is not None:
                pending_strike = dict(entry)
                continue
            flush_pending()
            pending_strike = dict(entry)
            continue
        flush_pending()
        if etype == "CASE_CLOSED" and str(entry.get("final_action") or "").lower() in {"ban", "kick", "cleared", "forgiven", "auto_finalized"}:
            continue
        title = _history_title_for_entry(entry)
        ts = _format_central_timestamp(str(entry.get("timestamp") or ""))
        append_event(f"{title} ({ts})", _history_detail_lines(entry))

    flush_pending()
    return lines


def _build_investigation_history_lines(case: Dict[str, Any]) -> tuple[list[str], int]:
    history = list(case.get("history") or [])
    lines: list[str] = []
    active_reports = 0
    for entry in history:
        etype = str(entry.get("type") or "").upper()
        if etype != "INVESTIGATION_REPORT":
            continue
        ts = _format_central_timestamp(str(entry.get("timestamp") or ""))
        actor = _actor_display(entry.get("reporter_id"), str(entry.get("reporter_name") or "UnknownReporter"))
        reason = _quoted_reason(str(entry.get("reason") or ""))
        if lines:
            lines.append("")
        if entry.get("removed"):
            lines.append(f"~~Investigation Report ({ts})~~")
            lines.append(f"~~↦ Made by: {actor}~~")
            lines.append(f"~~↦ Reason: {reason}~~")
            removed_by = _actor_display(entry.get("removed_by"), "Unknown")
            removed_at = _format_central_timestamp(str(entry.get("removed_at") or ""))
            lines.append(f"↦ Removed by: {removed_by} ({removed_at})")
        else:
            active_reports += 1
            lines.append(f"Investigation Report ({ts})")
            lines.append(f"↦ Made by: {actor}")
            lines.append(f"↦ Reason: {reason}")
    return lines, active_reports


def _build_case_embed(title: str, description: str, footer: str = "") -> discord.Embed:
    embed = discord.Embed(title=title, description=description)
    if footer:
        embed.set_footer(text=footer)
    return embed


async def _send_chunked_history(interaction: discord.Interaction, title: str, lines: list[str], *, summary_line: str):
    chunks: list[str] = []
    buf: list[str] = []
    current = 0
    for line in lines:
        add_len = len(line) + 1
        if current + add_len > 3800 and buf:
            chunks.append("\n".join(buf).rstrip())
            buf = [line]
            current = len(line)
        else:
            buf.append(line)
            current += add_len
    if buf:
        chunks.append("\n".join(buf).rstrip())
    if not chunks:
        chunks = [""]

    embeds = []
    for idx, chunk in enumerate(chunks, start=1):
        emb_title = title if idx == 1 else f"{title} (Part {idx})"
        footer = summary_line if idx == len(chunks) else ""
        embeds.append(_build_case_embed(emb_title, chunk or "No details recorded.", footer))

    await _send_ephemeral_response(interaction, embed=embeds[0])
    for extra in embeds[1:]:
        await interaction.followup.send(embed=extra, ephemeral=True)


def _sanitize_reason(reason: str) -> str:
    cleaned = " ".join((reason or "").strip().split())
    cleaned = cleaned.replace("@everyone", "@​everyone").replace("@here", "@​here")
    if len(cleaned) > MAX_REASON_LEN:
        cleaned = cleaned[: MAX_REASON_LEN - 1].rstrip() + "…"
    return cleaned


def _short_reason_preview(reason: str, *, limit: int = 100) -> str:
    cleaned = " ".join((reason or "").strip().split())
    cleaned = cleaned.replace("@everyone", "@​everyone").replace("@here", "@​here")
    if not cleaned:
        return "No reason provided"
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(1, limit - 1)].rstrip() + "…"


def _is_staff(member: Optional[discord.Member]) -> bool:
    if member is None:
        return False
    perms = member.guild_permissions
    return bool(perms.moderate_members or perms.kick_members or perms.ban_members or perms.administrator)


def _is_admin(member: Optional[discord.Member]) -> bool:
    if member is None:
        return False
    perms = member.guild_permissions
    return bool(perms.administrator)


async def _fetch_thread_by_name_or_id(
    forum: discord.ForumChannel,
    *,
    thread_id: Optional[int],
    name: str,
) -> Optional[discord.Thread]:
    if thread_id:
        th = forum.guild.get_thread(int(thread_id))
        if isinstance(th, discord.Thread) and th.parent_id == forum.id:
            return th
    lname = name.lower()
    for th in list(forum.threads):
        if th.name.lower() == lname:
            return th
    try:
        async for th in forum.archived_threads(limit=100):
            if th.name.lower() == lname:
                try:
                    await th.edit(archived=False)
                except Exception:
                    pass
                return th
    except Exception:
        pass
    return None


class ReportReasonModal(ui.Modal, title="Report User"):
    def __init__(self, cog: "ModerationCog", user_id: int):
        super().__init__()
        self._cog = cog
        self._user_id = int(user_id)
        self.reason = ui.TextInput(
            label="Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            placeholder=(
                "Write carefully. Staff may later summarize this for the user."
            ),
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        user = guild.get_member(self._user_id)
        if user is None:
            try:
                user = await guild.fetch_member(self._user_id)
            except Exception:
                user = None
        if user is None:
            await interaction.response.send_message("Unable to resolve that user in this server.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            msg = await self._cog.mgr.report_user(interaction, user, str(self.reason.value))
            await interaction.followup.send(msg, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Report failed: {e}", ephemeral=True)


class InvestigationSummaryButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Current Summary", style=discord.ButtonStyle.secondary, custom_id=f"moderation:inv:summary:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Summary is unavailable.", ephemeral=True)
            return
        await view.show_summary(interaction, self._user_id)


class InvestigationHistoryButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Full History", style=discord.ButtonStyle.secondary, custom_id=f"moderation:inv:history:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("History is unavailable.", ephemeral=True)
            return
        await view.show_history(interaction, self._user_id)


class PromoteToStrikeButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Promote to Strike", style=discord.ButtonStyle.primary, custom_id=f"moderation:inv:promote:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Promotion is unavailable.", ephemeral=True)
            return
        await view.begin_promote(interaction, self._user_id)


class InvestigationBanButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Ban", style=discord.ButtonStyle.danger, custom_id=f"moderation:inv:ban:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Ban is unavailable.", ephemeral=True)
            return
        await view.begin_ban(interaction, self._user_id)


class ActiveSummaryButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Current Summary", style=discord.ButtonStyle.secondary, custom_id=f"moderation:act:summary:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Summary is unavailable.", ephemeral=True)
            return
        await view.show_summary(interaction, self._user_id)


class ActiveHistoryButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Full History", style=discord.ButtonStyle.secondary, custom_id=f"moderation:act:history:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("History is unavailable.", ephemeral=True)
            return
        await view.show_history(interaction, self._user_id)


class ArchiveSummaryButton(ui.Button):
    def __init__(self, user_id: int, archive_case_id: str):
        super().__init__(label="Current Summary", style=discord.ButtonStyle.secondary, custom_id=f"moderation:arc:summary:{int(user_id)}:{archive_case_id}")
        self._user_id = int(user_id)
        self._archive_case_id = str(archive_case_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Summary is unavailable.", ephemeral=True)
            return
        await view.show_summary(interaction, self._user_id)


class ArchiveHistoryButton(ui.Button):
    def __init__(self, user_id: int, archive_case_id: str):
        super().__init__(label="Full History", style=discord.ButtonStyle.secondary, custom_id=f"moderation:arc:history:{int(user_id)}:{archive_case_id}")
        self._user_id = int(user_id)
        self._archive_case_id = str(archive_case_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("History is unavailable.", ephemeral=True)
            return
        await view.show_history(interaction, self._user_id)


class BanRecoveryButton(ui.Button):
    def __init__(self, user_id: int, archive_case_id: str):
        super().__init__(label="Ban Recovery", style=discord.ButtonStyle.primary, custom_id=f"moderation:arc:recover:{int(user_id)}:{archive_case_id}")
        self._user_id = int(user_id)
        self._archive_case_id = str(archive_case_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Ban Recovery is unavailable.", ephemeral=True)
            return
        await view.begin_ban_recovery(interaction, self._user_id, self._archive_case_id)


class BanRecoveryReasonModal(ui.Modal, title="Ban Recovery"):
    def __init__(self, cog: "ModerationCog", *, user_id: int, archive_case_id: str):
        super().__init__()
        self._cog = cog
        self._user_id = int(user_id)
        self._archive_case_id = str(archive_case_id)
        self.reason = ui.TextInput(
            label="Recovery Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            placeholder="Required. Write why this ban recovery is being prepared.",
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        await self._cog.mgr.prepare_ban_recovery(
            interaction,
            user_id=self._user_id,
            archive_case_id=self._archive_case_id,
            recovery_reason=str(self.reason.value or ""),
        )


class IncreaseStrikeButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Increase Strike", style=discord.ButtonStyle.primary, custom_id=f"moderation:act:increase:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Increase Strike is unavailable.", ephemeral=True)
            return
        await view.begin_increase_strike(interaction, self._user_id)


class ForgiveStrikeButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Forgive Strike", style=discord.ButtonStyle.danger, custom_id=f"moderation:act:forgive:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Forgive Strike is unavailable.", ephemeral=True)
            return
        await view.begin_forgive_strike(interaction, self._user_id)


class ClearCaseButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Clear Case", style=discord.ButtonStyle.danger, custom_id=f"moderation:act:clear:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Clear Case is unavailable.", ephemeral=True)
            return
        await view.begin_clear_case(interaction, self._user_id)


class ActiveBanButton(ui.Button):
    def __init__(self, user_id: int):
        super().__init__(label="Ban", style=discord.ButtonStyle.danger, custom_id=f"moderation:act:ban:{int(user_id)}")
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Ban is unavailable.", ephemeral=True)
            return
        await view.begin_ban(interaction, self._user_id)


class AcknowledgeReportButton(ui.Button):
    def __init__(self, user_id: int, alert_id: str):
        super().__init__(label="Acknowledge Report", style=discord.ButtonStyle.secondary, custom_id=f"moderation:alert:ack:{int(user_id)}:{alert_id}")
        self._user_id = int(user_id)
        self._alert_id = str(alert_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("That alert is unavailable.", ephemeral=True)
            return
        await view.acknowledge(interaction, self._user_id, self._alert_id)


class RemoveReportButton(ui.Button):
    def __init__(self, *, case_type: str, user_id: int):
        super().__init__(label="Remove Report", style=discord.ButtonStyle.danger, custom_id=f"moderation:{case_type}:remove:{int(user_id)}")
        self._case_type = str(case_type)
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if view is None:
            await interaction.response.send_message("Removal is unavailable.", ephemeral=True)
            return
        await view.begin_remove_report(interaction, self._case_type, self._user_id)


class RemoveReportSelect(ui.Select):
    def __init__(self, cog: "ModerationCog", *, case_type: str, user_id: int, options: list[discord.SelectOption]):
        super().__init__(placeholder="Choose a report to remove…", min_values=1, max_values=1, options=options, custom_id=f"moderation:{case_type}:remove_select:{int(user_id)}")
        self._cog = cog
        self._case_type = str(case_type)
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        token = str(self.values[0]) if self.values else ""
        if not token:
            await interaction.response.send_message("Choose a report first.", ephemeral=True)
            return
        await self._cog.mgr.remove_report_entry(interaction, case_type=self._case_type, user_id=self._user_id, token=token)
        if self.view is not None:
            self.view.stop()


class RemoveReportChoiceView(ui.View):
    def __init__(self, cog: "ModerationCog", *, case_type: str, user_id: int, options: list[discord.SelectOption]):
        super().__init__(timeout=180)
        self._cog = cog
        self.add_item(RemoveReportSelect(cog, case_type=case_type, user_id=user_id, options=options))
        self.add_item(CancelButton(custom_id=f"moderation:{case_type}:remove_cancel:{int(user_id)}"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return False
        return True


class ForgiveStrikeSelect(ui.Select):
    def __init__(self, cog: "ModerationCog", *, user_id: int, options: list[discord.SelectOption]):
        super().__init__(placeholder="Choose a strike reason to forgive…", min_values=1, max_values=1, options=options, custom_id=f"moderation:act:forgive_select:{int(user_id)}")
        self._cog = cog
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        strike_id = str(self.values[0]) if self.values else ""
        if not strike_id:
            await interaction.response.send_message("Choose a strike first.", ephemeral=True)
            return
        await self._cog.mgr.confirm_forgive_strike(interaction, user_id=self._user_id, strike_id=strike_id)
        if self.view is not None:
            self.view.stop()


class ForgiveStrikeChoiceView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, options: list[discord.SelectOption]):
        super().__init__(timeout=180)
        self.add_item(ForgiveStrikeSelect(cog, user_id=user_id, options=options))
        self.add_item(CancelButton(custom_id=f"moderation:act:forgive_cancel:{int(user_id)}"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return False
        return True


class ConfirmForgiveView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, strike_id: str):
        super().__init__(timeout=180)
        self._cog = cog
        self._user_id = int(user_id)
        self._strike_id = str(strike_id)
        self.add_item(ConfirmForgiveButton(cog, user_id=self._user_id, strike_id=self._strike_id))
        self.add_item(CancelButton(custom_id=f"moderation:act:forgive_confirm_cancel:{int(user_id)}"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return False
        return True


class ConfirmForgiveButton(ui.Button):
    def __init__(self, cog: "ModerationCog", *, user_id: int, strike_id: str):
        super().__init__(label="Confirm Forgive", style=discord.ButtonStyle.danger, custom_id=f"moderation:act:forgive_confirm:{int(user_id)}:{strike_id}")
        self._cog = cog
        self._user_id = int(user_id)
        self._strike_id = str(strike_id)

    async def callback(self, interaction: discord.Interaction):
        await self._cog.mgr.forgive_strike(interaction, user_id=self._user_id, strike_id=self._strike_id)
        if self.view is not None:
            self.view.stop()


class ConfirmClearCaseView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int):
        super().__init__(timeout=180)
        self.add_item(ConfirmClearCaseButton(cog, user_id=user_id))
        self.add_item(CancelButton(custom_id=f"moderation:act:clear_cancel:{int(user_id)}"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return False
        return True


class ConfirmClearCaseButton(ui.Button):
    def __init__(self, cog: "ModerationCog", *, user_id: int):
        super().__init__(label="Confirm Clear Case", style=discord.ButtonStyle.danger, custom_id=f"moderation:act:clear_confirm:{int(user_id)}")
        self._cog = cog
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        await self._cog.mgr.clear_active_case(interaction, user_id=self._user_id)
        if self.view is not None:
            self.view.stop()


class ActiveAlertView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, alert_id: str):
        super().__init__(timeout=None)
        self._cog = cog
        self.add_item(AcknowledgeReportButton(user_id, alert_id))

    async def acknowledge(self, interaction: discord.Interaction, user_id: int, alert_id: str):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self._cog.mgr.acknowledge_alert(interaction, int(user_id), str(alert_id))




class StaffStrikeModal(ui.Modal, title="Strike User"):
    def __init__(self, cog: "ModerationCog", user_id: int):
        super().__init__()
        self._cog = cog
        self._user_id = int(user_id)
        self.reason = ui.TextInput(
            label="Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            placeholder="Reason for this strike. This is a staff action.",
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message('Server only.', ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message('Admins only.', ephemeral=True)
            return
        user = guild.get_member(self._user_id)
        if user is None:
            try:
                user = await guild.fetch_member(self._user_id)
            except Exception:
                user = None
        if user is None:
            await interaction.response.send_message('Unable to resolve that user in this server.', ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            msg = await self._cog.mgr.strike_to_active(interaction, user, str(self.reason.value))
            await interaction.followup.send(msg, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f'Strike failed: {e}', ephemeral=True)

class ActiveBanReasonModal(ui.Modal, title="Ban User"):
    def __init__(self, cog: "ModerationCog", user_id: int):
        super().__init__()
        self._cog = cog
        self._user_id = int(user_id)
        self.reason = ui.TextInput(
            label="Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            placeholder="Required. Write why this active case is being manually banned.",
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await self._cog.mgr.manual_ban_active_case(interaction, user_id=self._user_id, reason=str(self.reason.value))


class InvestigationBanReasonModal(ui.Modal, title="Ban User"):
    def __init__(self, cog: "ModerationCog", user_id: int):
        super().__init__()
        self._cog = cog
        self._user_id = int(user_id)
        self.reason = ui.TextInput(
            label="Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            placeholder="Required. Write why this investigation case is being manually banned.",
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await self._cog.mgr.manual_ban_investigation_case(interaction, user_id=self._user_id, reason=str(self.reason.value))


class StrikeReasonModal(ui.Modal):
    def __init__(self, cog: "ModerationCog", *, user_id: int, mode: str, initial_text: str = ""):
        super().__init__(title="Define Strike Reason")
        self._cog = cog
        self._user_id = int(user_id)
        self._mode = str(mode)
        self.reason = ui.TextInput(
            label="Strike Reason",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=MAX_REASON_LEN,
            default=initial_text[:MAX_REASON_LEN] if initial_text else None,
            placeholder="Summarize or rewrite the report. This will be sent to the user.",
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        reason = _sanitize_reason(str(self.reason.value))
        if not reason:
            await interaction.response.send_message("Strike reason is required.", ephemeral=True)
            return
        await self._cog.mgr.execute_promote(interaction, self._user_id, mode=self._mode, strike_reason=reason, source_token=None)


class ChooseExistingReasonSelect(ui.Select):
    def __init__(self, cog: "ModerationCog", *, user_id: int, mode: str, options: list[discord.SelectOption]):
        super().__init__(placeholder="Use an existing report reason…", min_values=1, max_values=1, options=options, custom_id=f"moderation:reason:select:{mode}:{user_id}")
        self._cog = cog
        self._user_id = int(user_id)
        self._mode = str(mode)

    async def callback(self, interaction: discord.Interaction):
        selected = str(self.values[0]) if self.values else ""
        if not selected:
            await interaction.response.send_message("Select a reason first.", ephemeral=True)
            return
        resolved = await self._cog.mgr.resolve_selected_report_reason(interaction, self._user_id, selected)
        if not resolved:
            await interaction.response.send_message("That report reason is no longer available.", ephemeral=True)
            return
        await self._cog.mgr.execute_promote(interaction, self._user_id, mode=self._mode, strike_reason=resolved, source_token=selected)
        if self.view is not None:
            self.view.stop()


class SummaryReasonButton(ui.Button):
    def __init__(self, cog: "ModerationCog", *, user_id: int, mode: str, initial_text: str = ""):
        super().__init__(label="Write Summary", style=discord.ButtonStyle.primary, custom_id=f"moderation:reason:summary:{mode}:{user_id}")
        self._cog = cog
        self._user_id = int(user_id)
        self._mode = str(mode)
        self._initial_text = initial_text

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(StrikeReasonModal(self._cog, user_id=self._user_id, mode=self._mode, initial_text=self._initial_text))
        if self.view is not None:
            self.view.stop()


class CancelButton(ui.Button):
    def __init__(self, *, custom_id: str = "moderation:cancel"):
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger, custom_id=custom_id)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(content="Action cancelled.", view=None)
        if self.view is not None:
            self.view.stop()


class ConfirmPromoteButton(ui.Button):
    def __init__(self, cog: "ModerationCog", *, user_id: int):
        super().__init__(label="Confirm Promote", style=discord.ButtonStyle.success, custom_id=f"moderation:promote:confirm:{user_id}")
        self._cog = cog
        self._user_id = int(user_id)

    async def callback(self, interaction: discord.Interaction):
        await self._cog.mgr.show_reason_picker(interaction, int(self._user_id), mode="merge_inc")
        if self.view is not None:
            self.view.stop()


class PromoteExistingActiveConfirmView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int):
        super().__init__(timeout=180)
        self._cog = cog
        self.add_item(ConfirmPromoteButton(cog, user_id=user_id))
        self.add_item(CancelButton(custom_id=f"moderation:promote:cancel:{user_id}"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return False
        return True


class PromoteReasonHistoryButton(ui.Button):
    def __init__(self, cog: "ModerationCog", *, user_id: int):
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


class TutorialInteractiveView(ui.View):
    def __init__(
        self,
        cog: "ModerationCog",
        guild_id: int,
        session_id: str,
        tutorial_type: str,
        owner: discord.Member,
        steps: list[Dict[str, Any]],
    ):
        super().__init__(timeout=3600)
        self._cog = cog
        self._guild_id = int(guild_id)
        self._session_id = str(session_id)
        self._tutorial_type = str(tutorial_type).strip().lower()
        self._owner = owner
        self._steps = list(steps)
        self._index = 0
        self._sync_buttons()

    def _sync_buttons(self) -> None:
        total = max(len(self._steps), 1)
        self.step_indicator.label = f"Step {self._index + 1}/{total}"
        self.prev_step.disabled = self._index <= 0
        self.next_step.disabled = self._index >= total - 1

    async def _validate_owner_and_session(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None or int(interaction.guild.id) != self._guild_id:
            await _send_ephemeral_response(interaction, "This tutorial button can only be used in its original server.")
            return False
        state = _load_state()
        cfg = _guild_cfg(state, self._guild_id)
        _cleanup_expired_tutorial_sessions(cfg)
        rec = _tutorial_sessions(cfg).get(self._session_id)
        if not isinstance(rec, dict):
            await _send_ephemeral_response(interaction, "This tutorial session is already closed or expired.")
            return False
        if int(rec.get("owner_user_id") or 0) != int(interaction.user.id):
            await _send_ephemeral_response(interaction, "Only the tutorial owner can use this tutorial session.")
            return False
        return True

    async def _redraw(self, interaction: discord.Interaction) -> None:
        embed = self._cog.mgr.build_tutorial_step_embed(
            owner=self._owner,
            tutorial_type=self._tutorial_type,
            session_id=self._session_id,
            steps=self._steps,
            step_index=self._index,
        )
        self._sync_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    @ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary)
    async def prev_step(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._validate_owner_and_session(interaction):
            return
        self._index = max(self._index - 1, 0)
        await self._redraw(interaction)

    @ui.button(label="Step", style=discord.ButtonStyle.secondary, disabled=True)
    async def step_indicator(self, interaction: discord.Interaction, button: ui.Button):
        await _send_ephemeral_response(interaction, "Use Previous/Next to move through the tutorial.")

    @ui.button(label="Next ▶", style=discord.ButtonStyle.primary)
    async def next_step(self, interaction: discord.Interaction, button: ui.Button):
        if not await self._validate_owner_and_session(interaction):
            return
        self._index = min(self._index + 1, max(len(self._steps) - 1, 0))
        await self._redraw(interaction)

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
                await interaction.response.edit_message(view=self)
        except Exception:
            pass
        await _send_ephemeral_response(interaction, "Tutorial completed and cleaned up.")


class InvestigationCaseView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, report_count: int):
        super().__init__(timeout=None)
        self._cog = cog
        self._user_id = int(user_id)
        self._report_count = int(report_count or 0)
        self.add_item(InvestigationSummaryButton(self._user_id))
        if self._report_count > 0:
            self.add_item(InvestigationHistoryButton(self._user_id))
            self.add_item(RemoveReportButton(case_type="inv", user_id=self._user_id))
        self.add_item(PromoteToStrikeButton(self._user_id))
        self.add_item(InvestigationBanButton(self._user_id))

    async def show_summary(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("No investigation case found.", ephemeral=True)
            return
        lines = _current_summary_lines(
            user_mention=f"<@{int(user_id)}>",
            case_id=str(case.get("case_id") or "UnknownCase"),
            stage="Investigation",
            status=str(case.get("status") or "ACTIVE").upper(),
            history=list(case.get("history") or []),
        )
        await _send_chunked_history(interaction, f"Investigation Summary • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing current summary for <@{int(user_id)}>")

    async def show_history(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("No investigation history found.", ephemeral=True)
            return
        lines, active_reports = _build_investigation_history_lines(case)
        if not lines:
            await interaction.response.send_message("No investigation history found.", ephemeral=True)
            return
        await _send_chunked_history(interaction, f"Investigation History • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing {active_reports} active report(s) for <@{int(user_id)}>")

    async def begin_remove_report(self, interaction: discord.Interaction, case_type: str, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await self._cog.mgr.show_remove_report_picker(interaction, case_type=case_type, user_id=int(user_id))

    async def begin_ban(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.send_modal(InvestigationBanReasonModal(self._cog, int(user_id)))

    async def begin_promote(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return

        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Investigation case not found.", ephemeral=True)
            return
        if str(case.get("status") or "").upper() == "PROMOTED":
            await interaction.response.send_message("This action has already been handled.", ephemeral=True)
            return

        active_case = cfg.get("active_cases", {}).get(str(int(user_id)))
        mention = f"<@{int(user_id)}>"
        if active_case:
            content = (
                f"Active case already exists for {mention}.\n\n"
                f"Promoting this report will:\n"
                f"• merge it into the existing Active case\n"
                f"• increase the strike count"
            )
            view = PromoteExistingActiveConfirmView(self._cog, user_id=int(user_id))
            await interaction.response.send_message(content=content, ephemeral=True, view=view)
            return

        await self._cog.mgr.show_reason_picker(interaction, int(user_id), mode="create_strike1")


class ActiveCaseView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, history_count: int):
        super().__init__(timeout=None)
        self._cog = cog
        self._user_id = int(user_id)
        self.add_item(ActiveSummaryButton(self._user_id))
        if int(history_count or 0) > 0:
            self.add_item(ActiveHistoryButton(self._user_id))
        self.add_item(IncreaseStrikeButton(self._user_id))
        self.add_item(RemoveReportButton(case_type="act", user_id=self._user_id))
        self.add_item(ForgiveStrikeButton(self._user_id))
        self.add_item(ActiveBanButton(self._user_id))
        self.add_item(ClearCaseButton(self._user_id))

    async def show_summary(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("No active case found.", ephemeral=True)
            return
        lines = _current_summary_lines(
            user_mention=f"<@{int(user_id)}>",
            case_id=str(case.get("case_id") or "UnknownCase"),
            stage="Active",
            status=str(case.get("status") or "IN PROGRESS").upper(),
            history=list(case.get("history") or []),
        )
        await _send_chunked_history(interaction, f"Active Summary • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing current summary for <@{int(user_id)}>")

    async def begin_remove_report(self, interaction: discord.Interaction, case_type: str, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await self._cog.mgr.show_remove_report_picker(interaction, case_type=case_type, user_id=int(user_id))

    async def begin_increase_strike(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.send_modal(StaffStrikeModal(self._cog, int(user_id)))

    async def begin_forgive_strike(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await self._cog.mgr.show_forgive_picker(interaction, int(user_id))

    async def begin_clear_case(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.send_message("Clear this active case and move it to Archive? This ends the live case, removes any active timeout first if present, archives the case, and any future strike will start a new active case.", ephemeral=True, view=ConfirmClearCaseView(self._cog, user_id=int(user_id)))

    async def begin_ban(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await interaction.response.send_modal(ActiveBanReasonModal(self._cog, int(user_id)))

    async def show_history(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("No active case history found.", ephemeral=True)
            return
        history = list(case.get("history") or [])
        if not history:
            await interaction.response.send_message("No active case history found.", ephemeral=True)
            return

        lines = _render_full_history_lines(history)
        if not lines:
            await interaction.response.send_message("No active case history found.", ephemeral=True)
            return
        await _send_chunked_history(interaction, f"Active History • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing {len(lines)} history entr{'y' if len(lines)==1 else 'ies'} for <@{int(user_id)}>")


class ArchiveCaseView(ui.View):
    def __init__(self, cog: "ModerationCog", *, user_id: int, archive_case_id: str, archive_case: Optional[Dict[str, Any]] = None):
        super().__init__(timeout=None)
        self._cog = cog
        self._user_id = int(user_id)
        self._archive_case_id = str(archive_case_id)
        self.add_item(ArchiveSummaryButton(self._user_id, self._archive_case_id))
        self.add_item(ArchiveHistoryButton(self._user_id, self._archive_case_id))
        self._maybe_add_ban_recovery_button(archive_case)

    def _maybe_add_ban_recovery_button(self, archive_case: Optional[Dict[str, Any]] = None):
        try:
            case_obj = archive_case
            if case_obj is None:
                state = _load_state()
                for cfg in (state.get("guilds") or {}).values():
                    if not isinstance(cfg, dict):
                        continue
                    case_obj = (cfg.get("archive_cases") or {}).get(str(self._archive_case_id))
                    if case_obj is not None:
                        break
            if case_obj and _eligible_ban_recovery(case_obj):
                self.add_item(BanRecoveryButton(self._user_id, self._archive_case_id))
        except Exception:
            pass

    async def begin_ban_recovery(self, interaction: discord.Interaction, user_id: int, archive_case_id: str):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        await self._cog.mgr.begin_ban_recovery(interaction, user_id=int(user_id), archive_case_id=str(archive_case_id))

    async def show_summary(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await _send_ephemeral_response(interaction, "Mods/admins only.")
            return
        guild = interaction.guild
        if guild is None:
            await _send_ephemeral_response(interaction, "Server only.")
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        archive_cases = _normalize_archive_storage(cfg).setdefault("archive_cases", {})
        case = archive_cases.get(str(self._archive_case_id)) or {}
        if not case:
            await _send_ephemeral_response(interaction, "Archived case not found.")
            return
        display_handle = _display_handle(guild, int(user_id), str(case.get("display_name") or case.get("user_name") or user_id))
        recovery = case.get("ban_recovery") or {}
        lines = _current_summary_lines(
            user_mention=display_handle,
            case_id=str(case.get("case_id") or "UnknownCase"),
            stage="Archive",
            status="FINALIZED",
            history=list(case.get("history") or []),
            final_action=str(case.get("final_action") or ""),
            recovery_status=str(recovery.get("status") or ("Available" if _eligible_ban_recovery(case) else "")),
        )
        await _send_chunked_history(interaction, f"Archive Summary • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing archived summary for {display_handle}")

    async def show_history(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await _send_ephemeral_response(interaction, "Mods/admins only.")
            return
        guild = interaction.guild
        if guild is None:
            await _send_ephemeral_response(interaction, "Server only.")
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        archive_cases = _normalize_archive_storage(cfg).setdefault("archive_cases", {})
        case_ids = _get_archived_case_ids_for_user(cfg, int(user_id))
        if not case_ids:
            await _send_ephemeral_response(interaction, "No archived cases found.")
            return
        ordered_case_ids = [str(self._archive_case_id)] + [str(cid) for cid in case_ids if str(cid) != str(self._archive_case_id)]
        display_handle = _display_handle(guild, int(user_id), str(user_id))
        embeds: list[discord.Embed] = []
        total_cases = 0
        for case_id in ordered_case_ids:
            case = archive_cases.get(str(case_id)) or {}
            case_lines = _render_full_history_lines(list(case.get("history") or []))
            if not case_lines:
                continue
            total_cases += 1
            heading = f"__**Archived Case #{total_cases} • {case.get('case_id', 'UnknownCase')}**__"
            body = "\n".join([heading, "", *case_lines]).rstrip()
            footer = f"Showing full archived history for {total_cases} case(s) for {display_handle}"
            embeds.append(_build_case_embed(f"Archive History • {display_handle}", body, footer))
        if not embeds:
            await _send_ephemeral_response(interaction, "No archived case history found.")
            return
        await _send_ephemeral_response(interaction, embed=embeds[0])
        for extra in embeds[1:]:
            await interaction.followup.send(embed=extra, ephemeral=True)



async def _send_chunked_history(interaction: discord.Interaction, title: str, lines: list[str], *, summary_line: str):
    chunks = []
    buf = []
    current = 0
    for line in lines:
        add_len = len(line) + (2 if buf else 0)
        if current + add_len > 3800 and buf:
            chunks.append("\n\n".join(buf))
            buf = [line]
            current = len(line)
        else:
            buf.append(line)
            current += add_len
    if buf:
        chunks.append("\n\n".join(buf))

    first_description = chunks[0] if chunks else summary_line
    if len(first_description) + 2 + len(summary_line) <= 4096:
        first_description = f"{first_description}\n\n{summary_line}"
        remaining_chunks = chunks[1:]
    else:
        remaining_chunks = chunks[1:] + [summary_line]

    embed = discord.Embed(title=title, description=first_description)
    await interaction.response.send_message(embed=embed, ephemeral=True)
    for extra in remaining_chunks:
        await interaction.followup.send(embed=discord.Embed(description=extra), ephemeral=True)


class ModerationManager:
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def send_investigation_history(self, interaction: discord.Interaction, user_id: int):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("No investigation history found.", ephemeral=True)
            return
        lines, active_reports = _build_investigation_history_lines(case)
        if not lines:
            await interaction.response.send_message("No investigation history found.", ephemeral=True)
            return
        await _send_chunked_history(interaction, f"Investigation History • {case.get('case_id', 'UnknownCase')}", lines, summary_line=f"Showing {active_reports} active report(s) for <@{int(user_id)}>")

    def _build_status_embed(self, *, title: str, user: Optional[discord.abc.User] = None, case_id: Optional[str] = None, details: Optional[list[tuple[str,str,bool]]] = None, description: Optional[str] = None) -> discord.Embed:
        embed = discord.Embed(title=title, description=description)
        if user is not None:
            mention = getattr(user, 'mention', None) or f"<@{int(user.id)}>"
            embed.add_field(name="User", value=f"{mention}\nID: {int(user.id)}", inline=False)
            try:
                embed.set_thumbnail(url=user.display_avatar.url)
            except Exception:
                pass
        if case_id:
            embed.add_field(name="Case ID", value=str(case_id), inline=True)
        for name, value, inline in (details or []):
            embed.add_field(name=name, value=value, inline=inline)
        return embed

    async def _post_status_embed(self, guild: discord.Guild, *, title: str, user: Optional[discord.abc.User] = None, case_id: Optional[str] = None, details: Optional[list[tuple[str,str,bool]]] = None, description: Optional[str] = None, view=None, content=None):
        embed = self._build_status_embed(title=title, user=user, case_id=case_id, details=details, description=description)
        try:
            return await post_status(self.bot, content=content, embed=embed, view=view, guild=guild, allowed_mentions=discord.AllowedMentions(everyone=bool(content), roles=False, users=False))
        except Exception:
            return None

    async def ensure_threads(self, guild: discord.Guild) -> Optional[dict]:
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)

        forum = await get_or_create_ops_forum(guild)
        if forum is None:
            return None

        tags = list(forum.available_tags)
        by_name = {t.name: t for t in tags}
        changed = False
        for name in (ENABLED_TAG, DISABLED_TAG):
            if name not in by_name:
                tags.append(discord.ForumTag(name=name, emoji=None, moderated=False))
                changed = True
        if changed:
            try:
                await forum.edit(available_tags=tags)
                refreshed = guild.get_channel(forum.id)
                if isinstance(refreshed, discord.ForumChannel):
                    forum = refreshed
            except Exception:
                pass

        tags_by_name = {t.name: t for t in forum.available_tags}
        status_tag = tags_by_name.get(ENABLED_TAG if bool(cfg.get("enabled", True)) else DISABLED_TAG)

        names_and_keys = [
            (THREAD_INVESTIGATION, "investigation_thread_id"),
            (THREAD_ACTIVE, "active_thread_id"),
            (THREAD_ARCHIVE, "archive_thread_id"),
        ]

        ensured: Dict[str, discord.Thread] = {}
        for thread_name, key in names_and_keys:
            th = await _fetch_thread_by_name_or_id(
                forum,
                thread_id=cfg.get(key),
                name=thread_name,
            )
            if th is None:
                try:
                    result = await forum.create_thread(name=thread_name, content=f"{thread_name} initialized")
                    th = result.thread
                except Exception:
                    log.exception("moderation: failed to create thread %s", thread_name)
                    return None
            if th is None:
                return None
            cfg[key] = th.id
            ensured[key] = th
            if status_tag is not None:
                try:
                    await th.edit(applied_tags=[status_tag])
                except Exception:
                    pass

        _save_state(state)
        log.info(
            "offender threads ensured guild=%s investigation=%s active=%s archive=%s state=%s",
            getattr(guild, "name", guild.id),
            ensured["investigation_thread_id"].name,
            ensured["active_thread_id"].name,
            ensured["archive_thread_id"].name,
            ENABLED_TAG if bool(cfg.get("enabled", True)) else DISABLED_TAG,
        )
        return {
            "forum": forum,
            "investigation": ensured["investigation_thread_id"],
            "active": ensured["active_thread_id"],
            "archive": ensured["archive_thread_id"],
            "cfg": cfg,
        }

    def _build_investigation_embed(
        self,
        user: discord.Member,
        reporter: discord.abc.User,
        *,
        reason: str,
        case_id: str,
        report_count: int,
        status: str,
    ) -> discord.Embed:
        embed = discord.Embed(
            title=f"REPORT • {user.display_name}",
            description=(
                f"• Case ID ⭢ {case_id}\n"
                f"• Stage ⭢ Investigation\n"
                f"• Status ⭢ {status}\n\n"
                f"• Reports ⭢ {int(report_count or 0)}"
            ),
        )
        embed.add_field(name="User", value=f"{user.mention}\nID: {user.id}", inline=False)
        embed.set_footer(text=f"Latest report by {getattr(reporter, 'display_name', getattr(reporter, 'name', 'Unknown'))}: {_short_reason_preview(reason, limit=120)}")
        embed.set_thumbnail(url=user.display_avatar.url)
        return embed

    def _build_active_embed(
        self,
        user: discord.Member,
        moderator: discord.abc.User,
        *,
        reason: str,
        case_id: str,
        strike_count: int,
        status: str,
        history_count: int,
        source_label: str,
        prior_archive_count: int = 0,
    ) -> discord.Embed:
        embed = discord.Embed(
            title=f"ACTIVE CASE • {user.display_name}",
            description=(
                f"• Case ID ⭢ {case_id}\n"
                f"• Stage ⭢ Active\n"
                f"• Status ⭢ {status}\n\n"
                f"• Reports ⭢ {max(0, int(history_count or 0))}\n"
                f"• Strikes ⭢ {int(strike_count or 0)}"
            ),
        )
        settings = _guild_settings(_guild_cfg(_load_state(), user.guild.id))
        current_action = _current_action_label(settings, strike_count)
        embed.add_field(name="User", value=f"{user.mention}\nID: {user.id}", inline=False)
        embed.add_field(name="Current Action", value=current_action, inline=True)
        embed.add_field(name="Source", value=source_label, inline=True)
        if int(prior_archive_count or 0) > 0:
            embed.add_field(name="Prior Archived Cases", value=str(int(prior_archive_count)), inline=True)
        embed.set_footer(text=f"Latest strike by {getattr(moderator, 'display_name', getattr(moderator, 'name', 'Unknown'))}: {_short_reason_preview(reason, limit=120)}")
        embed.set_thumbnail(url=user.display_avatar.url)
        return embed

    def _build_archive_embed(
            self,
            user: discord.abc.User,
            *,
            archive_case: Dict[str, Any],
            prior_case_count: int = 0,
        ) -> discord.Embed:
            final_action = str(archive_case.get("final_action") or "")
            recovery = archive_case.get("ban_recovery") or {}
            recovery_status = str(recovery.get("status") or ("available" if _eligible_ban_recovery(archive_case) else "")).title()
            report_count, _ = _report_summary_lines(list(archive_case.get("history") or []))
            strike_count, _ = _strike_summary_lines(list(archive_case.get("history") or []))
            embed = discord.Embed(
                title=f"ARCHIVE CASE • {user.display_name}",
                description=(
                    f"• Case ID ⭢ {str(archive_case.get('case_id') or 'UnknownCase')}\n"
                    f"• Stage ⭢ Archive\n"
                    f"• Status ⭢ FINALIZED\n\n"
                    f"• Reports ⭢ {report_count}\n"
                    f"• Strikes ⭢ {strike_count}\n"
                    f"• Final Action ⭢ {_final_action_title(final_action)}"
                    + (f"\n• Recovery ⭢ {recovery_status}" if recovery_status else "")
                ),
                color=discord.Color.red() if str(final_action).lower() == "ban" else discord.Color.orange(),
            )
            embed.add_field(name="User", value=f"{user.mention}\nID: {user.id}", inline=False)
            closed_by = int(archive_case.get("closed_by") or 0)
            if closed_by:
                embed.add_field(name="Finalized By", value=f"<@{closed_by}>\nID: {closed_by}", inline=False)
            if int(prior_case_count or 0) > 0:
                embed.add_field(name="Prior Archived Cases", value=str(int(prior_case_count)), inline=True)
            embed.set_footer(text=_short_reason_preview(str(archive_case.get("final_reason") or archive_case.get("latest_reason") or "No reason provided"), limit=140))
            embed.set_thumbnail(url=user.display_avatar.url)
            return embed

    async def _fetch_user_object(self, user_id: int) -> Optional[discord.abc.User]:
        user = self.bot.get_user(int(user_id))
        if user is not None:
            return user
        try:
            return await self.bot.fetch_user(int(user_id))
        except Exception:
            return None

    def _invite_channel(self, guild: discord.Guild) -> Optional[discord.TextChannel]:
        if isinstance(guild.system_channel, discord.TextChannel) and guild.system_channel.permissions_for(guild.me).create_instant_invite:
            return guild.system_channel
        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if perms.create_instant_invite:
                return ch
        return None

    async def _make_ban_recovery_invite(self, guild: discord.Guild) -> Optional[discord.Invite]:
        ch = self._invite_channel(guild)
        if ch is None:
            return None
        try:
            return await ch.create_invite(max_age=604800, max_uses=1, unique=True, reason="Ban recovery return invite")
        except Exception:
            return None

    async def _refresh_archive_case_message(self, guild: discord.Guild, archive_case: Dict[str, Any], *, state: Optional[Dict[str, Any]] = None, ensured: Optional[Dict[str, discord.Thread]] = None):
        archive_thread = None
        if ensured:
            archive_thread = ensured.get("archive")
        if archive_thread is None:
            ensured = await self.ensure_threads(guild)
            archive_thread = (ensured or {}).get("archive") if ensured else None
        if archive_thread is None:
            return
        user_obj = await self._fetch_user_object(int(archive_case.get("user_id") or 0))
        if user_obj is None:
            user_obj = discord.Object(id=int(archive_case.get("user_id") or 0))
            user_obj.mention = f"<@{int(archive_case.get('user_id') or 0)}>"
        embed = self._build_archive_embed(user_obj, archive_case=archive_case, prior_case_count=max(0, len(_get_archived_case_ids_for_user(_guild_cfg(state or _load_state(), guild.id), int(archive_case.get("user_id") or 0))) - 1))
        msg_id = archive_case.get("message_id")
        if not msg_id:
            return
        try:
            msg = await archive_thread.fetch_message(int(msg_id))
            await msg.edit(embed=embed, view=ArchiveCaseView(self.bot.get_cog("ModerationCog"), user_id=int(archive_case.get("user_id") or 0), archive_case_id=str(archive_case.get("case_id") or ""), archive_case=archive_case))
        except Exception:
            pass


    def _find_pending_ban_recovery_for_user(self, cfg: Dict[str, Any], user_id: int) -> Optional[Dict[str, Any]]:
        pending = cfg.setdefault("pending_ban_recoveries", {})
        for rec in pending.values():
            if not isinstance(rec, dict):
                continue
            if int((rec or {}).get("user_id") or 0) == int(user_id):
                return rec
        return None

    async def _normalize_member_timeout_after_strike_change(self, guild: discord.Guild, user_id: int, strike_count: int, *, moderator: discord.abc.User, reason: str) -> tuple[bool, str]:
        try:
            member = await guild.fetch_member(int(user_id))
        except discord.NotFound:
            return False, "Member not present"
        except Exception as exc:
            return False, f"Fetch failed: {exc}"

        settings = _guild_settings(_guild_cfg(_load_state(), guild.id))
        action_cfg = _threshold_config(settings, int(strike_count or 0))
        action_type = str(action_cfg.get("type") or "none").lower()
        try:
            if action_type == "timeout":
                minutes = int(action_cfg.get("duration_minutes") or 0)
                if minutes > 0:
                    until = discord.utils.utcnow() + timedelta(minutes=minutes)
                    await member.timeout(until, reason=reason)
                    return True, action_cfg.get("label") or _current_action_label(settings, strike_count)
            if member.is_timed_out():
                await member.timeout(None, reason=reason)
                return True, "Timeout removed"
            return False, "No active timeout"
        except Exception as exc:
            return False, f"Timeout update failed: {exc}"

    async def _restore_ban_recovery_roles(self, member: discord.Member, role_ids: list[int]) -> list[int]:
        if not role_ids:
            return []
        restored: list[int] = []
        me = member.guild.me or member.guild.get_member(self.bot.user.id)
        roles_to_add = []
        for rid in role_ids:
            try:
                role = member.guild.get_role(int(rid))
            except Exception:
                role = None
            if role is None:
                continue
            if role.is_default():
                continue
            if me is not None and role >= me.top_role:
                continue
            roles_to_add.append(role)
        for role in roles_to_add:
            try:
                if role not in member.roles:
                    await member.add_roles(role, reason="Ban recovery completed")
                    restored.append(int(role.id))
            except Exception:
                continue
        return restored

    async def complete_pending_ban_recovery_for_member(self, member: discord.Member):
        guild = member.guild
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        pending_map = cfg.setdefault("pending_ban_recoveries", {})
        pending_key = None
        pending = None
        for key, rec in pending_map.items():
            if not isinstance(rec, dict):
                continue
            if int((rec or {}).get("user_id") or 0) == int(member.id):
                pending_key = str(key)
                pending = rec
                break
        if not pending_key or not isinstance(pending, dict):
            return

        active_cases = cfg.setdefault("active_cases", {})
        active_case = active_cases.get(str(int(member.id)))
        active_case_id = str((pending or {}).get("active_case_id") or "")
        if not isinstance(active_case, dict) and active_case_id:
            active_case = active_cases.get(active_case_id)
        if not isinstance(active_case, dict):
            recovered = dict((pending or {}).get("recovered_case_snapshot") or {})
            if recovered:
                active_case = recovered
                active_case["user_id"] = int(member.id)
                active_cases[str(int(member.id))] = active_case
        if not isinstance(active_case, dict):
            return

        restored_role_ids = await self._restore_ban_recovery_roles(member, [int(r) for r in list((pending or {}).get("archived_roles_snapshot") or []) if str(r).isdigit()])
        completed_at = _now_iso()
        active_case["status"] = "ACTIVE"
        active_case["pending_return"] = False
        active_case["updated_at"] = completed_at
        active_case.setdefault("history", []).append({
            "timestamp": completed_at,
            "type": "BAN_RECOVERY_COMPLETED",
            "moderator_id": int((pending or {}).get("approved_by_user_id") or 0),
            "reason": str((pending or {}).get("recovery_reason") or "Ban recovery completed on rejoin"),
            "recovery_id": str((pending or {}).get("recovery_id") or ""),
            "strike_count": int(active_case.get("strike_count") or 0),
            "restored_roles": restored_role_ids,
        })
        active_cases[str(int(member.id))] = active_case

        archive_case_id = str((pending or {}).get("source_archive_case_id") or "")
        archive_cases = _normalize_archive_storage(cfg).setdefault("archive_cases", {})
        archive_case = archive_cases.get(archive_case_id) if archive_case_id else None
        if isinstance(archive_case, dict):
            archive_case.setdefault("ban_recovery", {})["status"] = "completed"
            archive_case["ban_recovery"]["completed_at"] = completed_at
            archive_case["ban_recovery"]["completed_user_id"] = int(member.id)
            archive_case.setdefault("history", []).append({
                "timestamp": completed_at,
                "type": "BAN_RECOVERY_COMPLETED",
                "moderator_id": int((pending or {}).get("approved_by_user_id") or 0),
                "reason": str((pending or {}).get("recovery_reason") or "Ban recovery completed on rejoin"),
                "recovery_id": str((pending or {}).get("recovery_id") or ""),
            })

        ensured = await self.ensure_threads(guild)
        moderator_obj = guild.get_member(int(active_case.get("latest_moderator_id") or (pending or {}).get("approved_by_user_id") or self.bot.user.id)) or member
        active_thread = (ensured or {}).get("active") if ensured else None
        if active_thread is not None:
            msg = None
            mid = int(active_case.get("message_id") or 0)
            if mid:
                try:
                    msg = await active_thread.fetch_message(mid)
                except Exception:
                    msg = None
            if msg is None:
                msg = await self._find_existing_active_message(active_thread, member.id)
            embed = self._build_active_embed(
                member,
                moderator_obj,
                reason=str(active_case.get("latest_reason") or active_case.get("reason") or (pending or {}).get("recovery_reason") or "No reason provided"),
                case_id=str(active_case.get("case_id") or "UnknownCase"),
                strike_count=int(active_case.get("strike_count") or 0),
                status="ACTIVE",
                history_count=len(active_case.get("history") or []),
                source_label="Ban Recovery Completed",
                prior_archive_count=len(_get_archived_case_ids_for_user(cfg, int(member.id))),
            )
            view = self._build_active_view(int(member.id), len(active_case.get("history") or []))
            if msg is None:
                msg = await active_thread.send(embed=embed, view=view)
            else:
                await msg.edit(content=None, embed=embed, view=view)
            active_case["message_id"] = msg.id

        notice_thread_id = int((pending or {}).get("recovery_notice_thread_id") or 0)
        notice_message_id = int((pending or {}).get("recovery_notice_message_id") or 0)
        if notice_thread_id and notice_message_id:
            th = guild.get_thread(notice_thread_id) or guild.get_channel(notice_thread_id)
            if isinstance(th, discord.Thread):
                try:
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

    def _build_tutorial_payload(self, guild: discord.Guild, owner: discord.Member, tutorial_type: str, session_id: str) -> tuple[discord.Embed, ui.View]:
        tutorial_type = str(tutorial_type).strip().lower()
        if tutorial_type == "moderator":
            lines = [
                f"Current Summary: {owner.mention}",
                f"• Case ID: **tut-mod-1**",
                f"• Stage: **Investigation**",
                f"• Status: **ACTIVE**",
                "",
                "### Step 1 — Investigation thread controls",
                "This first practice case lives in **Offenders - Investigation**.",
                "Button guide for this card:",
                "↦ **Current Summary**: quick snapshot of current status + latest reason.",
                "↦ **Full History**: complete timeline with every report/strike entry.",
                "↦ **Remove Report**: admin-only cleanup when a report is duplicate/spam/invalid.",
                "↦ **Promote to Strike**: moves this offender from Investigation to Active.",
                "↦ **Issue Ban**: emergency hard action (admin-only).",
                "",
                "### Step 2 — Use fake reports to practice promote flow",
                "Use either existing reports or write your own summary reason:",
                '↦ 1: @ReporterAlpha: "User posted dox threat in #general at 14:22 UTC."',
                '↦ 2: @ReporterBravo: "User repeated slur after warning in thread #help-logs."',
                '↦ 3: @ReporterCharlie: "User evaded timeout with alt account (same phrasing/log pattern)."',
                "",
                "When you click **Promote to Strike**:",
                "↦ pick one report reason, or",
                "↦ click **Write Summary** and enter your own concise moderator summary.",
                "",
                "### Step 3 — Activate Investigation to end-to-end flow",
                "Practice this full path in order:",
                "↦ Review Investigation card + Full History.",
                "↦ Promote to Strike with chosen report/summary.",
                "↦ Open the Active card and validate strike count + threshold action.",
                "↦ Add/adjust reason notes if needed.",
                "↦ Finalize when policy threshold is reached.",
                "",
                "Goal: explain the entire investigation → strike → active moderation flow without prompts.",
            ]
        else:
            lines = [
                f"Current Summary: {owner.mention}",
                f"• Case ID: **tut-admin-1**",
                f"• Stage: **Archive**",
                f"• Status: **FINALIZED**",
                "",
                "### Archive / Recovery training case",
                "Reports: **2**",
                '↦ 1: @ReporterDelta: "Repeat harassment after prior timeout."',
                '↦ 2: @ReporterEcho: "Bypassed channel mute using emoji-only spam."',
                "",
                "Strikes: **2**",
                '↦ 1: @ModA: "Strike 1 • targeted insult + warning ignored"',
                '↦ 2: @ModB: "Strike 2 • repeat behavior within 24h"',
                "",
                "Final Action",
                f"↦ Approved by: {owner.mention}",
                "↦ Closed as ban threshold reached",
                "",
                "Admin checklist:",
                "↦ verify report evidence and strike chain before finalization.",
                "↦ verify archive timeline is complete and reasons are audit-ready.",
                "↦ practice recovery approval + invite flow for reversible actions.",
            ]
        )
        return _build_case_embed(
            f"{title} • {session_id}",
            "\n".join(lines),
            footer=f"Tutorial lock active for {owner.mention} • expires in {TUTORIAL_DURATION_HOURS} hour(s)",
        )

    def _build_tutorial_payload(self, guild: discord.Guild, owner: discord.Member, tutorial_type: str, session_id: str) -> tuple[discord.Embed, ui.View]:
        tutorial_type = str(tutorial_type).strip().lower()
        steps = self._tutorial_steps(tutorial_type)
        embed = self.build_tutorial_step_embed(
            owner=owner,
            tutorial_type=tutorial_type,
            session_id=session_id,
            steps=steps,
            step_index=0,
        )
        view = TutorialInteractiveView(self.bot.get_cog("ModerationCog"), guild.id, session_id, tutorial_type, owner, steps)
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
            "Strike State",
            f"↦ Remaining Strikes: **{strike_count}**",
            ("↦ Active Case Closed" if strike_count == 0 else "↦ Active Case Restored"),
        ]
        embed = discord.Embed(title=f"{case_label} • {display_name}", description="\n".join(lines))
        user_obj = guild.get_member(user_id)
        if user_obj is not None:
            embed.set_thumbnail(url=user_obj.display_avatar.url)
        return embed

    async def _upsert_recovered_pending_active_case(
        self,
        guild: discord.Guild,
        state: Dict[str, Any],
        active_case: Dict[str, Any],
        recovery: Dict[str, Any],
    ) -> None:
        ensured = await self.ensure_threads(guild)
        active_thread = (ensured or {}).get("active") if ensured else None
        if active_thread is None:
            return

        user_id = int((active_case or {}).get("user_id") or 0)
        display_name = str((active_case or {}).get("display_name") or (active_case or {}).get("user_name") or user_id)
        case_id = str((active_case or {}).get("case_id") or "")
        strike_count = int((active_case or {}).get("strike_count") or 0)
        recovery_reason = str((recovery or {}).get("recovery_reason") or "")
        recovery_id = str((recovery or {}).get("recovery_id") or "")
        approved_by = int((recovery or {}).get("approved_by_user_id") or 0)
        invite_url = str((recovery or {}).get("invite_url") or "")
        history_count = len((active_case or {}).get("history") or [])

        user_obj = await self._fetch_user_object(user_id)
        user_mention = getattr(user_obj, "mention", f"<@{user_id}>")
        thumb_url = str(getattr(getattr(user_obj, "display_avatar", None), "url", "") or "")
        embed = self._build_recovery_pending_embed(guild, active_case, recovery)

        msg = None
        msg_id = int((active_case or {}).get("message_id") or 0)
        if msg_id:
            try:
                msg = await active_thread.fetch_message(msg_id)
            except Exception:
                msg = None
        if msg is None:
            msg = await self._find_existing_active_message(active_thread, user_id)

        view = None if int((active_case or {}).get("strike_count") or 0) == 0 else self._build_active_view(user_id, history_count)
        if msg is None:
            msg = await active_thread.send(embed=embed, view=view)
        else:
            await msg.edit(content=None, embed=embed, view=view)

        active_case["message_id"] = msg.id
        active_case["thread_id"] = active_thread.id
        cfg = _guild_cfg(state, guild.id)
        cfg.setdefault("active_cases", {})[str(int(user_id))] = active_case
        if isinstance(recovery, dict):
            recovery["recovered_case_snapshot"] = dict(active_case)
        _save_state(state)

    async def _validate_ban_recovery_target(self, guild: discord.Guild, *, user_id: int, archive_case_id: str, state: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
        cfg = _guild_cfg(state, guild.id)
        archive_case = _normalize_archive_storage(cfg).setdefault("archive_cases", {}).get(str(archive_case_id))
        if archive_case is None or int(archive_case.get("user_id") or 0) != int(user_id):
            return None, cfg, "Archived ban case not found."
        if str(archive_case.get("final_action") or "").lower() != "ban":
            return None, cfg, "Ban Recovery is only available for archived ban cases."
        if _recovery_prepared(archive_case):
            return None, cfg, "Ban Recovery is already prepared for this case."
        if _recovery_completed(archive_case):
            return None, cfg, "Ban Recovery is already completed for this case."
        pending = cfg.setdefault("pending_ban_recoveries", {})
        if any(int((rec or {}).get("user_id") or 0) == int(user_id) for rec in pending.values() if isinstance(rec, dict)):
            return None, cfg, "This user already has a pending Ban Recovery."
        return archive_case, cfg, None

    async def begin_ban_recovery(self, interaction: discord.Interaction, *, user_id: int, archive_case_id: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        state = _load_state()
        archive_case, _cfg, err = await self._validate_ban_recovery_target(guild, user_id=int(user_id), archive_case_id=str(archive_case_id), state=state)
        if err:
            if err == "This user already has a pending Ban Recovery.":
                cfg = _guild_cfg(state, guild.id)
                pending = self._find_pending_ban_recovery_for_user(cfg, int(user_id))
                invite_url = str((pending or {}).get("invite_url") or "")
                recovery_id = str((pending or {}).get("recovery_id") or "")
                active_cases = cfg.setdefault("active_cases", {})
                active_case_id = str((pending or {}).get("active_case_id") or "")
                active_case = active_cases.get(str(int(user_id))) or (active_cases.get(active_case_id) if active_case_id else None)
                if not isinstance(active_case, dict):
                    recovered = dict((pending or {}).get("recovered_case_snapshot") or {})
                    if recovered:
                        active_case = recovered
                if isinstance(active_case, dict):
                    if not active_case.get("case_id"):
                        active_case["case_id"] = active_case_id or f"act-recov-{int(user_id)}"
                    active_case["user_id"] = int(user_id)
                    active_case["status"] = "recovery_pending"
                    active_case["strike_count"] = int((pending or {}).get("restored_strikes") or active_case.get("strike_count") or 3)
                    active_cases[str(int(user_id))] = active_case
                    pending["active_case_id"] = str(active_case.get("case_id"))
                    pending["recovered_case_snapshot"] = dict(active_case)
                    _save_state(state)
                if isinstance(active_case, dict):
                    try:
                        await self._upsert_recovered_pending_active_case(guild, state, active_case, pending or {})
                    except Exception:
                        pass
                if invite_url:
                    prescripted = (
                        f"You have been approved to return to the server. Use this invite to rejoin: {invite_url}\n"
                        "Once you return, your recovery will continue automatically."
                    )
                    debug_log.warning(
                        "moderation: ban recovery reused pending guild=%s user=%s moderator=%s case=%s recovery_id=%s",
                        getattr(guild, "name", guild.id),
                        int(user_id),
                        int(interaction.user.id),
                        str(archive_case_id),
                        recovery_id,
                    )
                    await interaction.response.send_message(
                        "This user already has a pending recovery in **Offenders - Active**.\n\n"
                        f"Invite: {invite_url}\n\n"
                        "Message to send:\n"
                        f"{prescripted}",
                        ephemeral=True,
                    )
                    return
            debug_log.warning("moderation: ban recovery denied guild=%s user=%s moderator=%s case=%s reason=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id), err)
            await interaction.response.send_message(err, ephemeral=True)
            return
        debug_log.warning("moderation: ban recovery opened guild=%s user=%s moderator=%s case=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id))
        await interaction.response.send_modal(BanRecoveryReasonModal(self.bot.get_cog("ModerationCog"), user_id=int(user_id), archive_case_id=str(archive_case_id)))

    async def prepare_ban_recovery(self, interaction: discord.Interaction, *, user_id: int, archive_case_id: str, recovery_reason: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        clean_reason = _sanitize_reason(recovery_reason)
        if not clean_reason:
            await interaction.response.send_message("Recovery reason is required.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        state = _load_state()
        archive_case, cfg, err = await self._validate_ban_recovery_target(guild, user_id=int(user_id), archive_case_id=str(archive_case_id), state=state)
        if err:
            debug_log.warning("moderation: ban recovery denied guild=%s user=%s moderator=%s case=%s reason=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id), err)
            await interaction.followup.send(err, ephemeral=True)
            return
        debug_log.warning("moderation: ban recovery submitted guild=%s user=%s moderator=%s case=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id))
        try:
            ban_entry = await guild.fetch_ban(discord.Object(id=int(user_id)))
            user_obj = ban_entry.user
        except discord.NotFound:
            user_obj = await self._fetch_user_object(int(user_id))
            if user_obj is None:
                await interaction.followup.send("Ban Recovery aborted. The user is no longer banned and could not be resolved.", ephemeral=True)
                return
        except Exception as exc:
            await interaction.followup.send(f"Ban Recovery aborted. I could not inspect the guild ban list: {exc}", ephemeral=True)
            return
        try:
            await guild.unban(user_obj, reason=f"Ban recovery prepared by {interaction.user} ({interaction.user.id}): {clean_reason}")
            debug_log.warning("moderation: ban recovery unban succeeded guild=%s user=%s moderator=%s case=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id))
        except Exception as exc:
            debug_log.warning("moderation: ban recovery unban failed guild=%s user=%s moderator=%s case=%s error=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id), str(exc)[:180])
            await interaction.followup.send(f"Ban Recovery aborted. I could not unban the user: {exc}", ephemeral=True)
            return
        invite = await self._make_ban_recovery_invite(guild)
        if invite is None:
            try:
                await guild.ban(user_obj, reason="Ban Recovery invite creation failed; reverting unban", delete_message_seconds=0)
            except Exception:
                pass
            await interaction.followup.send("Ban Recovery aborted. I could not create the one-use recovery invite.", ephemeral=True)
            return
        prepared_at = _now_iso()
        expires_at = _now_iso()
        try:
            expires_at = (discord.utils.utcnow() + timedelta(days=BAN_RECOVERY_INVITE_DAYS)).replace(tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
        recovery_id = _next_case_id(state, "recovery")
        pending_record = {
            "recovery_id": recovery_id,
            "user_id": int(user_id),
            "source_archive_case_id": str(archive_case_id),
            "source_active_case_id": str(archive_case.get("source_case_id") or ""),
            "recovery_reason": clean_reason,
            "approved_by_user_id": int(interaction.user.id),
            "approved_at": prepared_at,
            "invite_code": str(getattr(invite, "code", "") or ""),
            "invite_url": str(getattr(invite, "url", "") or ""),
            "invite_expires_at": expires_at,
            "archived_roles_snapshot": list(archive_case.get("role_ids_snapshot") or []),
            "archived_case_snapshot": dict(archive_case),
            "ban_triggering_strike_id": str(archive_case.get("ban_triggering_strike_id") or ""),
            "pending_recovery": True,
            "recovery_completed": False,
        }
        cfg.setdefault("pending_ban_recoveries", {})[str(recovery_id)] = pending_record

        # Immediately restore this archived ban case into Active as a real persisted recovery-pending case.
        active_cases = cfg.setdefault("active_cases", {})
        restored_case_id = _next_case_id(state, "active")
        restored_strikes = max(0, int(archive_case.get("strike_count") or 4) - 1)
        restored_case = {
            "case_id": restored_case_id,
            "user_id": int(user_id),
            "display_name": str(archive_case.get("display_name") or archive_case.get("user_name") or user_id),
            "user_name": str(archive_case.get("user_name") or archive_case.get("display_name") or user_id),
            "moderator_id": int(interaction.user.id),
            "reason": clean_reason,
            "strike_count": restored_strikes,
            "source": ("ban_recovery_zero" if restored_strikes == 0 else "ban_recovery"),
            "status": "recovery_pending",
            "history": list(archive_case.get("history") or []),
            "message_id": 0,
            "restored_from_archive_case_id": str(archive_case_id),
            "recovery_id": recovery_id,
            "pending_return": True,
            "created_at": _now_iso(),
        }
        active_cases[str(int(user_id))] = restored_case
        pending_record["active_case_id"] = restored_case_id
        pending_record["recovered_case_snapshot"] = dict(restored_case)
        pending_record["restored_strikes"] = int(restored_case.get("strike_count") or 0)

        archive_case["ban_recovery"] = {
            "status": "prepared",
            "approved_by": int(interaction.user.id),
            "approved_at": prepared_at,
            "reason": clean_reason,
            "recovery_id": recovery_id,
            "invite_code": str(getattr(invite, "code", "") or ""),
            "invite_expires_at": expires_at,
        }
        archive_case.setdefault("history", []).append({
            "timestamp": prepared_at,
            "type": "BAN_RECOVERY_PREPARED",
            "moderator_id": int(interaction.user.id),
            "reason": clean_reason,
            "recovery_id": recovery_id,
        })
        try:
            await self._upsert_recovered_pending_active_case(guild, state, restored_case, pending_record)
        except Exception:
            pass
        _save_state(state)
        debug_log.warning("moderation: ban recovery prepared guild=%s user=%s moderator=%s case=%s recovery_id=%s", getattr(guild, "name", guild.id), int(user_id), int(interaction.user.id), str(archive_case_id), str(recovery_id))
        ensured = await self.ensure_threads(guild)
        await self._refresh_archive_case_message(guild, archive_case, state=state, ensured=ensured)
        target_label = "a recovery-only pending return case" if restored_strikes == 0 else "**Offenders - Active**"
        notice_text = (
            f"Ban Recovery restored <@{int(user_id)}> from archive case `{archive_case_id}` into {target_label}.\n\n"
            f"Recovery invite: {invite.url}\n"
            f"This invite is 1 use and expires in {BAN_RECOVERY_INVITE_DAYS} day(s).\n"
            + ("The user now has 0 remaining strikes, so the recovery is being tracked without reopening a normal strike case." if restored_strikes == 0 else "The case has been restored to Active now. Role restoration and return completion will finish when the user rejoins.")
        )
        notice_msg = await interaction.followup.send(
            notice_text,
            ephemeral=False,
            wait=True,
        )
        try:
            pending_record["recovery_notice_message_id"] = int(getattr(notice_msg, "id", 0) or 0)
            pending_record["recovery_notice_thread_id"] = int(getattr(getattr(notice_msg, "channel", None), "id", 0) or 0)
            _save_state(state)
        except Exception:
            pass

    def _build_report_alert_embed(self, user: discord.Member, reporter: discord.abc.User, *, reason: str, active_case_id: str) -> discord.Embed:
        embed = discord.Embed(
            title=f"NEW REPORT • {user.display_name}",
            description="New report received for a user with an active strike case.",
        )
        embed.add_field(name="User", value=f"{user.mention}\nID: {user.id}", inline=False)
        embed.add_field(name="Reporter", value=f"{reporter.mention}\nID: {reporter.id}", inline=False)
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Active Case", value=active_case_id, inline=False)
        embed.set_thumbnail(url=user.display_avatar.url)
        return embed

    async def _apply_threshold_action(self, guild: discord.Guild, user: discord.Member, case: Dict[str, Any], *, reason: str, moderator: discord.abc.User, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        working_state = state if isinstance(state, dict) else _load_state()
        working_cfg = _guild_cfg(working_state, guild.id)
        settings = _guild_settings(working_cfg)
        strike_count = int(case.get("strike_count", 0) or 0)
        applied_threshold = int(case.get("last_applied_threshold", 0) or 0)
        action_cfg = _threshold_config(settings, strike_count)
        result = {
            "threshold": strike_count,
            "action_type": action_cfg.get("type"),
            "action_label": action_cfg.get("label"),
            "next_action_text": _next_action_text(settings, strike_count),
            "applied": False,
            "failed": False,
            "error": None,
        }
        if not bool(settings.get("auto_threshold_actions", True)):
            result["action_label"] = "Automatic threshold actions are disabled"
            return result
        if strike_count <= applied_threshold:
            return result
        try:
            if action_cfg.get("type") == "timeout":
                minutes = int(action_cfg.get("duration_minutes") or 0)
                if minutes > 0:
                    until = discord.utils.utcnow() + timedelta(minutes=minutes)
                    await user.timeout(until, reason=f"Strike {strike_count}: {reason}")
                    result["applied"] = True
            elif action_cfg.get("type") == "ban":
                working_cfg.setdefault("pending_ban_leave_notices", {})[str(user.id)] = {
                    "user_id": int(user.id),
                    "user_name": getattr(user, "display_name", user.name),
                    "avatar_url": str(getattr(getattr(user, "display_avatar", None), "url", "") or ""),
                    "reason": reason,
                    "moderator_id": getattr(moderator, "id", None),
                    "moderator_name": getattr(moderator, "display_name", getattr(moderator, "name", "Moderator")),
                    "case_id": str(case.get("case_id") or ""),
                    "strike_count": strike_count,
                    "created_at": _now_iso(),
                }
                _save_state(working_state)
                await guild.ban(user, reason=f"Strike {strike_count}: {reason}", delete_message_seconds=0)
                result["applied"] = True
        except Exception as exc:
            if action_cfg.get("type") == "ban":
                try:
                    working_cfg.setdefault("pending_ban_leave_notices", {}).pop(str(user.id), None)
                    _save_state(working_state)
                except Exception:
                    pass
            result["failed"] = True
            result["error"] = str(exc)
        strike_entries = _ensure_case_strike_entries(case)
        if strike_entries:
            strike_entries[-1]["action_applied"] = action_cfg.get("type")
        case["last_applied_threshold"] = strike_count
        case["last_action"] = action_cfg.get("type")
        case["last_action_at"] = _now_iso()
        history = case.setdefault("history", [])
        linked = False
        strike_id = str(strike_entries[-1].get("strike_id")) if strike_entries else ""
        for entry in reversed(history):
            if str(entry.get("type") or "") not in {"STRIKE_APPLIED", "STRIKE_INCREMENT"}:
                continue
            if strike_id and str(entry.get("strike_id") or "") != strike_id:
                continue
            entry["type"] = "STRIKE_APPLIED"
            entry["action"] = action_cfg.get("type")
            entry["label"] = action_cfg.get("label")
            entry["applied"] = bool(result["applied"])
            entry["failed"] = bool(result["failed"])
            entry["error"] = result.get("error")
            entry["timestamp"] = entry.get("timestamp") or _now_iso()
            linked = True
            break
        if not linked:
            history.append(
                {
                    "timestamp": _now_iso(),
                    "type": "STRIKE_APPLIED",
                    "moderator_id": getattr(moderator, "id", None),
                    "strike_count": strike_count,
                    "action": action_cfg.get("type"),
                    "label": action_cfg.get("label"),
                    "applied": bool(result["applied"]),
                    "failed": bool(result["failed"]),
                    "error": result.get("error"),
                    "reason": reason,
                    "strike_id": strike_id or None,
                }
            )
        return result

    async def _archive_case_after_ban(self, guild: discord.Guild, user: discord.Member, case: Dict[str, Any], *, moderator: discord.abc.User, reason: str, state: Dict[str, Any], ensured: Dict[str, discord.Thread]) -> Optional[Dict[str, Any]]:
        archive_case = await self._archive_case(guild, user, case, moderator=moderator, reason=reason, final_action="ban", finalization_source="threshold_ban", state=state, ensured=ensured)
        try:
            log.info(
                "moderation: case finalized guild=%s user=%s source_case=%s action=ban archived=%s",
                guild.id,
                user.id,
                case.get("case_id"),
                (archive_case or {}).get("case_id"),
            )
        except Exception:
            pass
        try:
            await post_status(
                self.bot,
                f"case finalized user={user.id} source={case.get('case_id')} action=ban archived={(archive_case or {}).get('case_id')}",
            )
        except Exception:
            pass
        return archive_case

    def _build_investigation_view(self, user_id: int, report_count: int):
        cog = self.bot.get_cog("ModerationCog")
        if cog is None:
            return None
        return InvestigationCaseView(cog, user_id=int(user_id), report_count=int(report_count or 0))

    def _build_active_view(self, user_id: int, history_count: int):
        cog = self.bot.get_cog("ModerationCog")
        if cog is None:
            return None
        return ActiveCaseView(cog, user_id=int(user_id), history_count=int(history_count or 0))

    async def _find_existing_investigation_message(self, thread: discord.Thread, user_id: int) -> Optional[discord.Message]:
        probe = f"ID: {user_id}"
        try:
            async for msg in thread.history(limit=100):
                if msg.author.id != self.bot.user.id:
                    continue
                if not msg.embeds:
                    continue
                emb = msg.embeds[0]
                if emb.title and emb.title.startswith("REPORT •"):
                    for field in emb.fields:
                        if field.name == "User" and probe in (field.value or ""):
                            return msg
        except Exception:
            log.exception("moderation: failed to scan investigation history")
        return None

    async def _find_existing_active_message(self, thread: discord.Thread, user_id: int) -> Optional[discord.Message]:
        probe = f"ID: {user_id}"
        try:
            async for msg in thread.history(limit=100):
                if msg.author.id != self.bot.user.id:
                    continue
                if not msg.embeds:
                    continue
                emb = msg.embeds[0]
                if emb.title and emb.title.startswith("STRIKE •"):
                    for field in emb.fields:
                        if field.name == "User" and probe in (field.value or ""):
                            return msg
        except Exception:
            log.exception("moderation: failed to scan active history")
        return None

    async def show_reason_picker(self, interaction: discord.Interaction, user_id: int, *, mode: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        inv_case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not inv_case:
            await interaction.response.send_message("Investigation case not found.", ephemeral=True)
            return
        history = list(inv_case.get("history") or [])
        options: list[discord.SelectOption] = []
        latest_text = ""
        recent = history[-MAX_OPTIONS:]
        start_idx = max(1, len(history) - len(recent) + 1)
        for idx, entry in enumerate(recent, start=start_idx):
            if entry.get("removed") or entry.get("promoted"):
                continue
            if str(entry.get("type") or "") != "INVESTIGATION_REPORT":
                continue
            reason = str(entry.get("reason") or "")
            latest_text = reason
            label = f"Report #{idx}"
            description = _short_reason_preview(reason, limit=100)
            options.append(discord.SelectOption(label=label, value=f"report:{idx}", description=description))
        if not options:
            open_reports = [e for e in history if str(e.get("type") or "") == "INVESTIGATION_REPORT" and not e.get("removed") and not e.get("promoted")]
            latest_text = str((open_reports[-1] if open_reports else {}).get("reason") or inv_case.get("latest_reason") or "")
            if latest_text:
                options = [discord.SelectOption(label="Latest open report", value="latest", description=_short_reason_preview(latest_text, limit=100))]
        mention = f"<@{int(user_id)}>"
        content = f"Define the strike reason for {mention}. Select one report reason or write a summary. This will be sent to the user."
        view = PromoteReasonChoiceView(self.bot.get_cog("ModerationCog"), user_id=int(user_id), mode=mode, options=options, initial_text=latest_text)
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=True, view=view)
        else:
            await interaction.response.send_message(content, ephemeral=True, view=view)

    async def show_remove_report_picker(self, interaction: discord.Interaction, *, case_type: str, user_id: int):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case_type = str(case_type)
        if case_type == "inv":
            case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        else:
            case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Case not found.", ephemeral=True)
            return
        history = list(case.get("history") or [])
        options: list[discord.SelectOption] = []
        for idx, entry in enumerate(history, start=1):
            etype = str(entry.get("type") or "")
            if entry.get("removed"):
                continue
            if case_type == "inv" and etype != "INVESTIGATION_REPORT":
                continue
            if case_type == "act" and etype not in {"INVESTIGATION_REPORT", "POST_STRIKE_REPORT"}:
                continue
            reason = str(entry.get("reason") or "")
            prefix = "Report" if etype == "INVESTIGATION_REPORT" else "Post-Strike"
            options.append(discord.SelectOption(label=f"{prefix} #{idx}", value=f"{case_type}:{idx}", description=_short_reason_preview(reason, limit=100)))
        if not options:
            await interaction.response.send_message("No removable reports found for this case.", ephemeral=True)
            return
        view = RemoveReportChoiceView(self.bot.get_cog("ModerationCog"), case_type=case_type, user_id=int(user_id), options=options[:25])
        if interaction.response.is_done():
            await interaction.followup.send("Select a report to remove.", ephemeral=True, view=view)
        else:
            await interaction.response.send_message("Select a report to remove.", ephemeral=True, view=view)

    async def remove_report_entry(self, interaction: discord.Interaction, *, case_type: str, user_id: int, token: str):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_admin(member):
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case_type = str(case_type)
        cases = cfg.get("investigation_cases", {}) if case_type == "inv" else cfg.get("active_cases", {})
        case = cases.get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Case not found.", ephemeral=True)
            return
        m = re.match(rf"^{case_type}:(\d+)$", str(token or ""))
        if not m:
            await interaction.response.send_message("That report is no longer available.", ephemeral=True)
            return
        idx = int(m.group(1)) - 1
        history = case.setdefault("history", [])
        if idx < 0 or idx >= len(history):
            await interaction.response.send_message("That report is no longer available.", ephemeral=True)
            return
        entry = history[idx]
        if entry.get("removed"):
            await interaction.response.send_message("That report has already been removed.", ephemeral=True)
            return
        etype = str(entry.get("type") or "")
        allowed = {"INVESTIGATION_REPORT"} if case_type == "inv" else {"INVESTIGATION_REPORT", "POST_STRIKE_REPORT"}
        if etype not in allowed:
            await interaction.response.send_message("That history entry cannot be removed.", ephemeral=True)
            return
        entry["removed"] = True
        entry["removed_by"] = interaction.user.id
        entry["removed_at"] = _now_iso()
        entry["remove_note"] = "Removed by admin"
        case["updated_at"] = _now_iso()

        user = guild.get_member(int(user_id))
        if user is None:
            try:
                user = await guild.fetch_member(int(user_id))
            except Exception:
                user = None
        if user is not None:
            ensured = await self.ensure_threads(guild)
            if ensured:
                if case_type == "inv":
                    active_reports = [e for e in history if str(e.get("type") or "") == "INVESTIGATION_REPORT" and not e.get("removed")]
                    case["report_count"] = len(active_reports)
                    if active_reports:
                        latest = active_reports[-1]
                        case["latest_reason"] = str(latest.get("reason") or "")
                        case["latest_reporter_id"] = latest.get("reporter_id")
                        reporter_obj = guild.get_member(int(latest.get("reporter_id") or 0)) if latest.get("reporter_id") else None
                        if reporter_obj is None:
                            reporter_obj = interaction.user
                        embed = self._build_investigation_embed(user, reporter_obj, reason=str(case.get("latest_reason") or "Removed report"), case_id=str(case.get("case_id") or "UnknownCase"), report_count=int(case.get("report_count") or 0), status=str(case.get("status") or "ACTIVE"))
                        msg = None
                        if case.get("message_id"):
                            try:
                                msg = await ensured["investigation"].fetch_message(int(case["message_id"]))
                            except Exception:
                                msg = None
                        if msg is None:
                            msg = await self._find_existing_investigation_message(ensured["investigation"], user.id)
                        if msg is not None:
                            await msg.edit(embed=embed, view=self._build_investigation_view(user.id, int(case.get("report_count") or 0)))
                            case["message_id"] = msg.id
                    else:
                        msg = None
                        if case.get("message_id"):
                            try:
                                msg = await ensured["investigation"].fetch_message(int(case["message_id"]))
                            except Exception:
                                msg = None
                        if msg is not None:
                            await msg.delete()
                        cases.pop(str(int(user_id)), None)
                else:
                    mod_id = int(case.get("latest_moderator_id") or interaction.user.id)
                    moderator_obj = guild.get_member(mod_id) or interaction.user
                    embed = self._build_active_embed(user, moderator_obj, reason=str(case.get("latest_reason") or "No reason recorded"), case_id=str(case.get("case_id") or "UnknownCase"), strike_count=int(case.get("strike_count") or 0), status=str(case.get("status") or "ACTIVE"), history_count=len(case.get("history") or []), source_label="Active", prior_archive_count=len(_get_archived_case_ids_for_user(cfg, int(user.id))))
                    msg = None
                    if case.get("message_id"):
                        try:
                            msg = await ensured["active"].fetch_message(int(case["message_id"]))
                        except Exception:
                            msg = None
                    if msg is None:
                        msg = await self._find_existing_active_message(ensured["active"], user.id)
                    if msg is not None:
                        await msg.edit(embed=embed, view=self._build_active_view(user.id, len(case.get("history") or [])))
                        case["message_id"] = msg.id

        _save_state(state)
        await self._post_status_embed(guild, title="Moderation • Report Removed", user=interaction.guild.get_member(int(user_id)) or interaction.client.get_user(int(user_id)) or discord.Object(id=int(user_id)), case_id=str(case.get('case_id') or ''), details=[("Case Type", "Investigation" if case_type == "inv" else "Active", True), ("Removed Entry", f"#{idx+1}", True), ("Removed By", interaction.user.mention, False)])
        await _edit_or_followup(interaction, content="Report removed and audit saved.", view=None)

    async def resolve_selected_report_reason(self, interaction: discord.Interaction, user_id: int, token: str) -> Optional[str]:
        guild = interaction.guild
        if guild is None:
            return None
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        inv_case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not inv_case:
            return None
        history = list(inv_case.get("history") or [])
        if not history:
            latest = _sanitize_reason(str(inv_case.get("latest_reason") or ""))
            return latest or None
        token = str(token or "").strip().lower()
        if token == "latest":
            open_reports = [e for e in history if str(e.get("type") or "") == "INVESTIGATION_REPORT" and not e.get("removed") and not e.get("promoted")]
            latest = _sanitize_reason(str((open_reports[-1] if open_reports else {}).get("reason") or inv_case.get("latest_reason") or ""))
            return latest or None
        m = re.match(r"^report:(\d+)$", token)
        if not m:
            return None
        idx = int(m.group(1))
        if idx < 1 or idx > len(history):
            return None
        entry = history[idx - 1] or {}
        if entry.get("removed") or entry.get("promoted") or str(entry.get("type") or "") != "INVESTIGATION_REPORT":
            return None
        reason = _sanitize_reason(str(entry.get("reason") or ""))
        return reason or None

    async def _prune_duplicate_case_messages(self, thread: discord.Thread, *, title_prefix: str, case_map: Dict[str, Any], kind: str) -> None:
        if self.bot.user is None:
            return
        user_msgs: Dict[str, list[discord.Message]] = {}
        try:
            async for msg in thread.history(limit=200):
                if msg.author.id != self.bot.user.id or not msg.embeds:
                    continue
                emb = msg.embeds[0]
                if not emb.title or not str(emb.title).startswith(title_prefix):
                    continue
                uid = None
                for field in emb.fields:
                    if field.name == "User":
                        match = re.search(r"ID:\s*(\d+)", str(field.value or ""))
                        if match:
                            uid = match.group(1)
                        break
                if not uid:
                    continue
                user_msgs.setdefault(uid, []).append(msg)
        except Exception:
            log.exception("moderation: failed to scan %s thread for stale messages", kind)
            return

        changed = False
        for uid, msgs in user_msgs.items():
            if len(msgs) <= 1 and uid in case_map:
                continue
            msgs.sort(key=lambda m: m.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            case = case_map.get(str(uid))
            keep_id = None
            if case and case.get("message_id"):
                keep_id = int(case["message_id"])
            elif msgs:
                keep_id = msgs[0].id
                if case is not None:
                    case["message_id"] = keep_id
                    changed = True
            for msg in msgs:
                if msg.id == keep_id:
                    continue
                try:
                    await msg.delete()
                    log.info("moderation: pruned stale %s message guild=%s user=%s message=%s", kind, getattr(thread.guild, "name", thread.guild.id), uid, msg.id)
                except Exception:
                    log.exception("moderation: failed to delete stale %s message %s", kind, msg.id)
        if changed:
            state = _load_state()
            cfg = _guild_cfg(state, thread.guild.id)
            target = cfg.setdefault("investigation_cases" if kind == "investigation" else "active_cases", {})
            for uid, case in case_map.items():
                if uid in target:
                    target[uid] = case
            _save_state(state)

    async def prune_stale_messages(self, guild: discord.Guild, ensured: Optional[dict] = None) -> None:
        ensured = ensured or await self.ensure_threads(guild)
        if not ensured:
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        await self._prune_duplicate_case_messages(
            ensured["investigation"],
            title_prefix="REPORT •",
            case_map=cfg.setdefault("investigation_cases", {}),
            kind="investigation",
        )
        await self._prune_duplicate_case_messages(
            ensured["active"],
            title_prefix="STRIKE •",
            case_map=cfg.setdefault("active_cases", {}),
            kind="active",
        )
        _save_state(state)

    async def report_user(self, interaction: discord.Interaction, user: discord.Member, reason: str) -> str:
        guild = interaction.guild
        if guild is None:
            raise RuntimeError("Server only")
        ensured = await self.ensure_threads(guild)
        if not ensured:
            raise RuntimeError("Moderation threads are unavailable")
        await self.prune_stale_messages(guild, ensured)
        safe_reason = _sanitize_reason(reason)
        if not safe_reason:
            raise RuntimeError("Reason is required")

        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        return await self._report_to_investigation(interaction, user, safe_reason, ensured)

    async def _report_to_investigation(self, interaction: discord.Interaction, user: discord.Member, safe_reason: str, ensured: dict) -> str:
        guild = interaction.guild
        if guild is None:
            raise RuntimeError("Server only")
        thread: discord.Thread = ensured["investigation"]
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        cases = cfg.setdefault("investigation_cases", {})
        case = cases.get(str(user.id))

        if case is None:
            existing_msg = await self._find_existing_investigation_message(thread, user.id)
            case = {
                "case_id": _next_case_id(state, "investigation"),
                "user_id": user.id,
                "status": "ACTIVE",
                "report_count": 0,
                "message_id": existing_msg.id if existing_msg else None,
                "thread_id": thread.id,
                "history": [],
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
            }
            cases[str(user.id)] = case

        case["status"] = str(case.get("status") or "ACTIVE").upper()
        case["report_count"] = int(case.get("report_count", 0) or 0) + 1
        case["thread_id"] = thread.id
        case["updated_at"] = _now_iso()
        case.setdefault("history", []).append(
            {
                "type": "INVESTIGATION_REPORT",
                "reporter_id": interaction.user.id,
                "reporter_name": getattr(interaction.user, "display_name", str(interaction.user)),
                "reason": safe_reason,
                "timestamp": _now_iso(),
            }
        )
        case["latest_reason"] = safe_reason
        case["latest_reporter_id"] = interaction.user.id

        embed = self._build_investigation_embed(
            user,
            interaction.user,
            reason=safe_reason,
            case_id=str(case["case_id"]),
            report_count=int(case["report_count"]),
            status=str(case["status"]),
        )

        msg: Optional[discord.Message] = None
        if case.get("message_id"):
            try:
                msg = await thread.fetch_message(int(case["message_id"]))
            except Exception:
                msg = None

        if msg is None:
            msg = await self._find_existing_investigation_message(thread, user.id)

        if msg is None:
            msg = await thread.send(embed=embed, view=self._build_investigation_view(user.id, int(case["report_count"])))
            action = "created"
        else:
            await msg.edit(embed=embed, view=self._build_investigation_view(user.id, int(case["report_count"])))
            action = "updated"

        case["message_id"] = msg.id
        _save_state(state)
        try:
            await post_status(self.bot, f"report received user={user.id} case={case.get('case_id')} reports={case.get('report_count')} thread=investigation")
        except Exception:
            pass
        await self._post_status_embed(guild, title=f"Moderation • Investigation {action.title()}", user=user, case_id=str(case['case_id']), details=[("Reports", str(case['report_count']), True), ("Message ID", str(msg.id), True), ("Reporter", interaction.user.mention, False), ("Thread", "Offenders - Investigation", True)])
        return f"Report submitted to {thread.mention}."

    async def _report_to_active(self, interaction: discord.Interaction, user: discord.Member, safe_reason: str, ensured: dict) -> str:
        guild = interaction.guild
        if guild is None:
            raise RuntimeError("Server only")
        thread: discord.Thread = ensured["active"]
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.setdefault("active_cases", {}).get(str(user.id))
        if case is None:
            raise RuntimeError("Active case not found")

        history = case.setdefault("history", [])
        alert_id = _next_case_id(state, "alert")
        entry = {
            "type": "POST_STRIKE_REPORT",
            "timestamp": _now_iso(),
            "reporter_id": interaction.user.id,
            "reporter_name": getattr(interaction.user, "display_name", str(interaction.user)),
            "reason": safe_reason,
            "alert_id": alert_id,
            "acknowledged": False,
            "acknowledged_by": None,
            "acknowledged_at": None,
        }
        history.append(entry)
        case["updated_at"] = _now_iso()
        case["thread_id"] = thread.id

        msg = None
        if case.get("message_id"):
            try:
                msg = await thread.fetch_message(int(case["message_id"]))
            except Exception:
                msg = None
        if msg is None:
            msg = await self._find_existing_active_message(thread, user.id)

        moderator_obj = guild.get_member(int(case.get("latest_moderator_id") or interaction.user.id)) or interaction.user
        active_embed = self._build_active_embed(
            user,
            moderator_obj,
            reason=str(case.get("latest_reason") or safe_reason),
            case_id=str(case["case_id"]),
            strike_count=int(case.get("strike_count", 0) or 0),
            status=str(case.get("status") or "ACTIVE"),
            history_count=len(history),
            source_label="Active",
        )
        if msg is None:
            msg = await thread.send(embed=active_embed, view=self._build_active_view(user.id, len(history)))
        else:
            await msg.edit(embed=active_embed, view=self._build_active_view(user.id, len(history)))
        case["message_id"] = msg.id

        alert_embed = self._build_report_alert_embed(user, interaction.user, reason=safe_reason, active_case_id=str(case["case_id"]))
        alert_msg = await post_status(guild, content="@here", embed=alert_embed, view=ActiveAlertView(self.bot.get_cog("ModerationCog"), user_id=user.id, alert_id=alert_id), allowed_mentions=discord.AllowedMentions(everyone=True, roles=False, users=False))
        entry["alert_message_id"] = alert_msg.id if alert_msg else None
        _save_state(state)
        await self._post_status_embed(guild, title="Moderation • Post-Strike Report", user=user, case_id=str(case['case_id']), details=[("Reporter", interaction.user.mention, False), ("Alert ID", str(getattr(alert_msg, 'id', None)), True), ("History Entries", str(len(history)), True)], view=ActiveAlertView(self.bot.get_cog("ModerationCog"), user_id=int(user.id), alert_id=str(alert_id)) if alert_msg else None, content='@here' if alert_msg else None)
        return "Report submitted and staff alerted in SYSTEM - STATUS."

    async def acknowledge_alert(self, interaction: discord.Interaction, user_id: int, alert_id: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.setdefault("active_cases", {}).get(str(int(user_id)))
        if case is None:
            await interaction.response.send_message("Active case not found.", ephemeral=True)
            return
        history = case.setdefault("history", [])
        target = None
        for entry in history:
            if str(entry.get("alert_id") or "") == str(alert_id):
                target = entry
                break
        if target is None:
            await interaction.response.send_message("This alert has already been handled.", ephemeral=True)
            return
        if target.get("acknowledged"):
            await interaction.response.send_message("This alert has already been handled.", ephemeral=True)
            return
        target["acknowledged"] = True
        target["acknowledged_by"] = interaction.user.id
        target["acknowledged_at"] = _now_iso()
        _save_state(state)
        try:
            await interaction.response.defer()
        except Exception:
            pass
        try:
            await interaction.message.delete()
        except Exception:
            try:
                await interaction.followup.send("Report acknowledged.", ephemeral=True)
            except Exception:
                pass

    async def strike_to_active(self, interaction: discord.Interaction, user: discord.Member, reason: str) -> str:
        guild = interaction.guild
        if guild is None:
            raise RuntimeError("Server only")
        ensured = await self.ensure_threads(guild)
        if not ensured:
            raise RuntimeError("Moderation threads are unavailable")
        await self.prune_stale_messages(guild, ensured)
        thread: discord.Thread = ensured["active"]
        safe_reason = _sanitize_reason(reason)
        if not safe_reason:
            raise RuntimeError("Reason is required")

        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        cases = cfg.setdefault("active_cases", {})
        case = cases.get(str(user.id))
        msg: Optional[discord.Message] = None
        if case is None:
            existing_msg = await self._find_existing_active_message(thread, user.id)
            case = {
                "case_id": _next_case_id(state, "active"),
                "user_id": user.id,
                "status": "ACTIVE",
                "strike_count": 0,
                "message_id": existing_msg.id if existing_msg else None,
                "thread_id": thread.id,
                "history": [],
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
            }
            cases[str(user.id)] = case
        else:
            if case.get("message_id"):
                try:
                    msg = await thread.fetch_message(int(case["message_id"]))
                except Exception:
                    msg = None
        if msg is None:
            msg = await self._find_existing_active_message(thread, user.id)

        case["status"] = "ACTIVE"
        case["thread_id"] = thread.id
        case["updated_at"] = _now_iso()
        _append_strike_entry(case, reason=safe_reason, moderator_id=interaction.user.id, source="command")
        case["latest_reason"] = safe_reason
        case["latest_moderator_id"] = interaction.user.id
        settings = _guild_settings(cfg)
        action_preview = _threshold_config(settings, int(case.get("strike_count", 0) or 0))
        notify_before_action = str(action_preview.get("type") or "") == "ban"
        if notify_before_action:
            await self._notify_user_of_strike(
                user,
                strike_count=int(case["strike_count"]),
                reason=safe_reason,
                action_label=str(action_preview.get("label") or "Banned from the server"),
                next_action_text=str(_next_action_text(settings, int(case["strike_count"])) or ""),
            )
        action_result = await self._apply_threshold_action(guild, user, case, reason=safe_reason, moderator=interaction.user, state=state)
        if str(action_result.get("action_type") or "") == "ban" and bool(action_result.get("applied")) and not bool(action_result.get("failed")):
            archive_case = await self._archive_case_after_ban(guild, user, case, moderator=interaction.user, reason=safe_reason, state=state, ensured=ensured)
            try:
                await self._post_status_embed(
                    guild,
                    title="Banned",
                    user=user,
                    case_id=str((archive_case or {}).get("case_id") or case.get("case_id") or "UnknownCase"),
                    details=[
                        ("Reason", safe_reason, False),
                        ("Moderator", interaction.user.mention, False),
                        ("Strike Level", str(int(case.get("strike_count") or 0)), True),
                        ("Case ID", str(case.get("case_id") or "UnknownCase"), True),
                    ],
                )
            except Exception:
                log.exception("moderation: failed to post case closure status", extra={"guild_id": guild.id, "user_id": user.id, "case_id": case.get("case_id")})
            return f"Strike recorded, user banned, and case archived in {ensured['archive'].mention}."

        embed = self._build_active_embed(
            user,
            interaction.user,
            reason=safe_reason,
            case_id=str(case["case_id"]),
            strike_count=int(case["strike_count"]),
            status=str(case["status"]),
            history_count=len(case.get("history") or []),
            source_label="/strike",
        )
        if msg is None:
            msg = await thread.send(embed=embed, view=self._build_active_view(user.id, len(case.get("history") or [])))
            action = "created"
        else:
            await msg.edit(embed=embed, view=self._build_active_view(user.id, len(case.get("history") or [])))
            action = "updated"
        case["message_id"] = msg.id
        _save_state(state)
        if not notify_before_action:
            try:
                await self._notify_user_of_strike(
                    user,
                    strike_count=int(case["strike_count"]),
                    reason=safe_reason,
                    action_label=str(action_result.get("action_label") or "No automatic action applied"),
                    next_action_text=str(action_result.get("next_action_text") or ""),
                )
            except Exception:
                log.exception("moderation: failed to notify user of strike", extra={"guild_id": guild.id, "user_id": user.id, "case_id": case.get("case_id")})
        try:
            await self._post_status_embed(
                guild,
                title=f"Moderation • Strike {action.title()}",
                user=user,
                case_id=str(case['case_id']),
                details=[
                    ("Strikes", str(case["strike_count"]), True),
                    ("Action", str(action_result.get("action_label") or "No automatic action applied"), False),
                    ("Message ID", str(msg.id), True),
                    ("Moderator", interaction.user.mention, False),
                ],
            )
        except Exception:
            log.exception("moderation: failed to post strike status", extra={"guild_id": guild.id, "user_id": user.id, "case_id": case.get("case_id")})
        return f"Strike recorded in {thread.mention}."

    async def execute_promote(self, interaction: discord.Interaction, user_id: int, *, mode: str, strike_reason: Optional[str], source_token: Optional[str] = None) -> None:
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        ensured = await self.ensure_threads(guild)
        if not ensured:
            await interaction.response.send_message("Moderation threads are unavailable.", ephemeral=True)
            return
        await self.prune_stale_messages(guild, ensured)
        inv_thread: discord.Thread = ensured["investigation"]
        active_thread: discord.Thread = ensured["active"]

        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        inv_cases = cfg.setdefault("investigation_cases", {})
        active_cases = cfg.setdefault("active_cases", {})
        inv_case = inv_cases.get(str(int(user_id)))
        if not inv_case:
            if interaction.response.is_done():
                await interaction.followup.send("Investigation case not found.", ephemeral=True)
            else:
                await interaction.response.send_message("Investigation case not found.", ephemeral=True)
            return
        if str(inv_case.get("status") or "").upper() == "PROMOTED":
            if interaction.response.is_done():
                await interaction.followup.send("This action has already been handled.", ephemeral=True)
            else:
                await interaction.response.send_message("This action has already been handled.", ephemeral=True)
            return

        user = guild.get_member(int(user_id))
        if user is None:
            try:
                user = await guild.fetch_member(int(user_id))
            except Exception:
                if interaction.response.is_done():
                    await interaction.followup.send("Unable to resolve that user in this server.", ephemeral=True)
                else:
                    await interaction.response.send_message("Unable to resolve that user in this server.", ephemeral=True)
                return

        if mode in {"create_strike1", "merge_inc"} and not _sanitize_reason(str(strike_reason or "")):
            if interaction.response.is_done():
                await interaction.followup.send("Strike reason is required.", ephemeral=True)
            else:
                await interaction.response.send_message("Strike reason is required.", ephemeral=True)
            return

        active_case = active_cases.get(str(int(user_id)))
        active_msg: Optional[discord.Message] = None
        created_active = False
        if active_case is None:
            existing_active = await self._find_existing_active_message(active_thread, user.id)
            active_case = {
                "case_id": _next_case_id(state, "active"),
                "user_id": user.id,
                "status": "ACTIVE",
                "strike_count": 0,
                "message_id": existing_active.id if existing_active else None,
                "thread_id": active_thread.id,
                "history": [],
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
            }
            active_cases[str(user.id)] = active_case
            active_msg = existing_active
            created_active = True
        else:
            if active_case.get("message_id"):
                try:
                    active_msg = await active_thread.fetch_message(int(active_case["message_id"]))
                except Exception:
                    active_msg = None
            if active_msg is None:
                active_msg = await self._find_existing_active_message(active_thread, user.id)

        promoted_at = _now_iso()
        inv_history = inv_case.setdefault("history", [])
        selected_entry = None
        selected_index = None
        if source_token:
            m = re.match(r"^report:(\d+)$", str(source_token or "").strip().lower())
            if m:
                idx = int(m.group(1)) - 1
                if 0 <= idx < len(inv_history):
                    candidate = inv_history[idx] or {}
                    if str(candidate.get("type") or "") == "INVESTIGATION_REPORT" and not candidate.get("removed") and not candidate.get("promoted"):
                        selected_entry = candidate
                        selected_index = idx
        if selected_entry is None:
            for idx in range(len(inv_history)-1, -1, -1):
                candidate = inv_history[idx] or {}
                if str(candidate.get("type") or "") == "INVESTIGATION_REPORT" and not candidate.get("removed") and not candidate.get("promoted"):
                    selected_entry = candidate
                    selected_index = idx
                    break
        if selected_entry is not None:
            active_case.setdefault("history", []).append(
                {
                    "timestamp": selected_entry.get("timestamp") or promoted_at,
                    "type": "INVESTIGATION_REPORT",
                    "source": "investigation",
                    "reporter_id": selected_entry.get("reporter_id"),
                    "reporter_name": selected_entry.get("reporter_name"),
                    "reason": selected_entry.get("reason"),
                    "investigation_case_id": inv_case.get("case_id"),
                    "investigation_report_index": int(selected_index or 0) + 1,
                }
            )
            selected_entry["promoted"] = True
            selected_entry["promoted_at"] = promoted_at
            selected_entry["promoted_by"] = interaction.user.id
            selected_entry["promoted_to_case_id"] = str(active_case.get("case_id") or "")

        open_reports_before = [e for e in inv_history if str(e.get("type") or "") == "INVESTIGATION_REPORT" and not e.get("removed") and not e.get("promoted")]
        increment_strike = mode in {"create_strike1", "merge_inc"}
        active_case.setdefault("history", []).append(
            {
                "timestamp": promoted_at,
                "type": "PROMOTION",
                "source": "investigation",
                "moderator_id": interaction.user.id,
                "investigation_case_id": inv_case.get("case_id"),
                "reason": strike_reason or str(active_case.get("latest_reason") or "Merged investigation into active case"),
                "report_count": int(len(open_reports_before) + (1 if selected_entry is not None else 0)),
            }
        )
        action_result = {"action_label": "No automatic action applied", "next_action_text": ""}
        notify_before_action = False
        if increment_strike:
            _append_strike_entry(active_case, reason=str(strike_reason), moderator_id=interaction.user.id, source="promotion")
            active_case["latest_reason"] = strike_reason
            active_case["latest_moderator_id"] = interaction.user.id
            settings = _guild_settings(cfg)
            action_preview = _threshold_config(settings, int(active_case.get("strike_count", 0) or 0))
            notify_before_action = str(action_preview.get("type") or "") == "ban"
            if notify_before_action:
                await self._notify_user_of_strike(
                    user,
                    strike_count=int(active_case.get("strike_count", 0) or 0),
                    reason=str(strike_reason),
                    action_label=str(action_preview.get("label") or "Banned from the server"),
                    next_action_text=str(_next_action_text(settings, int(active_case.get("strike_count", 0) or 0)) or ""),
                )
            action_result = await self._apply_threshold_action(guild, user, active_case, reason=str(strike_reason), moderator=interaction.user, state=state)
            if str(action_result.get("action_type") or "") == "ban" and bool(action_result.get("applied")) and not bool(action_result.get("failed")):
                inv_case["status"] = "PROMOTED"
                inv_case["updated_at"] = promoted_at
                if inv_case.get("message_id"):
                    try:
                        msg = await inv_thread.fetch_message(int(inv_case["message_id"]))
                        await msg.delete()
                    except Exception:
                        pass
                inv_cases.pop(str(user.id), None)
                archive_case = await self._archive_case_after_ban(guild, user, active_case, moderator=interaction.user, reason=str(strike_reason), state=state, ensured=ensured)
                try:
                    await self._post_status_embed(
                        guild,
                        title="Banned",
                        user=user,
                        case_id=str((archive_case or {}).get("case_id") or active_case.get("case_id") or "UnknownCase"),
                        details=[
                            ("Reason", str(strike_reason), False),
                            ("Moderator", interaction.user.mention, False),
                            ("Strike Level", str(int(active_case.get("strike_count") or 0)), True),
                            ("Case ID", str(active_case.get("case_id") or "UnknownCase"), True),
                        ],
                    )
                except Exception:
                    log.exception("moderation: failed to post case closure status", extra={"guild_id": guild.id, "user_id": user.id, "case_id": active_case.get("case_id")})
                result = f"Promoted <@{user.id}> to Active, applied final ban, and archived case {(archive_case or {}).get('case_id', 'UnknownCase')}."
                await _send_ephemeral_response(interaction, result)
                return
        active_case["status"] = "ACTIVE"
        active_case["thread_id"] = active_thread.id
        active_case["updated_at"] = promoted_at

        latest_reason = str(active_case.get("latest_reason") or strike_reason or inv_case.get("latest_reason") or "Merged investigation into active case")
        active_embed = self._build_active_embed(
            user,
            interaction.user,
            reason=latest_reason,
            case_id=str(active_case["case_id"]),
            strike_count=int(active_case.get("strike_count", 0) or 0),
            status=str(active_case["status"]),
            history_count=len(active_case.get("history") or []),
            source_label=("Investigation → Active" if created_active else "Investigation merged into Active"),
        )
        if active_msg is None:
            active_msg = await active_thread.send(embed=active_embed, view=self._build_active_view(user.id, len(active_case.get("history") or [])))
        else:
            await active_msg.edit(embed=active_embed, view=self._build_active_view(user.id, len(active_case.get("history") or [])))
        active_case["message_id"] = active_msg.id

        open_reports = [e for e in inv_case.get("history") or [] if str(e.get("type") or "") == "INVESTIGATION_REPORT" and not e.get("removed") and not e.get("promoted")]
        inv_case["report_count"] = len(open_reports)
        inv_case["updated_at"] = promoted_at
        inv_case["status"] = "ACTIVE" if open_reports else "PROMOTED"
        if open_reports:
            latest_open = open_reports[-1]
            inv_case["latest_reason"] = str(latest_open.get("reason") or "")
            inv_case["latest_reporter_id"] = latest_open.get("reporter_id")
            reporter_obj = guild.get_member(int(latest_open.get("reporter_id") or 0)) if latest_open.get("reporter_id") else None
            if reporter_obj is None:
                reporter_obj = interaction.user
            inv_embed = self._build_investigation_embed(
                user,
                reporter_obj,
                reason=str(inv_case.get("latest_reason") or "New report"),
                case_id=str(inv_case.get("case_id") or "UnknownCase"),
                report_count=int(inv_case.get("report_count") or 0),
                status=str(inv_case.get("status") or "ACTIVE"),
            )
            inv_msg = None
            if inv_case.get("message_id"):
                try:
                    inv_msg = await inv_thread.fetch_message(int(inv_case["message_id"]))
                except Exception:
                    inv_msg = None
            if inv_msg is None:
                inv_msg = await self._find_existing_investigation_message(inv_thread, user.id)
            if inv_msg is None:
                inv_msg = await inv_thread.send(embed=inv_embed, view=self._build_investigation_view(user.id, int(inv_case.get("report_count") or 0)))
            else:
                await inv_msg.edit(embed=inv_embed, view=self._build_investigation_view(user.id, int(inv_case.get("report_count") or 0)))
            inv_case["message_id"] = inv_msg.id
        else:
            if inv_case.get("message_id"):
                try:
                    msg = await inv_thread.fetch_message(int(inv_case["message_id"]))
                    await msg.delete()
                except Exception:
                    pass
            inv_cases.pop(str(user.id), None)
        _save_state(state)

        if increment_strike and strike_reason and not notify_before_action:
            await self._notify_user_of_strike(
                user,
                strike_count=int(active_case.get("strike_count", 0) or 0),
                reason=strike_reason,
                action_label=str(action_result.get("action_label") or "No automatic action applied"),
                next_action_text=str(action_result.get("next_action_text") or ""),
            )

        try:
            await post_status(
                self.bot,
                f"investigation promoted user={user.id} inv={inv_case.get('case_id')} active={active_case.get('case_id')} increment={1 if increment_strike else 0} action={action_result.get('action_type') if increment_strike else 'none'}",
            )
        except Exception:
            pass
        try:
            await self._post_status_embed(
                guild,
                title="Moderation • Investigation Promoted",
                user=user,
                case_id=str(active_case.get("case_id") or "UnknownCase"),
                details=[
                    ("Investigation", str(inv_case.get("case_id") or "UnknownCase"), True),
                    ("Increment", "Yes" if increment_strike else "No", True),
                    ("Action", str(action_result.get("action_label") or action_result.get("action_type") or "None"), True),
                    ("Moderator", interaction.user.mention, False),
                ],
            )
        except Exception:
            pass
        result = (
            f"Promoted <@{user.id}> to Active case {active_case.get('case_id')}."
            if created_active
            else f"Merged investigation into Active case {active_case.get('case_id')}."
        )
        await _send_ephemeral_response(interaction, result)

    async def _archive_case(self, guild: discord.Guild, user: discord.Member, case: Dict[str, Any], *, moderator: discord.abc.User, reason: str, final_action: str, finalization_source: str, state: Dict[str, Any], ensured: Dict[str, discord.Thread], timeout_removed_before_close: bool = False, punishment_reduced_before_finalization: bool = False) -> Optional[Dict[str, Any]]:
        cfg = _guild_cfg(state, guild.id)
        _normalize_archive_storage(cfg)
        archive_cases = cfg.setdefault("archive_cases", {})
        archive_index = cfg.setdefault("archive_index_by_user", {})
        archive_thread: discord.Thread = ensured["archive"]
        source_case_id = str(case.get("case_id") or "")
        for existing_id in _get_archived_case_ids_for_user(cfg, int(user.id)):
            existing_case = archive_cases.get(str(existing_id)) or {}
            if source_case_id and str(existing_case.get("source_case_id") or "") == source_case_id:
                try:
                    await self._refresh_archived_message(guild, existing_case)
                except Exception:
                    pass
                return existing_case
        archived_at = _now_iso()
        archive_case_id = _next_case_id(state, "archive")
        archive_case = {
            "case_id": archive_case_id,
            "user_id": int(user.id),
            "status": "ARCHIVED",
            "closed": True,
            "source_case_id": str(case.get("case_id") or ""),
            "source_case_type": str(case.get("status") or "Active").title(),
            "final_action": final_action,
            "finalization_source": finalization_source,
            "final_reason": reason,
            "strike_count": int(case.get("strike_count") or 0),
            "strikes": [dict(item) for item in _ensure_case_strike_entries(case)],
            "history": [dict(item) for item in list(case.get("history") or [])],
            "created_at": case.get("created_at") or archived_at,
            "updated_at": archived_at,
            "archived_at": archived_at,
            "closed_at": archived_at,
            "closed_by": getattr(moderator, "id", None),
            "original_moderator_id": case.get("latest_moderator_id") or case.get("moderator_id") or getattr(moderator, "id", None),
            "timeout_removed_before_close": bool(timeout_removed_before_close),
            "punishment_reduced_before_finalization": bool(punishment_reduced_before_finalization),
            "role_ids_snapshot": [int(role.id) for role in getattr(user, "roles", []) if getattr(role, "id", 0) != int(guild.default_role.id) and not getattr(role, "managed", False)],
            "ban_triggering_strike_id": _find_triggering_ban_strike_id(case) if str(final_action).lower() == "ban" else None,
            "ban_recovery": None,
            "message_id": None,
            "thread_id": archive_thread.id,
            "display_name": getattr(user, "display_name", getattr(user, "name", str(user.id))),
            "user_name": getattr(user, "name", getattr(user, "display_name", str(user.id))),
            "moderator_name": getattr(moderator, "display_name", getattr(moderator, "name", "Moderator")),
        }
        archive_case.setdefault("history", []).append({
            "timestamp": archived_at,
            "type": "CASE_CLOSED",
            "moderator_id": getattr(moderator, "id", None),
            "reason": reason,
            "final_action": final_action,
            "finalization_source": finalization_source,
            "strike_count": int(case.get("strike_count") or 0),
        })
        existing_case_ids = _get_archived_case_ids_for_user(cfg, int(user.id))
        archive_embed = self._build_archive_embed(user, archive_case=archive_case, prior_case_count=len(existing_case_ids))
        archive_msg = await archive_thread.send(embed=archive_embed, view=ArchiveCaseView(self.bot.get_cog("ModerationCog"), user_id=int(user.id), archive_case_id=str(archive_case_id), archive_case=archive_case))
        archive_case["message_id"] = archive_msg.id
        archive_cases[str(archive_case_id)] = archive_case
        archive_index.setdefault(str(int(user.id)), []).insert(0, str(archive_case_id))
        active_thread_id = case.get("thread_id")
        active_message_id = case.get("message_id")
        if active_thread_id and active_message_id:
            try:
                active_thread = ensured.get("active")
                if active_thread is not None and int(active_thread.id) == int(active_thread_id):
                    active_msg = await active_thread.fetch_message(int(active_message_id))
                    await active_msg.delete()
            except Exception:
                pass
        cfg.setdefault("active_cases", {}).pop(str(int(user.id)), None)

        inv_case = cfg.setdefault("investigation_cases", {}).pop(str(int(user.id)), None)
        if isinstance(inv_case, dict):
            inv_thread_id = inv_case.get("thread_id")
            inv_message_id = inv_case.get("message_id")
            if inv_thread_id and inv_message_id:
                try:
                    inv_thread = ensured.get("investigation")
                    if inv_thread is not None and int(inv_thread.id) == int(inv_thread_id):
                        inv_msg = await inv_thread.fetch_message(int(inv_message_id))
                        await inv_msg.delete()
                except Exception:
                    pass

        _save_state(state)
        return archive_case

    async def show_forgive_picker(self, interaction: discord.Interaction, user_id: int):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Active case not found.", ephemeral=True)
            return
        strikes = _ensure_case_strike_entries(case)
        if not strikes:
            await interaction.response.send_message("This case has no strike entries to forgive.", ephemeral=True)
            return
        debug_log.warning(
            "moderation: forgive picker opened guild=%s user=%s moderator=%s case=%s strikes=%s",
            getattr(guild, "name", guild.id),
            int(user_id),
            int(interaction.user.id),
            str(case.get("case_id") or "UnknownCase"),
            len(strikes),
        )
        options = []
        for idx, entry in enumerate(strikes, start=1):
            reason = str(entry.get("reason") or "No reason provided")
            label = f"Strike {idx}"
            options.append(discord.SelectOption(label=label, value=str(entry.get("strike_id") or idx), description=reason[:100]))
        await interaction.response.send_message("Choose which strike reason to remove.", ephemeral=True, view=ForgiveStrikeChoiceView(self.bot.get_cog("ModerationCog"), user_id=int(user_id), options=options[:MAX_OPTIONS]))

    async def confirm_forgive_strike(self, interaction: discord.Interaction, *, user_id: int, strike_id: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Active case not found.", ephemeral=True)
            return
        strikes = _ensure_case_strike_entries(case)
        target = next((s for s in strikes if str(s.get("strike_id")) == str(strike_id)), None)
        if target is None:
            await interaction.response.send_message("That strike could not be found.", ephemeral=True)
            return
        reason = str(target.get("reason") or "No reason provided")
        debug_log.warning(
            "moderation: forgive strike selected guild=%s user=%s moderator=%s case=%s strike_id=%s reason=%s",
            getattr(guild, "name", guild.id),
            int(user_id),
            int(interaction.user.id),
            str(case.get("case_id") or "UnknownCase"),
            str(strike_id),
            reason[:120],
        )
        await interaction.response.edit_message(content=f"Forgive this strike?\n\nReason: {reason}", view=ConfirmForgiveView(self.bot.get_cog("ModerationCog"), user_id=int(user_id), strike_id=str(strike_id)))

    async def forgive_strike(self, interaction: discord.Interaction, *, user_id: int, strike_id: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        ensured = await self.ensure_threads(guild)
        if not ensured:
            await interaction.response.send_message("Moderation threads are unavailable.", ephemeral=True)
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await interaction.response.send_message("Active case not found.", ephemeral=True)
            return
        strikes = _ensure_case_strike_entries(case)
        target = next((s for s in strikes if str(s.get("strike_id")) == str(strike_id)), None)
        if target is None:
            await interaction.response.send_message("That strike could not be found.", ephemeral=True)
            return
        removed_reason = str(target.get("reason") or "No reason provided")
        debug_log.warning(
            "moderation: forgive confirmed guild=%s user=%s moderator=%s case=%s strike_id=%s removed_reason=%s before_count=%s",
            getattr(guild, "name", guild.id),
            int(user_id),
            int(interaction.user.id),
            str(case.get("case_id") or "UnknownCase"),
            str(strike_id),
            removed_reason[:120],
            len(strikes),
        )
        strikes[:] = [s for s in strikes if str(s.get("strike_id")) != str(strike_id)]
        for idx, s in enumerate(strikes, start=1):
            s["threshold_level"] = idx
        case["strike_count"] = len(strikes)
        case["updated_at"] = _now_iso()
        case["last_applied_threshold"] = min(int(case.get("last_applied_threshold", 0) or 0), int(case.get("strike_count") or 0))
        case.setdefault("history", []).append({
            "timestamp": _now_iso(),
            "type": "STRIKE_FORGIVEN",
            "moderator_id": interaction.user.id,
            "reason": removed_reason,
            "strike_id": str(strike_id),
            "strike_count": int(case.get("strike_count") or 0),
        })
        user = guild.get_member(int(user_id))
        if user is None:
            try:
                user = await guild.fetch_member(int(user_id))
            except Exception:
                user = discord.Object(id=int(user_id))
        if int(case.get("strike_count") or 0) <= 0:
            timeout_was_removed = False
            timeout_status_text = "No active timeout"
            live_member: Optional[discord.Member] = None
            try:
                live_member = await guild.fetch_member(int(user_id))
                user = live_member
            except discord.NotFound:
                live_member = None
            except Exception as exc:
                await interaction.response.send_message(f"Forgive aborted. I could not fetch the live offender record first: {exc}", ephemeral=True)
                return

            if isinstance(live_member, discord.Member) and live_member.is_timed_out():
                try:
                    await live_member.timeout(None, reason=f"Case forgiven to zero by {interaction.user} ({interaction.user.id})")
                except Exception as exc:
                    await interaction.response.send_message(f"Forgive aborted. I could not remove the active timeout first: {exc}", ephemeral=True)
                    return

                try:
                    verify_member = await guild.fetch_member(int(user_id))
                except Exception as exc:
                    await interaction.response.send_message(f"Forgive aborted. Timeout removal could not be verified: {exc}", ephemeral=True)
                    return

                if verify_member.is_timed_out():
                    await interaction.response.send_message("Forgive aborted. The offender still has an active Discord timeout, so the case was not archived.", ephemeral=True)
                    return

                user = verify_member
                timeout_was_removed = True
                timeout_status_text = "Yes"
                case.setdefault("history", []).append({
                    "timestamp": _now_iso(),
                    "type": "TIMEOUT_REMOVED",
                    "moderator_id": interaction.user.id,
                    "reason": "Active Discord timeout removed before case closure",
                    "strike_count": int(case.get("strike_count") or 0),
                })

            debug_log.warning(
                "moderation: forgive reduced to zero guild=%s user=%s moderator=%s case=%s timeout_removed=%s",
                getattr(guild, "name", guild.id),
                int(user_id),
                int(interaction.user.id),
                str(case.get("case_id") or "UnknownCase"),
                timeout_status_text,
            )
            archive_case = await self._archive_case(
                guild,
                user,
                case,
                moderator=interaction.user,
                reason="Forgiven to zero strikes",
                final_action="cleared",
                finalization_source="forgive_to_zero",
                state=state,
                ensured=ensured,
                timeout_removed_before_close=timeout_was_removed,
                punishment_reduced_before_finalization=True,
            )
            try:
                await self._post_status_embed(
                    guild,
                    title="Moderation • Case Cleared",
                    user=user,
                    case_id=str((archive_case or {}).get("case_id") or case.get("case_id") or "UnknownCase"),
                    details=[("Removed Strike", removed_reason, False), ("Timeout Removed", timeout_status_text, True), ("Moderator", interaction.user.mention, False)],
                )
            except Exception:
                pass
            await interaction.response.edit_message(content="Strike forgiven. Case reached 0 and was archived.", view=None)
            return
        case["latest_reason"] = str(strikes[-1].get("reason") or removed_reason) if strikes else removed_reason
        case["latest_moderator_id"] = int(strikes[-1].get("issued_by") or interaction.user.id) if strikes else interaction.user.id
        timeout_changed = False
        timeout_status_text = "No timeout change"
        if isinstance(user, discord.Member):
            timeout_changed, timeout_status_text = await self._normalize_member_timeout_after_strike_change(
                guild,
                int(user_id),
                int(case.get("strike_count") or 0),
                moderator=interaction.user,
                reason=f"Strike forgiven by {interaction.user} ({interaction.user.id})",
            )
            case.setdefault("history", []).append({
                "timestamp": _now_iso(),
                "type": "TIMEOUT_REEVALUATED",
                "moderator_id": interaction.user.id,
                "reason": timeout_status_text,
                "strike_count": int(case.get("strike_count") or 0),
            })
        thread = ensured["active"]
        msg = None
        if case.get("message_id"):
            try:
                msg = await thread.fetch_message(int(case["message_id"]))
            except Exception:
                msg = None
        if msg is None and isinstance(user, discord.Member):
            msg = await self._find_existing_active_message(thread, user.id)
        moderator_obj = guild.get_member(int(case.get("latest_moderator_id") or interaction.user.id)) or interaction.user
        if isinstance(user, discord.Member):
            embed = self._build_active_embed(user, moderator_obj, reason=str(case.get("latest_reason") or removed_reason), case_id=str(case.get("case_id") or "UnknownCase"), strike_count=int(case.get("strike_count") or 0), status=str(case.get("status") or "ACTIVE"), history_count=len(case.get("history") or []), source_label="Active", prior_archive_count=len(_get_archived_case_ids_for_user(cfg, int(user.id))))
            if msg is not None:
                await msg.edit(embed=embed, view=self._build_active_view(int(user_id), len(case.get("history") or [])))
                case["message_id"] = msg.id
        _save_state(state)
        debug_log.warning(
            "moderation: forgive applied guild=%s user=%s moderator=%s case=%s remaining_strikes=%s",
            getattr(guild, "name", guild.id),
            int(user_id),
            int(interaction.user.id),
            str(case.get("case_id") or "UnknownCase"),
            int(case.get("strike_count") or 0),
        )
        try:
            await self._post_status_embed(guild, title="Moderation • Strike Forgiven", user=user, case_id=str(case.get("case_id") or "UnknownCase"), details=[("Removed Strike", removed_reason, False), ("Strikes Remaining", str(int(case.get("strike_count") or 0)), True), ("Moderator", interaction.user.mention, False)])
        except Exception:
            pass
        await interaction.response.edit_message(content=f"Forgave one strike. Remaining strikes: {int(case.get('strike_count') or 0)}.", view=None)

    async def manual_ban_investigation_case(self, interaction: discord.Interaction, *, user_id: int, reason: str):
        async def _send_ephemeral(message: str):
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)
            except discord.NotFound:
                log.info("moderation: investigation manual ban response expired user=%s", int(user_id))
            except Exception:
                log.exception("moderation: investigation manual ban response failed user=%s", int(user_id))

        guild = interaction.guild
        if guild is None:
            await _send_ephemeral("Server only.")
            return
        ensured = await self.ensure_threads(guild)
        if not ensured:
            await _send_ephemeral("Moderation threads are unavailable.")
            return
        clean_reason = _sanitize_reason(reason)
        if not clean_reason:
            await _send_ephemeral("Ban reason is required.")
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("investigation_cases", {}).get(str(int(user_id)))
        if not case:
            await _send_ephemeral("Investigation case not found.")
            return

        target_user_id = int(case.get("user_id") or user_id)
        user = guild.get_member(target_user_id)
        if user is None:
            try:
                user = await guild.fetch_member(target_user_id)
            except Exception as exc:
                await _send_ephemeral(f"Manual Ban aborted. I could not resolve the reported user: {exc}")
                return

        cfg.setdefault("pending_ban_leave_notices", {})[str(user.id)] = {
            "user_id": int(user.id),
            "user_name": getattr(user, "display_name", user.name),
            "avatar_url": str(getattr(getattr(user, "display_avatar", None), "url", "") or ""),
            "reason": clean_reason,
            "moderator_id": getattr(interaction.user, "id", None),
            "moderator_name": getattr(interaction.user, "display_name", getattr(interaction.user, "name", "Moderator")),
            "case_id": str(case.get("case_id") or ""),
            "strike_count": int(case.get("strike_count") or 0),
            "created_at": _now_iso(),
        }
        _save_state(state)

        try:
            await guild.ban(user, reason=f"Manual investigation-case ban by {interaction.user} ({interaction.user.id}): {clean_reason}", delete_message_seconds=0)
        except Exception as exc:
            try:
                cfg.setdefault("pending_ban_leave_notices", {}).pop(str(user.id), None)
                _save_state(state)
            except Exception:
                pass
            await _send_ephemeral(f"Manual Ban aborted. I could not ban the user: {exc}")
            return

        case.setdefault("history", []).append({
            "timestamp": _now_iso(),
            "type": "BAN",
            "moderator_id": interaction.user.id,
            "reason": clean_reason,
            "strike_count": int(case.get("strike_count") or 0),
            "action": "ban",
            "label": "Manual Ban",
            "applied": True,
        })
        case["updated_at"] = _now_iso()
        case["latest_reason"] = clean_reason
        case["latest_moderator_id"] = interaction.user.id
        archive_case = await self._archive_case(
            guild,
            user,
            case,
            moderator=interaction.user,
            reason=clean_reason,
            final_action="ban",
            finalization_source="manual_ban",
            state=state,
            ensured=ensured,
        )
        cfg.setdefault("investigation_cases", {}).pop(str(int(user_id)), None)
        try:
            inv_thread_id = case.get("thread_id")
            inv_message_id = case.get("message_id")
            if inv_thread_id and inv_message_id:
                inv_thread = ensured.get("investigation")
                if inv_thread is not None and int(inv_thread.id) == int(inv_thread_id):
                    inv_msg = await inv_thread.fetch_message(int(inv_message_id))
                    await inv_msg.delete()
        except Exception:
            pass
        _save_state(state)
        try:
            log.info(
                "moderation: investigation case finalized guild=%s user=%s source_case=%s action=ban archived=%s",
                guild.id,
                user.id,
                case.get("case_id"),
                (archive_case or {}).get("case_id"),
            )
        except Exception:
            pass
        try:
            await post_status(self.bot, f"report severe user={user.id} source={case.get('case_id')} action=ban archived={(archive_case or {}).get('case_id')}")
        except Exception:
            pass
        try:
            await self._post_status_embed(
                guild,
                title="Moderation • Manual Ban",
                user=user,
                case_id=str((archive_case or {}).get("case_id") or case.get("case_id") or "UnknownCase"),
                details=[
                    ("Reason", clean_reason, False),
                    ("Finalization Source", "Manual Ban", True),
                    ("Moderator", interaction.user.mention, False),
                    ("Origin", "Investigation", True),
                ],
            )
        except Exception:
            pass
        await _send_ephemeral(f"User banned and archived to Offenders - Archive as `{(archive_case or {}).get('case_id', 'UnknownCase')}`.")


    async def manual_ban_active_case(self, interaction: discord.Interaction, *, user_id: int, reason: str):
        async def _send_ephemeral(message: str):
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)
            except discord.NotFound:
                log.info("moderation: manual ban response expired user=%s", int(user_id))
            except Exception:
                log.exception("moderation: manual ban response failed user=%s", int(user_id))

        guild = interaction.guild
        if guild is None:
            await _send_ephemeral("Server only.")
            return
        ensured = await self.ensure_threads(guild)
        if not ensured:
            await _send_ephemeral("Moderation threads are unavailable.")
            return
        clean_reason = _sanitize_reason(reason)
        if not clean_reason:
            await _send_ephemeral("Ban reason is required.")
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await _send_ephemeral("Active case not found.")
            return

        target_user_id = int(case.get("user_id") or user_id)
        user = guild.get_member(target_user_id)
        if user is None:
            try:
                user = await guild.fetch_member(target_user_id)
            except Exception as exc:
                await _send_ephemeral(f"Manual Ban aborted. I could not resolve the active offender: {exc}")
                return

        cfg.setdefault("pending_ban_leave_notices", {})[str(user.id)] = {
            "user_id": int(user.id),
            "user_name": getattr(user, "display_name", user.name),
            "avatar_url": str(getattr(getattr(user, "display_avatar", None), "url", "") or ""),
            "reason": clean_reason,
            "moderator_id": getattr(interaction.user, "id", None),
            "moderator_name": getattr(interaction.user, "display_name", getattr(interaction.user, "name", "Moderator")),
            "case_id": str(case.get("case_id") or ""),
            "strike_count": int(case.get("strike_count") or 0),
            "created_at": _now_iso(),
        }
        _save_state(state)

        try:
            await guild.ban(user, reason=f"Manual active-case ban by {interaction.user} ({interaction.user.id}): {clean_reason}", delete_message_seconds=0)
        except Exception as exc:
            try:
                cfg.setdefault("pending_ban_leave_notices", {}).pop(str(user.id), None)
                _save_state(state)
            except Exception:
                pass
            await _send_ephemeral(f"Manual Ban aborted. I could not ban the user: {exc}")
            return

        case.setdefault("history", []).append({
            "timestamp": _now_iso(),
            "type": "BAN",
            "moderator_id": interaction.user.id,
            "reason": clean_reason,
            "strike_count": int(case.get("strike_count") or 0),
            "action": "ban",
            "label": "Manual Ban",
            "applied": True,
        })
        case["updated_at"] = _now_iso()
        case["latest_reason"] = clean_reason
        case["latest_moderator_id"] = interaction.user.id
        archive_case = await self._archive_case(
            guild,
            user,
            case,
            moderator=interaction.user,
            reason=clean_reason,
            final_action="ban",
            finalization_source="manual_ban",
            state=state,
            ensured=ensured,
        )
        try:
            log.info(
                "moderation: case finalized guild=%s user=%s source_case=%s action=ban archived=%s",
                guild.id,
                user.id,
                case.get("case_id"),
                (archive_case or {}).get("case_id"),
            )
        except Exception:
            pass
        try:
            await post_status(
                self.bot,
                f"case finalized user={user.id} source={case.get('case_id')} action=ban archived={(archive_case or {}).get('case_id')}",
            )
        except Exception:
            pass
        try:
            await self._post_status_embed(
                guild,
                title="Moderation • Manual Ban",
                user=user,
                case_id=str((archive_case or {}).get("case_id") or case.get("case_id") or "UnknownCase"),
                details=[
                    ("Reason", clean_reason, False),
                    ("Finalization Source", "Manual Ban", True),
                    ("Moderator", interaction.user.mention, False),
                ],
            )
        except Exception:
            pass
        await _send_ephemeral(f"User banned and archived to Offenders - Archive as `{(archive_case or {}).get('case_id')}`.")

    async def clear_active_case(self, interaction: discord.Interaction, *, user_id: int):
        async def _send_ephemeral(message: str):
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(message, ephemeral=True)
                else:
                    await interaction.response.send_message(message, ephemeral=True)
            except discord.NotFound:
                log.info("moderation: clear case response expired user=%s", int(user_id))
            except Exception:
                log.exception("moderation: clear case response failed user=%s", int(user_id))

        async def _finish_ephemeral(message: str):
            try:
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=message, view=None)
                else:
                    await interaction.response.edit_message(content=message, view=None)
            except discord.NotFound:
                try:
                    if interaction.response.is_done():
                        await interaction.followup.send(message, ephemeral=True)
                    else:
                        await interaction.response.send_message(message, ephemeral=True)
                except Exception:
                    log.info("moderation: clear case final response expired user=%s", int(user_id))
            except Exception:
                log.exception("moderation: clear case final response failed user=%s", int(user_id))

        guild = interaction.guild
        if guild is None:
            await _send_ephemeral("Server only.")
            return
        ensured = await self.ensure_threads(guild)
        if not ensured:
            await _send_ephemeral("Moderation threads are unavailable.")
            return
        state = _load_state()
        cfg = _guild_cfg(state, guild.id)
        case = cfg.get("active_cases", {}).get(str(int(user_id)))
        if not case:
            await _send_ephemeral("Active case not found.")
            return

        target_user_id = int(case.get("user_id") or user_id)
        user: discord.abc.User = guild.get_member(target_user_id) or self.bot.get_user(target_user_id) or discord.Object(id=target_user_id)

        fresh_member: Optional[discord.Member] = None
        try:
            fresh_member = await guild.fetch_member(target_user_id)
            user = fresh_member
        except discord.NotFound:
            pass
        except Exception as exc:
            await _send_ephemeral(f"Clear Case aborted. I could not fetch the live offender record first: {exc}")
            return

        timeout_was_removed = False
        timeout_status_text = "No active timeout"
        if isinstance(fresh_member, discord.Member) and fresh_member.is_timed_out():
            try:
                await fresh_member.timeout(None, reason=f"Case closed by {interaction.user} ({interaction.user.id})")
            except Exception as exc:
                await _send_ephemeral(f"Clear Case aborted. I could not remove the active timeout first: {exc}")
                return

            try:
                verify_member = await guild.fetch_member(target_user_id)
            except Exception as exc:
                await _send_ephemeral(f"Clear Case aborted. Timeout removal could not be verified: {exc}")
                return

            if verify_member.is_timed_out():
                await _send_ephemeral("Clear Case aborted. The offender still has an active Discord timeout, so the case was not archived.")
                return

            user = verify_member
            timeout_was_removed = True
            timeout_status_text = "Yes"
            case.setdefault("history", []).append({
                "timestamp": _now_iso(),
                "type": "TIMEOUT_REMOVED",
                "moderator_id": interaction.user.id,
                "reason": "Active Discord timeout removed before case closure",
                "strike_count": int(case.get("strike_count") or 0),
            })

        archive_case = await self._archive_case(guild, user, case, moderator=interaction.user, reason="Case cleared by admin", final_action="cleared", finalization_source="moderator_clear", state=state, ensured=ensured, timeout_removed_before_close=timeout_was_removed)
        try:
            await self._post_status_embed(
                guild,
                title="Moderation • Case Cleared",
                user=user,
                case_id=str((archive_case or {}).get("case_id") or case.get("case_id") or "UnknownCase"),
                details=[
                    ("Final Action", "Cleared", True),
                    ("Timeout Removed", timeout_status_text, True),
                    ("Moderator", interaction.user.mention, False),
                ],
            )
        except Exception:
            pass
        response = "Active case cleared and archived."
        if timeout_was_removed:
            response += " The active Discord timeout was removed first."
        response += " This live case is finished. Any future strike will start a new active case."
        await _finish_ephemeral(response)

    async def _notify_user_of_strike(self, user: discord.Member, *, strike_count: int, reason: str, action_label: str, next_action_text: str):
        try:
            embed = discord.Embed(
                title=f"You are in trouble • Read this",
                description="A moderation action has been recorded on your account.",
                color=discord.Color.red(),
            )
            embed.add_field(name="Status", value=f"Strike {strike_count}", inline=False)
            embed.add_field(name="Reason", value=reason, inline=False)
            embed.add_field(name="​", value="​", inline=False)
            embed.add_field(name="Action Taken", value=action_label, inline=False)
            if next_action_text:
                embed.add_field(name="Next Step", value=next_action_text, inline=False)
            await user.send(embed=embed)
        except Exception:
            log.info("moderation: unable to DM user=%s for strike notification", user.id)


class ModerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.mgr = ModerationManager(bot)

    def register_persistent_views(self):
        state = _load_state()
        for guild_id, cfg in (state.get("guilds") or {}).items():
            for user_id, case in (cfg.get("investigation_cases") or {}).items():
                try:
                    report_count = int(case.get("report_count", 0) or 0)
                    self.bot.add_view(InvestigationCaseView(self, user_id=int(user_id), report_count=report_count))
                except Exception:
                    log.exception("moderation: failed to register investigation view")
            for active_key, case in (cfg.get("active_cases") or {}).items():
                try:
                    resolved_user_id = int(case.get("user_id") or active_key)
                    history_count = len(case.get("history") or [])
                    self.bot.add_view(ActiveCaseView(self, user_id=resolved_user_id, history_count=history_count))
                except Exception:
                    log.exception("moderation: failed to register active view")
                    continue
                for entry in (case.get("history") or []):
                    if entry.get("type") == "POST_STRIKE_REPORT" and entry.get("alert_message_id") and not entry.get("acknowledged"):
                        try:
                            self.bot.add_view(ActiveAlertView(self, user_id=resolved_user_id, alert_id=str(entry.get("alert_id"))))
                        except Exception:
                            log.exception("moderation: failed to register alert view")
            for archive_case_id, case in (cfg.get("archive_cases") or {}).items():
                try:
                    mid = int(case.get("message_id") or 0)
                    if mid <= 0:
                        continue
                    self.bot.add_view(ArchiveCaseView(self, user_id=int(case.get("user_id") or 0), archive_case_id=str(archive_case_id)), message_id=mid)
                except Exception:
                    log.exception("moderation: failed to register archive view")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        try:
            await self.mgr.complete_pending_ban_recovery_for_member(member)
        except Exception:
            log.exception("moderation: ban recovery completion failed on join")

    @commands.Cog.listener()
    async def on_ready(self):
        self.register_persistent_views()
        for guild in self.bot.guilds:
            try:
                ensured = await self.mgr.ensure_threads(guild)
                await self.mgr.prune_stale_messages(guild, ensured)
            except Exception:
                log.exception("moderation: startup ensure failed")

    @app_commands.command(name="report", description="Report a user to moderators for investigation.")
    @app_commands.guild_only()
    async def report(self, interaction: discord.Interaction, user: discord.Member):
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if user.id == interaction.user.id:
            await interaction.response.send_message("You cannot report yourself.", ephemeral=True)
            return
        if user.bot:
            await interaction.response.send_message("You cannot report a bot.", ephemeral=True)
            return
        await interaction.response.send_modal(ReportReasonModal(self, user.id))

    @app_commands.command(name="strike", description="Record a staff strike for a user.")
    @app_commands.guild_only()
    @app_commands.default_permissions(moderate_members=True)
    async def strike(self, interaction: discord.Interaction, user: discord.Member, reason: str):
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not _is_staff(member):
            await interaction.response.send_message("Mods/admins only.", ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.response.send_message("Server only.", ephemeral=True)
            return
        if user.bot:
            await interaction.response.send_message("You cannot strike a bot.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            msg = await self.mgr.strike_to_active(interaction, user, reason.strip())
            await interaction.followup.send(msg, ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Strike failed: {e}", ephemeral=True)




async def report_user_context_menu(interaction: discord.Interaction, user: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message('Server only.', ephemeral=True)
        return
    if user.id == interaction.user.id:
        await interaction.response.send_message('You cannot report yourself.', ephemeral=True)
        return
    if user.bot:
        await interaction.response.send_message('You cannot report a bot.', ephemeral=True)
        return
    cog = interaction.client.get_cog('ModerationCog')
    if cog is None:
        await interaction.response.send_message('Moderation is unavailable right now.', ephemeral=True)
        return
    await interaction.response.send_modal(ReportReasonModal(cog, int(user.id)))


async def strike_user_context_menu(interaction: discord.Interaction, user: discord.Member):
    if interaction.guild is None:
        await interaction.response.send_message('Server only.', ephemeral=True)
        return
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not _is_admin(member):
        await interaction.response.send_message('Admins only.', ephemeral=True)
        return
    if user.bot:
        await interaction.response.send_message('You cannot strike a bot.', ephemeral=True)
        return
    cog = interaction.client.get_cog('ModerationCog')
    if cog is None:
        await interaction.response.send_message('Moderation is unavailable right now.', ephemeral=True)
        return
    await interaction.response.send_modal(StaffStrikeModal(cog, int(user.id)))

async def setup(bot: commands.Bot, registry: SettingsRegistry) -> None:
    existing = bot.get_cog("ModerationCog")
    if existing is not None:
        await bot.remove_cog("ModerationCog")
    await bot.add_cog(ModerationCog(bot))
    try:
        bot.tree.remove_command("Report User", type=discord.AppCommandType.user)
    except Exception:
        pass
    try:
        bot.tree.remove_command("Strike User", type=discord.AppCommandType.user)
    except Exception:
        pass
    try:
        report_cmd = app_commands.ContextMenu(name="Report User", callback=report_user_context_menu)
        bot.tree.add_command(report_cmd)
        log.info("context menu loaded: Report User")
    except Exception:
        log.exception("context menu failed: Report User")
    try:
        strike_cmd = app_commands.ContextMenu(name="Strike User", callback=strike_user_context_menu)
        strike_cmd.default_permissions = discord.Permissions(administrator=True)
        bot.tree.add_command(strike_cmd)
        log.info("context menu loaded: Strike User")
    except Exception:
        log.exception("context menu failed: Strike User")
    cog = bot.get_cog("ModerationCog")

    def status() -> str:
        state = _load_state()
        enabled = sum(1 for cfg in state.get("guilds", {}).values() if cfg.get("enabled", True))
        return f"✅ Enabled in {enabled} guild(s)" if enabled else "❌ Disabled"

    async def handler(interaction: discord.Interaction, ctx: Dict[str, Any]) -> Optional[dict]:
        if interaction.guild is None:
            return {"op": "respond", "payload": {"content": "Server only.", "ephemeral": True}}
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
            return {"op": "respond", "payload": {"content": "Admins only.", "ephemeral": True}}

        state = _load_state()
        cfg = _guild_cfg(state, interaction.guild.id)
        action = str(ctx.get("action") or "toggle").strip().lower()

        if action == "toggle":
            cfg["enabled"] = not bool(cfg.get("enabled", True))
            _save_state(state)
            if cog is not None:
                await cog.mgr.ensure_threads(interaction.guild)
            return {
                "op": "respond",
                "payload": {
                    "content": f"Moderation {'enabled' if cfg['enabled'] else 'disabled' }.",
                    "ephemeral": True,
                },
            }

        if action == "ensure" and cog is not None:
            ensured = await cog.mgr.ensure_threads(interaction.guild)
            if not ensured:
                return {"op": "respond", "payload": {"content": "Failed to ensure moderation threads.", "ephemeral": True}}
            return {
                "op": "respond",
                "payload": {
                    "content": f"Moderation threads ensured: {THREAD_INVESTIGATION}, {THREAD_ACTIVE}, {THREAD_ARCHIVE}",
                    "ephemeral": True,
                },
            }

        if action in {"mod_tutorial", "admin_tutorial"} and cog is not None:
            _cleanup_expired_tutorial_sessions(cfg)
            tutorial_type = "moderator" if action == "mod_tutorial" else "admin"
            if tutorial_type == "admin" and not interaction.user.guild_permissions.administrator:
                return {"op": "respond", "payload": {"content": "Admins only.", "ephemeral": True}}
            existing = _find_active_tutorial_session(cfg, int(interaction.user.id), tutorial_type)
            if existing:
                remaining = _format_remaining_tutorial_time(existing.get("expires_at"))
                return {"op": "respond", "payload": {"content": f"You already have an active {tutorial_type} tutorial lock. Remaining: {remaining}.", "ephemeral": True}}
            session_id = _next_case_id(state, "tutorial")
            expires_at = (discord.utils.utcnow() + timedelta(hours=TUTORIAL_DURATION_HOURS)).replace(tzinfo=timezone.utc).isoformat()
            _tutorial_sessions(cfg)[session_id] = {
                "session_id": session_id,
                "owner_user_id": int(interaction.user.id),
                "tutorial_type": tutorial_type,
                "status": "active",
                "started_at": _now_iso(),
                "expires_at": expires_at,
            }
            _save_state(state)
            embed, tut_view = cog.mgr._build_tutorial_payload(interaction.guild, interaction.user, tutorial_type, session_id)
            ensured = await cog.mgr.ensure_threads(interaction.guild)
            if not ensured:
                _tutorial_sessions(cfg).pop(session_id, None)
                _save_state(state)
                return {"op": "respond", "payload": {"content": "Training case lock created, but moderation threads are unavailable. Run Ensure Threads and try again.", "ephemeral": True}}
            target_thread = ensured["investigation"] if tutorial_type == "moderator" else ensured["archive"]
            opened_msg = None
            try:
                opened_msg = await target_thread.send(
                    content=f"{interaction.user.mention} your {tutorial_type} tutorial is ready in this thread.",
                    embed=embed,
                    view=tut_view,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            except Exception:
                _tutorial_sessions(cfg).pop(session_id, None)
                _save_state(state)
                log.exception("moderation tutorial post failed guild=%s tutorial_type=%s", interaction.guild.id, tutorial_type)
                return {"op": "respond", "payload": {"content": "Tutorial lock created, but I could not post it in the training thread.", "ephemeral": True}}
            jump_url = opened_msg.jump_url if opened_msg is not None else ""
            return {
                "op": "respond",
                "payload": {
                    "content": f"Tutorial opened in {target_thread.mention}. I also @mentioned you there.\nUse this link: {jump_url}",
                    "ephemeral": True,
                },
            }

        return None

    registry.register(
        SettingFeature(
            feature_id=PACK_META["id"],
            label=PACK_META["name"],
            description=PACK_META["description"],
            category=PACK_META["category"],
            category_description=PACK_META["category_description"],
            handler=handler,
            status=status,
            actions=[
                FeatureAction("toggle", "Toggle", "Enable or disable moderation intake commands and offender threads.", style="danger", row=1),
                FeatureAction("ensure", "Ensure Threads", "Create or restore Offenders - Investigation, Offenders - Active, and Offenders - Archive.", style="secondary", row=2),
                FeatureAction("mod_tutorial", "Moderator Tutorial", "Create a private guided training case for moderator workflow practice.", style="secondary", row=3),
                FeatureAction("admin_tutorial", "Admin Tutorial", "Create a private guided training case for full admin/archive workflow practice.", style="secondary", row=3),
            ],
        )
    )


async def teardown(bot: commands.Bot, registry: SettingsRegistry) -> None:
    try:
        bot.tree.remove_command("Report User", type=discord.AppCommandType.user)
        bot.tree.remove_command("Strike User", type=discord.AppCommandType.user)
    except Exception:
        pass
    cog = bot.get_cog("ModerationCog")
    if cog is not None:
        await bot.remove_cog("ModerationCog")
    registry.unregister(PACK_META["id"])
