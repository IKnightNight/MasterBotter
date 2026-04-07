
from __future__ import annotations

import json
import logging
import random
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import discord
from discord import ui
from discord.ext import commands, tasks

from plugins.settings.registry import SettingsRegistry, SettingFeature, FeatureAction
from plugins.settings.ops_forum import get_or_create_ops_forum, post_status

PACK_META = {"id": "welcome", "name": "Welcome Manager", "version": "J.24.8.7"}
CATEGORY = "Events"
CATEGORY_DESCRIPTION = "Event notifications and announcements."

STATE_PATH = Path(__file__).resolve().parent / "_data" / "state.json"
UNVERIFIED_STATE_PATH = Path(__file__).resolve().parent.parent / "unverified" / "_data" / "state.json"
MODERATION_STATE_PATH = Path(__file__).resolve().parent.parent / "moderation" / "_data" / "state.json"
THREAD_ACTIVE_NAME = "Grace - ACTIVE"
THREAD_COMPLETED_NAME = "Grace - COMPLETED"
ENABLED_TAG = "ENABLED"
DISABLED_TAG = "DISABLED"

DEFAULT_TEMPLATES = {
    "join": "Welcome {user_mention} to **{guild_name}**.",
    "leave": "{user_name} has left **{guild_name}**.",
    "grace_dm": "Hey {user_name},\n\nYou left **{guild_name}**. Your roles are being held for {grace_days} day(s). If you want to come back during that time, use this one-use invite:\n{invite_link}\n\nThis invite expires at {expires_at}. If you return before then, your saved roles will be restored automatically.",
}
DEFAULT_JOIN_PHRASES = [
    "joined us",
    "arrived",
    "made their entrance",
    "dropped in",
    "showed up",
    "rolled in",
    "stepped inside",
    "wandered in",
    "entered the scene",
    "pulled up",
    "clocked in",
    "made it through the door",
    "touched down",
    "came by",
    "popped in",
]
DEFAULT_LEAVE_PHRASES = [
    "headed out",
    "took their leave",
    "slipped away",
    "signed off",
    "stepped away",
    "made their exit",
    "dipped out",
    "moved on",
    "checked out",
    "disappeared into the void",
    "called it a day",
    "left the building",
    "bounced",
    "logged off",
    "faded into the distance",
]

DEFAULT_GUILD_CONFIG = {
    "enabled": True,
    "join_channel_id": None,
    "leave_channel_id": None,
    "autorole_id": None,
    "grace_role_ids": [],
    "grace_days": 14,
    "templates": deepcopy(DEFAULT_TEMPLATES),
    "grace_active_thread_id": None,
    "grace_completed_thread_id": None,
    "grace_records": {},
    "last_sweep_at": None,
}

log = logging.getLogger("bot.settings.welcome")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fromiso(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    try:
        s = str(raw)
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        log.exception("welcome: failed to load state")
    return {"guilds": {}}


def save_state(state: Dict[str, Any]) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        log.exception("welcome: failed to save state")


def _load_moderation_state() -> Dict[str, Any]:
    try:
        if MODERATION_STATE_PATH.exists():
            raw = json.loads(MODERATION_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception:
        log.exception("welcome: failed to load moderation state")
    return {"guilds": {}}


def _save_moderation_state(state: Dict[str, Any]) -> None:
    try:
        MODERATION_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        MODERATION_STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        log.exception("welcome: failed to save moderation state")


def _guild_cfg(state: Dict[str, Any], guild_id: int, *, create: bool = True) -> Optional[Dict[str, Any]]:
    guilds = state.setdefault("guilds", {})
    key = str(int(guild_id))
    if key not in guilds:
        if not create:
            return None
        guilds[key] = deepcopy(DEFAULT_GUILD_CONFIG)
    cfg = guilds[key]
    for k, v in DEFAULT_GUILD_CONFIG.items():
        if k not in cfg:
            cfg[k] = deepcopy(v)
    if not isinstance(cfg.get("templates"), dict):
        cfg["templates"] = deepcopy(DEFAULT_TEMPLATES)
    for k, v in DEFAULT_TEMPLATES.items():
        cfg["templates"].setdefault(k, v)
    if not isinstance(cfg.get("grace_records"), dict):
        cfg["grace_records"] = {}
    if not isinstance(cfg.get("grace_role_ids"), list):
        cfg["grace_role_ids"] = []
    if not isinstance(cfg.get("join_phrases"), list) or not cfg.get("join_phrases"):
        cfg["join_phrases"] = deepcopy(DEFAULT_JOIN_PHRASES)
    if not isinstance(cfg.get("leave_phrases"), list) or not cfg.get("leave_phrases"):
        cfg["leave_phrases"] = deepcopy(DEFAULT_LEAVE_PHRASES)
    return cfg


def _coerce_role_ids(raw: str) -> List[int]:
    out: List[int] = []
    for part in str(raw or "").replace(";", ",").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            out.append(int(s))
        except Exception:
            continue
    seen = set()
    unique: List[int] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        unique.append(x)
    return unique




def _coerce_phrases(raw: str, defaults: List[str]) -> List[str]:
    out: List[str] = []
    for line in str(raw or "").splitlines():
        s = line.strip().strip('-').strip()
        if not s:
            continue
        out.append(s)
    seen = set()
    unique: List[str] = []
    for s in out:
        key = s.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(s)
    return unique or deepcopy(defaults)


def _pick_phrase(values: List[str], defaults: List[str]) -> str:
    pool = [str(x).strip() for x in (values or []) if str(x).strip()]
    if not pool:
        pool = list(defaults)
    return random.choice(pool)

def _has_any_role(member: discord.Member, role_ids: List[int]) -> bool:
    wanted = {int(x) for x in role_ids if x}
    if not wanted:
        return False
    return any(r.id in wanted for r in member.roles)


def _render_template(template: str, **data: Any) -> str:
    try:
        return str(template).format(**data)
    except Exception:
        return str(template)


def _read_unverified_role_id(guild_id: int) -> Optional[int]:
    try:
        if not UNVERIFIED_STATE_PATH.exists():
            return None
        raw = json.loads(UNVERIFIED_STATE_PATH.read_text(encoding="utf-8"))
        cfg = (raw.get("guilds", {}) or {}).get(str(int(guild_id)), {})
        rid = cfg.get("unverified_role_id")
        return int(rid) if rid else None
    except Exception:
        return None


class WelcomeEmbedModal(ui.Modal, title="Welcome Manager Embeds"):
    def __init__(self, guild_id: int):
        super().__init__(timeout=600)
        self.guild_id = int(guild_id)
        state = load_state()
        cfg = _guild_cfg(state, self.guild_id) or deepcopy(DEFAULT_GUILD_CONFIG)

        self.join_phrases = ui.TextInput(
            label="Join phrases (one per line)",
            style=discord.TextStyle.paragraph,
            required=False,
            default="\n".join(str(x) for x in (cfg.get("join_phrases") or DEFAULT_JOIN_PHRASES)),
            max_length=1800,
        )
        self.leave_phrases = ui.TextInput(
            label="Leave phrases (one per line)",
            style=discord.TextStyle.paragraph,
            required=False,
            default="\n".join(str(x) for x in (cfg.get("leave_phrases") or DEFAULT_LEAVE_PHRASES)),
            max_length=1800,
        )

        for item in (self.join_phrases, self.leave_phrases):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        state = load_state()
        cfg = _guild_cfg(state, self.guild_id)
        assert cfg is not None
        cfg["join_phrases"] = _coerce_phrases(str(self.join_phrases.value or ""), DEFAULT_JOIN_PHRASES)
        cfg["leave_phrases"] = _coerce_phrases(str(self.leave_phrases.value or ""), DEFAULT_LEAVE_PHRASES)
        save_state(state)
        await interaction.response.send_message("Welcome embed settings saved.", ephemeral=True)


class WelcomeGraceModal(ui.Modal, title="Welcome Manager Grace"):
    def __init__(self, guild_id: int):
        super().__init__(timeout=600)
        self.guild_id = int(guild_id)
        state = load_state()
        cfg = _guild_cfg(state, self.guild_id) or deepcopy(DEFAULT_GUILD_CONFIG)

        self.grace_dm = ui.TextInput(
            label="Grace DM message",
            style=discord.TextStyle.paragraph,
            required=False,
            default=str(cfg.get("templates", {}).get("grace_dm") or DEFAULT_TEMPLATES["grace_dm"]),
            max_length=1000,
        )
        self.grace_days = ui.TextInput(
            label="Grace days",
            required=False,
            default=str(cfg.get("grace_days", 3)),
            max_length=10,
        )
        self.grace_roles = ui.TextInput(
            label="Grace role IDs (comma separated)",
            style=discord.TextStyle.paragraph,
            required=False,
            default=",".join(str(x) for x in (cfg.get("grace_role_ids") or [])),
            max_length=1000,
        )
        for item in (self.grace_dm, self.grace_days, self.grace_roles):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        state = load_state()
        cfg = _guild_cfg(state, self.guild_id)
        assert cfg is not None
        cfg["templates"]["grace_dm"] = str(self.grace_dm.value or DEFAULT_TEMPLATES["grace_dm"]).strip() or DEFAULT_TEMPLATES["grace_dm"]
        try:
            cfg["grace_days"] = max(1, int(str(self.grace_days.value or cfg.get("grace_days", 3)).strip()))
        except Exception:
            pass
        cfg["grace_role_ids"] = _coerce_role_ids(str(self.grace_roles.value or ""))
        save_state(state)
        await interaction.response.send_message("Welcome grace settings saved.", ephemeral=True)


@dataclass
class GraceRecord:
    user_id: int
    user_name: str
    saved_role_ids: List[int]
    left_at: str
    expires_at: str
    invite_url: str
    thread_id: Optional[int] = None
    message_id: Optional[int] = None
    status: str = "ACTIVE"
    preview: bool = False


class GraceCaseView(ui.View):
    def __init__(self, manager: "WelcomeManager", *, preview: bool = False):
        super().__init__(timeout=None)
        self.manager = manager
        self.preview = preview

    @ui.button(label="Expire Now", style=discord.ButtonStyle.danger, custom_id="welcome:grace:expire")
    async def expire_now(self, interaction: discord.Interaction, button: ui.Button):
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await self.manager.handle_expire_button(interaction, preview=self.preview)

    @ui.button(label="Send Recovery Copy", style=discord.ButtonStyle.primary, custom_id="welcome:grace:copy")
    async def send_recovery_copy(self, interaction: discord.Interaction, button: ui.Button):
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return
        await self.manager.handle_send_recovery_copy(interaction, preview=self.preview)


class WelcomeManager:
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._invite_cache: Dict[int, Dict[str, Tuple[int, Optional[int]]]] = {}
        self.sweep_grace_cases.start()

    def cog_unload(self):
        self.sweep_grace_cases.cancel()

    def state(self) -> Dict[str, Any]:
        return load_state()

    def save(self, state: Dict[str, Any]) -> None:
        save_state(state)

    def cfg(self, guild_id: int) -> Dict[str, Any]:
        state = self.state()
        cfg = _guild_cfg(state, guild_id)
        assert cfg is not None
        save_state(state)
        return cfg

    def is_enabled(self, guild_id: int) -> bool:
        cfg = _guild_cfg(self.state(), guild_id, create=False) or {}
        return bool(cfg.get("enabled"))

    async def dlog(self, event: str, guild: Optional[discord.Guild], **extra: Any) -> None:
        bits = [event]
        if guild is not None:
            bits.append(f"guild={guild.name}")
        for k, v in extra.items():
            if v is None or v == "":
                continue
            bits.append(f"{k}={v}")
        log.info(" ".join(bits))

    async def _snapshot_invites(self, guild: discord.Guild) -> None:
        try:
            invites = await guild.invites()
        except Exception:
            return
        snap: Dict[str, Tuple[int, Optional[int]]] = {}
        for inv in invites:
            try:
                snap[str(inv.code)] = (int(inv.uses or 0), int(inv.inviter.id) if inv.inviter else None)
            except Exception:
                continue
        self._invite_cache[int(guild.id)] = snap

    async def _resolve_inviter_for_join(self, guild: discord.Guild) -> Optional[discord.User]:
        before = self._invite_cache.get(int(guild.id), {})
        try:
            invites = await guild.invites()
        except Exception:
            return None
        after: Dict[str, Tuple[int, Optional[int]]] = {}
        inviter_id: Optional[int] = None
        for inv in invites:
            code = str(inv.code)
            uses = int(inv.uses or 0)
            iid = int(inv.inviter.id) if inv.inviter else None
            after[code] = (uses, iid)
            prev_uses, _prev_inviter = before.get(code, (0, iid))
            if uses > prev_uses and inviter_id is None:
                inviter_id = iid
        self._invite_cache[int(guild.id)] = after
        if not inviter_id:
            return None
        user = self.bot.get_user(inviter_id)
        if user is not None:
            return user
        try:
            return await self.bot.fetch_user(inviter_id)
        except Exception:
            return None

    def _resolve_autorole_id(self, guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[int]:
        rid = cfg.get("autorole_id")
        if rid:
            try:
                return int(rid)
            except Exception:
                pass
        fallback = _read_unverified_role_id(guild.id)
        return int(fallback) if fallback else None

    def _autodetect_channel(self, guild: discord.Guild, keywords: List[str]) -> Optional[discord.TextChannel]:
        candidates: List[discord.TextChannel] = []
        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if not perms.send_messages:
                continue
            name = ch.name.lower()
            if any(k in name for k in keywords):
                candidates.append(ch)
        if candidates:
            candidates.sort(key=lambda c: (c.position, c.id))
            return candidates[0]
        return None

    def _ensure_channels(self, guild: discord.Guild, cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
        changed = False
        if not cfg.get("join_channel_id"):
            ch = self._autodetect_channel(guild, ["welcome", "join"])
            if ch is not None:
                cfg["join_channel_id"] = ch.id
                changed = True
        if not cfg.get("leave_channel_id"):
            ch = self._autodetect_channel(guild, ["goodbye", "leave"])
            if ch is not None:
                cfg["leave_channel_id"] = ch.id
                changed = True
        if changed:
            self.save(state)

    async def _resolve_thread_by_id(self, guild: discord.Guild, thread_id: Optional[int]) -> Optional[discord.Thread]:
        if not thread_id:
            return None
        try:
            tid = int(thread_id)
        except Exception:
            return None
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

    def _invite_channel(self, guild: discord.Guild, cfg: Dict[str, Any]) -> Optional[discord.TextChannel]:
        join_id = cfg.get("join_channel_id")
        if join_id:
            ch = guild.get_channel(int(join_id))
            if isinstance(ch, discord.TextChannel) and ch.permissions_for(guild.me).create_instant_invite:
                return ch
        if isinstance(guild.system_channel, discord.TextChannel) and guild.system_channel.permissions_for(guild.me).create_instant_invite:
            return guild.system_channel
        for ch in guild.text_channels:
            perms = ch.permissions_for(guild.me)
            if perms.create_instant_invite:
                return ch
        return None

    async def _make_grace_invite(self, guild: discord.Guild, cfg: Dict[str, Any], seconds: Optional[int] = None) -> Optional[str]:
        ch = self._invite_channel(guild, cfg)
        if ch is None:
            return None
        max_age = int(seconds if seconds is not None else max(1, int(cfg.get("grace_days", 3))) * 86400)
        try:
            invite = await ch.create_invite(max_age=max_age, max_uses=1, unique=True, reason="Grace return invite")
            return invite.url
        except Exception:
            return None

    def _avatar_url_for_member(self, member: Optional[Union[discord.Member, discord.User]]) -> Optional[str]:
        if member is None:
            return None
        try:
            return member.display_avatar.url
        except Exception:
            return None

    async def _fetch_user(self, user_id: int) -> Optional[discord.User]:
        user = self.bot.get_user(int(user_id))
        if user is not None:
            return user
        try:
            return await self.bot.fetch_user(int(user_id))
        except Exception:
            return None

    async def _send_grace_dm(self, guild: discord.Guild, cfg: Dict[str, Any], rec: Dict[str, Any]) -> tuple[bool, str]:
        user_obj = await self._fetch_user(int(rec.get("user_id") or 0))
        if user_obj is None:
            return False, "User fetch failed"
        text = _render_template(
            cfg.get("templates", {}).get("grace_dm") or DEFAULT_TEMPLATES["grace_dm"],
            user_name=str(rec.get("user_name") or f"User {rec.get('user_id')}"),
            guild_name=guild.name,
            grace_days=max(1, int(cfg.get("grace_days", 3))),
            invite_link=rec.get("invite_url") or "Invite unavailable.",
            expires_at=rec.get("expires_at"),
        )
        try:
            dm = user_obj.dm_channel or await user_obj.create_dm()
            await dm.send(text, allowed_mentions=discord.AllowedMentions.none())
            return True, ""
        except discord.Forbidden as e:
            return False, f"DM failed: {e.__class__.__name__}"
        except discord.HTTPException as e:
            return False, f"DM failed: {e.__class__.__name__}"
        except Exception as e:
            return False, f"DM failed: {e.__class__.__name__}"

    async def _ensure_runtime(self, guild: discord.Guild, *, force_status_log: bool = False) -> Optional[Dict[str, discord.Thread]]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id)
        assert cfg is not None
        self._ensure_channels(guild, cfg, state)
        try:
            forum = await get_or_create_ops_forum(guild)
        except Exception:
            forum = None
        if not isinstance(forum, discord.ForumChannel):
            await self.dlog("grace threads unavailable", guild, reason="ops_forum_missing")
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
        status_tag = tags_by_name.get(ENABLED_TAG if cfg.get("enabled") else DISABLED_TAG)

        async def sync_starter(thread: discord.Thread, name: str) -> None:
            try:
                starter = await forum.fetch_message(thread.id)
            except Exception:
                starter = None
            content = f"Master Botter: {name} queue for grace cases."
            if starter is not None:
                try:
                    if (starter.content or "") != content:
                        await starter.edit(content=content)
                except Exception:
                    pass

        async def ensure_thread(name: str, key: str) -> Optional[discord.Thread]:
            thread = await self._resolve_thread_by_id(guild, cfg.get(key))
            if isinstance(thread, discord.Thread):
                if status_tag is not None:
                    try:
                        await thread.edit(applied_tags=[status_tag])
                    except Exception:
                        pass
                await sync_starter(thread, name)
                return thread
            existing_threads = list(getattr(forum, "threads", [])) + [
                t for t in guild.threads if getattr(t, "parent_id", None) == forum.id
            ]
            for th in existing_threads:
                if isinstance(th, discord.Thread) and th.name == name:
                    cfg[key] = th.id
                    self.save(state)
                    if status_tag is not None:
                        try:
                            await th.edit(applied_tags=[status_tag])
                        except Exception:
                            pass
                    await sync_starter(th, name)
                    return th
            kwargs = {"name": name, "content": f"Master Botter: {name} queue for grace cases."}
            if status_tag is not None:
                kwargs["applied_tags"] = [status_tag]
            created = await forum.create_thread(**kwargs)
            thread = created.thread if hasattr(created, "thread") else created[0] if isinstance(created, tuple) else created
            if isinstance(thread, discord.Thread):
                cfg[key] = thread.id
                self.save(state)
                await self.dlog("grace thread created", guild, thread=name)
                return thread
            return None

        active = await ensure_thread(THREAD_ACTIVE_NAME, "grace_active_thread_id")
        completed = await ensure_thread(THREAD_COMPLETED_NAME, "grace_completed_thread_id")
        if not isinstance(active, discord.Thread) or not isinstance(completed, discord.Thread):
            return None
        await self.dlog("grace threads ensured", guild, active=THREAD_ACTIVE_NAME, completed=THREAD_COMPLETED_NAME, state=ENABLED_TAG if cfg.get("enabled") else DISABLED_TAG)
        if force_status_log:
            try:
                await post_status(guild, f"welcome: grace threads ensured -> {THREAD_ACTIVE_NAME} / {THREAD_COMPLETED_NAME}")
            except Exception:
                pass
        return {"ACTIVE": active, "COMPLETED": completed}

    async def _fetch_message(self, thread: discord.Thread, message_id: Optional[int]) -> Optional[discord.Message]:
        if not message_id:
            return None
        try:
            return await thread.fetch_message(int(message_id))
        except Exception:
            return None

    async def _build_case_embed(self, guild: discord.Guild, record: Dict[str, Any], *, outcome: Optional[str] = None, preview: bool = False, viewer: Optional[discord.abc.User] = None) -> discord.Embed:
        active = str(record.get("status") or "ACTIVE").upper() == "ACTIVE"
        title = THREAD_ACTIVE_NAME if active else THREAD_COMPLETED_NAME
        color = discord.Color.orange() if active else discord.Color.green()
        if preview:
            title = f"[PREVIEW] {title}"
            color = discord.Color.blurple() if not active else discord.Color.orange()
        embed = discord.Embed(title=title, color=color)
        user_id = int(record.get("user_id") or 0)
        user_name = str(record.get("user_name") or f"User {user_id}")
        mention = f"<@{user_id}>" if user_id else user_name
        embed.add_field(name="User", value=mention, inline=True)
        embed.add_field(name="User ID", value=str(user_id or "Unknown"), inline=True)
        embed.add_field(name="Saved Roles", value=str(len(record.get("saved_role_ids") or [])), inline=True)
        embed.add_field(name="Expires", value=str(record.get("expires_at") or "n/a"), inline=False)
        dm_status = str(record.get("dm_status") or "Unknown")
        if dm_status:
            embed.add_field(name="DM Status", value=dm_status, inline=False)
        avatar_url = str(record.get("avatar_url") or "").strip()
        if avatar_url:
            try:
                embed.set_thumbnail(url=avatar_url)
            except Exception:
                pass
        if viewer is not None:
            embed.add_field(name="Admin Viewer", value=getattr(viewer, "mention", None) or getattr(viewer, "name", str(viewer)), inline=False)
        if outcome:
            embed.add_field(name="Outcome", value=outcome, inline=False)
        if preview:
            embed.set_footer(text="Preview only — no real records, DMs, or invites are created.")
        else:
            embed.set_footer(text="Grace return is silent. Public leave is delayed until expiry or force close.")
        return embed

    async def _update_case_message(self, guild: discord.Guild, record: Dict[str, Any], *, outcome: Optional[str] = None, preview: bool = False, viewer: Optional[discord.abc.User] = None) -> None:
        threads = await self._ensure_runtime(guild)
        if not threads:
            return
        active_thread = threads["ACTIVE"]
        completed_thread = threads["COMPLETED"]
        active = str(record.get("status") or "ACTIVE").upper() == "ACTIVE"
        target_thread = active_thread if active else completed_thread
        msg_key = "message_id" if active else "completed_message_id"
        msg = await self._fetch_message(target_thread, record.get(msg_key))
        embed = await self._build_case_embed(guild, record, outcome=outcome, preview=preview, viewer=viewer)
        view = None if not active else GraceCaseView(self, preview=preview)
        if msg is not None:
            try:
                await msg.edit(embed=embed, view=view)
                record[msg_key] = msg.id
            except Exception:
                pass
        else:
            try:
                sent = await target_thread.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
                record[msg_key] = sent.id
            except Exception:
                return

        if not active and record.get("message_id"):
            old_msg = await self._fetch_message(active_thread, record.get("message_id"))
            if old_msg is not None:
                try:
                    await old_msg.delete()
                except Exception:
                    try:
                        await old_msg.edit(view=None)
                    except Exception:
                        pass
            record["message_id"] = None

    def _build_join_embed(self, member: discord.Member, cfg: Dict[str, Any], inviter: Optional[discord.abc.User] = None) -> discord.Embed:
        phrase = _pick_phrase(list(cfg.get("join_phrases") or []), DEFAULT_JOIN_PHRASES)
        embed = discord.Embed(title="Welcome", description=f"{member.mention} has {phrase}", color=discord.Color.green())
        avatar_url = self._avatar_url_for_member(member)
        if avatar_url:
            embed.set_thumbnail(url=avatar_url)
        inviter_value = getattr(inviter, "mention", None) or "Unknown"
        embed.add_field(name="Invited By", value=inviter_value, inline=False)
        return embed

    def _build_leave_embed(self, member: Union[discord.Member, discord.User, str, Dict[str, Any]], cfg: Dict[str, Any]) -> discord.Embed:
        phrase = _pick_phrase(list(cfg.get("leave_phrases") or []), DEFAULT_LEAVE_PHRASES)
        who = "User"
        avatar_url = None
        if isinstance(member, dict):
            user_id = int(member.get("user_id") or 0)
            user_name = str(member.get("user_name") or f"User {user_id}").strip()
            who = f"<@{user_id}>" if user_id else (user_name or "User")
            avatar_url = str(member.get("avatar_url") or "").strip() or None
        elif isinstance(member, str):
            who = member.strip() or "User"
        else:
            who = getattr(member, "mention", None) or getattr(member, "display_name", None) or getattr(member, "name", "User")
            avatar_url = self._avatar_url_for_member(member)
        embed = discord.Embed(title="Farewell", description=f"{who} has {phrase}", color=discord.Color.orange())
        if avatar_url:
            try:
                embed.set_thumbnail(url=avatar_url)
            except Exception:
                pass
        return embed



    def _build_moderation_leave_embed(self, notice: Dict[str, Any]) -> discord.Embed:
        action = str(notice.get("action") or "ban").strip().lower()
        title = "Banned" if action == "ban" else ("Kicked" if action == "kick" else "Farewell")
        user_id = int(notice.get("user_id") or 0)
        user_name = str(notice.get("user_name") or f"User {user_id}").strip()
        who = f"<@{user_id}>" if user_id else (user_name or "User")
        verb = "was banned from the server" if action == "ban" else ("was kicked from the server" if action == "kick" else "has left the server")
        color = discord.Color.red() if action == "ban" else (discord.Color.orange() if action == "kick" else discord.Color.orange())
        embed = discord.Embed(title=title, description=f"{who} {verb}", color=color)
        avatar_url = str(notice.get("avatar_url") or "").strip() or None
        if avatar_url:
            try:
                embed.set_thumbnail(url=avatar_url)
            except Exception:
                pass
        reason = str(notice.get("reason") or "").strip()
        moderator_name = str(notice.get("moderator_name") or "").strip()
        moderator_id = int(notice.get("moderator_id") or 0) if str(notice.get("moderator_id") or "").strip() else 0
        case_id = str(notice.get("case_id") or "").strip()
        strike_count = str(notice.get("strike_count") or "").strip()
        if reason:
            embed.add_field(name="Reason", value=reason, inline=False)
        if moderator_name or moderator_id:
            embed.add_field(name="Moderator", value=(f"<@{moderator_id}>" if moderator_id else moderator_name), inline=False)
        if strike_count:
            embed.add_field(name="Strike Level", value=strike_count, inline=True)
        if case_id:
            embed.add_field(name="Case ID", value=case_id, inline=True)
        return embed

    async def _post_moderation_leave(self, guild: discord.Guild, notice: Dict[str, Any], cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
        self._ensure_channels(guild, cfg, state)
        cid = cfg.get("leave_channel_id")
        ch = guild.get_channel(int(cid)) if cid else None
        if not isinstance(ch, discord.TextChannel):
            return
        try:
            await ch.send(embed=self._build_moderation_leave_embed(notice), allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            pass

    async def _post_join(self, member: discord.Member, cfg: Dict[str, Any], state: Dict[str, Any], inviter: Optional[discord.abc.User] = None) -> None:
        self._ensure_channels(member.guild, cfg, state)
        cid = cfg.get("join_channel_id")
        ch = member.guild.get_channel(int(cid)) if cid else None
        if not isinstance(ch, discord.TextChannel):
            return
        try:
            await ch.send(embed=self._build_join_embed(member, cfg, inviter), allowed_mentions=discord.AllowedMentions(users=[member]))
        except Exception:
            pass

    async def _post_leave(self, guild: discord.Guild, member_or_user: Union[discord.Member, discord.User, str, Dict[str, Any]], cfg: Dict[str, Any], state: Dict[str, Any]) -> None:
        self._ensure_channels(guild, cfg, state)
        cid = cfg.get("leave_channel_id")
        ch = guild.get_channel(int(cid)) if cid else None
        if not isinstance(ch, discord.TextChannel):
            return
        try:
            await ch.send(embed=self._build_leave_embed(member_or_user, cfg), allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            pass

    async def _restore_roles(self, member: discord.Member, role_ids: List[int]) -> int:
        restored = 0
        for rid in role_ids:
            role = member.guild.get_role(int(rid))
            if role is None:
                continue
            if role.is_default():
                continue
            if role >= member.guild.me.top_role:
                continue
            if role in member.roles:
                continue
            try:
                await member.add_roles(role, reason="Grace return restore")
                restored += 1
            except Exception:
                continue
        return restored

    async def on_member_join(self, member: discord.Member) -> None:
        state = self.state()
        cfg = _guild_cfg(state, member.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        records = cfg.setdefault("grace_records", {})
        rec = records.get(str(member.id))
        now = _utcnow()
        if isinstance(rec, dict):
            expires = _fromiso(rec.get("expires_at"))
            if expires and now <= expires:
                restored = await self._restore_roles(member, list(rec.get("saved_role_ids") or []))
                rec["status"] = "COMPLETED"
                await self._update_case_message(member.guild, rec, outcome="Returned", preview=False)
                records.pop(str(member.id), None)
                self.save(state)
                await self.dlog("grace return restored", member.guild, user=member.display_name, restored=restored)
                return
            records.pop(str(member.id), None)
            self.save(state)

        inviter = await self._resolve_inviter_for_join(member.guild)
        rid = self._resolve_autorole_id(member.guild, cfg)
        if rid:
            role = member.guild.get_role(int(rid))
            if role is not None and role < member.guild.me.top_role and role not in member.roles:
                try:
                    await member.add_roles(role, reason="Welcome autorole")
                except Exception:
                    pass
        await self._post_join(member, cfg, state, inviter)
        await self.dlog("welcome join posted", member.guild, user=member.display_name, autorole=rid or "none")

    async def on_member_remove(self, member: discord.Member) -> None:
        state = self.state()
        cfg = _guild_cfg(state, member.guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return
        mod_state = _load_moderation_state()
        mod_cfg = mod_state.setdefault("guilds", {}).get(str(int(member.guild.id))) or {}
        pending_notices = mod_cfg.setdefault("pending_ban_leave_notices", {}) if isinstance(mod_cfg, dict) else {}
        notice = pending_notices.pop(str(int(member.id)), None) if isinstance(pending_notices, dict) else None
        if notice:
            if isinstance(mod_cfg, dict):
                mod_state.setdefault("guilds", {})[str(int(member.guild.id))] = mod_cfg
                _save_moderation_state(mod_state)
            notice.setdefault("user_id", int(member.id))
            notice.setdefault("user_name", member.display_name)
            notice.setdefault("avatar_url", self._avatar_url_for_member(member) or "")
            await self._post_moderation_leave(member.guild, notice, cfg, state)
            await self.dlog("moderation leave announced", member.guild, user=member.display_name, action=str(notice.get("action") or "ban"), case=notice.get("case_id") or "")
            return
        records = cfg.setdefault("grace_records", {})
        if _has_any_role(member, [int(x) for x in cfg.get("grace_role_ids") or []]):
            if str(member.id) in records and str(records[str(member.id)].get("status")).upper() == "ACTIVE":
                await self.dlog("grace leave skipped", member.guild, user=member.display_name, reason="active_record_exists")
                return
            now = _utcnow()
            expires = now + timedelta(days=max(1, int(cfg.get("grace_days", 3))))
            invite = await self._make_grace_invite(member.guild, cfg)
            saved_roles = [r.id for r in member.roles if not r.is_default() and r < member.guild.me.top_role]
            threads = await self._ensure_runtime(member.guild, force_status_log=True)
            rec = {
                "user_id": member.id,
                "user_name": member.display_name,
                "saved_role_ids": saved_roles,
                "left_at": _iso(now),
                "expires_at": _iso(expires),
                "invite_url": invite or "Invite unavailable.",
                "message_id": None,
                "completed_message_id": None,
                "status": "ACTIVE",
                "avatar_url": self._avatar_url_for_member(member) or "",
                "dm_status": "Pending",
            }
            sent, err = await self._send_grace_dm(member.guild, cfg, rec)
            rec["dm_status"] = "Sent" if sent else f"Failed ({err})"
            await self._update_case_message(member.guild, rec, outcome=None, preview=False)
            records[str(member.id)] = rec
            self.save(state)
            await self.dlog("grace leave delayed", member.guild, user=member.display_name, roles=len(saved_roles), dm_sent="yes" if sent else "no", dm_error=err or None)
            return

        await self._post_leave(member.guild, member, cfg, state)
        await self.dlog("normal leave announced", member.guild, user=member.display_name)

    async def handle_expire_button(self, interaction: discord.Interaction, *, preview: bool) -> None:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.channel, discord.Thread):
            await interaction.response.send_message("Thread only.", ephemeral=True)
            return
        threads = await self._ensure_runtime(guild)
        if not threads:
            await interaction.response.send_message("Grace threads unavailable.", ephemeral=True)
            return
        if preview:
            record = {
                "user_id": interaction.user.id,
                "user_name": getattr(interaction.user, "display_name", interaction.user.name),
                "saved_role_ids": [1, 2, 3],
                "expires_at": _iso(_utcnow()),
                "invite_url": "https://discord.gg/preview",
                "message_id": interaction.message.id if interaction.message else None,
                "completed_message_id": None,
                "status": "COMPLETED",
                "avatar_url": self._avatar_url_for_member(interaction.user) or "",
                "dm_status": "Preview",
            }
            await self._update_case_message(guild, record, outcome="Forced Closed", preview=True, viewer=interaction.user)
            await interaction.response.send_message("Preview case expired.", ephemeral=True)
            return

        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if not cfg:
            await interaction.response.send_message("No config.", ephemeral=True)
            return
        target = None
        for uid, rec in (cfg.get("grace_records") or {}).items():
            if int(rec.get("message_id") or 0) == int(getattr(interaction.message, "id", 0)):
                target = (uid, rec)
                break
        if not target:
            await interaction.response.send_message("Case not found.", ephemeral=True)
            return
        uid, rec = target
        await self._post_leave(guild, rec, cfg, state)
        rec["status"] = "COMPLETED"
        await self._update_case_message(guild, rec, outcome="Forced Closed", preview=False)
        (cfg.get("grace_records") or {}).pop(uid, None)
        self.save(state)
        await interaction.response.send_message("Grace case force-closed.", ephemeral=True)

    async def handle_send_recovery_copy(self, interaction: discord.Interaction, *, preview: bool) -> None:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.channel, discord.Thread):
            await interaction.response.send_message("Thread only.", ephemeral=True)
            return
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if not cfg:
            await interaction.response.send_message("No config.", ephemeral=True)
            return
        if preview:
            preview_invite = "https://discord.gg/preview"
            text = _render_template(
                cfg.get("templates", {}).get("grace_dm") or DEFAULT_TEMPLATES["grace_dm"],
                user_name=getattr(interaction.user, "display_name", interaction.user.name),
                guild_name=guild.name,
                grace_days=max(1, int(cfg.get("grace_days", 14))),
                invite_link=preview_invite,
                expires_at=_iso(_utcnow() + timedelta(days=max(1, int(cfg.get("grace_days", 14))))),
            )
            await interaction.response.send_message(text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
            return

        target = None
        for uid, rec in (cfg.get("grace_records") or {}).items():
            if int(rec.get("message_id") or 0) == int(getattr(interaction.message, "id", 0)):
                target = (uid, rec)
                break
        if not target:
            await interaction.response.send_message("Case not found.", ephemeral=True)
            return
        uid, rec = target
        expires = _fromiso(rec.get("expires_at")) or (_utcnow() + timedelta(days=max(1, int(cfg.get("grace_days", 14)))))
        stored_invite = str(rec.get("invite_url") or "").strip()
        if not stored_invite or stored_invite == "Invite unavailable.":
            remaining = max(60, int((expires - _utcnow()).total_seconds()))
            invite = await self._make_grace_invite(guild, cfg, seconds=remaining)
            rec["invite_url"] = invite or stored_invite or "Invite unavailable."
        text = _render_template(
            cfg.get("templates", {}).get("grace_dm") or DEFAULT_TEMPLATES["grace_dm"],
            user_name=str(rec.get("user_name") or f"User {uid}"),
            guild_name=guild.name,
            grace_days=max(1, int(cfg.get("grace_days", 14))),
            invite_link=rec["invite_url"],
            expires_at=rec.get("expires_at"),
        )
        await interaction.response.send_message(text, ephemeral=True, allowed_mentions=discord.AllowedMentions.none())
        self.save(state)
        await self._update_case_message(guild, rec, outcome=None, preview=False)

    async def create_preview(self, interaction: discord.Interaction) -> None:
        guild = interaction.guild
        if guild is None:
            return
        threads = await self._ensure_runtime(guild, force_status_log=True)
        if not threads:
            await interaction.response.send_message("Could not ensure grace threads.", ephemeral=True)
            return
        active_thread = threads["ACTIVE"]
        state = self.state()
        cfg = _guild_cfg(state, guild.id)
        assert cfg is not None
        record = {
            "user_id": interaction.user.id,
            "user_name": getattr(interaction.user, "display_name", interaction.user.name),
            "saved_role_ids": [111, 222, 333],
            "left_at": _iso(_utcnow()),
            "expires_at": _iso(_utcnow() + timedelta(days=max(1, int(cfg.get("grace_days", 3))))),
            "invite_url": "https://discord.gg/preview",
            "message_id": None,
            "completed_message_id": None,
            "status": "ACTIVE",
            "avatar_url": self._avatar_url_for_member(interaction.user) or "",
            "dm_status": "Preview",
        }
        sent = await active_thread.send(embed=await self._build_case_embed(guild, record, preview=True, viewer=interaction.user), view=GraceCaseView(self, preview=True), allowed_mentions=discord.AllowedMentions.none())
        record["message_id"] = sent.id
        await interaction.response.send_message(f"Preview message ready in {active_thread.mention}", ephemeral=True)

    async def rescan_guild(self, guild: discord.Guild) -> Dict[str, int]:
        state = self.state()
        cfg = _guild_cfg(state, guild.id, create=False)
        if not cfg or not cfg.get("enabled"):
            return {"scanned": 0, "expired": 0}
        await self._ensure_runtime(guild)
        scanned = 0
        expired = 0
        records = cfg.setdefault("grace_records", {})
        now = _utcnow()
        for uid, rec in list(records.items()):
            scanned += 1
            member = guild.get_member(int(uid))
            if member is not None:
                continue
            expires = _fromiso(rec.get("expires_at"))
            if expires is None or now < expires:
                continue
            await self._post_leave(guild, rec, cfg, state)
            rec["status"] = "COMPLETED"
            await self._update_case_message(guild, rec, outcome="Expired", preview=False)
            records.pop(uid, None)
            expired += 1
        cfg["last_sweep_at"] = _iso(now)
        self.save(state)
        return {"scanned": scanned, "expired": expired}

    @tasks.loop(hours=12)
    async def sweep_grace_cases(self):
        await self.bot.wait_until_ready()
        state = self.state()
        now = _utcnow()
        for guild in self.bot.guilds:
            cfg = _guild_cfg(state, guild.id, create=False)
            if not cfg or not cfg.get("enabled"):
                continue
            last = _fromiso(cfg.get("last_sweep_at"))
            if last and (now - last) < timedelta(hours=11, minutes=30):
                continue
            await self.rescan_guild(guild)

    @sweep_grace_cases.before_loop
    async def _before_sweep(self):
        await self.bot.wait_until_ready()
        # avoid immediate boot collision / Sunday reboot spam
        await discord.utils.sleep_until(_utcnow() + timedelta(minutes=15))


class WelcomeCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.mgr = WelcomeManager(bot)
        bot.add_view(GraceCaseView(self.mgr, preview=False))
        bot.add_view(GraceCaseView(self.mgr, preview=True))

    def cog_unload(self):
        self.mgr.cog_unload()

    @commands.Cog.listener()
    async def on_ready(self):
        for guild in self.bot.guilds:
            try:
                await self.mgr._ensure_runtime(guild)
                await self.mgr._snapshot_invites(guild)
            except Exception:
                log.exception("welcome: startup ensure failed")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.mgr.on_member_join(member)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await self.mgr.on_member_remove(member)

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        guild = invite.guild
        if guild is not None:
            await self.mgr._snapshot_invites(guild)

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        guild = invite.guild
        if guild is not None:
            await self.mgr._snapshot_invites(guild)


async def setup(bot: commands.Bot, registry: SettingsRegistry) -> None:
    existing = bot.get_cog("WelcomeCog")
    if existing is not None:
        await bot.remove_cog("WelcomeCog")
    await bot.add_cog(WelcomeCog(bot))
    manager = bot.get_cog("WelcomeCog").mgr  # type: ignore[attr-defined]

    def status() -> str:
        state = load_state()
        enabled = sum(1 for cfg in state.get("guilds", {}).values() if cfg.get("enabled"))
        return f"✅ Enabled in {enabled} guild(s)" if enabled else "❌ Disabled"

    async def handler(interaction: discord.Interaction, ctx: Dict[str, Any]) -> Optional[dict]:
        if interaction.guild is None:
            return {"op": "respond", "payload": {"content": "Server only.", "ephemeral": True}}
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
            return {"op": "respond", "payload": {"content": "Admins only.", "ephemeral": True}}
        action = str(ctx.get("action") or "toggle").strip().lower()
        state = load_state()
        cfg = _guild_cfg(state, interaction.guild.id)
        assert cfg is not None
        manager._ensure_channels(interaction.guild, cfg, state)
        if action == "toggle":
            cfg["enabled"] = not bool(cfg.get("enabled"))
            save_state(state)
            forum = await manager._ensure_runtime(interaction.guild, force_status_log=bool(cfg.get("enabled")))
            if cfg.get("enabled") and forum is None:
                return {"op": "respond", "payload": {"content": "Welcome Manager enabled, but grace threads could not be ensured.", "ephemeral": True}}
            return {"op": "toggle", "is_on": bool(cfg.get("enabled"))}
        if action == "configure_embeds":
            return {"op": "modal", "modal": WelcomeEmbedModal(interaction.guild.id)}
        if action == "configure_grace":
            return {"op": "modal", "modal": WelcomeGraceModal(interaction.guild.id)}
        if action == "preview":
            await manager.create_preview(interaction)
            return None
        if action in {"rescan", "baseline"}:
            await manager._ensure_runtime(interaction.guild)
            result = await manager.rescan_guild(interaction.guild)
            return {"op": "respond", "payload": {"content": f"Grace rescan complete. scanned={result['scanned']} expired={result['expired']}", "ephemeral": True}}
        return None

    registry.register(
        SettingFeature(
            feature_id="welcome",
            label="Welcome Manager",
            description="Welcome/farewell embeds, delayed grace departures, silent grace returns, and grace cases inside Grace - ACTIVE / Grace - COMPLETED.",
            category=CATEGORY,
            category_description=CATEGORY_DESCRIPTION,
            handler=handler,
            status=status,
            actions=[
                FeatureAction("configure_embeds", "Configure Embeds", "Set editable quippy phrase lists for welcome/farewell embeds.", row=1),
                FeatureAction("configure_grace", "Configure Grace", "Set grace DM template, grace days, and grace role IDs.", row=1),
                FeatureAction("preview", "Preview Grace Case", "Post a working preview case into Grace - ACTIVE with live preview buttons.", row=2),
                FeatureAction("rescan", "Rescan Grace Cases", "Sweep grace records now, finalize expired departures, and keep restart-safe state clean.", row=2),
            ],
        )
    )


async def teardown(bot: commands.Bot, registry: SettingsRegistry) -> None:
    cog = bot.get_cog("WelcomeCog")
    if cog is not None:
        await bot.remove_cog("WelcomeCog")
    registry.unregister("welcome")
