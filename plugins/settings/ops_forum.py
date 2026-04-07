import asyncio
import re
import json
import logging
import os
from pathlib import Path
from typing import Optional

import discord

from core.log_formatter import build_log, guild_value, thread_value

log = logging.getLogger(__name__)
debug_log = logging.getLogger("debug.ops")

ANSI_RESET = "[0m"
ANSI_LIGHT_BLUE = "[94m"
ANSI_RED = "[91m"

STATE_PATH = Path(__file__).resolve().parent / "_data" / "ops_forum_state.json"
_OPS_BOOT_LOCK = asyncio.Lock()
_OPS_BOOTSTRAPPED: dict[int, bool] = {}


def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        log.exception("ops: failed to save state")


def _parse_env_int(name: str) -> Optional[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def debug_enabled() -> bool:
    return os.getenv("DEBUG_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}


def _ops_guild_id() -> Optional[int]:
    return _parse_env_int("OPS_GUILD_ID") or _parse_env_int("OPS_DEBUG_GUILD_ID") or _parse_env_int("DEBUG_GUILD_ID") or _parse_env_int("WORKSHOP_GUILD_ID")


def _forum_name() -> str:
    return os.getenv("OPS_FORUM_NAME", "master-botter").strip() or "master-botter"


def _thread_name(which: str, default: str) -> str:
    return os.getenv(which, default).strip() or default


def _state_bucket(state: dict, guild_id: int) -> dict:
    guilds = state.setdefault("guilds", {})
    return guilds.setdefault(str(guild_id), {})


async def _resolve_ops_guild(bot: discord.Client) -> Optional[discord.Guild]:
    gid = _ops_guild_id()
    if gid:
        guild = bot.get_guild(gid)
        if guild:
            return guild
        try:
            guild = await bot.fetch_guild(gid)
            return bot.get_guild(gid) or guild
        except Exception:
            pass
    if len(bot.guilds) == 1:
        return bot.guilds[0]
    return None


async def _find_forum(guild: discord.Guild) -> Optional[discord.ForumChannel]:
    state = _load_state()
    bucket = _state_bucket(state, guild.id)
    fid = bucket.get("forum_id")
    if fid:
        ch = guild.get_channel(int(fid))
        if isinstance(ch, discord.ForumChannel):
            return ch
    name = _forum_name().lower()
    for ch in guild.channels:
        if isinstance(ch, discord.ForumChannel) and ch.name.lower() == name:
            bucket["forum_id"] = ch.id
            _save_state(state)
            return ch
    return None


async def get_or_create_ops_forum(guild: discord.Guild):
    forum = await _find_forum(guild)
    if forum:
        return forum
    try:
        create_forum = getattr(guild, "create_forum", None)
        if callable(create_forum):
            forum = await create_forum(_forum_name())
        else:
            raise AttributeError("Guild has no create_forum method")
        state = _load_state()
        bucket = _state_bucket(state, guild.id)
        bucket["forum_id"] = forum.id
        _save_state(state)
        log.info(build_log("ops forum created", guild=guild, channel=forum, forum_id=forum.id, name=forum.name))
        return forum
    except Exception:
        log.exception("ops: failed to create/find forum")
        return None


async def _fetch_starter_message(thread: discord.Thread) -> Optional[discord.Message]:
    try:
        return await thread.parent.fetch_message(thread.id) if thread.parent else None
    except Exception:
        try:
            return await thread.fetch_message(thread.id)
        except Exception:
            return None


async def _apply_enabled_tag(forum: discord.ForumChannel, thread: discord.Thread) -> None:
    try:
        tag = None
        for t in forum.available_tags:
            if t.name.lower() == "enabled":
                tag = t
                break
        if tag is None:
            try:
                tag = await forum.create_tag(name="ENABLED")
            except Exception:
                tag = None
        if tag is not None:
            applied = list(thread.applied_tags)
            if not any(x.id == tag.id for x in applied):
                applied.append(tag)
                await thread.edit(applied_tags=applied)
    except Exception:
        pass


async def _ensure_named_thread(forum: discord.ForumChannel, name: str) -> Optional[discord.Thread]:
    lname = name.lower()
    for t in forum.threads:
        if t.name.lower() == lname:
            await _apply_enabled_tag(forum, t)
            return t
    try:
        async for t in forum.archived_threads(limit=100):
            if t.name.lower() == lname:
                try:
                    await t.edit(archived=False)
                except Exception:
                    pass
                await _apply_enabled_tag(forum, t)
                return t
    except Exception:
        pass
    try:
        result = await forum.create_thread(name=name, content=f"{name} initialized")
        thread = result.thread
        if thread:
            await _apply_enabled_tag(forum, thread)
        return thread
    except Exception:
        log.exception("ops: failed to ensure thread %s", name)
        return None


async def _create_named_thread(
    forum: discord.ForumChannel,
    name: str,
    *,
    content: Optional[str] = None,
    embed=None,
    embeds=None,
    view=None,
) -> Optional[discord.Thread]:
    kwargs = {"name": name}
    if content is not None:
        kwargs["content"] = content
    if embed is not None:
        kwargs["embed"] = embed
    elif embeds is not None:
        kwargs["embeds"] = embeds
    if view is not None:
        kwargs["view"] = view
    try:
        result = await forum.create_thread(**kwargs)
        thread = result.thread
        if thread:
            await _apply_enabled_tag(forum, thread)
        return thread
    except Exception:
        log.exception("ops: failed to create thread %s", name)
        return None


async def _delete_named_threads(forum: discord.ForumChannel, names: list[str]) -> None:
    wanted = {n.lower() for n in names}
    seen: set[int] = set()

    for t in list(forum.threads):
        if t.name.lower() in wanted and t.id not in seen:
            seen.add(t.id)
            try:
                await t.delete()
            except Exception:
                pass

    try:
        async for t in forum.archived_threads(limit=100):
            if t.name.lower() in wanted and t.id not in seen:
                seen.add(t.id)
                try:
                    await t.delete()
                except Exception:
                    pass
    except Exception:
        pass


async def ensure_ops_threads(target) -> dict:
    bot = target
    guild = await _resolve_ops_guild(bot)
    if guild is None:
        log.warning("ops: no guild resolved for ops threads")
        return {}

    async with _OPS_BOOT_LOCK:
        state = _load_state()
        bucket = _state_bucket(state, guild.id)

        if _OPS_BOOTSTRAPPED.get(guild.id):
            return {
                "guild_id": guild.id,
                "forum_id": bucket.get("forum_id"),
                "startup_thread_id": bucket.get("startup_thread_id"),
                "status_thread_id": bucket.get("status_thread_id"),
                "debug_thread_id": bucket.get("debug_thread_id"),
            }

        forum = await get_or_create_ops_forum(guild)
        if forum is None:
            return {}

        startup_name = _thread_name("OPS_THREAD_STARTUP", "SYSTEM - STARTUP")
        status_name = _thread_name("OPS_THREAD_STATUS", "SYSTEM - STATUS")
        debug_name = _thread_name("OPS_THREAD_DEBUG", "SYSTEM - DEBUG")

        await _delete_named_threads(forum, [startup_name, status_name, debug_name])

        status = await _ensure_named_thread(forum, status_name)
        debug = await _ensure_named_thread(forum, debug_name)

        bucket.update({
            "forum_id": forum.id,
            "startup_thread_id": None,
            "status_thread_id": status.id if status else None,
            "debug_thread_id": debug.id if debug else None,
        })
        _save_state(state)
        _OPS_BOOTSTRAPPED[guild.id] = True
        log.info(build_log("threads reset", guild=guild, startup=None, status=thread_value(status, guild=guild), debug=thread_value(debug, guild=guild)))
        return {
            "guild_id": guild.id,
            "forum_id": forum.id,
            "startup_thread_id": None,
            "status_thread_id": status.id if status else None,
            "debug_thread_id": debug.id if debug else None,
        }


async def _resolve_thread(bot: discord.Client, guild: Optional[discord.Guild], key: str, env_name: str, default_name: str) -> Optional[discord.Thread]:
    if guild is None:
        guild = await _resolve_ops_guild(bot)
    if guild is None:
        return None

    if not _OPS_BOOTSTRAPPED.get(guild.id):
        await ensure_ops_threads(bot)

    state = _load_state()
    bucket = _state_bucket(state, guild.id)
    tid = bucket.get(key)
    if tid:
        ch = guild.get_thread(int(tid)) or guild.get_channel(int(tid))
        if isinstance(ch, discord.Thread):
            return ch

    forum = await get_or_create_ops_forum(guild)
    if forum is None:
        return None
    thread = await _ensure_named_thread(forum, _thread_name(env_name, default_name))
    if thread:
        bucket[key] = thread.id
        _save_state(state)
    return thread


async def _create_startup_thread(bot: discord.Client, *, content: Optional[str] = None, embed=None, embeds=None, view=None) -> Optional[discord.Thread]:
    guild = await _resolve_ops_guild(bot)
    if guild is None:
        return None
    forum = await get_or_create_ops_forum(guild)
    if forum is None:
        return None

    state = _load_state()
    bucket = _state_bucket(state, guild.id)
    startup_name = _thread_name("OPS_THREAD_STARTUP", "SYSTEM - STARTUP")

    existing_id = bucket.get("startup_thread_id")
    if existing_id:
        existing = guild.get_thread(int(existing_id)) or guild.get_channel(int(existing_id))
        if isinstance(existing, discord.Thread):
            return existing

    thread = await _create_named_thread(
        forum,
        startup_name,
        content=content,
        embed=embed,
        embeds=embeds,
        view=view,
    )
    if thread is None:
        return None

    bucket["startup_thread_id"] = thread.id
    _save_state(state)
    return thread




async def post_thread_message(thread, content=None, embed=None, embeds=None, view=None, **kwargs):
    if thread is None:
        return None
    try:
        if embeds is not None:
            return await thread.send(content=content, embeds=embeds, view=view, allowed_mentions=kwargs.get("allowed_mentions"))
        return await thread.send(content=content, embed=embed, view=view, allowed_mentions=kwargs.get("allowed_mentions"))
    except Exception:
        log.exception("ops thread delivery failed")
        return None

async def post_startup_globally(bot: discord.Client, content=None, embed=None, embeds=None, view=None, **kwargs) -> None:
    try:
        guild = await _resolve_ops_guild(bot)
        if guild is None:
            log.warning("ops startup fallback: no guild resolved")
            return
        await ensure_ops_threads(bot)
        thread = await _create_startup_thread(bot, content=content, embed=embed, embeds=embeds, view=view)
        if thread is None:
            log.warning("ops startup fallback: no startup thread configured")
            return
        log.info(build_log("startup posted", guild=guild, thread=thread))
    except Exception:
        log.exception("ops: startup post failed")

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(value):
    try:
        return ANSI_RE.sub("", value or "")
    except Exception:
        return value




def _generic_status_embed(content: str) -> discord.Embed:
    raw = (content or '').strip()
    desc = raw
    if raw.startswith('@here'):
        desc = raw[len('@here'):].strip(' \n:-')
    elif raw.startswith('@everyone'):
        desc = raw[len('@everyone'):].strip(' \n:-')
    if not desc:
        desc = 'Status updated.'
    return discord.Embed(title='System • Status', description=desc[:4096], color=discord.Color.blurple())

def _resolve_target(target, kwargs):
    bot = target if isinstance(target, discord.Client) else getattr(target, "client", None) or getattr(target, "bot", None)
    guild = kwargs.get("guild")
    if guild is None:
        guild = target if isinstance(target, discord.Guild) else getattr(target, "guild", None)
    return bot, guild


async def post_status(target, content=None, embed=None, view=None, **kwargs):
    bot, guild = _resolve_target(target, kwargs)
    if content:
        log.info(build_log("status", guild=guild, detail=content))
    if embed is None and content:
        raw_content = str(content).strip()
        mention = None
        if raw_content.startswith('@here'):
            mention = '@here'
        elif raw_content.startswith('@everyone'):
            mention = '@everyone'
        embed = _generic_status_embed(raw_content)
        content = mention
    try:
        if bot is None and guild is not None:
            bot = guild._state._get_client() if hasattr(guild, "_state") else None
    except Exception:
        pass
    if bot is None:
        return None
    thread = await _resolve_thread(bot, guild, "status_thread_id", "OPS_THREAD_STATUS", "SYSTEM - STATUS")
    if thread is None:
        return None
    try:
        return await thread.send(content=content, embed=embed, view=view, allowed_mentions=kwargs.get("allowed_mentions"))
    except Exception:
        log.exception("ops status thread delivery failed: %s", content)
        return None


async def post_debug(target, content=None, embed=None, view=None, **kwargs):
    if not debug_enabled():
        return None
    bot, guild = _resolve_target(target, kwargs)
    message = content or ""
    thread_message = _strip_ansi(message)
    debug_log.warning("ops: %s", message)
    try:
        if bot is None and guild is not None:
            bot = guild._state._get_client() if hasattr(guild, "_state") else None
    except Exception:
        pass
    if bot is None:
        return None
    thread = await _resolve_thread(bot, guild, "debug_thread_id", "OPS_THREAD_DEBUG", "SYSTEM - DEBUG")
    if thread is None:
        return None
    try:
        return await thread.send(content=thread_message, embed=embed, view=view, allowed_mentions=kwargs.get("allowed_mentions"))
    except Exception:
        log.exception("ops debug thread delivery failed: %s", message)
        return None
