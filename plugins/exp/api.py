from __future__ import annotations

import time
from typing import Any, Dict

from .service import ExpProfile, ExpService

_service: ExpService | None = None


def get_service() -> ExpService:
    global _service
    if _service is None:
        _service = ExpService()
    return _service


async def get_config(guild_id: int) -> Dict[str, Any]:
    return await get_service().get_config(guild_id)


async def set_config_field(guild_id: int, key: str, value: Any) -> Dict[str, Any]:
    return await get_service().set_config_field(guild_id, key, value)


async def get_profile(guild_id: int, user_id: int) -> ExpProfile:
    return await get_service().get_profile(guild_id, user_id)


async def award_xp(guild_id: int, user_id: int, amount: int) -> ExpProfile:
    return await get_service().award_xp(guild_id, user_id, amount)


async def adjust_xp(guild_id: int, user_id: int, delta: int) -> ExpProfile:
    return await get_service().adjust_xp(guild_id, user_id, delta)


async def set_xp(guild_id: int, user_id: int, xp: int) -> ExpProfile:
    return await get_service().set_xp(guild_id, user_id, xp)


async def reset_profile(guild_id: int, user_id: int) -> ExpProfile:
    return await get_service().reset_profile(guild_id, user_id)


async def try_award_message(guild_id: int, user_id: int) -> bool:
    """Award message XP if enabled and past cooldown.

    This is intentionally defensive: older builds of ExpService may not expose the
    public wrapper method name expected by listeners.
    """
    svc = get_service()

    fn = getattr(svc, "try_award_message", None)
    if callable(fn):
        return await fn(guild_id, user_id)

    # Back-compat fallback (call the underlying sync function via the service runner)
    run = getattr(svc, "_run", None)
    sync = getattr(svc, "_try_award_msg_sync", None)
    if callable(run) and callable(sync):
        return await run(sync, guild_id, user_id, int(time.time()))

    return False


async def try_award_reaction(guild_id: int, user_id: int) -> bool:
    svc = get_service()

    fn = getattr(svc, "try_award_reaction", None)
    if callable(fn):
        return await fn(guild_id, user_id)

    run = getattr(svc, "_run", None)
    sync = getattr(svc, "_try_award_react_sync", None)
    if callable(run) and callable(sync):
        return await run(sync, guild_id, user_id, int(time.time()))

    return False


async def try_award_voice(guild_id: int, user_id: int) -> bool:
    """Award voice XP if enabled and past tick."""
    svc = get_service()

    fn = getattr(svc, "try_award_voice", None)
    if callable(fn):
        return await fn(guild_id, user_id)

    run = getattr(svc, "_run", None)
    sync = getattr(svc, "_try_award_voice_sync", None)
    if callable(run) and callable(sync):
        return await run(sync, guild_id, user_id)

    return False


async def get_leaderboard(guild_id: int, *, limit: int = 10, offset: int = 0):
    return await get_service().get_leaderboard(guild_id, limit=limit, offset=offset)


async def get_rank(guild_id: int, user_id: int):
    return await get_service().get_rank(guild_id, user_id)


async def count_profiles(guild_id: int) -> int:
    return await get_service().count_profiles(guild_id)
