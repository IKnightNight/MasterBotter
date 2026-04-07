from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import structlog

log = structlog.get_logger("bot.exp")

_PACK_DIR = Path(__file__).resolve().parent
_DATA_DIR = _PACK_DIR / "_data"
_DB_FILE = _DATA_DIR / "exp.sqlite"


@dataclass(frozen=True)
class ExpProfile:
    guild_id: int
    user_id: int
    xp: int
    level: int
    xp_to_next: int


_DEFAULT_CFG: Dict[str, Any] = {
    "enabled": True,
    "msg_enabled": True,
    "msg_xp": 15,
    "msg_cooldown": 60,     # seconds
    "react_enabled": True,
    "react_xp": 5,
    "react_cooldown": 30,   # seconds
    "voice_enabled": True,
    "voice_xp": 8,
    "voice_tick": 300,       # seconds
}


# Simple curve: total_xp_for_level(n) = 150 * n^2
def level_from_xp(xp: int) -> int:
    if xp <= 0:
        return 0
    return int((xp / 150) ** 0.5)


def total_xp_for_level(level: int) -> int:
    if level <= 0:
        return 0
    return 150 * (level ** 2)


def xp_to_next_level(xp: int) -> int:
    lvl = level_from_xp(xp)
    next_total = total_xp_for_level(lvl + 1)
    return max(0, next_total - xp)


class ExpService:
    """
    SQLite-backed EXP store.

    - One DB shared for all guilds, keyed by (guild_id, user_id)
    - Config stored per guild (row in config table)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None

    def _connect(self) -> sqlite3.Connection:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        if self._conn is None:
            self._conn = sqlite3.connect(_DB_FILE.as_posix(), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._init_schema(self._conn)
        return self._conn

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS profiles (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                xp INTEGER NOT NULL DEFAULT 0,
                last_msg_ts INTEGER NOT NULL DEFAULT 0,
                last_react_ts INTEGER NOT NULL DEFAULT 0,
                last_voice_ts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                guild_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL,
                msg_enabled INTEGER NOT NULL,
                msg_xp INTEGER NOT NULL,
                msg_cooldown INTEGER NOT NULL,
                react_enabled INTEGER NOT NULL,
                react_xp INTEGER NOT NULL,
                react_cooldown INTEGER NOT NULL,
                voice_enabled INTEGER NOT NULL,
                voice_xp INTEGER NOT NULL,
                voice_tick INTEGER NOT NULL
            )
            """
        )

        # migrations (SQLite has no ADD COLUMN IF NOT EXISTS)
        for stmt in (
            "ALTER TABLE profiles ADD COLUMN last_voice_ts INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE config ADD COLUMN voice_enabled INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE config ADD COLUMN voice_xp INTEGER NOT NULL DEFAULT 8",
            "ALTER TABLE config ADD COLUMN voice_tick INTEGER NOT NULL DEFAULT 300",
        ):
            try:
                cur.execute(stmt)
            except Exception:
                pass

        conn.commit()

    async def _run(self, fn, *args, **kwargs):
        # run sqlite ops off the event loop
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _get_cfg_sync(self, guild_id: int) -> Dict[str, Any]:
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            row = cur.execute("SELECT * FROM config WHERE guild_id=?", (guild_id,)).fetchone()
            if row is None:
                # seed defaults
                cur.execute(
                    """
                    INSERT INTO config (
                        guild_id, enabled, msg_enabled, msg_xp, msg_cooldown,
                        react_enabled, react_xp, react_cooldown,
                        voice_enabled, voice_xp, voice_tick
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        guild_id,
                        int(_DEFAULT_CFG["enabled"]),
                        int(_DEFAULT_CFG["msg_enabled"]),
                        int(_DEFAULT_CFG["msg_xp"]),
                        int(_DEFAULT_CFG["msg_cooldown"]),
                        int(_DEFAULT_CFG["react_enabled"]),
                        int(_DEFAULT_CFG["react_xp"]),
                        int(_DEFAULT_CFG["react_cooldown"]),
                        int(_DEFAULT_CFG["voice_enabled"]),
                        int(_DEFAULT_CFG["voice_xp"]),
                        int(_DEFAULT_CFG["voice_tick"]),
                    ),
                )
                conn.commit()
                return dict(_DEFAULT_CFG)
            cfg = {
                "enabled": bool(row["enabled"]),
                "msg_enabled": bool(row["msg_enabled"]),
                "msg_xp": int(row["msg_xp"]),
                "msg_cooldown": int(row["msg_cooldown"]),
                "react_enabled": bool(row["react_enabled"]),
                "react_xp": int(row["react_xp"]),
                "react_cooldown": int(row["react_cooldown"]),
                "voice_enabled": int(row["voice_enabled"]),
                "voice_xp": int(row["voice_xp"]),
                "voice_tick": int(row["voice_tick"]),
            }
            return cfg

    async def get_config(self, guild_id: int) -> Dict[str, Any]:
        return await self._run(self._get_cfg_sync, guild_id)

    def _set_cfg_field_sync(self, guild_id: int, key: str, value: Any) -> Dict[str, Any]:
        cfg = self._get_cfg_sync(guild_id)
        cfg[key] = value

        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE config SET
                    enabled=?,
                    msg_enabled=?,
                    msg_xp=?,
                    msg_cooldown=?,
                    react_enabled=?,
                    react_xp=?,
                    react_cooldown=?,
                    voice_enabled=?,
                    voice_xp=?,
                    voice_tick=?
                WHERE guild_id=?
                """,
                (
                    int(bool(cfg["enabled"])),
                    int(bool(cfg["msg_enabled"])),
                    int(cfg["msg_xp"]),
                    int(cfg["msg_cooldown"]),
                    int(bool(cfg["react_enabled"])),
                    int(cfg["react_xp"]),
                    int(cfg["react_cooldown"]),
                    int(bool(cfg.get("voice_enabled", True))),
                    int(cfg.get("voice_xp", 8)),
                    int(cfg.get("voice_tick", 300)),
                    guild_id,
                ),
            )
            conn.commit()
        return cfg

    async def set_config_field(self, guild_id: int, key: str, value: Any) -> Dict[str, Any]:
        return await self._run(self._set_cfg_field_sync, guild_id, key, value)

    def _ensure_profile_sync(self, guild_id: int, user_id: int) -> None:
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR IGNORE INTO profiles (guild_id, user_id, xp, last_msg_ts, last_react_ts, last_voice_ts) VALUES (?, ?, 0, 0, 0, 0)",
                (guild_id, user_id),
            )
            conn.commit()

    def _get_profile_sync(self, guild_id: int, user_id: int) -> ExpProfile:
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            row = conn.cursor().execute(
                "SELECT xp FROM profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
            xp = int(row["xp"]) if row else 0
        lvl = level_from_xp(xp)
        return ExpProfile(guild_id=guild_id, user_id=user_id, xp=xp, level=lvl, xp_to_next=xp_to_next_level(xp))

    async def get_profile(self, guild_id: int, user_id: int) -> ExpProfile:
        return await self._run(self._get_profile_sync, guild_id, user_id)

    def _award_xp_sync(self, guild_id: int, user_id: int, amount: int) -> ExpProfile:
        if amount <= 0:
            return self._get_profile_sync(guild_id, user_id)
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            cur.execute(
                "UPDATE profiles SET xp = xp + ? WHERE guild_id=? AND user_id=?",
                (int(amount), guild_id, user_id),
            )
            conn.commit()
        return self._get_profile_sync(guild_id, user_id)

    async def award_xp(self, guild_id: int, user_id: int, amount: int) -> ExpProfile:
        return await self._run(self._award_xp_sync, guild_id, user_id, amount)


    async def adjust_xp(self, guild_id: int, user_id: int, delta: int) -> ExpProfile:
        """Admin adjustment: can add or remove XP (clamped to 0)."""
        return await self._run(self._adjust_xp_sync, guild_id, user_id, delta)

    async def set_xp(self, guild_id: int, user_id: int, xp: int) -> ExpProfile:
        """Admin set: set XP to an exact value (clamped to 0)."""
        return await self._run(self._set_xp_sync, guild_id, user_id, xp)

    async def reset_profile(self, guild_id: int, user_id: int) -> ExpProfile:
        """Admin reset: clears XP and cooldown timestamps."""
        return await self._run(self._reset_profile_sync, guild_id, user_id)

    def _adjust_xp_sync(self, guild_id: int, user_id: int, delta: int) -> ExpProfile:
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            # Clamp to 0
            cur.execute(
                "UPDATE profiles SET xp = MAX(0, xp + ?) WHERE guild_id=? AND user_id=?",
                (int(delta), guild_id, user_id),
            )
            conn.commit()
        return self._get_profile_sync(guild_id, user_id)

    def _set_xp_sync(self, guild_id: int, user_id: int, xp: int) -> ExpProfile:
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            cur.execute(
                "UPDATE profiles SET xp = ? WHERE guild_id=? AND user_id=?",
                (max(0, int(xp)), guild_id, user_id),
            )
            conn.commit()
        return self._get_profile_sync(guild_id, user_id)

    def _reset_profile_sync(self, guild_id: int, user_id: int) -> ExpProfile:
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            cur.execute(
                "UPDATE profiles SET xp=0, last_msg_ts=0, last_react_ts=0 WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            )
            conn.commit()
        return self._get_profile_sync(guild_id, user_id)

    def _try_award_msg_sync(self, guild_id: int, user_id: int, now_ts: int) -> bool:
        cfg = self._get_cfg_sync(guild_id)
        if not cfg["enabled"] or not cfg["msg_enabled"]:
            return False
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT last_msg_ts FROM profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
            last_ts = int(row["last_msg_ts"]) if row else 0
            if now_ts - last_ts < int(cfg["msg_cooldown"]):
                return False
            cur.execute(
                "UPDATE profiles SET last_msg_ts=? , xp = xp + ? WHERE guild_id=? AND user_id=?",
                (now_ts, int(cfg["msg_xp"]), guild_id, user_id),
            )
            conn.commit()
            return True

    async def try_award_message(self, guild_id: int, user_id: int) -> bool:
        return await self._run(self._try_award_msg_sync, guild_id, user_id, int(time.time()))

    def _try_award_react_sync(self, guild_id: int, user_id: int, now_ts: int) -> bool:
        cfg = self._get_cfg_sync(guild_id)
        if not cfg["enabled"] or not cfg["react_enabled"]:
            return False
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT last_react_ts FROM profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
            last_ts = int(row["last_react_ts"]) if row else 0
            if now_ts - last_ts < int(cfg["react_cooldown"]):
                return False
            cur.execute(
                "UPDATE profiles SET last_react_ts=? , xp = xp + ? WHERE guild_id=? AND user_id=?",
                (now_ts, int(cfg["react_xp"]), guild_id, user_id),
            )
            conn.commit()
            return True


    def _try_award_voice_sync(self, guild_id: int, user_id: int) -> bool:
        cfg = self._get_cfg_sync(guild_id)
        if not cfg.get("enabled", True) or not cfg.get("voice_enabled", True):
            return False

        tick = int(cfg.get("voice_tick", 300))
        xp_amt = int(cfg.get("voice_xp", 8))
        now = int(time.time())

        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            row = conn.cursor().execute(
                "SELECT last_voice_ts FROM profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
            last = int(row["last_voice_ts"] or 0) if row else 0
            if tick > 0 and (now - last) < tick:
                return False

            cur = conn.cursor()
            cur.execute(
                """
                UPDATE profiles
                SET xp = xp + ?, last_voice_ts=?
                WHERE guild_id=? AND user_id=?
                """,
                (xp_amt, now, guild_id, user_id),
            )
            conn.commit()
        return True

    async def try_award_reaction(self, guild_id: int, user_id: int) -> bool:
        return await self._run(self._try_award_react_sync, guild_id, user_id, int(time.time()))

    async def try_award_voice(self, guild_id: int, user_id: int) -> bool:
        return await self._run(self._try_award_voice_sync, guild_id, user_id)


    def _get_leaderboard_sync(self, guild_id: int, limit: int, offset: int) -> List[Tuple[int, int, int]]:
        """Return list of (user_id, xp, level) sorted by xp desc."""
        conn = self._connect()
        with self._lock:
            rows = conn.cursor().execute(
                "SELECT user_id, xp FROM profiles WHERE guild_id=? ORDER BY xp DESC, user_id ASC LIMIT ? OFFSET ?",
                (guild_id, int(limit), int(offset)),
            ).fetchall()
        out: List[Tuple[int, int, int]] = []
        for r in rows or []:
            uid = int(r["user_id"])
            xp = int(r["xp"])
            out.append((uid, xp, level_from_xp(xp)))
        return out

    async def get_leaderboard(self, guild_id: int, *, limit: int = 10, offset: int = 0) -> List[Tuple[int, int, int]]:
        return await self._run(self._get_leaderboard_sync, guild_id, int(limit), int(offset))

    def _count_profiles_sync(self, guild_id: int) -> int:
        conn = self._connect()
        with self._lock:
            row = conn.cursor().execute(
                "SELECT COUNT(*) AS c FROM profiles WHERE guild_id=?",
                (guild_id,),
            ).fetchone()
        return int(row["c"]) if row else 0

    async def count_profiles(self, guild_id: int) -> int:
        return await self._run(self._count_profiles_sync, guild_id)

    def _get_rank_sync(self, guild_id: int, user_id: int) -> Tuple[int, int]:
        """Return (rank, total) for user_id within guild."""
        self._ensure_profile_sync(guild_id, user_id)
        conn = self._connect()
        with self._lock:
            row = conn.cursor().execute(
                "SELECT xp FROM profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
            xp = int(row["xp"]) if row else 0

            # rank = 1 + count users with higher xp,
            # plus tie-breaker by smaller user_id so ordering is deterministic
            row2 = conn.cursor().execute(
                "SELECT COUNT(*) AS c FROM profiles WHERE guild_id=? AND (xp > ? OR (xp = ? AND user_id < ?))",
                (guild_id, xp, xp, user_id),
            ).fetchone()
            better = int(row2["c"]) if row2 else 0

            row3 = conn.cursor().execute(
                "SELECT COUNT(*) AS c FROM profiles WHERE guild_id=?",
                (guild_id,),
            ).fetchone()
            total = int(row3["c"]) if row3 else 0

        return (better + 1, total)

    async def get_rank(self, guild_id: int, user_id: int) -> Tuple[int, int]:
        return await self._run(self._get_rank_sync, guild_id, user_id)

