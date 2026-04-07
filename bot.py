"""Master Bot (L.12)

Project recipe is stored in `RECIPE_INTERNAL.txt` (not surfaced by the bot).
"""

from __future__ import annotations

import logging
import asyncio
import aiohttp
import structlog
import os
import sys
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# Load environment variables from .env (if present) before reading BOT_TOKEN.
# IMPORTANT: do NOT override existing environment variables provided by the host panel.
# If override=True, an incomplete/old .env can clobber panel-provided secrets.
dotenv_path = Path(__file__).resolve().parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path, override=False)
else:
    load_dotenv(find_dotenv(usecwd=True), override=False)


# Keep this name exactly.
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

VERSION = "L.12"

# Expose build id for other modules (e.g., startup/status embeds).
# Force it to match this deployment so the startup message always shows the
# correct build even if the panel has an older env var.
os.environ["BOT_BUILD_ID"] = VERSION


# ----------------------------
# Logging
# ----------------------------
ANSI_RESET = "\x1b[0m"
ANSI_YELLOW = "\x1b[33m"
ANSI_PURPLE = "\x1b[95m"
BOT_TAG = f"{ANSI_YELLOW}[BOT]{ANSI_RESET}"
DEBUG_TAG = f"{ANSI_PURPLE}[DEBUG]{ANSI_RESET}"

def _structlog_render(logger, name, event_dict):
    # Render structlog events as plain text without extra timestamps/levels.
    event = event_dict.pop("event", "")
    if event_dict:
        extras = " ".join(f"{k}={v}" for k, v in event_dict.items())
        return f"{event} {extras}".strip()
    return str(event)


class TaggedFormatter(structlog.stdlib.ProcessorFormatter):
    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        if not record.name.startswith("debug") and str(rendered).startswith("boot: version="):
            return f"{ANSI_YELLOW}[BOT] {rendered}{ANSI_RESET}"
        tag = DEBUG_TAG if record.name.startswith("debug") else BOT_TAG
        return f"{tag} {rendered}"


def setup_logging() -> None:
    debug_mode = os.getenv("DEBUG_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
    level = os.getenv("LOG_LEVEL", "DEBUG" if debug_mode else "INFO").upper().strip()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        level = "DEBUG" if debug_mode else "INFO"

    # Formatter: fixed [BOT] tag + message only (PebbleHost already prefixes timestamps).
    processor_formatter = TaggedFormatter(
        processor=_structlog_render,
        foreign_pre_chain=[],
        fmt="%(message)s",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(processor_formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


    # Suppress discord.py INFO/DEBUG noise. Keep WARNING+ only.
    for name in ("discord", "discord.client", "discord.gateway", "discord.http"):
        lg = logging.getLogger(name)
        lg.setLevel(logging.WARNING)
        lg.propagate = True


    # Configure structlog to funnel into stdlib logging without its own timestamp/level decoration.
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

setup_logging()
log = logging.getLogger("bot")

def build_intents() -> discord.Intents:
    # All intents (including privileged). You must also enable privileged intents
    # in the Discord Developer Portal for them to actually be delivered.
    return discord.Intents.all()

def _diag_intents(intents: discord.Intents) -> str:
    parts = []
    # A few high-signal ones:
    parts.append(f"members={intents.members}")
    parts.append(f"presences={intents.presences}")
    parts.append(f"message_content={intents.message_content}")
    parts.append(f"guilds={intents.guilds}")
    parts.append(f"messages={intents.messages}")
    parts.append(f"reactions={intents.reactions}")
    return ", ".join(parts)

class Bot(commands.AutoShardedBot):
    def __init__(self) -> None:
        self.boot_utc = datetime.now(timezone.utc)
        # Provide it even if the rest of the bot uses stdlib logging.
        if not hasattr(self, "log"): 
            self.log = structlog.get_logger("bot")

        # Note: discord.py manages shard state internally; `.shards` is a read-only property.
        intents = build_intents()

        # - Keep "!" for any existing text commands.
        # - Always allow @mention as a prefix.
        async def _prefixes(bot: commands.Bot, message: discord.Message):
            return commands.when_mentioned_or("!p ", "!p")(bot, message)

        super().__init__(
            command_prefix=_prefixes,
            intents=intents,
        )
        log.info("boot: version=%s python=%s discord.py=%s", VERSION, sys.version.split()[0], discord.__version__)
        log.info("boot: intents: %s", _diag_intents(intents))

    async def setup_hook(self) -> None:
        if getattr(self, "http_session", None) is None:
            self.http_session = aiohttp.ClientSession()

        log.info("boot: loading extension plugins.settings.setting")
        await self.load_extension("plugins.settings.setting")
        await self.load_extension("plugins.exp.exp")
        log.info("boot: extension loaded")
        self.loop.create_task(self._loop_lag_monitor())

        # NOTE: We intentionally avoid a global sync during startup.
        # Global commands can linger and appear as duplicates alongside guild
        # commands. We clear/sync explicitly in on_ready.


    async def _loop_lag_monitor(self) -> None:
        await self.wait_until_ready()
        loop = asyncio.get_running_loop()
        last = loop.time()
        while not self.is_closed():
            await asyncio.sleep(1.0)
            now = loop.time()
            drift = now - last - 1.0
            last = now
            if drift > 1.5:
                log.warning("loop_lag: seconds=%.2f", drift)

    async def on_ready(self) -> None:
        # Diagnostics snapshot on each ready
        try:
            guild_count = len(self.guilds)
        except Exception:
            guild_count = -1
        log.info("ready: logged in as %s (%s)", self.user, self.user.id if self.user else "n/a")
        log.info("ready: guilds=%s latency_ms=%.0f", guild_count, (self.latency * 1000.0))

        # COMMAND PUBLISHING STRATEGY
        # Keep this simple to avoid "duplicate commands" and avoid breaking interactions.
        #
        # - If DEV_GUILD_ID is set, sync ONLY to that guild (instant updates).
        # - Otherwise, sync globally.
        try:
            dev_gid = os.getenv("DEV_GUILD_ID", "").strip()
            if dev_gid.isdigit():
                dev_obj = discord.Object(id=int(dev_gid))
                await self.tree.sync(guild=dev_obj)
                log.info("ready: app_commands: synced (dev guild)")
            else:
                synced_guilds = []
                for g in self.guilds:
                    try:
                        await self.tree.sync(guild=g)
                        synced_guilds.append(f"{g.name}({g.id})")
                    except Exception:
                        log.exception("ready: app_commands: failed syncing guild %s", g.id)
                if synced_guilds:
                    log.info("ready: app_commands: synced (guild) guilds=%s", ", ".join(synced_guilds))
                else:
                    await self.tree.sync()
                    log.info("ready: app_commands: synced (global)")
        except Exception:
            log.exception("ready: app_commands: failed syncing")


    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:  # type: ignore[name-defined]
        # Keep errors visible in logs; UI responses are handled by handlers.
        log.exception("app_command_error: %r", error)

    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError) -> None:
        # Suppress noisy "CommandNotFound" errors (especially around the !p router).
        if isinstance(error, commands.CommandNotFound):
            return
        log.exception("command_error: %s", error)

    async def close(self) -> None:
        # Gracefully close shared aiohttp session used by some feature packs.
        try:
            sess = getattr(self, "http_session", None)
            if sess is not None and not sess.closed:
                await sess.close()
        except Exception:
            pass
        await super().close()

def main() -> None:
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is missing. Put it in .env or panel environment variables.")

    bot = Bot()
    bot.run(token)

if __name__ == "__main__":
    main()
