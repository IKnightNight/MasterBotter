from __future__ import annotations

import importlib
import pkgutil
import sys
from dataclasses import dataclass
from typing import Dict, List, Tuple

from discord.ext import commands
import structlog

from .registry import SettingsRegistry

FEATURES_PKG = "plugins.settings.features"

log = structlog.get_logger("bot.settings")

# ANSI colors (if the terminal strips them, output still reads fine)
ANSI_RESET = "\033[0m"
ANSI_GREEN = "\033[92m"
ANSI_RED = "\033[91m"
ANSI_YELLOW = "\033[93m"


def _color(text: str, color: str) -> str:
    return f"{color}{text}{ANSI_RESET}"


@dataclass
class LoadedFeature:
    feature_id: str
    module_name: str  # plugins.settings.features.<id>.pack


class SettingsFeatureManager:
    def __init__(self, bot: commands.Bot, registry: SettingsRegistry) -> None:
        self.bot = bot
        self.registry = registry
        self.loaded: Dict[str, LoadedFeature] = {}

    def _modname(self, feature_id: str) -> str:
        return f"{FEATURES_PKG}.{feature_id}.pack"

    def discover(self) -> list[str]:
        importlib.invalidate_caches()
        pkg = importlib.import_module(FEATURES_PKG)
        ids: list[str] = []
        for m in pkgutil.iter_modules(pkg.__path__, FEATURES_PKG + "."):
            ids.append(m.name.split(".")[-1])
        ids.sort()
        return ids

    def _clear_feature_modules(self, feature_ids: list[str]) -> None:
        prefixes = [f"{FEATURES_PKG}.{fid}" for fid in feature_ids]
        to_remove: list[str] = []
        for name in list(sys.modules.keys()):
            if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes):
                to_remove.append(name)
        for name in to_remove:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()

    async def load_all(self) -> List[Tuple[str, str, str]]:
        loaded_meta: List[Tuple[str, str, str]] = []
        for fid in self.discover():
            meta = await self.load(fid)
            loaded_meta.append(meta)

            loaded_fid = meta[0]
            log.info(f"settings.{_color(loaded_fid, ANSI_GREEN)}: loaded")

        return loaded_meta

    async def load(self, feature_id: str) -> Tuple[str, str, str]:
        mod_name = self._modname(feature_id)

        # Always import the latest file contents.
        sys.modules.pop(mod_name, None)
        importlib.invalidate_caches()
        mod = importlib.import_module(mod_name)

        meta = getattr(mod, "PACK_META", {}) or {}
        fid = meta.get("id") or feature_id
        label = meta.get("name") or fid
        version = meta.get("version") or "0.0.0"

        setup = getattr(mod, "setup", None)
        if not callable(setup):
            raise RuntimeError(f"{mod_name} missing async setup(bot, registry)")

        await setup(self.bot, self.registry)
        self.loaded[fid] = LoadedFeature(fid, mod_name)
        return (fid, label, version)

    async def unload(self, feature_id: str) -> None:
        info = self.loaded.get(feature_id)
        mod_name = info.module_name if info else self._modname(feature_id)

        mod = sys.modules.get(mod_name)
        if mod is not None:
            teardown = getattr(mod, "teardown", None)
            if callable(teardown):
                await teardown(self.bot, self.registry)

        self.registry.unregister(feature_id)
        sys.modules.pop(mod_name, None)
        self.loaded.pop(feature_id, None)

        log.info(f"settings.{_color(feature_id, ANSI_YELLOW)}: unloaded")

    async def reload_all(self) -> List[Tuple[str, str, str]]:
        current_ids = list(self.loaded.keys())
        discovered_ids = self.discover()
        target_ids = sorted(set(current_ids) | set(discovered_ids))

        # First unload everything currently active.
        for fid in current_ids:
            await self.unload(fid)

        # Then aggressively clear cached modules for every feature package.
        self._clear_feature_modules(target_ids)

        loaded_meta: List[Tuple[str, str, str]] = []
        try:
            for fid in discovered_ids:
                meta = await self.load(fid)
                loaded_meta.append(meta)

                loaded_fid = meta[0]
                log.info(f"settings.{_color(loaded_fid, ANSI_GREEN)}: loaded")
        except Exception:
            # Leave the registry in a known-empty state after a failed refresh.
            for fid in list(self.loaded.keys()):
                try:
                    await self.unload(fid)
                except Exception:
                    pass
            self._clear_feature_modules(target_ids)
            raise

        return loaded_meta

    async def reload(self, feature_id: str) -> Tuple[str, str, str]:
        await self.unload(feature_id)
        self._clear_feature_modules([feature_id])
        meta = await self.load(feature_id)

        loaded_fid = meta[0]
        log.info(f"settings.{_color(loaded_fid, ANSI_GREEN)}: loaded")
        return meta
