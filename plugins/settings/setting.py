from __future__ import annotations

import logging
import re
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from discord import ui

from .feature_manager import SettingsFeatureManager
from .registry import SettingsRegistry, SettingFeature, FeatureAction
from .ops_forum import post_startup_globally, post_status, post_debug, ensure_ops_threads

BUILD_ID = os.getenv("BOT_BUILD_ID", "L.5")
log = logging.getLogger("bot.settings")


def _log_id_mode() -> bool:
    return os.getenv("LOG_ID_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}


def _settings_user_label(user: discord.abc.User) -> str:
    if _log_id_mode():
        return str(int(user.id))
    return getattr(user, "display_name", None) or getattr(user, "name", None) or "UnknownUser"


def _settings_guild_label(guild: Optional[discord.Guild]) -> str:
    if guild is None:
        return "DM" if not _log_id_mode() else "unknown"
    if _log_id_mode():
        return str(int(guild.id))
    return getattr(guild, "name", None) or "UnknownGuild"


@dataclass(frozen=True)
class Page:
    kind: str  # "root" | "category" | "feature"
    category: Optional[str] = None
    feature_id: Optional[str] = None


class BackButton(ui.Button):
    def __init__(self):
        super().__init__(label="Back", style=discord.ButtonStyle.danger, custom_id="settings:back")

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return
        if not view.stack:
            return
        view.current = view.stack.pop()
        view.render()
        await interaction.response.edit_message(view=view)


class RefreshButton(ui.Button):
    def __init__(self):
        super().__init__(label="Refresh", style=discord.ButtonStyle.success, custom_id="settings:refresh")

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return

        view.current = Page(kind="root")
        view.stack.clear()

        ok = True
        msg = ""
        try:
            loaded = await view.mgr.reload_all()
            interaction.client.settings_features_loaded = loaded  # type: ignore[attr-defined]
            msg = f"Reload OK: {len(loaded)} feature(s): " + ", ".join([x[0] for x in loaded])
            log.info(f"settings: refresh -> loaded features count={len(loaded)}")
        except Exception as e:
            ok = False
            msg = f"Reload FAILED: {type(e).__name__}"
            log.exception("settings: refresh failed")

        # update visible status line in the root header
        try:
            now = discord.utils.utcnow()
            view.last_refresh = now.strftime("%H:%M:%S UTC")
        except Exception:
            view.last_refresh = "Just now"
        view.last_refresh_ok = ok

        view.render()
        await interaction.response.edit_message(view=view)

        # ephemeral confirmation
        try:
            await interaction.followup.send(msg, ephemeral=True)
        except Exception:
            pass



class FeatureActionButton(ui.Button):
    def __init__(self, feature_id: str, action_id: str, label: str, style: discord.ButtonStyle, row: int):
        super().__init__(label=label, style=style, custom_id=f"settings:action:{feature_id}:{action_id}", row=row)
        self._fid = feature_id
        self._action = action_id

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return
        reg: SettingsRegistry = interaction.client.settings_registry  # type: ignore[attr-defined]

        feat = reg.get(self._fid)
        if feat is None:
            view.render()
            await interaction.response.edit_message(view=view)
            return

        result = await feat.handler(interaction, {"action": self._action})

        if isinstance(result, dict) and result.get("op") == "modal":
            modal = result.get("modal")
            if modal is not None:
                await interaction.response.send_modal(modal)
            return

        

        if isinstance(result, dict) and result.get("op") == "respond":
            payload = result.get("payload") or {}
            content = payload.get("content", "")
            ephemeral = bool(payload.get("ephemeral", True))
            embed = payload.get("embed", None)
            resp_view = payload.get("view", None)
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(content=content or None, embed=embed, view=resp_view, ephemeral=ephemeral)
                else:
                    await interaction.response.send_message(content=content or None, embed=embed, view=resp_view, ephemeral=ephemeral)
            except Exception:
                pass

        view.render()
        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(view=view)
            else:
                await interaction.response.edit_message(view=view)
        except Exception:
            pass



class OpenCategoryButton(ui.Button):
    def __init__(self, category: str):
        super().__init__(label="Open", style=discord.ButtonStyle.secondary, custom_id=f"settings:cat:{category}")
        self._category = category

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return
        view.stack.append(view.current)
        view.current = Page(kind="category", category=self._category)
        view.render()
        await interaction.response.edit_message(view=view)


class OpenFeatureButton(ui.Button):
    def __init__(self, feature_id: str):
        super().__init__(label="Open", style=discord.ButtonStyle.secondary, custom_id=f"settings:feat:{feature_id}")
        self._fid = feature_id

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return
        reg: SettingsRegistry = interaction.client.settings_registry  # type: ignore[attr-defined]

        feat = reg.get(self._fid)
        if feat is None:
            view.render()
            await interaction.response.edit_message(view=view)
            return

        # If the feature declares an 'open' action, treat Open as an immediate action
        # (e.g., open a modal) instead of navigating to a feature page.
        if feat.actions and any((a.action_id == "open") for a in feat.actions):
            result = await feat.handler(interaction, {"action": "open"})

            if isinstance(result, dict) and result.get("op") == "modal":
                modal = result.get("modal")
                if modal is not None:
                    await interaction.response.send_modal(modal)
                return

            if isinstance(result, dict) and result.get("op") == "respond":
                payload = result.get("payload") or {}
                content = payload.get("content", "")
                ephemeral = bool(payload.get("ephemeral", True))
                await interaction.response.send_message(content, ephemeral=ephemeral)
                return

            # default: re-render in place
            view.render()
            await interaction.response.edit_message(view=view)
            return

        # Default behavior: navigate to feature page
        view.stack.append(view.current)
        view.current = Page(kind="feature", feature_id=self._fid)
        view.render()
        await interaction.response.edit_message(view=view)


class ToggleFeatureButton(ui.Button):
    def __init__(self, feature_id: str, is_on: bool):
        label = "Toggle [ON]" if is_on else "Toggle [OFF]"
        style = discord.ButtonStyle.success if is_on else discord.ButtonStyle.danger
        super().__init__(label=label, style=style, custom_id=f"settings:toggle:{feature_id}")
        self._fid = feature_id

    async def callback(self, interaction: discord.Interaction):
        view = self.view  # type: ignore[assignment]
        if view is None:
            return
        reg: SettingsRegistry = interaction.client.settings_registry  # type: ignore[attr-defined]

        feat = reg.get(self._fid)
        if feat is None:
            view.render()
            await interaction.response.edit_message(view=view)
            return

        result = await feat.handler(interaction, {"action": "toggle"})

        if isinstance(result, dict) and result.get("op") == "toggle":
            try:
                view.toggle_state[self._fid] = bool(result.get("is_on"))
            except Exception:
                pass

        if isinstance(result, dict) and result.get("op") == "modal":
            modal = result.get("modal")
            if modal is not None:
                await interaction.response.send_modal(modal)
            return

        if isinstance(result, dict) and result.get("op") == "respond":
            payload = result.get("payload") or {}
            content = payload.get("content", "")
            ephemeral = bool(payload.get("ephemeral", True))
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(content=content, ephemeral=ephemeral)
                else:
                    await interaction.response.send_message(content=content, ephemeral=ephemeral)
            except Exception:
                pass

        view.render()
        try:
            if interaction.response.is_done():
                await interaction.edit_original_response(view=view)
            else:
                await interaction.response.edit_message(view=view)
        except Exception:
            pass


class SettingsLayout(ui.LayoutView):
    @staticmethod
    def _short_desc(text: str, limit: int = 80) -> str:
        # Keep category lists tight: first non-empty line, no bullets, truncated.
        if not text:
            return ""
        lines = [ln.strip() for ln in str(text).splitlines() if ln.strip()]
        if not lines:
            return ""
        s = lines[0]
        s = re.sub(r"^[•\-*]\s*", "", s)
        if len(s) > limit:
            s = s[: limit - 1].rstrip() + "…"
        return s

    def __init__(self, reg: SettingsRegistry, mgr: SettingsFeatureManager):
        super().__init__(timeout=900)
        self.reg = reg
        self.mgr = mgr

        self.stack: List[Page] = []
        self.current: Page = Page(kind="root")

        self.last_refresh: str = "Never"
        self.last_refresh_ok: bool | None = None

        # Per-view toggle cache (feature_id -> is_on). Used when status() is not available or is async.
        self.toggle_state: Dict[str, bool] = {}

        self.container = ui.Container()
        self.add_item(self.container)

        self.render()

    def _status_line(self, feat: SettingFeature) -> str:
        if feat.status:
            try:
                return feat.status()
            except Exception:
                return ""
        return ""

    def _is_on(self, feat: SettingFeature) -> bool:
        if feat.feature_id in self.toggle_state:
            return bool(self.toggle_state.get(feat.feature_id))
        s = (self._status_line(feat) or "").strip().lower()
        if s.startswith("✅"):
            return True
        if s.startswith("❌"):
            return False
        if "enabled" in s and "disabled" not in s:
            return True
        if "disabled" in s:
            return False
        return False

    def _clear(self) -> None:
        self.container.clear_items()

    def _sep(self) -> None:
        self.container.add_item(ui.Separator())

    def _add_root_header(self) -> None:
        self.container.add_item(ui.TextDisplay("**Server Settings**"))
        self.container.add_item(ui.TextDisplay("Change the settings for this server."))
        row = ui.ActionRow()
        row.add_item(RefreshButton())
        self.container.add_item(row)
        self._sep()

    def _add_sub_header(self, title: str, desc: str, toggle_feature_id: str | None = None) -> None:
        self.container.add_item(ui.TextDisplay(f"**{title}**"))
        self.container.add_item(ui.TextDisplay(desc))
        row = ui.ActionRow()
        row.add_item(BackButton())
        if toggle_feature_id:
            feat = self.reg.get(toggle_feature_id)
            if feat is not None:
                row.add_item(ToggleFeatureButton(feat.feature_id, is_on=self._is_on(feat)))
        self.container.add_item(row)
        self._sep()

    def _add_section(self, title: str, desc: str, button: ui.Button) -> None:
        text = ui.TextDisplay(f"**{title}**" + (f"\n{desc}" if desc else ""))
        self.container.add_item(ui.Section(text, accessory=button))
        self._sep()


    def _categories_aggregate(self) -> Dict[str, Dict[str, object]]:
        cats: Dict[str, Dict[str, object]] = {}
        for f in self.reg.all():
            cat = (f.category or "General").strip() or "General"
            cdesc = (f.category_description or "").strip() or "No description provided."

            if cat not in cats:
                cats[cat] = {"description": cdesc, "features": [f]}
            else:
                cats[cat]["features"].append(f)  # type: ignore[index]
                existing = str(cats[cat]["description"])
                if existing != cdesc:
                    log.warning(
                        "settings: category_description conflict for '%s' (keeping first). first=%r new=%r feature=%s",
                        cat, existing, cdesc, f.feature_id
                    )

        for cat in cats:
            cats[cat]["features"] = sorted(  # type: ignore[index]
                cats[cat]["features"],  # type: ignore[index]
                key=lambda x: x.label.lower()
            )

        return dict(sorted(cats.items(), key=lambda kv: kv[0].lower()))

    def render_root(self) -> None:
        self._add_root_header()

        cats = self._categories_aggregate()
        if not cats:
            self.container.add_item(ui.TextDisplay("**No categories available**\nNo settings features are installed."))
            return

        for cat, meta in cats.items():
            self._add_section(cat, str(meta["description"]), OpenCategoryButton(cat))

    def render_category(self, category: str) -> None:
        self._add_sub_header(category, "Select a feature.")

        cats = self._categories_aggregate()
        meta = cats.get(category)
        feats: List[SettingFeature] = []
        if meta:
            feats = meta["features"]  # type: ignore[assignment]

        if not feats:
            self.container.add_item(ui.TextDisplay("**No features**\nNo features are installed in this category."))
            return

        for f in feats:
            self._add_section(f.label, f.description or "No description provided.", OpenFeatureButton(f.feature_id))

    def render_feature(self, feature_id: str) -> None:
        feat = self.reg.get(feature_id)
        if feat is None:
            self._add_sub_header("Feature not found", "This feature is no longer installed.")
            return

        self._add_sub_header(feat.label, feat.description or "Feature settings.", toggle_feature_id=feat.feature_id)


        actions = feat.actions or []
        if actions:
            style_map = {
                "primary": discord.ButtonStyle.primary,
                "secondary": discord.ButtonStyle.secondary,
                "success": discord.ButtonStyle.success,
                "danger": discord.ButtonStyle.danger,
            }

            # Feature page body contract:
            # ---- SEPARATOR ----
            # Button Description                 [Button]
            # ---- SEPARATOR ----
            #
            # Use a Section with accessory button to render description + button.
            for a in actions:
                st = style_map.get((a.style or "secondary").lower(), discord.ButtonStyle.secondary)

                # Button label is intentionally short. Description is shown in the section text.
                btn_label = "Open"
                if st == discord.ButtonStyle.danger:
                    btn_label = "Run"

                btn = FeatureActionButton(feat.feature_id, a.action_id, btn_label, st, row=1)
                # Left side text uses action label as the description line.
                self._add_section(a.label, a.description or "", btn)

    def render(self) -> None:
        self._clear()
        if self.current.kind == "root":
            self.render_root()
        elif self.current.kind == "category":
            self.render_category(self.current.category or "General")
        elif self.current.kind == "feature":
            self.render_feature(self.current.feature_id or "")
        else:
            self.render_root()


def build_startup_status_embed(feats: List[Tuple[str, str, str]]) -> discord.Embed:
    embed = discord.Embed(title="Bot restarted / updated")
    lines = [f"Bot Version: {BUILD_ID}", f"Settings features loaded: {len(feats)}"]

    if feats:
        lines.extend(["", "Modules"])
        lines.extend(f"{label} - {ver}" for _fid, label, ver in sorted(feats, key=lambda x: x[1].lower()))
    else:
        lines.extend(["", "Modules", "No features installed."])

    embed.description = "\n".join(lines)
    return embed


async def post_startup_status(bot: commands.Bot) -> None:
    if getattr(bot, "_startup_status_posted", False):
        return
    bot._startup_status_posted = True

    feats = getattr(bot, "settings_features_loaded", [])
    embed = build_startup_status_embed(feats)

    try:
        await ensure_ops_threads(bot)
        await post_startup_globally(bot, embed=embed)
    except Exception:
        log.exception("ops: startup post failed")


class SettingsReadyOnce(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        await post_startup_status(self.bot)


@app_commands.default_permissions(administrator=True)
@app_commands.checks.has_permissions(administrator=True)
@app_commands.command(name="settings", description="Open server settings")
async def settings_cmd(interaction: discord.Interaction):
    if _log_id_mode():
        log.info("cmd: /settings user_id=%s guild_id=%s", _settings_user_label(interaction.user), _settings_guild_label(interaction.guild))
    else:
        log.info("cmd: /settings user=%s guild=%s", _settings_user_label(interaction.user), _settings_guild_label(interaction.guild))
    await interaction.response.defer(ephemeral=True)

    reg: SettingsRegistry = interaction.client.settings_registry  # type: ignore[attr-defined]
    mgr: SettingsFeatureManager = interaction.client.settings_feature_manager  # type: ignore[attr-defined]

    view = SettingsLayout(reg, mgr)
    interaction.client._settings_active_view = view  # type: ignore[attr-defined]

    await interaction.followup.send(view=view, ephemeral=True)


async def setup(bot: commands.Bot) -> None:
    if not hasattr(bot, "settings_registry"):
        bot.settings_registry = SettingsRegistry()  # type: ignore[attr-defined]
    if not hasattr(bot, "settings_feature_manager"):
        bot.settings_feature_manager = SettingsFeatureManager(bot, bot.settings_registry)  # type: ignore[attr-defined]

    loaded = await bot.settings_feature_manager.load_all()  # type: ignore[attr-defined]
    bot.settings_features_loaded = loaded  # type: ignore[attr-defined]
    log.info(f"settings: loaded features count={len(loaded)}")

    bot.tree.add_command(settings_cmd)
    await bot.add_cog(SettingsReadyOnce(bot))
