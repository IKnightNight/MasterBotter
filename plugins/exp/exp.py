from __future__ import annotations

import structlog
import discord
from discord import app_commands
from discord.ext import commands, tasks

from .api import get_profile, get_rank, get_leaderboard, count_profiles
from . import api as exp_api
from plugins.settings.ops_forum import post_debug, post_status, debug_enabled

log = structlog.get_logger("bot.exp")


def _cmd_meta(interaction: discord.Interaction) -> dict:
    guild = interaction.guild
    channel = interaction.channel
    return {
        "user": str(interaction.user),
        "user_id": int(interaction.user.id),
        "guild": getattr(guild, "name", "DM"),
        "guild_id": getattr(guild, "id", None),
        "channel": getattr(channel, "name", str(channel) if channel else "unknown"),
        "channel_id": getattr(channel, "id", None),
    }



def _clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

def build_progress_bar(total_xp: int, xp_to_next: int, segments: int = 10) -> tuple[str, int]:
    """Return (bar, percent) using 🟨 filled and ⬛ empty blocks."""
    denom = total_xp + xp_to_next
    progress = (total_xp / denom) if denom > 0 else 0.0
    progress = _clamp01(progress)

    filled = int(progress * segments)
    if filled > segments:
        filled = segments

    bar = ("🟨" * filled) + ("⬛" * (segments - filled))
    percent = int(round(progress * 100))
    return bar, percent

def _color_for_level(level: int, *, max_level: int = 50) -> discord.Color:
    """Interpolate from dark to gold as level increases."""
    if max_level <= 0:
        max_level = 1
    t = _clamp01(level / max_level)

    start = 0x2B2D31  # dark grey
    end = 0xFFD700    # gold

    sr, sg, sb = (start >> 16) & 0xFF, (start >> 8) & 0xFF, start & 0xFF
    er, eg, eb = (end >> 16) & 0xFF, (end >> 8) & 0xFF, end & 0xFF

    r = int(sr + (er - sr) * t)
    g = int(sg + (eg - sg) * t)
    b = int(sb + (eb - sb) * t)
    return discord.Color((r << 16) + (g << 8) + b)


def build_exp_embed(
    target: discord.abc.User,
    display_name: str,
    prof,
    *,
    rank: tuple[int, int] | None = None,
) -> discord.Embed:
    bar, percent = build_progress_bar(int(prof.xp), int(prof.xp_to_next), segments=10)

    embed = discord.Embed(
        title="EXP Profile",
        description=f"**{display_name}** (`{str(target)}`)",
        color=_color_for_level(int(prof.level)),
    )
    embed.set_author(name=display_name, icon_url=target.display_avatar.url)
    embed.set_thumbnail(url=target.display_avatar.url)

    embed.add_field(name="Level", value=str(prof.level), inline=True)
    embed.add_field(name="Total XP", value=str(prof.xp), inline=True)
    embed.add_field(name="XP to next", value=str(prof.xp_to_next), inline=True)
    embed.add_field(name="Progress", value=f"{bar} {percent}%", inline=False)

    if rank is not None:
        r, total = rank
        embed.add_field(name="Server Rank", value=f"#{r} / {total}", inline=True)

    embed.set_footer(text="Usage: /exp [user]  |  /leaderboard")
    return embed


class ExpCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.api = exp_api
        try:
            self.voice_tick.start()
        except Exception:
            pass


    def cog_unload(self):
        try:
            self.voice_tick.cancel()
        except Exception:
            pass

    @tasks.loop(seconds=30)
    async def voice_tick(self):
        # Award voice EXP while users remain connected to voice channels.
        try:
            for guild in self.bot.guilds:
                try:
                    cfg = await self.api.get_config(int(guild.id))
                except Exception:
                    continue

                if not cfg.get("enabled", True) or not cfg.get("voice_enabled", True):
                    continue

                channels = list(getattr(guild, "voice_channels", [])) + list(getattr(guild, "stage_channels", []))
                for ch in channels:
                    for member in getattr(ch, "members", []):
                        if getattr(member, "bot", False):
                            continue
                        await self.api.try_award_voice(int(guild.id), int(member.id))
        except Exception as e:
            log.warning("exp: voice_tick_failed", error=str(e))

    @voice_tick.before_loop
    async def _before_voice_tick(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is None:
            return
        if message.author.bot:
            return
        # message_content intent must be enabled (it is)
        from .api import try_award_message
        try:
            await try_award_message(message.guild.id, message.author.id)
        except Exception as e:
            log.error("exp: award_message_failed", error=str(e))

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.abc.User):
        # Note: on_reaction_add may fire for both cached and uncached messages depending on intents.
        if user.bot:
            return
        msg = reaction.message
        if msg.guild is None:
            return
        from .api import try_award_reaction
        try:
            await try_award_reaction(msg.guild.id, user.id)
        except Exception as e:
            log.error("exp: award_reaction_failed", error=str(e))


@app_commands.command(
    name="exp",
    description="View EXP profile. Use /leaderboard for server ranks.",
)
@app_commands.describe(
    user="User to view (optional)",
)
async def exp_cmd(
    interaction: discord.Interaction,
    user: discord.Member | None = None,
):
    if interaction.guild_id is None or interaction.guild is None:
        await interaction.response.send_message("EXP is only available in servers.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False, thinking=False)
    log.info("cmd: /exp", **_cmd_meta(interaction))
    target: discord.abc.User = user or interaction.user
    display_name = target.display_name if isinstance(target, discord.Member) else str(target)

    prof = await get_profile(interaction.guild_id, int(target.id))
    rank = await get_rank(interaction.guild_id, int(target.id))

    embed = build_exp_embed(target, display_name, prof, rank=rank)
    await interaction.followup.send(
        embed=embed,
        ephemeral=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )


@app_commands.command(
    name="leaderboard",
    description="View the server EXP leaderboard (paginated).",
)
async def leaderboard_cmd(interaction: discord.Interaction):
    if interaction.guild_id is None or interaction.guild is None:
        await interaction.response.send_message("Leaderboards are only available in servers.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=False, thinking=False)
    log.info("cmd: /leaderboard", **_cmd_meta(interaction))
    view = _ExpLeaderboardView(owner_id=int(interaction.user.id), guild_id=int(interaction.guild_id), page_size=10)
    embed = await view.build_embed(interaction)
    await interaction.followup.send(
        embed=embed,
        view=view,
        ephemeral=False,
        allowed_mentions=discord.AllowedMentions.none(),
    )


class _ExpLeaderboardView(discord.ui.View):
    def __init__(self, owner_id: int, guild_id: int, *, page_size: int = 10):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.guild_id = guild_id
        self.page_size = page_size
        self.page = 0

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return int(interaction.user.id) == int(self.owner_id)

    async def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        offset = self.page * self.page_size
        rows = await get_leaderboard(self.guild_id, limit=self.page_size, offset=offset)
        total = await count_profiles(self.guild_id)

        title = "EXP Leaderboard"
        desc = "Top users by total XP.\nUsage: /leaderboard"
        embed = discord.Embed(title=title, description=desc, color=discord.Color.gold())

        if total <= 0:
            embed.add_field(name="No data", value="No profiles yet.", inline=False)
            return embed

        if not rows:
            embed.add_field(name="No data", value="No results on this page.", inline=False)
            return embed

        lines = []
        for i, (uid, xp, lvl) in enumerate(rows, start=offset + 1):
            member = interaction.guild.get_member(uid) if interaction.guild else None
            name = member.display_name if member else f"User {uid}"
            lines.append(f"**#{i}** — {name} • Level **{lvl}** • XP **{xp}**")

        embed.add_field(name="Ranks", value="\n".join(lines), inline=False)
        last_index = min(total, offset + self.page_size)
        embed.set_footer(text="Usage: /exp [user]  |  /leaderboard")
        return embed

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=0)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
        embed = await self.build_embed(interaction)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=0)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        total = await count_profiles(self.guild_id)
        max_page = max(0, (total - 1) // self.page_size) if total else 0
        if self.page < max_page:
            self.page += 1
        embed = await self.build_embed(interaction)
        await interaction.response.edit_message(embed=embed, view=self)

class _ExpPreviewView(discord.ui.View):
    def __init__(self, owner_id: int, states: list[tuple[int,int,int]], start_index: int = 0):
        super().__init__(timeout=300)
        self.owner_id = owner_id
        self.states = states
        self.index = start_index

    def _embed(self, interaction: discord.Interaction) -> discord.Embed:
        level, total_xp, xp_to_next = self.states[self.index]
        # Fake a minimal profile-like object
        class _P:  # noqa: N801
            def __init__(self, level: int, xp: int, xp_to_next: int):
                self.level = level
                self.xp = xp
                self.xp_to_next = xp_to_next
        prof = _P(level, total_xp, xp_to_next)
        display_name = interaction.user.display_name
        return build_exp_embed(interaction.user, display_name, prof)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only the invoker can click
        return interaction.user.id == self.owner_id

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.danger)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index = (self.index - 1) % len(self.states)
        await interaction.response.edit_message(embed=self._embed(interaction), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.success)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.index = (self.index + 1) % len(self.states)
        await interaction.response.edit_message(embed=self._embed(interaction), view=self)

    @discord.ui.button(label="Close", style=discord.ButtonStyle.secondary)
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()






async def exp_user_context_menu(interaction: discord.Interaction, user: discord.Member):
    if interaction.guild_id is None or interaction.guild is None:
        await interaction.response.send_message('EXP is only available in servers.', ephemeral=True)
        return
    await interaction.response.defer(ephemeral=False, thinking=False)
    prof = await get_profile(interaction.guild_id, int(user.id))
    rank = await get_rank(interaction.guild_id, int(user.id))
    embed = build_exp_embed(user, user.display_name, prof, rank=rank)
    await interaction.followup.send(embed=embed, ephemeral=False, allowed_mentions=discord.AllowedMentions.none())

async def setup(bot: commands.Bot):
    # register command
    bot.tree.add_command(exp_cmd)
    bot.tree.add_command(leaderboard_cmd)
    try:
        bot.tree.add_command(app_commands.ContextMenu(name='View EXP', callback=exp_user_context_menu))
        log.info('context menu loaded: View EXP')
    except Exception:
        log.exception('context menu failed: View EXP')
    await bot.add_cog(ExpCog(bot))
    log.info("exp: loaded")


async def teardown(bot: commands.Bot):
    try:
        bot.tree.remove_command("exp", type=app_commands.Command)
        bot.tree.remove_command("leaderboard", type=app_commands.Command)
        bot.tree.remove_command('View EXP', type=discord.AppCommandType.user)
    except Exception:
        pass