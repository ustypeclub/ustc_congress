from __future__ import annotations

import asyncio
import datetime as dt
import io
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Union, Tuple

import discord
from discord import app_commands
from discord.ext import commands

# --- Timezone handling (Windows-safe) ---
try:
    from zoneinfo import ZoneInfo
    try:
        ET = ZoneInfo("America/New_York")
    except Exception:
        try:
            import tzdata  # noqa: F401
            ET = ZoneInfo("America/New_York")
        except Exception:
            ET = dt.timezone(dt.timedelta(hours=-5), name="ET")
except Exception:
    ET = dt.timezone(dt.timedelta(hours=-5), name="ET")

# Logger for USTC Congress
log = logging.getLogger("ustc_congress")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

DEFAULT_EXPIRATION_MINUTES = 1440  # 24h default

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso(dtobj: Optional[dt.datetime]) -> Optional[str]:
    return dtobj.isoformat() if dtobj else None

def from_iso(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def fmt_abs_et(ts_utc: dt.datetime) -> str:
    try:
        return ts_utc.astimezone(ET).strftime("%a, %b %d, %Y • %I:%M %p ET")
    except Exception:
        return ts_utc.strftime("%a, %b %d, %Y • %H:%M UTC")

def humanize_delta(future_utc: dt.datetime) -> str:
    delta = (future_utc - utcnow())
    secs = int(delta.total_seconds())
    if secs <= 0:
        return "now"
    mins, s = divmod(secs, 60)
    hrs, mins = divmod(mins, 60)
    days, hrs = divmod(hrs, 24)
    parts = []
    if days: parts.append(f"{days}d")
    if hrs:  parts.append(f"{hrs}h")
    if mins: parts.append(f"{mins}m")
    if not parts: parts.append(f"{s}s")
    return "in " + " ".join(parts[:2])

# --------------------------
# Data structures
# --------------------------
@dataclass
class Motion:
    id: int
    title: str
    text: str
    author_id: int
    created_at: str  # ISO
    status: str = "active"  # active|passed|failed|killed|expired|tied
    majority: str = "1/2"
    votes: Dict[int, str] = field(default_factory=dict)  # user_id -> yes|no|abstain
    reasons: Dict[int, str] = field(default_factory=dict)
    finished_at: Optional[str] = None
    thread_id: Optional[int] = None
    live_message_id: Optional[int] = None
    expires_at: Optional[str] = None  # ISO

    @staticmethod
    def parse_majority(s: str) -> float:
        s = s.strip()
        if s.endswith("%"):
            try:
                return max(0.0, min(1.0, float(s[:-1]) / 100.0))
            except Exception:
                return 0.5
        if "/" in s:
            try:
                num, den = s.split("/", 1)
                num, den = float(num.strip()), float(den.strip())
                return max(0.0, min(1.0, num / den)) if den else 1.0
            except Exception:
                return 0.5
        try:
            v = float(s)
            return max(0.0, min(1.0, v))
        except Exception:
            return 0.5

    def majority_threshold(self) -> float:
        return self.parse_majority(self.majority)

    def tallies(self, council: "Council", guild: discord.Guild) -> Tuple[float, float, float]:
        yes = no = abstain = 0.0
        for uid, choice in self.votes.items():
            member = guild.get_member(uid)
            if not member:
                continue
            w = council.vote_weight_for(member)
            if choice == "yes": yes += w
            elif choice == "no": no += w
            else: abstain += w
        return yes, no, abstain

    def format_voters(self, guild: discord.Guild) -> str:
        lines: List[str] = []
        def tag(uid: int) -> str:
            m = guild.get_member(uid)
            return m.mention if m else f"<@{uid}>"
        for uid, choice in self.votes.items():
            reason = self.reasons.get(uid, "")
            if reason:
                lines.append(f"{tag(uid)} — **{choice.upper()}** — _{reason[:150]}_")
            else:
                lines.append(f"{tag(uid)} — **{choice.upper()}**")
        return "\n".join(lines) if lines else "No votes yet."

    def embed_live(self, council: "Council", guild: discord.Guild) -> discord.Embed:
        yes, no, abstain = self.tallies(council, guild)
        e = discord.Embed(
            title=f"📜 Live Motion — {self.title}",
            description=self.text[:4000],
            color=discord.Color.blurple()
        )
        e.add_field(
            name="Tallies",
            value=f"✅ Yes: {yes:.0f}\n❌ No: {no:.0f}\n➖ Abstain: {abstain:.0f}",
            inline=True
        )
        e.add_field(
            name="Voters",
            value=self.format_voters(guild)[:1000],
            inline=False
        )

        # Footer: just majority information
        footer = f"Majority: {self.majority}"
        e.set_footer(text=footer)

        # Expires: show as its own field so Discord renders the relative timestamp
        if self.expires_at:
            ex = from_iso(self.expires_at)
            if ex:
                epoch = int(ex.timestamp())
                e.add_field(name="Expires", value=f"<t:{epoch}:R>", inline=False)

        return e

    def embed_result(self, council: "Council", guild: discord.Guild, outcome: str) -> discord.Embed:
        yes, no, abstain = self.tallies(council, guild)
        colors = {
            "passed": discord.Color.green(),
            "failed": discord.Color.red(),
            "killed": discord.Color.greyple(),
            "expired": discord.Color.dark_grey(),
            "tied": discord.Color.gold(),
        }
        title_map = {"passed": "PASSED", "failed": "FAILED", "killed": "KILLED", "expired": "EXPIRED", "tied": "TIED"}
        e = discord.Embed(
            title=f"🏁 Motion {title_map.get(outcome, outcome.upper())} — {self.title}",
            description=self.text[:4000],
            color=colors.get(outcome, discord.Color.dark_grey())
        )
        e.add_field(
            name="Tallies",
            value=f"✅ Yes: {yes:.0f}\n❌ No: {no:.0f}\n➖ Abstain: {abstain:.0f}",
            inline=True
        )
        e.add_field(name="Voters", value=self.format_voters(guild)[:1000], inline=False)
        e.set_footer(text=f"Majority: {self.majority}")
        return e


@dataclass
class Council:
    guild_id: int
    channel_id: int
    name: str
    motions: List[Motion] = field(default_factory=list)
    current_motion: Optional[Motion] = None
    motion_queue: List[Motion] = field(default_factory=list)
    next_motion_id: int = 1
    # config & runtime
    config: Dict[str, Any] = field(default_factory=dict)
    vote_weights: Dict[str, int] = field(default_factory=dict)
    cooldowns: Dict[int, str] = field(default_factory=dict)
    # stats
    proposed_count: Dict[int, int] = field(default_factory=dict)
    voted_count: Dict[int, int] = field(default_factory=dict)
    miss_streak: Dict[int, int] = field(default_factory=dict)
    # live message
    live_message_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "guild_id": self.guild_id,
            "channel_id": self.channel_id,
            "name": self.name,
            "motions": [asdict(m) for m in self.motions],
            "current_motion": asdict(self.current_motion) if self.current_motion else None,
            "motion_queue": [asdict(m) for m in self.motion_queue],
            "next_motion_id": self.next_motion_id,
            "config": self.config,
            "vote_weights": self.vote_weights,
            "cooldowns": self.cooldowns,
            "proposed_count": self.proposed_count,
            "voted_count": self.voted_count,
            "miss_streak": self.miss_streak,
            "live_message_id": self.live_message_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Council":
        c = cls(d["guild_id"], d["channel_id"], d["name"])
        c.motions = [Motion(**m) for m in d.get("motions", [])]
        if d.get("current_motion"):
            c.current_motion = Motion(**d["current_motion"])
        c.motion_queue = [Motion(**m) for m in d.get("motion_queue", [])]
        c.next_motion_id = d.get("next_motion_id", 1)
        c.config = d.get("config", {})
        c.vote_weights = {k: int(v) for k, v in d.get("vote_weights", {}).items()}
        c.cooldowns = d.get("cooldowns", {})
        c.proposed_count = {int(k): int(v) for k, v in d.get("proposed_count", {}).items()}
        c.voted_count = {int(k): int(v) for k, v in d.get("voted_count", {}).items()}
        c.miss_streak = {int(k): int(v) for k, v in d.get("miss_streak", {}).items()}
        c.live_message_id = d.get("live_message_id")
        return c

    # ABSOLUTE weighting: user override > sum of role weights > 1
    def vote_weight_for(self, member: discord.Member) -> float:
        ukey = str(member.id)
        if ukey in self.vote_weights:
            return float(self.vote_weights[ukey])
        role_sum = 0
        for role in member.roles:
            if str(role.id) in self.vote_weights:
                role_sum += int(self.vote_weights[str(role.id)])
        return float(role_sum if role_sum > 0 else 1)

    # Early finish check (no queue mutation here)
    def maybe_finish(self, guild: discord.Guild) -> Optional[Motion]:
        m = self.current_motion
        if not m:
            return None

        councilor_role_id = self._get_role_id("councilor.role")
        eligible = 0
        for member in guild.members:
            if member.bot:
                continue
            if councilor_role_id:
                role = guild.get_role(councilor_role_id)
                if role and role in member.roles:
                    eligible += 1
            else:
                eligible += 1
        if eligible == 0:
            eligible = 1

        yes, no, abstain = m.tallies(self, guild)
        threshold = m.majority_threshold()
        end_on_reach = self.config.get("majority.reached.ends", True)

        passed = failed = False
        if end_on_reach:
            if yes / eligible >= threshold:
                passed = True
            elif no / eligible >= (1 - threshold):
                failed = True

        if not (passed or failed):
            return None

        m.status = "passed" if passed else "failed"
        m.finished_at = iso(utcnow())
        self.motions.append(m)
        self.current_motion = None
        return m

    def _get_role_id(self, key: str) -> Optional[int]:
        try:
            val = self.config.get(key)
            return int(val) if val is not None else None
        except Exception:
            return None

    def _get_channel_id(self, key: str) -> Optional[int]:
        try:
            val = self.config.get(key)
            return int(val) if val is not None else None
        except Exception:
            return None


# --------------------------
# Persistence
# --------------------------
class Store:
    # Renamed data filename for USTC Congress
    def __init__(self, path: str = "ustc_congress_data.json") -> None:
        self.path = path
        self.data: Dict[str, Any] = {"councils": {}, "meta": {"schema_version": 4}}
        self.load()

    @staticmethod
    def _ck(guild_id: int, channel_id: int) -> str:
        return f"{guild_id}:{channel_id}"

    def load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {"councils": {}, "meta": {"schema_version": 4}}

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2)

    def get_council(self, guild_id: int, channel_id: int) -> Optional[Council]:
        d = self.data["councils"].get(self._ck(guild_id, channel_id))
        return Council.from_dict(d) if d else None

    def put_council(self, c: Council) -> None:
        # ensure old hours key is removed if present
        if "motion.expiration.hours" in c.config:
            del c.config["motion.expiration.hours"]
        self.data["councils"][self._ck(c.guild_id, c.channel_id)] = c.to_dict()
        self.save()

    def del_council(self, guild_id: int, channel_id: int) -> bool:
        k = self._ck(guild_id, channel_id)
        if k in self.data["councils"]:
            del self.data["councils"][k]
            self.save()
            return True
        return False


# --------------------------
# Cog
# --------------------------
class Votum(commands.Cog):
    def __init__(self, bot: commands.Bot, store: Store) -> None:
        self.bot = bot
        self.store = store
        self.expirer_started = False

    # Helpers
    async def _require_guild(self, interaction: discord.Interaction) -> Optional[discord.Guild]:
        if not interaction.guild:
            await interaction.response.send_message("This command must be used in a server.", ephemeral=True)
            return None
        return interaction.guild

    async def _require_admin(self, interaction: discord.Interaction) -> bool:
        user = interaction.user
        assert isinstance(user, (discord.Member, discord.User))
        if isinstance(user, discord.Member) and user.guild_permissions.manage_guild:
            return True
        await interaction.response.send_message("You need **Manage Server** to run this.", ephemeral=True)
        return False

    def _council(self, guild: discord.Guild, channel: discord.abc.GuildChannel) -> Optional[Council]:
        return self.store.get_council(guild.id, channel.id)  # type: ignore[arg-type]

    def _title_unique(self, council: Council, title: str) -> bool:
        existing = {m.title for m in council.motions}
        if council.current_motion:
            existing.add(council.current_motion.title)
        existing.update([m.title for m in council.motion_queue])
        return title not in existing

    def _get_voting_channel(self, c: Council, guild: discord.Guild) -> Optional[discord.TextChannel]:
        ch = guild.get_channel(c.channel_id)
        return ch if isinstance(ch, discord.TextChannel) else None

    def _get_announce_channel(self, c: Council, guild: discord.Guild) -> Optional[discord.TextChannel]:
        # Only return a channel if announcement.channel is explicitly set
        ch_id = c._get_channel_id("announcement.channel")
        if ch_id:
            ch = guild.get_channel(ch_id)
            if isinstance(ch, discord.TextChannel):
                return ch
        return None

    def _get_announce_ping_roles(self, c: Council, guild: discord.Guild) -> List[discord.Role]:
        raw = str(c.config.get("announcement.ping.roles", "") or "").strip()
        roles: List[discord.Role] = []
        if not raw:
            return roles
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        for p in parts:
            try:
                rid = int(p.strip("<@&> "))
                r = guild.get_role(rid)
                if r:
                    roles.append(r)
            except Exception:
                continue
        return roles

    # Admin: council create/rename/remove
    @app_commands.command(name="council", description="Create, rename, or remove a council in this channel.")
    @app_commands.describe(action="create|rename|remove", name="Name for create/rename")
    async def council(self, interaction: discord.Interaction, action: str, name: Optional[str] = None) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        if not await self._require_admin(interaction): return
        action = action.lower()
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]

        if action == "create":
            if c:
                await interaction.response.send_message("This channel already has a council.", ephemeral=True); return
            council = Council(guild.id, interaction.channel.id, name or "Council")  # type: ignore[attr-defined]
            self.store.put_council(council)
            await interaction.response.send_message(f"Created council **{council.name}** in this channel.")
        elif action == "rename":
            if not c:
                await interaction.response.send_message("No council in this channel.", ephemeral=True); return
            if not name:
                await interaction.response.send_message("Provide a new name.", ephemeral=True); return
            c.name = name
            self.store.put_council(c)
            await interaction.response.send_message(f"Renamed council to **{name}**.")
        elif action == "remove":
            if not c:
                await interaction.response.send_message("No council in this channel.", ephemeral=True); return
            self.store.del_council(guild.id, interaction.channel.id)
            await interaction.response.send_message("Council removed from this channel.")
        else:
            await interaction.response.send_message("Unknown action. Use create|rename|remove.", ephemeral=True)

    @app_commands.command(name="councilstats", description="Show statistics for this council (embed).")
    async def councilstats(self, interaction: discord.Interaction) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c:
            await interaction.response.send_message("No council in this channel.", ephemeral=True); return

        def top5(d: Dict[int,int]) -> List[Tuple[int,int]]:
            return sorted(d.items(), key=lambda kv: (-kv[1], kv[0]))[:5]

        proposed = top5(c.proposed_count)
        voted = top5(c.voted_count)

        e = discord.Embed(title=f"📊 Council Stats — {c.name}", color=discord.Color.blurple())
        e.add_field(name="Active Motion", value="Yes" if c.current_motion else "No", inline=True)
        e.add_field(name="Queued", value=str(len(c.motion_queue)), inline=True)
        e.add_field(name="Total Motions", value=str(len(c.motions)), inline=True)

        def fmt_users(pairs: List[Tuple[int,int]], title: str) -> None:
            if not pairs:
                e.add_field(name=title, value="(none)", inline=False); return
            lines = []
            for uid, n in pairs:
                m = guild.get_member(uid)
                if not m or m.bot: continue
                role_id = c._get_role_id("councilor.role")
                if role_id:
                    role = guild.get_role(role_id)
                    if role and role not in m.roles: continue
                lines.append(f"{m.mention} — **{n}**")
            e.add_field(name=title, value="\n".join(lines) if lines else "(none)", inline=False)

        fmt_users(proposed, "🏛️ Proposed Motions Leaderboard (Top 5)")
        fmt_users(voted, "🗳️ Voted on Motions Leaderboard (Top 5)")

        streak_pairs = [(uid, n) for uid, n in c.miss_streak.items() if n > 0]
        if streak_pairs:
            lines = []
            for uid, n in sorted(streak_pairs, key=lambda kv: (-kv[1], kv[0])):
                m = guild.get_member(uid)
                if not m or m.bot:
                    continue
                role_id = c._get_role_id("councilor.role")
                if role_id:
                    role = guild.get_role(role_id)
                    if role and role not in m.roles:
                        continue
                lines.append(f"{m.mention} — **{n}**")
            e.add_field(name="🚨 Missed Votes Streak", value="\n".join(lines) if lines else "(none)", inline=False)
        else:
            e.add_field(name="🚨 Missed Votes Streak", value="(none)", inline=False)

        await interaction.response.send_message(embed=e)

    @app_commands.command(name="setweight", description="Set the ABSOLUTE vote weight for a user or role.")
    @app_commands.describe(target="User or role", weight="Integer >= 1")
    async def setweight(self, interaction: discord.Interaction, target: Union[discord.Member, discord.User, discord.Role], weight: int) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        if not await self._require_admin(interaction): return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c: await interaction.response.send_message("No council in this channel.", ephemeral=True); return
        if weight < 1: await interaction.response.send_message("Weight must be >= 1.", ephemeral=True); return

        c.vote_weights[str(target.id)] = int(weight)
        self.store.put_council(c)
        await interaction.response.send_message(f"Set weight for {getattr(target,'mention',target.id)} to **{weight}**.")

    @app_commands.command(name="voteweights", description="Show current vote weights for this council.")
    async def voteweights(self, interaction: discord.Interaction) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c: await interaction.response.send_message("No council in this channel.", ephemeral=True); return
        if not c.vote_weights:
            await interaction.response.send_message("No custom vote weights set."); return
        lines = []
        for sid, w in c.vote_weights.items():
            m = guild.get_member(int(sid))
            r = guild.get_role(int(sid))
            mention = m.mention if m else (r.mention if r else sid)
            lines.append(f"{mention}: {w}")
        await interaction.response.send_message("\n".join(lines))

    @app_commands.command(name="config", description="Configure a council setting (use $remove to clear).")
    async def config(self, interaction: discord.Interaction, key: str, value: str) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        if not await self._require_admin(interaction): return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c: await interaction.response.send_message("No council in this channel.", ephemeral=True); return

        # Reject removed key
        if key == "motion.expiration.hours":
            await interaction.response.send_message("`motion.expiration.hours` has been removed. Use `motion.expiration.minutes`.", ephemeral=True)
            return

        if value == "$remove":
            if key in c.config:
                del c.config[key]
                self.store.put_council(c)
            await interaction.response.send_message(f"Removed `{key}`.")
            return

        v: Any = value
        low = key.lower()
        try:
            if low.endswith(".role"):
                v = int(value) if value.isdigit() else int(value.strip("<@&>"))
            elif low.endswith(".channel") or low == "forward.to":
                v = int(value) if value.isdigit() else int(value.strip("<#>"))
            elif value.lower() in ("true", "false"):
                v = value.lower() == "true"
            elif value.isdigit():
                v = int(value)
            else:
                try: v = float(value)
                except Exception: v = value
        except Exception:
            v = value

        # Validate minutes key bounds
        if key == "motion.expiration.minutes":
            try:
                mins = int(v)
                if mins < 1 or mins > 10080:
                    raise ValueError
            except Exception:
                await interaction.response.send_message("`motion.expiration.minutes` must be an integer 1–10080.", ephemeral=True)
                return

        c.config[key] = v
        self.store.put_council(c)
        await interaction.response.send_message(f"Set `{key}` = `{v}`")

    @app_commands.command(name="configlist", description="List available configuration keys for USTC Congress.")
    async def configlist(self, interaction: discord.Interaction) -> None:
        """Ephemeral list of all config keys with brief descriptions."""
        e = discord.Embed(
            title="USTC Congress — Configuration Keys",
            description="These keys are used with `/config key:<name> value:<value>`.",
            color=discord.Color.blurple()
        )

        e.add_field(
            name="Timing & Majority",
            value=(
                "`motion.expiration.minutes` — Minutes before a motion auto-expires.\n"
                "`majority.default` — Default majority (e.g. `1/2`, `2/3`, `60%`).\n"
                "`majority.reached.ends` — If true, motion can end early once majority is mathematically reached."
            ),
            inline=False
        )
        e.add_field(
            name="Motion Creation & Queue",
            value=(
                "`councilor.motion.disable` — If true, only admins can create motions.\n"
                "`propose.role` — Role required to propose motions.\n"
                "`motion.queue` — If true, allow multiple motions to queue."
            ),
            inline=False
        )
        e.add_field(
            name="Voting & Reasons",
            value=(
                "`councilor.role` — Role required to vote.\n"
                "`reason.required.yes` — Require a reason for `/yes` (default false).\n"
                "`reason.required.no` — Require a reason for `/no` (default false).\n"
                "`reason.required.abstain` — Require a reason for `/abstain` (default false)."
            ),
            inline=False
        )
        e.add_field(
            name="Announcements & Transcripts",
            value=(
                "`announcement.channel` — Channel where motion results are announced (in addition to the voting channel).\n"
                "`announcement.ping.roles` — Comma-separated role IDs to ping **in the announcement channel only**.\n"
                "`keep.transcripts` — If true, attach a JSON transcript of the deliberation thread and keep the thread."
            ),
            inline=False
        )
        e.add_field(
            name="Deprecated / Special",
            value=(
                "`motion.expiration.hours` — Deprecated; use `motion.expiration.minutes` instead."
            ),
            inline=False
        )

        await interaction.response.send_message(embed=e, ephemeral=True)

    # Motion create/view/kill
    @app_commands.command(name="motion", description="Create, view, or kill a motion.")
    @app_commands.describe(
        action="new|view|kill",
        title="Unique title (<=5000 chars) for new or set-title",
        text="Text for new motion",
        majority="Majority like 1/2 or 66%"
    )
    async def motion(self, interaction: discord.Interaction, action: str, title: Optional[str] = None, text: Optional[str] = None, majority: Optional[str] = None) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c: await interaction.response.send_message("No council in this channel.", ephemeral=True); return

        action = action.lower()
        if action == "view":
            if c.current_motion:
                await interaction.response.send_message(embed=c.current_motion.embed_live(c, guild))
            else:
                await interaction.response.send_message("No active motion.")
            return

        if action == "kill":
            m = c.current_motion
            if not m: await interaction.response.send_message("No active motion."); return
            user = interaction.user
            if not (user.id == m.author_id or (isinstance(user, discord.Member) and user.guild_permissions.manage_guild)):
                await interaction.response.send_message("Only the motion author or an admin can kill a motion.", ephemeral=True); return
            await self._resolve_and_announce(c, guild, outcome="killed")
            await interaction.response.send_message(f"Killed motion **{m.title}**.")
            return

        if action == "new":
            # creation gating
            if c.config.get("councilor.motion.disable", False):
                if not (isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.manage_guild):
                    await interaction.response.send_message("New motions are disabled in this council.", ephemeral=True); return

            # proposer role (optional)
            prop_role_id = c._get_role_id("propose.role")
            if prop_role_id and isinstance(interaction.user, discord.Member):
                role = guild.get_role(prop_role_id)
                if role and role not in interaction.user.roles and not interaction.user.guild_permissions.manage_guild:
                    await interaction.response.send_message("You do not have permission to propose motions.", ephemeral=True); return

            if not text:
                await interaction.response.send_message("Provide motion text.", ephemeral=True); return

            # Title rules
            if title:
                if len(title) > 5000:
                    await interaction.response.send_message("Title too long (max 5000).", ephemeral=True); return
                if not self._title_unique(c, title):
                    await interaction.response.send_message("Title must be unique in this council.", ephemeral=True); return
                final_title = title
            else:
                final_title = f"Motion #{c.next_motion_id} — {text[:80]}"

            maj = (majority or str(c.config.get("majority.default", "1/2"))).strip()
            m = Motion(
                id=c.next_motion_id,
                title=final_title,
                text=text,
                author_id=interaction.user.id,
                created_at=iso(utcnow()),
                majority=maj,
            )
            c.next_motion_id += 1

            # expiration (minutes)
            minutes = c.config.get("motion.expiration.minutes", DEFAULT_EXPIRATION_MINUTES)
            try:
                minutes = int(minutes)
            except Exception:
                minutes = DEFAULT_EXPIRATION_MINUTES
            if minutes > 0:
                m.expires_at = iso(utcnow() + dt.timedelta(minutes=minutes))

            # queue behavior
            if c.current_motion and not c.config.get("motion.queue", False):
                await interaction.response.send_message("A motion is already active. Enable `motion.queue` to queue another.", ephemeral=True); return

            # stats: proposer count
            c.proposed_count[interaction.user.id] = c.proposed_count.get(interaction.user.id, 0) + 1

            if c.current_motion:
                c.motion_queue.append(m)
                self.store.put_council(c)
                await interaction.response.send_message(f"Queued **{m.title}**.")
            else:
                c.current_motion = m
                self.store.put_council(c)
                await self._post_live_and_thread(c, guild, ping_new=True)
                await interaction.response.send_message(f"Created **{m.title}**.", suppress_embeds=True)
            return

        await interaction.response.send_message("Unknown action. Use new|view|kill.", ephemeral=True)

    @app_commands.command(name="motion_set_title", description="Set the title of the current motion (must be unique).")
    async def motion_set_title(self, interaction: discord.Interaction, title: str) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c or not c.current_motion:
            await interaction.response.send_message("No active motion.", ephemeral=True); return
        if len(title) > 5000:
            await interaction.response.send_message("Title too long (max 5000).", ephemeral=True); return
        if not self._title_unique(c, title):
            await interaction.response.send_message("Title must be unique in this council.", ephemeral=True); return
        c.current_motion.title = title
        self.store.put_council(c)
        await interaction.response.send_message(f"Renamed motion to **{title}**.")

    # Voting
    async def _cast(self, interaction: discord.Interaction, choice: str, reason: Optional[str]) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c or not c.current_motion:
            await interaction.response.send_message("No active motion.", ephemeral=True); return

        # Enforce councilor.role for voting
        councilor_id = c._get_role_id("councilor.role")
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not (member and councilor_id and (guild.get_role(councilor_id) in member.roles)):
            await interaction.response.send_message("Only members with the councilor role can vote.", ephemeral=True)
            return

        # Reasons required? (defaults now false for yes/no)
        require_map = {
            "yes": c.config.get("reason.required.yes", False),
            "no": c.config.get("reason.required.no", False),
            "abstain": c.config.get("reason.required.abstain", False),
        }
        if require_map.get(choice, False) and not reason:
            await interaction.response.send_message(f"A reason is required for {choice}.", ephemeral=True); return

        uid = interaction.user.id
        first_vote_for_user = uid not in c.current_motion.votes
        c.current_motion.votes[uid] = choice
        if reason:
            c.current_motion.reasons[uid] = reason

        # Immediate streak reset on any vote
        c.miss_streak[uid] = 0

        if first_vote_for_user:
            c.voted_count[uid] = c.voted_count.get(uid, 0) + 1

        finished = c.maybe_finish(guild)
        self.store.put_council(c)
        if finished:
            await self._resolve_post_actions(c, guild, outcome=finished.status)
            await interaction.followup.send("Recorded your vote.", ephemeral=True)
            return

        # Edit existing live embed (or recreate without re-ping if missing)
        await self._post_live_and_thread(c, guild, ping_new=False, update_only=True)
        await interaction.response.send_message(f"Recorded your {choice} vote.", ephemeral=True)

    @app_commands.command(name="yes", description="Vote YES on the current motion.")
    async def yes(self, interaction: discord.Interaction, reason: Optional[str] = None) -> None:
        await self._cast(interaction, "yes", reason)

    @app_commands.command(name="no", description="Vote NO on the current motion.")
    async def no(self, interaction: discord.Interaction, reason: Optional[str] = None) -> None:
        await self._cast(interaction, "no", reason)

    @app_commands.command(name="abstain", description="Abstain on the current motion.")
    async def abstain(self, interaction: discord.Interaction, reason: Optional[str] = None) -> None:
        await self._cast(interaction, "abstain", reason)

    @app_commands.command(name="lazyvoters", description="Mention councilors who haven't voted yet.")
    async def lazyvoters(self, interaction: discord.Interaction) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c or not c.current_motion:
            await interaction.response.send_message("No active motion.", ephemeral=True); return

        councilor_role_id = c._get_role_id("councilor.role")
        mentions: List[str] = []
        for m in guild.members:
            if m.bot: continue
            if councilor_role_id:
                role = guild.get_role(councilor_role_id)
                if role and role in m.roles and m.id not in c.current_motion.votes:
                    mentions.append(m.mention)
            else:
                if m.id not in c.current_motion.votes:
                    mentions.append(m.mention)

        if not mentions:
            await interaction.response.send_message("Everyone has voted!")
        else:
            await interaction.response.send_message("Lazy voters: " + ", ".join(mentions))

    @app_commands.command(name="archive", description="View past motions or export as JSON.")
    @app_commands.describe(range="Range like '1-5' or single '3'", export="Attach a JSON archive")
    async def archive(self, interaction: discord.Interaction, range: Optional[str] = None, export: Optional[bool] = False) -> None:
        guild = await self._require_guild(interaction)
        if not guild: return
        c = self._council(guild, interaction.channel)  # type: ignore[arg-type]
        if not c:
            await interaction.response.send_message("No council in this channel.", ephemeral=True); return

        if export:
            payload = json.dumps(c.to_dict(), indent=2).encode("utf-8")
            file = discord.File(io.BytesIO(payload), filename=f"council_{c.name.replace(' ','_')}.json")
            await interaction.response.send_message("Archive export:", file=file)
            return

        if not range:
            subset = c.motions[-5:] if c.motions else []
        else:
            subset = []
            try:
                if "-" in range:
                    a, b = map(int, range.split("-", 1))
                    ids = {i for i in range(min(a, b), max(a, b) + 1)}
                else:
                    ids = {int(range)}
                for m in c.motions:
                    if m.id in ids: subset.append(m)
            except Exception:
                await interaction.response.send_message("Bad range. Try '1-5' or '3'.", ephemeral=True); return

        if not subset:
            await interaction.response.send_message("No matches."); return

        lines = [f"#{m.id} — {m.status.upper()} — {m.title[:80]}" for m in subset]
        await interaction.response.send_message("\n".join(lines))

    # Posting / announcing helpers
    async def _post_live_and_thread(self, c: Council, guild: discord.Guild, ping_new: bool, update_only: bool = False) -> None:
        channel = self._get_voting_channel(c, guild)
        if not isinstance(channel, discord.TextChannel):
            return
        m = c.current_motion
        if not m:
            return

        live_embed = m.embed_live(c, guild)
        allowed = discord.AllowedMentions(roles=True, users=False, everyone=False)

        if update_only:
            if m.live_message_id:
                try:
                    msg = await channel.fetch_message(m.live_message_id)
                    await msg.edit(embed=live_embed, allowed_mentions=allowed)
                    return
                except Exception:
                    pass  # stale/missing -> recreate below (without re-ping)
            ping_new = False  # do not ping on recreate

        # Creation path
        content = None
        if ping_new:
            pings = []
            rid = c._get_role_id("councilor.role")
            if rid:
                role = guild.get_role(rid)
                if role:
                    pings.append(role.mention)
            content = " ".join(pings) if pings else None

        msg = await channel.send(content=content, embed=live_embed, silent=False, allowed_mentions=allowed)
        m.live_message_id = msg.id
        c.live_message_id = msg.id
        self.store.put_council(c)

        try:
            await msg.pin(reason="USTC Congress live motion")
        except Exception:
            pass

        if not update_only and m.thread_id is None:
            try:
                thread = await msg.create_thread(name=f"🧵 Deliberation — {m.title[:90]}", auto_archive_duration=1440)
                m.thread_id = thread.id
                self.store.put_council(c)
            except Exception as e:
                log.warning("Failed to create deliberation thread: %s", e)

    async def _resolve_and_announce(self, c: Council, guild: discord.Guild, outcome: str) -> None:
        m = c.current_motion
        if not m:
            return
        m.status = outcome
        m.finished_at = iso(utcnow())
        c.motions.append(m)
        c.current_motion = None
        self.store.put_council(c)
        await self._resolve_post_actions(c, guild, outcome=outcome)

    async def _resolve_post_actions(self, c: Council, guild: discord.Guild, outcome: str) -> None:
        # Channels
        voting_ch = self._get_voting_channel(c, guild)
        announce_ch = self._get_announce_channel(c, guild)
        if not voting_ch:
            return

        m = c.motions[-1]

        # Missed streaks (killed motions don't count)
        if outcome not in ("killed",):
            councilor_role_id = c._get_role_id("councilor.role")
            elig: List[int] = []
            for member in guild.members:
                if member.bot: continue
                if councilor_role_id:
                    role = guild.get_role(councilor_role_id)
                    if role and role not in member.roles: continue
                elig.append(member.id)
            for uid in elig:
                if uid in m.votes:
                    c.miss_streak[uid] = 0
                else:
                    c.miss_streak[uid] = c.miss_streak.get(uid, 0) + 1

        # Result embed + optional transcript bytes
        embed = m.embed_result(c, guild, outcome)

        transcript_bytes: Optional[bytes] = None
        if m.thread_id and c.config.get("keep.transcripts", False):
            try:
                thread = guild.get_thread(m.thread_id)
                if thread:
                    msgs = [{
                        "id": msg.id,
                        "author_id": msg.author.id if msg.author else None,
                        "created_at": iso(msg.created_at) if hasattr(msg, "created_at") else None,
                        "content": msg.content,
                    } async for msg in thread.history(limit=None, oldest_first=True)]
                    transcript_bytes = json.dumps(
                        {"thread_id": m.thread_id, "messages": msgs},
                        indent=2
                    ).encode("utf-8")
            except Exception as e:
                log.warning("Transcript export failed: %s", e)

        allowed_no_pings = discord.AllowedMentions(roles=False, users=False, everyone=False)
        allowed_with_pings = discord.AllowedMentions(roles=True, users=False, everyone=False)

        # Files for voting channel (and later, for announcement channel if configured)
        files_for_voting: List[discord.File] = []
        if transcript_bytes:
            files_for_voting.append(
                discord.File(io.BytesIO(transcript_bytes), filename=f"deliberation_{m.id}.json")
            )

        # Always send result in the voting channel, without role pings
        await voting_ch.send(
            embed=embed,
            files=files_for_voting or None,
            allowed_mentions=allowed_no_pings
        )

        # Role pings and also transcript in the explicit announcement channel (if configured and distinct)
        if announce_ch and announce_ch.id != voting_ch.id:
            ping_roles = self._get_announce_ping_roles(c, guild)
            content = " ".join(r.mention for r in ping_roles) if ping_roles else None

            files_for_announce: List[discord.File] = []
            if transcript_bytes:
                files_for_announce.append(
                    discord.File(io.BytesIO(transcript_bytes), filename=f"deliberation_{m.id}.json")
                )

            await announce_ch.send(
                content=content,
                embed=embed,
                files=files_for_announce or None,
                allowed_mentions=allowed_with_pings
            )

        # Auto-announce next in queue in voting channel (ALWAYS here)
        if c.config.get("motion.queue", False) and c.motion_queue:
            c.current_motion = c.motion_queue.pop(0)
            self.store.put_council(c)
            log.info("Auto-promoted motion #%s from queue: %s", c.current_motion.id, c.current_motion.title)
            await self._post_live_and_thread(c, guild, ping_new=True)

        # Cleanup thread if not keeping transcripts
        if m.thread_id and not c.config.get("keep.transcripts", False):
            try:
                thread = guild.get_thread(m.thread_id)
                if thread:
                    await thread.delete(reason="USTC Congress: motion resolved, transcripts not kept")
            except Exception as e:
                log.warning("Thread deletion failed: %s", e)

    @app_commands.command(name="votinghelp", description="Show voting and motion commands.")
    async def votinghelp(self, interaction: discord.Interaction) -> None:
        e = discord.Embed(title="USTC Congress — Voting Commands", color=discord.Color.blurple())
        e.description = "You must have the configured councilor role to vote."

        # Voting commands
        e.add_field(name="/yes reason:<optional>", value="Vote YES on the current motion.", inline=False)
        e.add_field(name="/no reason:<optional>", value="Vote NO on the current motion.", inline=False)
        e.add_field(name="/abstain reason:<optional>", value="Abstain from voting.", inline=False)
        e.add_field(name="/lazyvoters", value="Mention councilors who haven’t voted.", inline=False)

        # Motion usage
        e.add_field(
            name="/motion usage",
            value=(
                "`/motion action:new text:<required> title:<optional> majority:<optional>` — "
                "Create (or queue) a new motion.\n"
                "`/motion action:view` — Show the current live motion.\n"
                "`/motion action:kill` — Author or admin can kill the active motion."
            ),
            inline=False
        )

        e.set_footer(text="Tip: Voting replies are private; the live motion embed shows tallies and expiration.")
        await interaction.response.send_message(embed=e, ephemeral=True)

    # Expiration background loop
    async def _expiration_loop(self) -> None:
        await self.bot.wait_until_ready()
        log.info("Expiration loop started")
        while not self.bot.is_closed():
            try:
                for g in self.bot.guilds:
                    for ch in g.text_channels:
                        c = self.store.get_council(g.id, ch.id)
                        if not c or not c.current_motion: continue
                        m = c.current_motion
                        if not m.expires_at: continue
                        ex = from_iso(m.expires_at)
                        if not ex or utcnow() < ex: continue

                        yes, no, _ = m.tallies(c, g)
                        if yes > no:
                            await self._resolve_and_announce(c, g, "passed")
                        elif no > yes:
                            await self._resolve_and_announce(c, g, "failed")
                        else:
                            # Tie outcome (no tiebreaker)
                            await self._resolve_and_announce(c, g, "tied")
            except Exception as e:
                log.warning("Expiration loop error: %s", e)
            await asyncio.sleep(60)


# --------------------------
# Bootstrap
# --------------------------
def build_bot() -> commands.Bot:
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    intents.message_content = True

    bot = commands.Bot(command_prefix="!", intents=intents)
    store = Store()
    votum_cog = Votum(bot, store)

    @bot.event
    async def on_ready() -> None:
        log.info("Logged in as %s (%s)", bot.user, bot.user.id)
        # per-guild add + sync
        for g in bot.guilds:
            for cmd in votum_cog.__cog_app_commands__:
                try:
                    bot.tree.add_command(cmd, guild=g)
                except Exception:
                    pass
            try:
                cmds = await bot.tree.sync(guild=g)
                log.info("Synced %d commands to guild %s (%s)", len(cmds), g.name, g.id)
            except Exception as e:
                log.warning("Guild sync failed for %s: %s", g.name, e)

        try:
            await bot.tree.sync()
        except Exception as e:
            log.warning("Global sync failed: %s", e)

        if not votum_cog.expirer_started:
            bot.loop.create_task(votum_cog._expiration_loop())
            votum_cog.expirer_started = True

    async def setup_hook():
        await bot.add_cog(votum_cog)
    bot.setup_hook = setup_hook

    return bot

def main() -> None:
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN environment variable is not set.")

    bot = build_bot()
    bot.run(token)

if __name__ == "__main__":
    main()