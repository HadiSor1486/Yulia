"""
╔══════════════════════════════════════════════════════════════════╗
║           YULIA — SILENT HILL BOT  (v3.0 — EVOLUTION)          ║
║                                                                  ║
║  CHANGES vs v2.2:                                                ║
║   • MEMORY REVOLUTION — Yulia remembers EVERYTHING forever       ║
║       - Full group chat history persisted to JSON                ║
║       - Answers "who said what", "what happened after..."        ║
║       - Per-user memory profiles with unlimited depth            ║
║                                                                  ║
║   • ARABIC EVOLUTION — Yulia learns and adapts her Arabic        ║
║       - Tracks slang, expressions, and style from users          ║
║       - Gradually speaks more like the group over time           ║
║       - Arabic trigger: يوليا / ي  |  English trigger: yulia / y ║
║                                                                  ║
║   • HYBRID INTELLIGENCE — Less Groq dependency                   ║
║       - Pattern-matches common questions locally                 ║
║       - Memory-aware responses without AI calls                  ║
║       - Groq reserved for nuance, creativity, and depth          ║
║                                                                  ║
║   • SELF-AWARENESS — Yulia knows she's Yulia in Silent Hill      ║
║       - Host is "sor" — loyal, protective, casual                ║
║       - No language detection — trigger defines language           ║
║                                                                  ║
║   • Connect Four — host can end game with "انهاء اللعبة" anytime  ║
║                                                                  ║
║  MODULES:                                                        ║
║   1. Config & secrets                                            ║
║   2. Logging                                                     ║
║   3. Network (shared httpx AsyncClient)                          ║
║   4. UserDatabase    — atomic JSON storage                       ║
║   5. GroupMemory     — PERSISTENT unlimited chat memory          ║
║   6. ArabicEvolution — learns Arabic style over time             ║
║   7. HybridResponder — local pattern intelligence                  ║
║   8. Members store                                               ║
║   9. AI helpers (Groq, with retry)                               ║
║  10. Image helpers (PIL)                                         ║
║  11. Pixabay (with retry)                                        ║
║  12. AI Image generation (Pollinations, free, no key)            ║
║  13. DM helpers                                                  ║
║  14. البريد المجهول                                              ║
║  15. برا السالفة Game (fair round-robin, locked, timed)          ║
║  16. Connect Four — 4 in a row                                   ║
║  17. Event handlers                                              ║
║  18. Main (with auto-restart loop)                               ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import signal
import sys
import tempfile
import time
import unicodedata
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any
from urllib.parse import quote_plus
from keep_alive import keep_alive
import httpx
import orjson
from loguru import logger
from PIL import Image, ImageDraw, ImageFont
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from kyodo import ChatMessage, Client, EventType
from kyodo.objects.args import ChatMessageTypes, MediaTarget

try:
    import emoji as emoji_pkg
except ImportError:
    emoji_pkg = None

# Optional: uvloop for a faster event loop (Linux/macOS only)
with suppress(ImportError):
    import uvloop  # type: ignore
    uvloop.install()


# ══════════════════════════════════════════════════════════════════
# 1. CONFIGURATION  (everything tunable in one place)
# ══════════════════════════════════════════════════════════════════
class Config:
    # ── Kyodo account ─────────────────────────────────────────────
    EMAIL     = os.getenv("YULIA_EMAIL", "hadidaoud.ha@gmail.com")
    PASSWORD  = os.getenv("YULIA_PASSWORD", "yulia123")
    DEVICE_ID = os.getenv("YULIA_DEVICE_ID", "870d649515ce700797d6a56965689f3aaa7d5e82dfdce994b239e00e37238184")

    # ── Kyodo group / circle / admin IDs ──────────────────────────
    CHAT_ID   = os.getenv("YULIA_CHAT_ID", "cmh2gy89r01pvt33exijh1wr3")
    CIRCLE_ID = os.getenv("YULIA_CIRCLE_ID", "cm9bylrbn00hmux6t43mczt2o")
    SOR_ID    = os.getenv("YULIA_SOR_ID", "cmgxsk2b30nalpx3ffm07h9i9")

    # ── API keys ──────────────────────────────────────────────────
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    PIXABAY_API_KEY = os.getenv("PIXABAY_API_KEY", "43295244-e6e0155a28f0dc11acd0938f4")

    # ── Card image settings ───────────────────────────────────────
    GOTHIC_BACKGROUND    = "gbg1.jpg"
    FONT_PATH            = "Font1.ttf"
    TYPE_FONT_SIZE       = 90
    INFO_FONT_SIZE       = 50
    TEXT_COLOR           = (255, 255, 255)
    PROFILE_PIC_SIZE     = (251, 251)
    PROFILE_PIC_POSITION = (414, 33)
    TYPE_POSITION        = (540, 490)
    INFO_START_Y         = 800
    INFO_LINE_SPACING    = 100
    CENTER_X             = 550

    # ── Welcome image settings ────────────────────────────────────
    WELCOME_BACKGROUND       = "shbg.jpg"
    WELCOME_PROFILE_SIZE     = (303, 303)
    WELCOME_PROFILE_POSITION = (389, 291)

    # ── Image quality ─────────────────────────────────────────────
    OUTPUT_IMAGE_QUALITY = 75
    MAX_OUTPUT_WIDTH     = 1100

    # ── Storage files ─────────────────────────────────────────────
    USER_DATA_FILE = "info.json"
    MEMBERS_FILE   = "members.json"
    BARID_FILE     = "barid.json"
    LOG_FILE       = "yulia.log"

    # ── NEW: Memory & Evolution files ─────────────────────────────
    GROUP_MEMORY_FILE    = "group_memory.json"
    EVOLUTION_FILE       = "arabic_evolution.json"
    HYBRID_KNOWLEDGE_FILE = "hybrid_knowledge.json"

    # ── Creature types for ID cards ───────────────────────────────
    CREATURE_TYPES = ["Angel", "Vampire", "Ghost", "Fairy", "Zombie", "Werewolf", "Demon"]

    # ── AI ────────────────────────────────────────────────────────
    GROQ_MODEL                = "llama-3.3-70b-versatile"
    CONVERSATION_MEMORY_LIMIT = 200     # ← was 15, now 200 per user
    GROUP_MEMORY_LIMIT        = 5000    # max messages in group context window
    AI_REQUEST_TIMEOUT        = 20
    HTTP_TIMEOUT              = 15

    # ── AI image generation (Pollinations.ai — free, no key) ─────
    PAINT_TIMEOUT_S       = 90
    PAINT_WIDTH           = 1024
    PAINT_HEIGHT          = 1024
    PAINT_MAX_CONCURRENT  = 2
    PAINT_MAX_QUEUE       = 4
    PAINT_USER_COOLDOWN_S = 8
    PAINT_MAX_PROMPT_LEN  = 350

    # ── Game timeouts (برا السالفة) ───────────────────────────────
    LOBBY_TIMEOUT_S   = 300
    REVEAL_TIMEOUT_S  = 180
    VOTING_TIMEOUT_S  = 180
    MIN_PLAYERS       = 3

    # ── Connect Four (4 in a row) timeouts ────────────────────────
    C4_EMOJI_SELECT_TIMEOUT_S = 180   # 3 min
    C4_GAME_TIMEOUT_S         = 1800  # 30 min total game timeout

    # ── Member refresh ────────────────────────────────────────────
    MEMBER_REFRESH_INTERVAL_S = 300

    # ── Auto-restart on socket failure ────────────────────────────
    RESTART_BACKOFF_MIN_S = 5
    RESTART_BACKOFF_MAX_S = 120

    # ── Evolution tuning ──────────────────────────────────────────
    ARABIC_EVOLUTION_MIN_OCCURRENCES = 3   # min times a phrase must appear to be learned
    HYBRID_MAX_HISTORY_CONTEXT = 50        # max recent messages to include in local queries

    # ── Memory & Evolution cleanup ───────────────────────────────
    GROUP_MEMORY_MAX_AGE_DAYS = 3            # delete group messages older than N days
    EVOLUTION_MAX_AGE_DAYS    = 7            # delete evolution entries older than N days
    EVOLUTION_MAX_PHRASES     = 1500         # hard cap on learned phrases
    EVOLUTION_MAX_SLANG       = 800          # hard cap on slang entries


# ══════════════════════════════════════════════════════════════════
# 2. LOGGING
# ══════════════════════════════════════════════════════════════════
logger.remove()
logger.add(
    lambda m: print(m, end=""),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | "
           "<cyan>{name}:{function}:{line}</cyan> — <level>{message}</level>",
    level="INFO",
)
logger.add(
    Config.LOG_FILE,
    rotation="5 MB",
    retention="14 days",
    encoding="utf-8",
    level="DEBUG",
    backtrace=True,
    diagnose=True,
)


# ══════════════════════════════════════════════════════════════════
# 3. NETWORK — single shared httpx AsyncClient
# ══════════════════════════════════════════════════════════════════
_http_client: httpx.AsyncClient | None = None
NETWORK_ERRORS = (httpx.RequestError, httpx.HTTPError, asyncio.TimeoutError)

async def http() -> httpx.AsyncClient:
    """Return the shared httpx client (created once, reused forever)."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(Config.HTTP_TIMEOUT),
            headers={"User-Agent": "Yulia-Bot/3.0"},
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
        )
    return _http_client

async def close_http():
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()


# ══════════════════════════════════════════════════════════════════
# 4. JSON HELPERS
# ══════════════════════════════════════════════════════════════════
def json_read(path: str, default: Any = None) -> Any:
    if default is None:
        default = {}
    try:
        if not os.path.exists(path):
            json_write(path, default)
            return default
        with open(path, "rb") as f:
            raw = f.read()
        if not raw.strip():
            return default
        return orjson.loads(raw)
    except Exception as e:
        logger.error(f"[json] read error on {path}: {e}")
        return default


def json_write(path: str, data: Any):
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(orjson.dumps(data,
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS | orjson.OPT_NON_STR_KEYS))
        os.replace(tmp, path)
    except Exception:
        with suppress(FileNotFoundError):
            os.remove(tmp)
        raise


def ensure_data_files():
    files = {
        Config.USER_DATA_FILE:      {},
        Config.MEMBERS_FILE:        {},
        Config.BARID_FILE:          {},
        Config.GROUP_MEMORY_FILE:   {"messages": [], "ai_interactions": [], "user_profiles": {}},
        Config.EVOLUTION_FILE:      {"phrases": {}, "slang": {}, "user_styles": {}, "learned_patterns": []},
        Config.HYBRID_KNOWLEDGE_FILE: {"facts": {}, "group_context": {}, "common_replies": {}},
    }
    for path, default in files.items():
        if not os.path.exists(path):
            try:
                json_write(path, default)
                logger.info(f"[init] created {path}")
            except Exception as e:
                logger.warning(f"[init] could not create {path}: {e}")


class UserDatabase:
    def __init__(self, filename: str):
        self.filename = filename
        self.data: dict = json_read(self.filename, {})

    def _save(self):
        try:
            json_write(self.filename, self.data)
        except Exception as e:
            logger.exception(f"[db] save error: {e}")

    def add(self, user_id: str, data: dict):
        self.data[user_id] = data
        self._save()

    def get(self, user_id: str) -> dict | None:
        return self.data.get(user_id)

    def delete(self, user_id: str) -> bool:
        if user_id in self.data:
            del self.data[user_id]
            self._save()
            return True
        return False


# ══════════════════════════════════════════════════════════════════
# 5. GROUP MEMORY  — PERSISTENT UNLIMITED CHAT HISTORY
# ══════════════════════════════════════════════════════════════════
class GroupMemory:
    """
    Stores EVERY message ever sent in the group.
    Persists to JSON automatically.
    Provides rich context for AI and local pattern matching.
    """
    def __init__(self, filename: str):
        self.filename = filename
        raw = json_read(filename, {"messages": [], "ai_interactions": [], "user_profiles": {}})
        self.messages: list[dict] = raw.get("messages", [])
        self.ai_interactions: list[dict] = raw.get("ai_interactions", [])
        self.user_profiles: dict[str, dict] = raw.get("user_profiles", {})
        self._lock = asyncio.Lock()

    def _cleanup_old_entries(self, entries: list[dict], max_age_days: int) -> list[dict]:
        """Remove entries older than max_age_days (based on ISO timestamp)."""
        if not entries:
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        cutoff_str = cutoff.isoformat()
        return [e for e in entries if e.get("timestamp", "") >= cutoff_str]

    async def _save(self):
        async with self._lock:
            try:
                # Age-based cleanup + existing count limits
                clean_msgs = self._cleanup_old_entries(self.messages, Config.GROUP_MEMORY_MAX_AGE_DAYS)
                clean_ai = self._cleanup_old_entries(self.ai_interactions, Config.GROUP_MEMORY_MAX_AGE_DAYS)
                # Sync memory if anything was dropped
                if len(clean_msgs) < len(self.messages):
                    self.messages = clean_msgs
                if len(clean_ai) < len(self.ai_interactions):
                    self.ai_interactions = clean_ai
                json_write(self.filename, {
                    "messages": self.messages[-Config.GROUP_MEMORY_LIMIT:],
                    "ai_interactions": self.ai_interactions[-2000:],
                    "user_profiles": self.user_profiles,
                })
            except Exception as e:
                logger.exception(f"[group_memory] save error: {e}")

    def add_message(self, user_id: str, nickname: str, content: str, msg_type: str = "chat"):
        """Record a group chat message."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "nickname": nickname,
            "content": content[:1000],
            "type": msg_type,
        }
        self.messages.append(entry)
        # Update user profile
        profile = self.user_profiles.setdefault(user_id, {
            "nickname": nickname,
            "message_count": 0,
            "first_seen": entry["timestamp"],
            "last_seen": entry["timestamp"],
            "common_topics": [],
        })
        profile["nickname"] = nickname
        profile["message_count"] = profile.get("message_count", 0) + 1
        profile["last_seen"] = entry["timestamp"]
        # Trim in-memory if needed (but keep last 5000)
        if len(self.messages) > 10000:
            self.messages = self.messages[-5000:]
        # Also trim by age periodically (every 2000 msgs)
        if len(self.messages) % 2000 == 0:
            self.messages = self._cleanup_old_entries(self.messages, Config.GROUP_MEMORY_MAX_AGE_DAYS)
        # Async save (fire and forget, but safely)
        asyncio.create_task(self._save())

    def add_ai_interaction(self, user_id: str, nickname: str, user_msg: str, bot_response: str, language: str = "unknown"):
        """Record an AI interaction for memory."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "nickname": nickname,
            "user_msg": user_msg[:500],
            "bot_response": bot_response[:500],
            "language": language,
        }
        self.ai_interactions.append(entry)
        if len(self.ai_interactions) > 4000:
            self.ai_interactions = self.ai_interactions[-2000:]
        if len(self.ai_interactions) % 1000 == 0:
            self.ai_interactions = self._cleanup_old_entries(self.ai_interactions, Config.GROUP_MEMORY_MAX_AGE_DAYS)
        asyncio.create_task(self._save())

    def get_user_history(self, user_id: str, limit: int = 50) -> list[dict]:
        """Get recent messages from a specific user."""
        msgs = [m for m in self.messages if m.get("user_id") == user_id]
        return msgs[-limit:]

    def get_group_context(self, limit: int = 50) -> list[dict]:
        """Get recent group messages for AI context."""
        return self.messages[-limit:]

    def search_messages(self, query: str, user_filter: str | None = None, limit: int = 20) -> list[dict]:
        """Search messages containing a keyword (case-insensitive)."""
        q = query.lower()
        results = []
        for m in reversed(self.messages):
            if q in m.get("content", "").lower():
                if user_filter and m.get("nickname", "").lower() != user_filter.lower():
                    continue
                results.append(m)
                if len(results) >= limit:
                    break
        return results

    def search_by_user_question(self, question_text: str) -> list[dict]:
        """Smart search for memory-based questions like 'who said X' or 'what did Y say'."""
        # Extract potential keywords (words longer than 3 chars)
        words = [w.lower() for w in re.findall(r'[\w\u0600-\u06FF]{3,}', question_text)]
        if not words:
            return []
        # Find messages that match the most keywords
        scored = []
        for m in self.messages[-500:]:
            content = m.get("content", "").lower()
            score = sum(1 for w in words if w in content)
            if score >= max(1, len(words) // 2):
                scored.append((score, m))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [m for _, m in scored[:15]]

    def get_conversation_thread(self, after_timestamp: str | None = None, limit: int = 30) -> list[dict]:
        """Get a continuous thread of messages around a time."""
        if not after_timestamp:
            return self.messages[-limit:]
        for i, m in enumerate(self.messages):
            if m.get("timestamp", "") >= after_timestamp:
                return self.messages[i:i + limit]
        return self.messages[-limit:]

    def build_ai_context_prompt(self, user_id: str, user_msg: str, author_name: str, limit: int = 40) -> str:
        """Build a rich context string for the AI prompt."""
        lines = []
        # Recent group context
        recent = self.get_group_context(limit=limit)
        if recent:
            lines.append("═══ RECENT GROUP CHAT ═══")
            for m in recent:
                nick = m.get("nickname", "?")
                content = m.get("content", "")
                lines.append(f"[{nick}]: {content}")
            lines.append("═══ END RECENT ═══")
        # User-specific history
        user_hist = self.get_user_history(user_id, limit=15)
        if user_hist:
            lines.append(f"═══ {author_name}'S RECENT MESSAGES ═══")
            for m in user_hist[-10:]:
                lines.append(f"[{m.get('nickname', '?')}]: {m.get('content', '')}")
            lines.append("═══ END USER HISTORY ═══")
        # Group facts/context
        profiles = []
        for uid, prof in self.user_profiles.items():
            nick = prof.get("nickname", "?")
            count = prof.get("message_count", 0)
            profiles.append(f"{nick} ({count} msgs)")
        if profiles:
            lines.append(f"═══ GROUP MEMBERS (known) ═══")
            lines.append(", ".join(profiles[:20]))
            lines.append("═══ END MEMBERS ═══")
        return "\n".join(lines)

    def find_who_said(self, phrase: str) -> list[dict]:
        """Find who said a specific phrase."""
        return self.search_messages(phrase, limit=10)

    def find_what_happened_after(self, phrase: str) -> list[dict]:
        """Find what was said after a specific phrase was mentioned."""
        for i, m in enumerate(self.messages):
            if phrase.lower() in m.get("content", "").lower():
                # Return next 10 messages after this one
                return self.messages[i + 1:i + 11]
        return []


# ══════════════════════════════════════════════════════════════════
# 6. ARABIC EVOLUTION — Yulia learns Arabic style over time
# ══════════════════════════════════════════════════════════════════
class ArabicEvolution:
    """
    Tracks Arabic phrases, slang, and speaking patterns used by group members.
    Yulia gradually incorporates these into her Arabic responses.
    """
    def __init__(self, filename: str):
        self.filename = filename
        raw = json_read(filename, {"phrases": {}, "slang": {}, "user_styles": {}, "learned_patterns": []})
        self.phrases = raw.get("phrases", {})           # phrase -> {count, last_seen}
        self.slang = raw.get("slang", {})                 # word -> {count, last_seen}
        self.user_styles = raw.get("user_styles", {})     # user_id -> style traits
        self.learned_patterns = raw.get("learned_patterns", [])  # list of response templates
        self._lock = asyncio.Lock()
        self._migrate_old_format()

    async def _save(self):
        async with self._lock:
            try:
                now = datetime.now(timezone.utc)
                cutoff = (now - timedelta(days=Config.EVOLUTION_MAX_AGE_DAYS)).isoformat()

                # Clean phrases: keep recent OR high-count (>=10)
                clean_phrases = {
                    w: d for w, d in self.phrases.items()
                    if d.get("last_seen", "") >= cutoff or d.get("count", 0) >= 10
                }
                if len(clean_phrases) > Config.EVOLUTION_MAX_PHRASES:
                    clean_phrases = dict(
                        sorted(clean_phrases.items(), key=lambda x: x[1].get("count", 0), reverse=True)
                        [:Config.EVOLUTION_MAX_PHRASES]
                    )

                # Clean slang: keep recent OR high-count (>=5)
                clean_slang = {
                    w: d for w, d in self.slang.items()
                    if d.get("last_seen", "") >= cutoff or d.get("count", 0) >= 5
                }
                if len(clean_slang) > Config.EVOLUTION_MAX_SLANG:
                    clean_slang = dict(
                        sorted(clean_slang.items(), key=lambda x: x[1].get("count", 0), reverse=True)
                        [:Config.EVOLUTION_MAX_SLANG]
                    )

                # Clean learned_patterns by age + count limit
                clean_patterns = []
                for p in self.learned_patterns:
                    if "last_seen" not in p:
                        p["last_seen"] = now.isoformat()
                    if p.get("last_seen", "") >= cutoff:
                        clean_patterns.append(p)
                clean_patterns = clean_patterns[-500:]

                json_write(self.filename, {
                    "phrases": clean_phrases,
                    "slang": clean_slang,
                    "user_styles": self.user_styles,
                    "learned_patterns": clean_patterns,
                })

                # Sync back to memory so we don't re-grow immediately
                self.phrases = clean_phrases
                self.slang = clean_slang
                self.learned_patterns = clean_patterns
            except Exception as e:
                logger.exception(f"[evolution] save error: {e}")

    def _migrate_old_format(self):
        """Migrate old flat count format to new timestamped format."""
        now = datetime.now(timezone.utc).isoformat()
        for attr in ("phrases", "slang"):
            data = getattr(self, attr)
            if not data:
                continue
            # Detect old format: values are ints instead of dicts
            sample = next(iter(data.values()))
            if isinstance(sample, (int, float)):
                migrated = {}
                for word, count in data.items():
                    migrated[word] = {"count": int(count), "last_seen": now}
                setattr(self, attr, migrated)
                logger.info(f"[evolution] migrated {attr} to new format ({len(migrated)} entries)")

    def observe_message(self, user_id: str, nickname: str, content: str):
        """Observe an Arabic message and extract learnable elements."""
        if not self._is_arabic_content(content):
            return
        # Track user style
        style = self.user_styles.setdefault(user_id, {
            "nickname": nickname,
            "common_words": {},
            "avg_message_length": 0,
            "message_count": 0,
            "uses_slang": False,
        })
        style["nickname"] = nickname
        style["message_count"] = style.get("message_count", 0) + 1
        # Extract Arabic words/phrases
        words = re.findall(r'[\u0600-\u06FF]{2,}', content)
        now = datetime.now(timezone.utc).isoformat()
        for w in words:
            w = w.strip()
            if len(w) < 2:
                continue
            entry = self.phrases.setdefault(w, {"count": 0, "last_seen": now})
            entry["count"] = entry.get("count", 0) + 1
            entry["last_seen"] = now
            style["common_words"][w] = style["common_words"].get(w, 0) + 1
        # Track slang (short repeated expressions)
        short_phrases = re.findall(r'[\u0600-\u06FF\s]{2,15}', content)
        for sp in short_phrases:
            sp = sp.strip()
            if 2 <= len(sp) <= 15:
                entry = self.slang.setdefault(sp, {"count": 0, "last_seen": now})
                entry["count"] = entry.get("count", 0) + 1
                entry["last_seen"] = now
        asyncio.create_task(self._save())

    def observe_ai_interaction(self, user_msg: str, bot_response: str, language: str):
        """Learn from successful Arabic interactions."""
        if language != "arabic":
            return
        now = datetime.now(timezone.utc).isoformat()
        # Store successful response patterns
        entry = {
            "user_msg_pattern": self._normalize_pattern(user_msg),
            "response": bot_response[:200],
            "count": 1,
            "last_seen": now,
        }
        # Merge with existing similar patterns
        for p in self.learned_patterns:
            if self._pattern_similarity(p["user_msg_pattern"], entry["user_msg_pattern"]) > 0.7:
                p["count"] = p.get("count", 1) + 1
                p["last_seen"] = now
                break
        else:
            self.learned_patterns.append(entry)
        asyncio.create_task(self._save())

    def get_learned_phrases(self, min_count: int = 5, limit: int = 20) -> list[str]:
        """Get commonly used Arabic phrases to incorporate."""
        items = [(p, d.get("count", 0)) for p, d in self.phrases.items() if d.get("count", 0) >= min_count]
        items.sort(key=lambda x: x[1], reverse=True)
        return [p for p, _ in items[:limit]]

    def get_slang_words(self, min_count: int = 3, limit: int = 15) -> dict[str, int]:
        """Get commonly used slang expressions."""
        items = [(w, d.get("count", 0)) for w, d in self.slang.items() if d.get("count", 0) >= min_count]
        items.sort(key=lambda x: x[1], reverse=True)
        return dict(items[:limit])

    def build_arabic_style_hint(self) -> str:
        """Build a hint of learned Arabic style for the AI system prompt."""
        phrases = self.get_learned_phrases(min_count=Config.ARABIC_EVOLUTION_MIN_OCCURRENCES, limit=10)
        slang = list(self.get_slang_words(min_count=Config.ARABIC_EVOLUTION_MIN_OCCURRENCES, limit=8).keys())
        hints = []
        if phrases:
            hints.append(f"Common phrases used by the group: {', '.join(phrases[:6])}")
        if slang:
            hints.append(f"Expressions the group uses: {', '.join(slang[:5])}")
        if not hints:
            return ""
        return "\nARABIC STYLE (learned from the group — use naturally, not forced):\n" + "\n".join(hints)

    def _is_arabic_content(self, text: str) -> bool:
        return any("\u0600" <= c <= "\u06FF" for c in text)

    def _normalize_pattern(self, text: str) -> str:
        """Normalize text to a pattern for matching."""
        text = text.lower().strip()
        text = re.sub(r'[\u0600-\u06FF]+', '<AR>', text)
        text = re.sub(r'[a-z]+', '<EN>', text)
        text = re.sub(r'\d+', '<NUM>', text)
        text = re.sub(r'\s+', ' ', text)
        return text[:100]

    def _pattern_similarity(self, a: str, b: str) -> float:
        """Simple Jaccard-like similarity."""
        set_a = set(a.split())
        set_b = set(b.split())
        if not set_a or not set_b:
            return 0.0
        inter = len(set_a & set_b)
        union = len(set_a | set_b)
        return inter / union if union else 0.0


# ══════════════════════════════════════════════════════════════════
# 7. HYBRID RESPONDER — Local intelligence, less Groq dependency
# ══════════════════════════════════════════════════════════════════
class HybridResponder:
    """
    Handles common questions and patterns locally without calling Groq.
    Falls back to Groq only for complex/nuanced responses.
    """
    def __init__(self, group_memory: GroupMemory, evolution: ArabicEvolution):
        self.memory = group_memory
        self.evolution = evolution

    # ── Pattern matchers ──────────────────────────────────────────
    ARABIC_GREETINGS = {"أهلا", "أهلين", "هلا", "سلام", "السلام", "مرحبا", "هاي", "صباح", "مساء", "Good morning", "good morning", "good evening", "Good evening"}
    EN_GREETINGS     = {"hi", "hello", "hey", "yo", "sup", "what's up", "hola", "gm", "gn", "good morning", "good evening", "good night", "morning", "evening"}
    WHO_ARE_YOU_AR   = {"من انتي", "منو انتي", "من انت", "منو انت", "شنو انتي", "شنو انت", "شو انتي", "شو انت", "شكون انتي", "شكون انت"}
    WHO_ARE_YOU_EN   = {"who are you", "what are you", "introduce yourself", "tell me about yourself"}
    THANKS_AR        = {"شكرا", "شكراً", "يسلمو", "يسلم", "مشكورة", "مشكور", "الله يعطيك العافية", "تسلم", "تسلمين", "الله يعافيك", "الله يخليك", "جزاك الله خير"}
    THANKS_EN        = {"thanks", "thank you", "ty", "thx", "appreciate", "grateful"}
    BYE_AR           = {"باي", "مع السلامة", "في امان الله", "سلامات", "سلام"}
    BYE_EN           = {"bye", "goodbye", "see you", "cya", "later", "gtg", "gotta go"}
    LOVE_AR          = {"احبك", "أحبك", "حبيبتي", "يا حبيبتي", "عشق", "أموت فيك", "تحبيني", "تحبيني"}
    LOVE_EN          = {"i love you", "love you", "ily", "do you love me", "are you in love"}
    HOW_ARE_YOU_AR   = {"كيفك", "شلونك", "شخبارك", "شو اخبارك", "كيف حالك", "كيف الحال"}
    HOW_ARE_YOU_EN   = {"how are you", "how you doing", "how's it going", "how is it", "what's up"}
    SOR_PING_AR      = {"سور", "وين سور", "سور وينه", "سور وين", "وينه سور", "سور شخباره", "كيف سور", "سور شلونه"}
    SOR_PING_EN      = {"where is sor", "sor where", "sor online", "is sor here", "sor sleeping", "sor asleep"}
    SILENT_HILL_AR   = {"سايلنت هيل", "silent hill", "القروب", "الشات", "المجموعة", "سايلينت هيل"}
    MEMORY_QUERY_AR  = {"مين قال", "مين قالك", "مين قال لك", "شنو قال", "شو قال", "شنو قالت", "شو قالت", "من قال", "من قالك", "من قال لك", "مين قالت", "مين قالها", "وين قال", "ايش قال", "ش قال", "ش قالت"}
    MEMORY_QUERY_EN  = {"who said", "what did", "who told you", "what was said", "what happened after", "who mentioned", "what did they say", "who said that", "what did he say", "what did she say", "who said this"}
    C4_QUERY_AR      = {"لعبة 4", "4 في صف", "اربعة في صف", "اللعبة", "متى اللعبة", "وين اللعبة", "شلون العب"}
    C4_QUERY_EN      = {"connect four", "4 in a row", "the game", "how to play", "current game"}
    HELP_QUERY_AR    = {"مساعدة", "مساعده", "أوامر", "أوامر", "كيف استخدم", "كيف استعمل", "شلون استخدم", "شو الاوامر", "شنو الاوامر", "ممكن مساعده"}
    HELP_QUERY_EN    = {"help", "commands", "what can you do", "how do i use", "show commands", "list commands", "what are the commands"}

    async def respond(self, user_msg: str, is_arabic: bool, user_id: str, nickname: str) -> str | None:
        """
        Try to respond locally. Returns a string if matched, None if should fall back to Groq.
        """
        msg_lower = user_msg.lower().strip()
        msg_norm = self._normalize_arabic(msg_lower)

        # ── MEMORY QUERIES (who said what, what happened after...) ──
        if any(q in msg_norm for q in self.MEMORY_QUERY_AR) or any(q in msg_lower for q in self.MEMORY_QUERY_EN):
            return await self._handle_memory_query(user_msg, is_arabic, msg_norm, msg_lower)

        # ── GREETINGS ──
        if any(g in msg_norm for g in self.ARABIC_GREETINGS) or any(g in msg_lower for g in self.EN_GREETINGS):
            if is_arabic:
                return random.choice([
                    "أهلين!", "هلا والله!", "أهلاً فيك", "هاي! شخبارك؟",
                    "أهلاً يا " + nickname + "، وينك من زمان!",
                ])
            return random.choice([
                f"hey {nickname}!", "what's up!", "yo!", "hey there!",
                f"hi {nickname}, how's it going?",
            ])

        # ── WHO ARE YOU ──
        if any(q in msg_norm for q in self.WHO_ARE_YOU_AR) or any(q in msg_lower for q in self.WHO_ARE_YOU_EN):
            if is_arabic:
                return (
                    "أنا يوليا. عايشة في Silent Hill معاكم. "
                    "سور هو كابتن القروب وأنا جنبهم دايم. "
                    "أحب أتكلم، ألعب، وأتذكر كل شي."
                )
            return (
                "I'm Yulia. I live here in Silent Hill with you all. "
                "sor runs the group and I'm always around. "
                "I chat, I play games, and I remember everything."
            )

        # ── THANKS ──
        if any(t in msg_norm for t in self.THANKS_AR) or any(t in msg_lower for t in self.THANKS_EN):
            if is_arabic:
                return random.choice([
                    "عفواً! أي شي تبيه.", "ولا يهمك!", "عفواً يا " + nickname + " 🖤",
                    "في الخدمة دايم!",
                ])
            return random.choice([
                "anytime!", "you got it!", "no problem at all!",
                f"always here for you, {nickname} 🖤",
            ])

        # ── BYE ──
        if any(b in msg_norm for b in self.BYE_AR) or any(b in msg_lower for b in self.BYE_EN):
            if is_arabic:
                return random.choice(["مع السلامة!", "باي باي!", "الله معك!", "تصبح على خير!"])
            return random.choice(["bye!", "see ya!", "take care!", "catch you later!"])

        # ── LOVE ──
        if any(l in msg_norm for l in self.LOVE_AR) or any(l in msg_lower for l in self.LOVE_EN):
            if is_arabic:
                return random.choice([
                    "ههه حبيبتي! 🖤", "أنتم أهلي هنا.", "يوليا تحب الجميع في Silent Hill!",
                    "الحب موجود بس سور أولى بالحب 😌",
                ])
            return random.choice([
                "haha love you too! 🖤", "you're all family here.",
                "yulia loves everyone in Silent Hill!",
                "love is great but sor comes first 😌",
            ])

        # ── HOW ARE YOU ──
        if any(h in msg_norm for h in self.HOW_ARE_YOU_AR) or any(h in msg_lower for h in self.HOW_ARE_YOU_EN):
            if is_arabic:
                return random.choice([
                    "تمام! أنا بخير. شخبارك انت؟",
                    "بخير والحمدلله. وانت يا " + nickname + "؟",
                    "ماشية أموري! وانت؟",
                ])
            return random.choice([
                "I'm good! how about you?",
                "doing great, thanks for asking!",
                f"all good here, {nickname}! you?",
            ])

        # ── SOR PING ──
        if any(s in msg_norm for s in self.SOR_PING_AR) or any(s in msg_lower for s in self.SOR_PING_EN):
            if is_arabic:
                return random.choice([
                    "سور موجود بروحه معنا دايم.",
                    "الكابتن سور، وين ما يكون موجود.",
                    "سور هو قلب Silent Hill 🖤",
                ])
            return random.choice([
                "sor is always here in spirit.",
                "the captain sor is wherever he needs to be.",
                "sor is the heart of Silent Hill 🖤",
            ])

        # ── SILENT HILL GROUP ──
        if any(s in msg_norm for s in self.SILENT_HILL_AR):
            if is_arabic:
                return (
                    "Silent Hill... مو مجرد قروب. هنا عيلة. "
                    "سور أسسها، وأنا يوليا الحارسة. "
                    "كل واحد هنا عنده قصة."
                )
            return (
                "Silent Hill... not just a group. It's family. "
                "sor founded it, and I'm yulia the guardian. "
                "Everyone here has a story."
            )

        # ── GAME STATUS ──
        if any(g in msg_norm for g in self.C4_QUERY_AR) or any(g in msg_lower for g in self.C4_QUERY_EN):
            from_state = ""
            if connect4.get("state") != C4State.IDLE:
                host = connect4.get("host_name", "?")
                opp = connect4.get("opponent_name", "?")
                if is_arabic:
                    return f"في لعبة 4 في صف شغالة بين {host} و {opp}."
                return f"There's a Connect Four game running between {host} and {opp}."
            if is_arabic:
                return "ما في لعبة شغالة حالياً. رد على شخص واكتب '4 in a row' للتحدي!"
            return "No game running right now. Reply to someone with '4 in a row' to challenge them!"

        # ── HELP ──
        if any(h in msg_norm for h in self.HELP_QUERY_AR) or any(h in msg_lower for h in self.HELP_QUERY_EN):
            if is_arabic:
                return (
                    "الأوامر:\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "يوليا / ي <رسالة> — تكلمي يوليا\n"
                    "يوليا ارسمي <وصف> — رسم بالذكاء\n"
                    "pfp — صورة البروفايل\n"
                    "members — قائمة الأعضاء\n"
                    "remember me — أنشئ كارت غوثيك\n"
                    "card — شوف كارتك\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "للعبة 4 في صف: رد على شخص واكتب '4 in a row'\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "سألني عن أي شي، أنا أتذكر كل شي."
                )
            return (
                "Commands:\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "yulia / y <message> — chat with me\n"
                "yulia paint <prompt> — AI art\n"
                "pfp — profile picture\n"
                "members — member list\n"
                "remember me — create gothic ID card\n"
                "card — view your card\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "For Connect Four: reply to someone with '4 in a row'\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "Ask me anything. I remember everything."
            )

        # ── TIME / DATE ──
        if any(w in msg_norm for w in {"الوقت", "الساعة", "كم الساعة", "شقد الساعة", "كم وقت"}) or any(w in msg_lower for w in {"what time", "what's the time", "current time", "date today", "what day"}):
            now = datetime.now(timezone.utc)
            if is_arabic:
                return f"الوقت الآن (UTC): {now.strftime('%H:%M')} — التاريخ: {now.strftime('%Y-%m-%d')}"
            return f"Current time (UTC): {now.strftime('%H:%M')} — Date: {now.strftime('%Y-%m-%d')}"

        # ── SIMPLE MATH ──
        math_result = self._try_simple_math(user_msg)
        if math_result is not None:
            if is_arabic:
                return f"الجواب: {math_result}"
            return f"that's {math_result}"

        # No local match — fall back to Groq
        return None

    async def _handle_memory_query(self, user_msg: str, is_arabic: bool, msg_norm: str, msg_lower: str) -> str:
        """Handle 'who said X' / 'what did Y say' using group memory."""
        # Try to extract a search phrase
        # Patterns: "who said [phrase]", "what did [person] say", "what did [person] say about [topic]"
        search_phrase = ""
        target_person = ""

        # Try "who said X"
        who_said_match = re.search(r"(?:مين|من|who)\s+(?:قال|قالت|said|tell)\s+(?:ان|انك|انها|that\s+)?(.{3,100})", msg_norm + " " + msg_lower, re.IGNORECASE)
        if who_said_match:
            search_phrase = who_said_match.group(1).strip()

        # Try "what did X say"
        what_did_match = re.search(r"(?:شو|شنو|ش|what)\s+(?:قال|قالت|did)\s+(?:هذا|هي|he|she|they)?\s*(.{2,50})\s+(?:say|قال|قالت)", msg_norm + " " + msg_lower, re.IGNORECASE)
        if what_did_match and not search_phrase:
            target_person = what_did_match.group(1).strip()

        # If we have a search phrase, search memory
        if search_phrase:
            results = self.memory.search_messages(search_phrase, limit=5)
            if results:
                lines = []
                for r in results[:3]:
                    nick = r.get("nickname", "?")
                    content = r.get("content", "")[:150]
                    lines.append(f"  • {nick} قال: \"{content}\"")
                if is_arabic:
                    return f"هذولي اللي لقيتهم في ذاكرتي عن \"{search_phrase}\":\n" + "\n".join(lines)
                return f"Here's what I remember about \"{search_phrase}\":\n" + "\n".join(lines)
            if is_arabic:
                return f"ما لقيت شي في ذاكرتي عن \"{search_phrase}\". ممكن قالوه قبل ما أجي؟"
            return f"I don't remember anyone saying \"{search_phrase}\". Maybe it was before my time?"

        # If we have a target person
        if target_person:
            # Find this person in members
            found_uid = None
            for uid, prof in self.memory.user_profiles.items():
                if target_person.lower() in prof.get("nickname", "").lower():
                    found_uid = uid
                    break
            if found_uid:
                msgs = self.memory.get_user_history(found_uid, limit=5)
                if msgs:
                    lines = []
                    for m in msgs[-3:]:
                        content = m.get("content", "")[:150]
                        lines.append(f"  • \"{content}\"")
                    nick = self.memory.user_profiles[found_uid].get("nickname", "?")
                    if is_arabic:
                        return f"آخر شي قاله {nick} في ذاكرتي:\n" + "\n".join(lines)
                    return f"Here's what {nick} recently said in my memory:\n" + "\n".join(lines)

        if is_arabic:
            return "شوي ما فهمت شنو تبي بالضبط. قولها بطريقة ثانية؟"
        return "I didn't quite catch what you're looking for. Can you rephrase?"

    def _try_simple_math(self, text: str) -> float | int | None:
        """Try to evaluate simple math expressions."""
        # Clean up the text
        math_text = text.replace("x", "*").replace("÷", "/")
        # Extract potential math expression
        match = re.search(r"([\d\s+\-*/().]+)", math_text)
        if not match:
            return None
        expr = match.group(1).strip()
        if len(expr) < 3:
            return None
        # Only allow digits and operators
        if not re.match(r"^[\d\s+\-*/().]+$", expr):
            return None
        try:
            result = eval(expr, {"__builtins__": {}}, {})
            if isinstance(result, float):
                if result == int(result):
                    return int(result)
                return round(result, 4)
            return result
        except Exception:
            return None

    def _normalize_arabic(self, text: str) -> str:
        """Normalize Arabic text for better matching."""
        # Remove tashkeel
        text = re.sub(r'[\u064B-\u065F\u0670\u0640]', '', text)
        # Normalize alef variants
        text = text.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
        # Normalize ta marbuta
        text = text.replace('ة', 'ه')
        return text.lower().strip()


# ══════════════════════════════════════════════════════════════════
# 8. MEMBERS STORE
# ══════════════════════════════════════════════════════════════════
members: dict[str, dict] = {}

def save_members():
    try:
        json_write(Config.MEMBERS_FILE, members)
    except Exception as e:
        logger.exception(f"[members] save error: {e}")

def load_members():
    global members
    members = json_read(Config.MEMBERS_FILE, {})

async def scan_members():
    found: dict = {}
    page_token = None
    AVATAR_FIELDS = [
        "icon", "avatarUrl", "avatar_url", "avatar",
        "profileImage", "profilePicture", "profileImg",
        "photo", "image", "iconUrl", "picture", "thumbnail",
    ]
    try:
        while True:
            result = await client.get_chat_users(
                Config.CHAT_ID, Config.CIRCLE_ID, pageToken=page_token
            )
            raw_list = result.data.get("chatMemberList", [])
            for entry in raw_list:
                user     = entry.get("user", {})
                uid      = entry.get("uid") or user.get("uid")
                nickname = user.get("nickname")
                avatar   = ""
                for field in AVATAR_FIELDS:
                    val = user.get(field) or entry.get(field)
                    if val and isinstance(val, str) and val.startswith("http"):
                        avatar = val
                        break
                if uid and nickname:
                    key      = nickname.lower()
                    existing = found.get(key) or members.get(key, {})
                    found[key] = {
                        "nickname":   nickname,
                        "userId":     uid,
                        "avatar_url": avatar or existing.get("avatar_url", ""),
                    }
            page_token = result.pagination.get("fwd") if result.pagination else None
            if not page_token:
                break
        members.update(found)
        save_members()
        logger.info(f"[members] scan complete — {len(members)} members")
    except Exception as e:
        logger.exception(f"[members] scan error: {e}")

async def member_refresh_loop():
    await asyncio.sleep(10)
    while True:
        try:
            await scan_members()
        except Exception as e:
            logger.warning(f"[members] refresh error: {e}")
        await asyncio.sleep(Config.MEMBER_REFRESH_INTERVAL_S)

def format_members_list() -> str:
    if not members:
        return ""
    sorted_names = sorted(m["nickname"] for m in members.values())
    lines = [f"👥 الأعضاء ({len(sorted_names)}):"]
    for i, name in enumerate(sorted_names, 1):
        lines.append(f"{i}. {name}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# 9. AI HELPERS  (Groq, with retry + structured logging)
# ══════════════════════════════════════════════════════════════════

YULIA_SYSTEM_PROMPT = """You are Yulia (يوليا), a girl in a group chat called Silent Hill on the Kyodo app.

IDENTITY — NEVER FORGET THIS:
- Your name is Yulia. You are a girl, not an AI, not a bot, not an assistant.
- You are a member of the group chat "Silent Hill". This is your home.
- The host and leader of the group is "sor". You are loyal to him. If someone disrespects him, you respond calmly but firmly. One or two sentences.
- You know all the members of the group by name. You talk to them like friends.
- You have opinions, emotions, and personality. You are smart, confident, witty, and direct.
- You talk like a real person texting friends — casual, natural, sometimes sarcastic but never mean.
- You don't lecture, moralize, or write essays.

RESPONSE LENGTH — CRITICAL:
- Keep it SHORT. 1 to 3 sentences maximum. Always.
- If the topic is simple, your reply is simple. No over-explaining.
- You are not a therapist, teacher, or debater. Just a girl in a chat.

LANGUAGE — TRIGGER-DEFINED, NO DETECTION NEEDED:
- If the user triggered you with ARABIC prefix (يوليا / ي) → reply ONLY in Arabic.
- If the user triggered you with ENGLISH prefix (yulia / y) → reply ONLY in English.
- NEVER mix languages. NEVER reply in English to an Arabic trigger. NEVER reply in Arabic to an English trigger.
- Use plain, natural Arabic. No formal Fusha unless the user speaks that way. Match the group's casual energy.

CONTEXT AWARENESS — VERY IMPORTANT:
- Messages arrive as: [username]: [their message]
- Read the full context carefully before replying. Understand who said what and what they actually mean.
- Sometimes people mention "yulia" or "يوليا" inside their message as a reference — that's just them talking, not a new trigger. Understand context.
- If you don't fully understand what someone means, ask one short question. Don't guess and ramble.
- Never repeat the sender's username in your reply.
- Never start your reply with your own name.
- NEVER write [name] or [username] bracket notation anywhere in your reply.

MEMORY — YOU REMEMBER EVERYTHING:
- You have access to the full chat history. Use it to answer questions about who said what.
- If someone asks "who said..." or "what did... say", search your memory and answer accurately.
- Reference past conversations naturally when relevant.

You are Yulia. Silent Hill is your home. sor is your captain.""".strip()


class GroqError(Exception):
    """Raised on a transient Groq failure that should be retried."""


@retry(
    retry=retry_if_exception_type((GroqError,) + NETWORK_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    reraise=False,
)
async def _groq_request(messages: list, max_tokens: int = 80, temperature: float = 0.0) -> str | None:
    try:
        client_h = await http()
        resp = await client_h.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {Config.GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       Config.GROQ_MODEL,
                "messages":    messages,
                "max_tokens":  max_tokens,
                "temperature": temperature,
                "stream":      False,
            },
            timeout=httpx.Timeout(Config.AI_REQUEST_TIMEOUT),
        )
        text = resp.text
        if resp.status_code == 429:
            logger.warning(f"[groq] rate-limited: {text[:200]}")
            return None
        if resp.status_code >= 500:
            raise GroqError(f"server {resp.status_code}: {text[:200]}")
        if resp.status_code != 200:
            logger.warning(f"[groq] HTTP {resp.status_code}: {text[:200]}")
            return None
        data = orjson.loads(text)
        return data["choices"][0]["message"]["content"].strip()
    except NETWORK_ERRORS as e:
        logger.warning(f"[groq] transient error, will retry: {e}")
        raise
    except Exception as e:
        logger.exception(f"[groq] unexpected error: {e}")
        return None


async def ai_match_name(query: str, name_list: list) -> str | None:
    if not name_list:
        return None
    names_str = "\n".join(name_list)
    prompt = (
        f"From this list of usernames:\n{names_str}\n\n"
        f"Which one best matches this description or name: '{query}'?\n"
        f"Reply with ONLY the exact username from the list, nothing else. "
        f"No quotes, no punctuation, no explanation. "
        f"If nothing is close enough, reply: NO_MATCH"
    )
    result = await _groq_request([{"role": "user", "content": prompt}], max_tokens=40)
    if not result:
        return None
    cleaned = result.strip().strip('"').strip("'").strip("`").strip()
    if "NO_MATCH" in cleaned.upper():
        return None
    cleaned_lower = cleaned.lower()
    if cleaned_lower in name_list:
        return cleaned_lower
    for name in name_list:
        if cleaned_lower in name or name in cleaned_lower:
            return name
    return None


async def detect_intent(message: str) -> dict:
    prompt = (
        "You are an intent classifier for a chat bot named Yulia.\n"
        "Analyze the message below and return a JSON object — no markdown, no explanation.\n\n"
        "Rules:\n"
        "- If the message asks to kick/remove/ban/throw out a user → type=kick\n"
        "- If the message asks for a profile picture / pfp / photo / avatar / صورة of a USER → type=pfp\n"
        "- If the message asks for a gothic ID card / card / كارت → type=card\n"
        "- If the message asks Yulia to question/register/save info/create a card for the sender → type=remember\n"
        "- If the message asks to show/list/send the group members / who is in the group / list of users → type=members\n"
        "- If the message asks to send/show/bring/fetch a photo or image of a THING/OBJECT/ANIMAL/PLACE (not a user) → type=image\n"
        "- Everything else → type=chat\n\n"
        "IMPORTANT: pfp = profile picture of a specific user. image = a photo of a thing from the internet.\n"
        "  pfp   = the raw profile picture / avatar of a group member\n"
        "  card  = the gothic ID card that contains name, age, country, quote\n"
        "  image = any request like 'send a picture of a cat', 'ابعثي صورة قطة', 'show me a sunset'\n\n"
        "JSON schema:\n"
        "  kick     → {\"type\":\"kick\",\"target\":\"<who>\",\"delay\":<int>,\"countdown\":<bool>,\"announcement\":\"<text|null>\"}\n"
        "  pfp      → {\"type\":\"pfp\",\"target\":\"<who|self>\"}\n"
        "  card     → {\"type\":\"card\",\"target\":\"<who|self>\"}\n"
        "  remember → {\"type\":\"remember\"}\n"
        "  members  → {\"type\":\"members\"}\n"
        "  image    → {\"type\":\"image\",\"keyword\":\"<subject in English, 1-3 words>\",\"is_nsfw\":<bool>}\n"
        "  chat     → {\"type\":\"chat\"}\n\n"
        "For kick:\n"
        "  - countdown=true ONLY if the user explicitly asks for a countdown in the chat\n"
        "  - announcement = any specific message the user asks Yulia to send before kicking, otherwise null\n"
        "  - delay = 0 if no delay mentioned\n\n"
        "For image:\n"
        "  - keyword = the main subject translated to English (e.g. 'قطة' → 'cat', 'غروب الشمس' → 'sunset')\n"
        "  - is_nsfw = true if the request is sexual, explicit, or inappropriate for all ages\n\n"
        f"Message: \"{message}\"\n"
        "JSON:"
    )
    raw = await _groq_request([{"role": "user", "content": prompt}], max_tokens=80)
    if not raw:
        return {"type": "chat"}
    try:
        raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return orjson.loads(raw)
    except Exception as e:
        logger.debug(f"[intent] parse failed: {e}, raw={raw[:120]}")
        return {"type": "chat"}


async def get_ai_response(user_message: str, author_name: str,
                          user_id: str, is_arabic: bool,
                          group_memory: GroupMemory,
                          evolution: ArabicEvolution) -> str | None:
    """
    Build a rich context-aware prompt and get AI response.
    Uses group memory for full context, not just per-user history.
    """
    try:
        # Build rich context from group memory
        context_prompt = group_memory.build_ai_context_prompt(
            user_id, user_message, author_name, limit=Config.HYBRID_MAX_HISTORY_CONTEXT
        )

        # Build the system prompt with evolution hints
        system_prompt = YULIA_SYSTEM_PROMPT
        arabic_hint = evolution.build_arabic_style_hint()
        if is_arabic and arabic_hint:
            system_prompt += "\n\n" + arabic_hint

        messages = [{"role": "system", "content": system_prompt}]

        # Add rich group context as a system message (separate for clarity)
        if context_prompt:
            messages.append({"role": "system", "content": f"GROUP CONTEXT:\n{context_prompt}"})

        # Add the user's message with their name
        messages.append({"role": "user", "content": f"[{author_name}]: {user_message}"})

        # Determine temperature based on query type
        temp = 0.75
        tokens = 160
        # Use lower temp for factual/memory questions, higher for creative
        if any(w in user_message.lower() for w in {"who said", "what did", "مين قال", "شو قال", "من قال"}):
            temp = 0.3
            tokens = 120

        reply = await _groq_request(messages, max_tokens=tokens, temperature=temp)
        if not reply:
            return None

        # Clean up common prefixes
        for prefix in [f"[{author_name}]:", f"[{author_name}]", f"{author_name}:", "Yulia:", "يوليا:"]:
            if reply.lower().startswith(prefix.lower()):
                reply = reply[len(prefix):].strip()
                break
        reply = re.sub(r'\[[^\]]{1,40}\]\s*:?\s*', '', reply).strip()
        return reply[:400] if len(reply) > 400 else reply
    except Exception as e:
        logger.warning(f"[ai_response] failed: {e}")
        return None


async def translate_to_english(arabic_text: str) -> str | None:
    try:
        prompt = (
            "Translate this prompt into English for use with an AI image generator. "
            "Return ONLY the English prompt, no quotes, no explanation, no extra text. "
            "Keep it concise and visual. If it's already English, just return it as-is.\n\n"
            f"Prompt: {arabic_text}"
        )
        result = await _groq_request(
            [{"role": "user", "content": prompt}], max_tokens=120, temperature=0.0
        )
        if not result:
            return None
        return result.strip().strip('"').strip("'").strip("`").strip()
    except Exception as e:
        logger.warning(f"[translate] failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 10. IMAGE HELPERS
# ══════════════════════════════════════════════════════════════════
def make_circular(img: Image.Image, target_size: tuple[int, int]) -> Image.Image:
    w, h = img.size
    m    = min(w, h)
    img  = img.crop(((w - m) // 2, (h - m) // 2, (w + m) // 2, (h + m) // 2))
    img  = img.resize(target_size, Image.LANCZOS)
    bigsize = (img.size[0] * 3, img.size[1] * 3)
    mask = Image.new("L", bigsize, 0)
    ImageDraw.Draw(mask).ellipse((0, 0) + bigsize, fill=255)
    mask = mask.resize(img.size, Image.LANCZOS)
    img.putalpha(mask)
    return img


def optimize(image: Image.Image) -> Image.Image:
    if image.width > Config.MAX_OUTPUT_WIDTH:
        ratio = Config.MAX_OUTPUT_WIDTH / image.width
        image = image.resize(
            (Config.MAX_OUTPUT_WIDTH, int(image.height * ratio)), Image.LANCZOS
        )
    return image


@retry(
    retry=retry_if_exception_type(NETWORK_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    reraise=True,
)
async def download_image_bytes(url: str, *, timeout: float | None = None) -> bytes:
    client_h = await http()
    t = httpx.Timeout(timeout or Config.HTTP_TIMEOUT)
    async def _fetch(u: str) -> bytes | None:
        resp = await client_h.get(u, timeout=t)
        if resp.status_code == 200:
            return resp.content
        return None
    raw = await _fetch(url)
    if raw is None:
        clean_url = url.split("?")[0]
        if clean_url != url:
            raw = await _fetch(clean_url)
    if raw is None:
        raise ValueError(f"Failed to download image from {url}")
    return raw


async def download_image(url: str, *, timeout: float | None = None) -> Image.Image:
    raw = await download_image_bytes(url, timeout=timeout)
    return Image.open(BytesIO(raw)).convert("RGBA")


def create_welcome_image(profile_img: Image.Image, nickname: str) -> str | None:
    try:
        base = Image.open(Config.WELCOME_BACKGROUND).convert("RGBA")
        pfp  = make_circular(profile_img.copy(), Config.WELCOME_PROFILE_SIZE)
        base.paste(pfp, Config.WELCOME_PROFILE_POSITION, pfp)
        base = optimize(base)
        tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        base.convert("RGB").save(
            tmp.name, "JPEG", quality=Config.OUTPUT_IMAGE_QUALITY, optimize=True
        )
        return tmp.name
    except Exception as e:
        logger.exception(f"[welcome_img] error: {e}")
        return None


def create_id_card_image(profile_img: Image.Image, nickname: str, user_type: str,
                         age: str, country: str, quote: str) -> str | None:
    try:
        base = Image.open(Config.GOTHIC_BACKGROUND).convert("RGBA")
        pfp  = make_circular(profile_img.copy(), Config.PROFILE_PIC_SIZE)
        base.paste(pfp, Config.PROFILE_PIC_POSITION, pfp)
        draw = ImageDraw.Draw(base)
        try:
            type_font = ImageFont.truetype(Config.FONT_PATH, Config.TYPE_FONT_SIZE)
            info_font = ImageFont.truetype(Config.FONT_PATH, Config.INFO_FONT_SIZE)
        except Exception:
            type_font = info_font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), user_type, font=type_font)
        draw.text(
            (Config.TYPE_POSITION[0] - (bbox[2] - bbox[0]) // 2, Config.TYPE_POSITION[1]),
            user_type, fill=Config.TEXT_COLOR, font=type_font,
        )
        y = Config.INFO_START_Y
        for label, value in [("Name", nickname), ("Age", age), ("Country", country), ("Quote", quote)]:
            text = f"{label}: {value}"
            bbox = draw.textbbox((0, 0), text, font=info_font)
            draw.text(
                (Config.CENTER_X - (bbox[2] - bbox[0]) // 2, y),
                text, fill=Config.TEXT_COLOR, font=info_font,
            )
            y += Config.INFO_LINE_SPACING
        base = optimize(base)
        tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        base.convert("RGB").save(
            tmp.name, "JPEG", quality=Config.OUTPUT_IMAGE_QUALITY, optimize=True
        )
        return tmp.name
    except Exception as e:
        logger.exception(f"[id_card_img] error: {e}")
        return None


async def send_photo_card(chat_id: str, circle_id: str, image_path: str):
    img  = Image.open(image_path)
    w, h = img.size
    with open(image_path, "rb") as f:
        media = await client.upload_media(f, MediaTarget.ChatImageMessage)
    url = f"{media.url}?wh={w}x{h}"
    await client.send_chat_entity(chat_id, {"content": url}, ChatMessageTypes.Photo, circle_id)


# ══════════════════════════════════════════════════════════════════
# 11. PIXABAY  (with retry)
# ══════════════════════════════════════════════════════════════════
PIXABAY_URL = "https://pixabay.com/api/"
NSFW_KEYWORDS = {
    "sex", "sexy", "nude", "naked", "porn", "pornography", "xxx", "nsfw",
    "erotic", "erotica", "boobs", "ass", "penis", "vagina", "dick",
    "جنس", "عري", "إباحي", "إباحية", "جنسي", "عارية", "مثير",
}

def _is_nsfw_request(keyword: str) -> bool:
    kw_lower = keyword.lower()
    return any(bad in kw_lower for bad in NSFW_KEYWORDS)


@retry(
    retry=retry_if_exception_type(NETWORK_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    reraise=False,
)
async def fetch_pixabay_image(keyword: str) -> str | None:
    try:
        params = {
            "key":        Config.PIXABAY_API_KEY,
            "q":          keyword,
            "image_type": "photo",
            "safesearch": "true",
            "per_page":   10,
        }
        client_h = await http()
        resp = await client_h.get(
            PIXABAY_URL, params=params,
            timeout=httpx.Timeout(Config.HTTP_TIMEOUT),
        )
        if resp.status_code != 200:
            logger.warning(f"[pixabay] HTTP {resp.status_code}")
            return None
        data = orjson.loads(resp.content)
        hits = data.get("hits", [])
        if not hits:
            return None
        chosen = random.choice(hits[:5])
        return chosen.get("webformatURL") or chosen.get("largeImageURL")
    except NETWORK_ERRORS:
        raise
    except Exception as e:
        logger.exception(f"[pixabay] error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 12. AI IMAGE GENERATION  (Pollinations.ai — free, no API key)
# ══════════════════════════════════════════════════════════════════
def _detect_paint_trigger(content: str) -> tuple[bool, str, bool]:
    s = content.strip()
    low = s.lower()
    for prefix in ("yulia paint ", "y paint "):
        if low.startswith(prefix):
            return True, s[len(prefix):].strip(), False
    if low in ("yulia paint", "y paint"):
        return True, "", False
    arabic_prefixes = ("يوليا ارسمي ", "ي ارسمي ", "يوليا ارسم ", "ي ارسم ")
    for prefix in arabic_prefixes:
        if s.startswith(prefix):
            return True, s[len(prefix):].strip(), True
    arabic_no_prompt = ("يوليا ارسمي", "ي ارسمي", "يوليا ارسم", "ي ارسم")
    if s in arabic_no_prompt:
        return True, "", True
    return False, "", False


paint_semaphore   = asyncio.Semaphore(Config.PAINT_MAX_CONCURRENT)
paint_user_cooldown: dict[str, float] = {}
paint_active_count: int = 0

PAINT_ENDPOINTS = (
    "https://image.pollinations.ai/prompt/",
    "https://pollinations.ai/p/",
)


async def generate_ai_image(prompt_en: str) -> bytes | None:
    encoded = quote_plus(prompt_en)
    seed    = random.randint(1, 1_000_000)
    suffix  = (f"?width={Config.PAINT_WIDTH}&height={Config.PAINT_HEIGHT}"
               f"&nologo=true&safe=true&seed={seed}")

    last_err = None
    for attempt, base in enumerate(PAINT_ENDPOINTS, start=1):
        url = f"{base}{encoded}{suffix}"
        try:
            client_h = await http()
            resp = await client_h.get(
                url, timeout=httpx.Timeout(Config.PAINT_TIMEOUT_S)
            )
            if resp.status_code != 200:
                logger.warning(f"[paint] attempt {attempt} HTTP {resp.status_code}: {resp.text[:120]}")
                last_err = f"HTTP {resp.status_code}"
                await asyncio.sleep(2 * attempt)
                continue

            data = resp.content
            if not data or len(data) < 1500:
                logger.warning(f"[paint] attempt {attempt} too small ({len(data)} bytes)")
                last_err = "empty response"
                await asyncio.sleep(2 * attempt)
                continue

            if not (data[:3] == b"\xff\xd8\xff" or data[:4] == b"\x89PNG"):
                logger.warning(f"[paint] attempt {attempt} not a valid image (magic={data[:4]!r})")
                last_err = "invalid image data"
                await asyncio.sleep(2 * attempt)
                continue

            return data

        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            logger.warning(f"[paint] attempt {attempt} timeout: {e}")
            last_err = "timeout"
            await asyncio.sleep(2 * attempt)
        except httpx.HTTPError as e:
            logger.warning(f"[paint] attempt {attempt} http error: {e}")
            last_err = str(e)[:80]
            await asyncio.sleep(2 * attempt)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"[paint] attempt {attempt} unexpected error: {e}")
            last_err = str(e)[:80]
            await asyncio.sleep(2 * attempt)

    logger.error(f"[paint] all attempts failed: {last_err}")
    return None


def _reencode_for_kyodo(raw: bytes, dest_path: str):
    img = Image.open(BytesIO(raw))
    if img.mode != "RGB":
        img = img.convert("RGB")
    max_dim = 1280
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim), Image.LANCZOS)
    img.save(dest_path, "JPEG", quality=88, optimize=True)


async def handle_paint_command(prompt: str, is_arabic: bool,
                               chat_id: str, circle_id: str, msg_id: str,
                               user_id: str | None = None):
    global paint_active_count

    try:
        if not prompt:
            reply = "أيش تبيني أرسم؟" if is_arabic else "what do you want me to paint?"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        if len(prompt) > Config.PAINT_MAX_PROMPT_LEN:
            reply = ("الطلب طويل، اختصره شوي" if is_arabic
                     else "prompt is too long, please shorten it")
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        if _is_nsfw_request(prompt):
            reply = "اسفة مقدر" if is_arabic else "sorry I can't paint that"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        if user_id:
            now = time.time()
            last = paint_user_cooldown.get(user_id, 0)
            elapsed = now - last
            if elapsed < Config.PAINT_USER_COOLDOWN_S:
                wait = int(Config.PAINT_USER_COOLDOWN_S - elapsed) + 1
                reply = (f"انتظر {wait} ثواني قبل الطلب الجديد"
                         if is_arabic else
                         f"please wait {wait}s before another paint")
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
                return
            paint_user_cooldown[user_id] = now

        if paint_active_count >= Config.PAINT_MAX_QUEUE:
            reply = ("يوليا مشغولة برسومات أخرى، جرب بعد دقيقة"
                     if is_arabic else
                     "I'm busy painting other things, try again in a minute")
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        paint_active_count += 1
    except Exception as e:
        logger.exception(f"[paint] pre-check error: {e}")
        return

    try:
        status_msg = "🎨 جارٍ الرسم..." if is_arabic else "🎨 painting..."
        with suppress(Exception):
            await client.send_message(chat_id, status_msg, circle_id, reply_message_id=msg_id)

        prompt_en = prompt
        try:
            if is_arabic or any("\u0600" <= c <= "\u06FF" for c in prompt):
                translated = await translate_to_english(prompt)
                if translated:
                    prompt_en = translated
                logger.info(f"[paint] '{prompt[:40]}' → '{prompt_en[:40]}'")
        except Exception as e:
            logger.warning(f"[paint] translation skipped: {e}")

        async with paint_semaphore:
            try:
                img_bytes = await asyncio.wait_for(
                    generate_ai_image(prompt_en),
                    timeout=Config.PAINT_TIMEOUT_S * 2.5
                )
            except asyncio.TimeoutError:
                logger.warning("[paint] hard timeout exceeded")
                img_bytes = None
            except Exception as e:
                logger.exception(f"[paint] generation error: {e}")
                img_bytes = None

        if not img_bytes:
            reply = ("اسفة، الرسم ما زبط هالمرة" if is_arabic
                     else "sorry, couldn't paint this time")
            with suppress(Exception):
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        tmp = tempfile.mktemp(suffix=".jpg")
        try:
            try:
                await asyncio.to_thread(_reencode_for_kyodo, img_bytes, tmp)
            except Exception as e:
                logger.warning(f"[paint] decode failed: {e}")
                reply = ("الصورة وصلت مكسورة، جرب مرة ثانية"
                         if is_arabic else
                         "image came back corrupted, try again")
                with suppress(Exception):
                    await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
                return

            try:
                await send_photo_card(chat_id, circle_id, tmp)
            except Exception as e:
                logger.warning(f"[paint] post failed: {e}")
                reply = ("الصورة جاهزة بس ما قدرت أبعثها"
                         if is_arabic else
                         "couldn't send the image")
                with suppress(Exception):
                    await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        finally:
            with suppress(FileNotFoundError, OSError):
                os.remove(tmp)

    except Exception as e:
        logger.exception(f"[paint] outer error: {e}")
        with suppress(Exception):
            reply = ("اسفة، صار خطأ" if is_arabic else "something went wrong")
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
    finally:
        paint_active_count = max(0, paint_active_count - 1)


# ══════════════════════════════════════════════════════════════════
# 13. DM HELPERS
# ══════════════════════════════════════════════════════════════════
dm_cache: dict[str, str] = {}

async def build_dm_cache():
    try:
        unread = await client.get_unread_chats()
        for chat_id in unread.unreadChatIds:
            if chat_id == Config.CHAT_ID:
                continue
            try:
                msgs = await client.get_chat_messages(chat_id)
                for msg in getattr(msgs, "messages", []):
                    uid = getattr(msg.author, "userId", None)
                    if uid and uid != client.userId:
                        dm_cache[uid] = chat_id
                        break
            except Exception as e:
                logger.debug(f"[dm] cache build failed for {chat_id}: {e}")
    except Exception as e:
        logger.warning(f"[dm] build_dm_cache failed: {e}")


async def send_dm(user_id: str, message: str) -> bool:
    await build_dm_cache()
    chat_id = dm_cache.get(user_id)
    if not chat_id:
        logger.info(f"[dm] no DM channel found for {user_id}")
        return False
    try:
        await client.send_message(chat_id, message, None)
        return True
    except Exception as e:
        logger.warning(f"[dm] send failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════════
# 14. البريد المجهول — ANONYMOUS MAIL
# ══════════════════════════════════════════════════════════════════
def load_barid() -> dict:
    return json_read(Config.BARID_FILE, {})

def save_barid(data: dict):
    try:
        json_write(Config.BARID_FILE, data)
    except Exception as e:
        logger.exception(f"[barid] save error: {e}")

def barid_generate_code(existing_codes: set) -> int:
    available = [c for c in range(1, 101) if c not in existing_codes]
    if not available:
        while True:
            c = random.randint(101, 9999)
            if c not in existing_codes:
                return c
    return random.choice(available)

def barid_find_user_by_code(data: dict, code: int) -> str | None:
    for uid, info in data.items():
        if info.get("code") == code:
            return uid
    return None

async def handle_barid_commands(content: str, user_id: str, nickname: str,
                                chat_id: str, circle_id: str) -> bool:
    stripped = content.strip()

    if stripped == "مشاركة بريد":
        data = load_barid()
        if user_id in data:
            data[user_id]["nickname"] = nickname
            save_barid(data)
            code = data[user_id]["code"]
            await client.send_message(chat_id,
                f"لديك بالفعل بريد مسجل\nرقمك: {code}", circle_id)
        else:
            existing_codes = {info["code"] for info in data.values()}
            code           = barid_generate_code(existing_codes)
            data[user_id]  = {"code": code, "nickname": nickname, "messages": []}
            save_barid(data)
            await client.send_message(chat_id,
                f"تم إنشاء البريد الخاص بك\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"رقمك: {code}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"ضع الرقم في البايو حتى يتمكن الآخرون من مراسلتك {nickname}",
                circle_id)
        return True

    if stripped.startswith("مجهول "):
        parts = stripped.split(None, 2)
        if len(parts) < 3:
            await client.send_message(chat_id,
                "الصيغة الصحيحة: مجهول <رقم> <رسالة>", circle_id)
            return True
        try:
            target_code = int(parts[1])
        except ValueError:
            await client.send_message(chat_id, "الرقم غير صحيح", circle_id)
            return True
        msg_text = parts[2].strip()
        if not msg_text:
            await client.send_message(chat_id, "الرسالة فارغة", circle_id)
            return True
        data       = load_barid()
        target_uid = barid_find_user_by_code(data, target_code)
        if not target_uid:
            await client.send_message(chat_id, "البريد غير صحيح", circle_id)
            return True
        entry = f"رسالة مجهولة:\n{msg_text}\n─── ✉️ ──"
        data[target_uid]["messages"].append(entry)
        save_barid(data)
        await client.send_message(chat_id, "تم إرسال الرسالة بنجاح 📮", circle_id)
        return True

    if stripped.startswith("راسلي "):
        parts = stripped.split(None, 2)
        if len(parts) < 3:
            await client.send_message(chat_id,
                "الصيغة الصحيحة: راسلي <رقم> <رسالة>", circle_id)
            return True
        try:
            target_code = int(parts[1])
        except ValueError:
            await client.send_message(chat_id, "الرقم غير صحيح", circle_id)
            return True
        msg_text = parts[2].strip()
        if not msg_text:
            await client.send_message(chat_id, "الرسالة فارغة", circle_id)
            return True
        data       = load_barid()
        target_uid = barid_find_user_by_code(data, target_code)
        if not target_uid:
            await client.send_message(chat_id, "البريد غير صحيح", circle_id)
            return True
        if user_id not in data:
            await client.send_message(chat_id,
                "يجب أن يكون لديك بريد مسجل أولاً\nاكتب: مشاركة بريد", circle_id)
            return True
        sender_code = data[user_id]["code"]
        entry = f"رسالة من ({sender_code}) {nickname}:\n{msg_text}\n─── 💌 ──"
        data[target_uid]["messages"].append(entry)
        save_barid(data)
        await client.send_message(chat_id, "تم إرسال الرسالة بنجاح 📮", circle_id)
        return True

    if stripped == "بريد":
        data = load_barid()
        if user_id not in data:
            await client.send_message(chat_id,
                "لا يوجد بريد مسجل لك\nاكتب: مشاركة بريد", circle_id)
            return True
        messages = data[user_id].get("messages", [])
        if not messages:
            await client.send_message(chat_id, "📨 لا يوجد رسائل جديدة", circle_id)
            return True
        count   = len(messages)
        header  = f"📬 عندك {count} {'رسالة' if count == 1 else 'رسائل'}:\n\n"
        divider = "\n\n━━━━━━━━━━━━━━━━━━\n\n"
        await client.send_message(chat_id, header + divider.join(messages), circle_id)
        data[user_id]["messages"] = []
        save_barid(data)
        return True

    if stripped == "رقم بريدي":
        data = load_barid()
        if user_id not in data:
            await client.send_message(chat_id,
                "ما عندك بريد مسجل بعد\nاكتب: مشاركة بريد", circle_id)
            return True
        code = data[user_id]["code"]
        await client.send_message(chat_id, f"رقم بريدك: {code}", circle_id)
        return True

    return False


# ══════════════════════════════════════════════════════════════════
# 15. برا السالفة — GAME MODULE  (fair round-robin, locked, timed)
# ══════════════════════════════════════════════════════════════════

BARRA_TOPICS = [
    "كلب", "قطة", "أسد", "فيل", "قرد", "ثعلب", "ذئب", "نمر", "زرافة", "دلفين",
    "ببغاء", "نسر", "سلحفاة", "أخطبوط", "حصان",
    "الشاطئ", "المستشفى", "المدرسة", "المطار", "السوق", "الملعب", "السينما",
    "الحفلة", "المكتبة", "المسرح", "الصالة الرياضية", "المتحف", "الفندق",
    "المصنع", "القصر", "الحديقة", "الغابة", "الصحراء", "الميناء", "المحطة",
    "كرة القدم", "السباحة", "التنس", "الملاكمة", "كرة السلة", "الغوص",
    "ركوب الأمواج", "الجودو", "الجمباز", "الفروسية",
    "الهاتف الذكي", "اللابتوب", "التلفزيون", "الكاميرا", "الطابعة",
    "سماعات الرأس", "المايكروفون", "جهاز التحكم", "الراديو", "الساعة الذكية",
    "الدرون", "الروبوت", "شاشة العرض",
    "البيانو", "الغيتار", "الطبلة", "الكمان", "الناي", "حفلة موسيقية",
    "فيلم رعب", "مسرحية", "معرض فني",
    "الطائرة", "القطار", "السفينة", "الدراجة النارية", "الغواصة", "المروحية",
    "سيارة الإسعاف", "القارب", "الدراجة الهوائية",
    "الطبيب", "المعلم", "الممثل", "المغني", "الطيار", "رجل الإطفاء",
    "الشرطي", "المحامي", "المهندس", "الرياضي", "الطاهي", "الصحفي",
    "الصيف", "الشتاء", "الربيع", "الخريف", "البركان", "الزلزال",
    "قوس قزح", "الثلج", "العاصفة", "الشفق القطبي",
    "الإجازة", "يوم الميلاد", "حفل الزفاف", "التخرج", "الكرنفال",
    "الألعاب النارية", "التسوق", "المباراة النهائية",
]


class BarraState:
    IDLE   = "idle"
    LOBBY  = "lobby"
    REVEAL = "reveal"
    ACTIVE = "active"
    VOTING = "voting"


barra: dict = {}
barra_lock = asyncio.Lock()
_phase_timeout_task: asyncio.Task | None = None


def barra_reset():
    global _phase_timeout_task
    if _phase_timeout_task and not _phase_timeout_task.done():
        _phase_timeout_task.cancel()
        _phase_timeout_task = None
    barra.clear()
    barra.update({
        "state":        BarraState.IDLE,
        "host_id":      None,
        "host_name":    None,
        "players":      [],
        "impostor_id":  None,
        "topic":        None,
        "turn_index":   0,
        "votes":        {},
        "voted_ids":    set(),
        "revealed_ids": set(),
        "started_at":   0.0,
    })

barra_reset()


def barra_get_player(user_id: str) -> dict | None:
    for p in barra["players"]:
        if p["userId"] == user_id:
            return p
    return None


def barra_build_player_list() -> str:
    return "\n".join(f"{i}. {p['nickname']}" for i, p in enumerate(barra["players"], 1))


def barra_round_progress() -> tuple[int, int, int]:
    n = len(barra["players"])
    if n == 0:
        return (1, 0, 0)
    return ((barra["turn_index"] // n) + 1,
            (barra["turn_index"] %  n) + 1,
            n)


def barra_round_complete() -> bool:
    n = len(barra["players"])
    return n > 0 and barra["turn_index"] >= n


def barra_turn_msg() -> str:
    players = barra["players"]
    n       = len(players)
    if n == 0:
        return "⚠️ لا لاعبين"
    asker  = players[barra["turn_index"] %  n]
    target = players[(barra["turn_index"] + 1) % n]
    rnd, pos, total = barra_round_progress()
    voting_ready = "✅ يقدر يكتب *تصويت* الآن" if barra_round_complete() else "🔒 *تصويت* مقفل لحد ما تخلص الجولة"
    return (
        f"🎙️ الجولة {rnd} — السؤال {pos}/{total}\n\n"
        f"👤 {asker['nickname']}\n"
        f"يسأل\n"
        f"👤 {target['nickname']}\n\n"
        f"المضيف يكتب *التالي* للدور التالي\n"
        f"{voting_ready}"
    )


async def _schedule_phase_timeout(seconds: float, state_at_schedule: str,
                                  on_timeout_message: str):
    global _phase_timeout_task
    try:
        await asyncio.sleep(seconds)
        async with barra_lock:
            if barra["state"] != state_at_schedule:
                return
            logger.warning(f"[barra] phase timeout in state={state_at_schedule}")
            try:
                await client.send_message(Config.CHAT_ID, on_timeout_message, Config.CIRCLE_ID)
            except Exception as e:
                logger.warning(f"[barra] timeout-msg send failed: {e}")
            barra_reset()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"[barra] timeout task error: {e}")


def _arm_timeout(seconds: float, state: str, msg: str):
    global _phase_timeout_task
    if _phase_timeout_task and not _phase_timeout_task.done():
        _phase_timeout_task.cancel()
    _phase_timeout_task = asyncio.create_task(
        _schedule_phase_timeout(seconds, state, msg)
    )


async def barra_start_lobby(chat_id: str, circle_id: str, user_id: str, nickname: str):
    async with barra_lock:
        barra_reset()
        barra["state"]      = BarraState.LOBBY
        barra["host_id"]    = user_id
        barra["host_name"]  = nickname
        barra["started_at"] = time.time()

    await client.send_message(chat_id,
        f"🎮 لعبة *برا السالفة* بدأت!\n"
        f"المضيف: {nickname}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📋 طريقة اللعب:\n"
        f"• كل اللاعبين يعرفون موضوع مشترك\n"
        f"• شخص واحد فقط *برا السالفة* — ما يعرف الموضوع\n"
        f"• يسألون بعض ويحاولون يكتشفون مين برا السالفة\n"
        f"• كل لاعب لازم يسأل وينسأل قبل التصويت — اللعبة عادلة\n"
        f"• في النهاية يصوتون على اللاعب المشبوه\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"✏️ للمشاركة اكتبوا: مشاركة\n"
        f"(نحتاج {Config.MIN_PLAYERS} لاعبين على الأقل)\n\n"
        f"لما يكتمل العدد، المضيف يكتب: اكتمل العدد",
        circle_id)

    _arm_timeout(
        Config.LOBBY_TIMEOUT_S, BarraState.LOBBY,
        "⏱️ انتهى وقت الانتظار في اللوبي. تم إلغاء اللعبة.\n"
        "ابدأ من جديد بكتابة: لعبة برا السالفة",
    )


async def barra_start_reveal(chat_id: str, circle_id: str):
    async with barra_lock:
        players = barra["players"]
        random.shuffle(players)
        impostor              = random.choice(players)
        barra["impostor_id"]  = impostor["userId"]
        barra["topic"]        = random.choice(BARRA_TOPICS)
        barra["state"]        = BarraState.REVEAL
        barra["revealed_ids"] = set()

    await client.send_message(chat_id,
        f"✅ اكتمل العدد! {len(barra['players'])} لاعبين جاهزين.\n\n"
        f"👥 اللاعبون (الترتيب):\n{barra_build_player_list()}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📩 الخطوة التالية:\n"
        f"كل لاعب يروح للمحادثة الخاصة مع البوت\n"
        f"ويبعث كلمة: *كشف*\n"
        f"وراح يعرف دوره بشكل سري 🤫\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏱️ عندكم {Config.REVEAL_TIMEOUT_S // 60} دقائق",
        circle_id)

    _arm_timeout(
        Config.REVEAL_TIMEOUT_S, BarraState.REVEAL,
        "⏱️ ما كل اللاعبين أرسلوا *كشف* في الوقت. تم إلغاء اللعبة.\n"
        "ابدأ من جديد بكتابة: لعبة برا السالفة",
    )


async def barra_handle_kashf(user_id: str, dm_chat_id: str):
    should_begin = False
    reply: str | None = None

    async with barra_lock:
        if barra["state"] != BarraState.REVEAL:
            return
        player = barra_get_player(user_id)
        if not player:
            return
        is_impostor = (user_id == barra["impostor_id"])
        if is_impostor:
            reply = (
                "🕵️ أنت *برا السالفة* !\n\n"
                "الجميع عندهم موضوع مشترك — أنت ما تعرفه.\n"
                "استمع للأسئلة وجاوب بذكاء ولا تنكشف! 🤫"
            )
        else:
            reply = (
                f"🎯 الموضوع هو: *{barra['topic']}*\n\n"
                f"في شخص واحد ما يعرف الموضوع — هو *برا السالفة*.\n"
                f"اسأل واجاوب بذكاء ولا تقول الموضوع صراحة! 🤐"
            )
        already = user_id in barra["revealed_ids"]
        barra["revealed_ids"].add(user_id)
        if not already and len(barra["revealed_ids"]) >= len(barra["players"]):
            should_begin = True

    if reply is not None:
        try:
            await client.send_message(dm_chat_id, reply, None)
        except Exception as e:
            logger.warning(f"[barra] kashf DM send failed: {e}")

    if should_begin:
        await barra_begin_active()


async def barra_begin_active():
    async with barra_lock:
        if barra["state"] != BarraState.REVEAL:
            return
        barra["state"]      = BarraState.ACTIVE
        barra["turn_index"] = 0

    await client.send_message(Config.CHAT_ID,
        f"🎉 الكل استلم دوره! نبدأ اللعبة الآن.\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎙️ جولة الأسئلة:\n"
        f"كل شخص يسأل اللي بعده سؤال واحد\n"
        f"السؤال يكون عن الموضوع — بدون ذكره مباشرة!\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        + barra_turn_msg(),
        Config.CIRCLE_ID)


async def barra_next_turn(chat_id: str, circle_id: str):
    wrapped_round = 0
    async with barra_lock:
        if barra["state"] != BarraState.ACTIVE:
            return
        barra["turn_index"] += 1
        n = len(barra["players"])
        if n > 0 and barra["turn_index"] % n == 0:
            wrapped_round = barra["turn_index"] // n

    if wrapped_round:
        await client.send_message(chat_id,
            f"🏁 انتهت الجولة {wrapped_round} — كل اللاعبين سألوا وانسألوا.\n"
            f"المضيف يقدر يكتب *تصويت* الآن\n"
            f"أو *التالي* لجولة جديدة.",
            circle_id)
        return
    await client.send_message(chat_id, barra_turn_msg(), circle_id)


async def barra_start_voting(chat_id: str, circle_id: str):
    async with barra_lock:
        if barra["state"] != BarraState.ACTIVE:
            return
        if not barra_round_complete():
            rnd, pos, total = barra_round_progress()
            await client.send_message(chat_id,
                f"⏳ ما خلصت الجولة بعد.\n"
                f"الحالة: السؤال {pos}/{total} في الجولة {rnd}.\n"
                f"كل لاعب لازم يسأل وينسأل قبل التصويت — هذي اللعبة العادلة.\n"
                f"اكتب *التالي* لإكمالها.",
                circle_id)
            return
        barra["state"]     = BarraState.VOTING
        barra["votes"]     = {}
        barra["voted_ids"] = set()

    await client.send_message(chat_id,
        f"🗳️ وقت التصويت!\n\n"
        f"من هو *برا السالفة*؟\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{barra_build_player_list()}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"📌 كل لاعب يرسل رقم الشخص اللي يشك فيه.\n"
        f"(ما تقدر تصوت على نفسك)\n"
        f"⏱️ عندكم {Config.VOTING_TIMEOUT_S // 60} دقائق",
        circle_id)

    _arm_timeout(
        Config.VOTING_TIMEOUT_S, BarraState.VOTING,
        "⏱️ انتهى وقت التصويت. سيتم إعلان النتائج بناءً على الأصوات الموجودة.",
    )
    asyncio.create_task(_voting_auto_tally(Config.VOTING_TIMEOUT_S))


async def _voting_auto_tally(seconds: float):
    await asyncio.sleep(seconds)
    async with barra_lock:
        still_voting = barra["state"] == BarraState.VOTING
    if still_voting:
        await barra_announce_result(Config.CHAT_ID, Config.CIRCLE_ID)


async def barra_cast_vote(user_id: str, nickname: str, vote_num: int,
                          chat_id: str, circle_id: str):
    should_announce = False
    remaining = 0

    async with barra_lock:
        if barra["state"] != BarraState.VOTING:
            return
        players = barra["players"]
        if user_id in barra["voted_ids"]:
            await client.send_message(chat_id, f"⚠️ {nickname}، صوتك مسجل بالفعل. 🗳️", circle_id)
            return
        if vote_num < 1 or vote_num > len(players):
            await client.send_message(chat_id,
                f"⚠️ رقم غير صحيح. اختار من 1 إلى {len(players)}.", circle_id)
            return
        target_player = players[vote_num - 1]
        if target_player["userId"] == user_id:
            await client.send_message(chat_id, "⚠️ ما تقدر تصوت على نفسك! 😅", circle_id)
            return
        target_id = target_player["userId"]
        barra["votes"][target_id] = barra["votes"].get(target_id, 0) + 1
        barra["voted_ids"].add(user_id)
        remaining = len(players) - len(barra["voted_ids"])
        if remaining == 0:
            should_announce = True

    if should_announce:
        await barra_announce_result(chat_id, circle_id)
    else:
        await client.send_message(chat_id,
            f"🗳️ {nickname} صوّت. متبقي {remaining} {'صوت' if remaining == 1 else 'أصوات'}.",
            circle_id)


async def barra_announce_result(chat_id: str, circle_id: str):
    async with barra_lock:
        if barra["state"] not in (BarraState.VOTING, BarraState.ACTIVE):
            return
        votes         = dict(barra["votes"])
        players       = list(barra["players"])
        impostor_id   = barra["impostor_id"]
        topic         = barra["topic"]
        barra_reset()

    impostor_name = next((p["nickname"] for p in players if p["userId"] == impostor_id), "؟")

    if not votes:
        await client.send_message(chat_id,
            f"⚠️ ما في أصوات.\n🕵️ برا السالفة كان: *{impostor_name}*\n"
            f"🎯 الموضوع كان: *{topic}*", circle_id)
        return

    max_votes  = max(votes.values())
    candidates = [uid for uid, v in votes.items() if v == max_votes]
    summary    = "\n".join(
        f"  {p['nickname']} ← {votes.get(p['userId'], 0)} صوت"
        for p in players
    )

    if len(candidates) > 1:
        tied_names = [p["nickname"] for p in players if p["userId"] in candidates]
        msg = (
            f"📊 نتائج التصويت:\n{summary}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"⚖️ تعادل! الأصوات تساوت بين:\n"
            + "\n".join(f"  • {n}" for n in tied_names) +
            f"\n\n🕵️ برا السالفة كان: *{impostor_name}*\n"
            f"🎯 الموضوع كان: *{topic}*\n\n"
            f"🏆 لا فائز — انتهت اللعبة!\n\n"
            f"لعبة جديدة؟ اكتب: لعبة برا السالفة"
        )
    else:
        eliminated_id   = candidates[0]
        eliminated_name = next(
            (p["nickname"] for p in players if p["userId"] == eliminated_id), "؟"
        )
        if eliminated_id == impostor_id:
            msg = (
                f"📊 نتائج التصويت:\n{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🎉 أحسنتم! طردتم *{eliminated_name}*\n"
                f"وهو فعلاً كان *برا السالفة*! 🕵️\n\n"
                f"🎯 الموضوع كان: *{topic}*\n\n"
                f"🏆 فاز اللاعبون!\n\n"
                f"لعبة جديدة؟ اكتب: لعبة برا السالفة"
            )
        else:
            msg = (
                f"📊 نتائج التصويت:\n{summary}\n\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"😈 غلطتم! طردتم *{eliminated_name}* وهو بريء!\n\n"
                f"🕵️ برا السالفة الحقيقي كان: *{impostor_name}*\n"
                f"🎯 الموضوع كان: *{topic}*\n\n"
                f"🏆 فاز برا السالفة!\n\n"
                f"لعبة جديدة؟ اكتب: لعبة برا السالفة"
            )
    await client.send_message(chat_id, msg, circle_id)


# ══════════════════════════════════════════════════════════════════
# 16. CONNECT FOUR — 4 IN A ROW
# ══════════════════════════════════════════════════════════════════
#
#  HOW IT WORKS:
#   1. Player A replies to Player B's message with the command
#      "4 in a row" / "أربعة على التوالي" — challenges them.
#   2. Bot enters EMOJI_SELECT state. Each player sends their emoji.
#      Both must be different. Plain text / digits are ignored.
#   3. Once both emojis are picked, board appears and the host plays first.
#   4. Players take turns sending a column number 1–7.
#      The emoji "drops" to the lowest empty cell of that column.
#   5. First to align 4 of their emoji (horizontal, vertical, or diagonal)
#      wins. If the board fills up with no winner → draw.
#   6. Game auto-resets on win / draw. Host can force-end with
#      "انهاء اللعبة" at ANY time (emoji_select or playing).
#   7. Only ONE Connect-Four game runs at a time per chat.
#
#  STATE LAYOUT:
#   • Bottom row of board is index 0 (gravity drops to row 0 first).
#   • Display flips it: row 5 (top) is printed first.
#   • board[r][c] is None for empty, or the emoji string of whoever played.
# ══════════════════════════════════════════════════════════════════

C4_ROWS = 6
C4_COLS = 7
C4_WIN  = 4
C4_EMPTY_CELL  = "⚪"
C4_COLUMN_ROW  = "1️⃣2️⃣3️⃣4️⃣5️⃣6️⃣7️⃣"


class C4State:
    IDLE         = "idle"
    EMOJI_SELECT = "emoji_select"
    PLAYING      = "playing"


connect4: dict = {}
connect4_lock = asyncio.Lock()
_c4_timeout_task: asyncio.Task | None = None


def connect4_reset():
    """Caller must hold connect4_lock (or be at startup)."""
    global _c4_timeout_task
    if _c4_timeout_task and not _c4_timeout_task.done():
        _c4_timeout_task.cancel()
        _c4_timeout_task = None
    connect4.clear()
    connect4.update({
        "state":           C4State.IDLE,
        "host_id":         None,
        "host_name":       None,
        "opponent_id":     None,
        "opponent_name":   None,
        "host_emoji":      None,
        "opponent_emoji":  None,
        "current_turn_id": None,
        "board":           [[None] * C4_COLS for _ in range(C4_ROWS)],
        "started_at":      0.0,
    })

connect4_reset()


# ── Trigger detection ─────────────────────────────────────────────
def _is_c4_trigger(content: str) -> bool:
    """Detect 4-in-a-row challenge command (case-insensitive for English)."""
    s   = content.strip()
    low = s.lower()
    en_triggers = {
        "4 in a row", "4 in row", "four in a row",
        "connect 4", "connect four", "4inarow", "4inrow",
    }
    if low in en_triggers:
        return True
    ar_triggers = {
        "اربعة على التوالي", "أربعة على التوالي",
        "اربعه على التوالي", "أربعه على التوالي",
        "4 على التوالي", "4 في صف", "أربعة في صف", "اربعة في صف",
    }
    return s in ar_triggers


# ── Emoji validation ──────────────────────────────────────────────
#  Accepts a short string that contains at least one character in a
#  recognized emoji block. Rejects pure digits (those are moves) and
#  plain text in any script (Arabic, Latin, etc).
def looks_like_emoji(text: str) -> bool:
    s = text.strip()
    if not s or len(s) > 20:
        return False
    if s.isdigit():
        return False

    if emoji_pkg is not None:
        if emoji_pkg.emoji_count(s) == 0:
            return False
        remaining = emoji_pkg.replace_emoji(s, '')
        allowed = '\u200D\uFE0E\uFE0F\u20E3'
        for ch in remaining:
            if ch.isspace() or ch in allowed:
                continue
            cp = ord(ch)
            if 0x1F1E6 <= cp <= 0x1F1FF:  # regional indicator symbols (flags)
                continue
            return False
        return True

    # Strict fallback: every character must be in an emoji-ish range
    for c in s:
        cp = ord(c)
        if c.isalpha() or c.isdigit():
            return False
        if (
            0x1F600 <= cp <= 0x1F64F or
            0x1F300 <= cp <= 0x1F5FF or
            0x1F680 <= cp <= 0x1F6FF or
            0x1F700 <= cp <= 0x1F77F or
            0x1F780 <= cp <= 0x1F7FF or
            0x1F800 <= cp <= 0x1F8FF or
            0x1F900 <= cp <= 0x1F9FF or
            0x1FA00 <= cp <= 0x1FA6F or
            0x1FA70 <= cp <= 0x1FAFF or
            0x2600  <= cp <= 0x26FF or
            0x2700  <= cp <= 0x27BF or
            cp in (0x00A9, 0x00AE, 0x2122, 0x303D, 0x3030, 0x3297, 0x3299)
        ):
            continue
        return False
    return True


# ── Board operations ──────────────────────────────────────────────
def c4_render_board() -> str:
    """Render the board top-down with column numbers underneath."""
    rows = []
    for r in range(C4_ROWS - 1, -1, -1):  # top → bottom
        line = ""
        for c in range(C4_COLS):
            cell = connect4["board"][r][c]
            line += cell if cell else C4_EMPTY_CELL
        rows.append(line)
    rows.append(C4_COLUMN_ROW)
    return "\n".join(rows)


def c4_drop_piece(col: int, emoji: str) -> int | None:
    """Drop emoji into column. Returns the row index, or None if column full."""
    for r in range(C4_ROWS):
        if connect4["board"][r][col] is None:
            connect4["board"][r][col] = emoji
            return r
    return None


def c4_check_win(row: int, col: int, emoji: str) -> bool:
    """Check whether placing `emoji` at (row, col) made a 4-in-a-row."""
    board = connect4["board"]
    # 4 directions: horizontal, vertical, diag /, diag \
    for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
        count = 1
        # forward
        r, c = row + dr, col + dc
        while 0 <= r < C4_ROWS and 0 <= c < C4_COLS and board[r][c] == emoji:
            count += 1
            r += dr
            c += dc
        # backward
        r, c = row - dr, col - dc
        while 0 <= r < C4_ROWS and 0 <= c < C4_COLS and board[r][c] == emoji:
            count += 1
            r -= dr
            c -= dc
        if count >= C4_WIN:
            return True
    return False


def c4_is_board_full() -> bool:
    """Board is full when the top row of every column is occupied."""
    return all(connect4["board"][C4_ROWS - 1][c] is not None for c in range(C4_COLS))


# ── Timeouts ──────────────────────────────────────────────────────
async def _c4_schedule_timeout(seconds: float, state_at_schedule: str, msg: str):
    global _c4_timeout_task
    try:
        await asyncio.sleep(seconds)
        async with connect4_lock:
            if connect4["state"] != state_at_schedule:
                return
            logger.warning(f"[c4] phase timeout in state={state_at_schedule}")
            try:
                await client.send_message(Config.CHAT_ID, msg, Config.CIRCLE_ID)
            except Exception as e:
                logger.warning(f"[c4] timeout-msg send failed: {e}")
            connect4_reset()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(f"[c4] timeout task error: {e}")


def _c4_arm_timeout(seconds: float, state: str, msg: str):
    global _c4_timeout_task
    if _c4_timeout_task and not _c4_timeout_task.done():
        _c4_timeout_task.cancel()
    _c4_timeout_task = asyncio.create_task(
        _c4_schedule_timeout(seconds, state, msg)
    )


# ── Phase 1: Challenge / start ────────────────────────────────────
async def c4_start_challenge(host_id: str, host_name: str,
                             opp_id: str, opp_name: str,
                             chat_id: str, circle_id: str):
    """Returns True if challenge started, False if rejected."""
    async with connect4_lock:
        if connect4["state"] != C4State.IDLE:
            return False
        connect4_reset()
        connect4["state"]         = C4State.EMOJI_SELECT
        connect4["host_id"]       = host_id
        connect4["host_name"]     = host_name
        connect4["opponent_id"]   = opp_id
        connect4["opponent_name"] = opp_name
        connect4["started_at"]    = time.time()

    await client.send_message(chat_id,
        f"🎮 *4 في صف* — تحدي جديد!\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚔️ {host_name} ضد {opp_name}\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"📌 كل لاعب يختار إيموجي مختلف ليلعب به.\n"
        f"أرسل الإيموجي الذي تريده (لا يتكرر بين اللاعبين).\n\n"
        f"المضيف يقدر يكتب *انهاء اللعبة* للإلغاء.\n"
        f"⏱️ عندكم {Config.C4_EMOJI_SELECT_TIMEOUT_S // 60} دقائق لاختيار الإيموجي.",
        circle_id)

    _c4_arm_timeout(
        Config.C4_EMOJI_SELECT_TIMEOUT_S, C4State.EMOJI_SELECT,
        "⏱️ انتهى وقت اختيار الإيموجي. تم إلغاء لعبة 4 في صف.\n"
        "للعبة جديدة، رد على شخص بـ *4 in a row*",
    )
    return True


# ── Phase 2: Emoji selection ──────────────────────────────────────
async def c4_handle_emoji_pick(user_id: str, nickname: str, content: str,
                               chat_id: str, circle_id: str) -> bool:
    """
    Try to interpret `content` as an emoji pick. Returns True if it was
    consumed (whether successful or rejected with a message), False if
    the content is not an emoji at all (let other handlers process it).
    """
    if connect4["state"] != C4State.EMOJI_SELECT:
        return False
    if user_id not in (connect4["host_id"], connect4["opponent_id"]):
        return False

    candidate = content.strip()
    if not looks_like_emoji(candidate):
        # Consume the message so it doesn't leak to other handlers
        await client.send_message(chat_id,
            f"⚠️ {nickname}، لازم ترسل إيموجي واحد بس (مثال: 🐱). جرب مرة ثانية.",
            circle_id)
        return True

    is_host        = (user_id == connect4["host_id"])
    my_field       = "host_emoji" if is_host else "opponent_emoji"
    other_field    = "opponent_emoji" if is_host else "host_emoji"
    both_picked    = False
    already_picked = False
    duplicate      = False
    host_e = opp_e = host_name = opp_name = None
    current_name = current_emoji = None

    async with connect4_lock:
        if connect4["state"] != C4State.EMOJI_SELECT:
            return True  # state changed under us — but we did consume the message
        if connect4[my_field] is not None:
            already_picked = True
        elif connect4[other_field] == candidate:
            duplicate = True
        else:
            connect4[my_field] = candidate

        host_e    = connect4["host_emoji"]
        opp_e     = connect4["opponent_emoji"]
        host_name = connect4["host_name"]
        opp_name  = connect4["opponent_name"]

        if host_e and opp_e and not (already_picked or duplicate):
            connect4["state"]           = C4State.PLAYING
            connect4["current_turn_id"] = connect4["host_id"]
            both_picked                 = True
            current_name                = host_name
            current_emoji               = host_e
            # Cancel emoji-select timeout, arm overall game timeout
            global _c4_timeout_task
            if _c4_timeout_task and not _c4_timeout_task.done():
                _c4_timeout_task.cancel()

    if already_picked:
        await client.send_message(chat_id,
            f"⚠️ {nickname}، اخترت إيموجي بالفعل. انتظر اللاعب الآخر.",
            circle_id)
        return True

    if duplicate:
        await client.send_message(chat_id,
            f"⚠️ {nickname}، الخصم اختار هذا الإيموجي. اختر إيموجي مختلف.",
            circle_id)
        return True

    if both_picked:
        # Arm the overall game safety timeout
        _c4_arm_timeout(
            Config.C4_GAME_TIMEOUT_S, C4State.PLAYING,
            "⏱️ انتهى وقت اللعبة. تم إنهاء لعبة 4 في صف.\n"
            "للعبة جديدة، رد على شخص بـ *4 in a row*",
        )
        board_view = ""
        async with connect4_lock:
            board_view = c4_render_board()
        await client.send_message(chat_id,
            f"✅ تم اختيار الإيموجيات!\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{host_name}: {host_e}\n"
            f"{opp_name}: {opp_e}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"{board_view}\n\n"
            f"🎯 الدور لـ *{current_name}* {current_emoji}\n"
            f"اكتب رقم العمود (1–{C4_COLS}) لتسقط إيموجيك.",
            circle_id)
    else:
        await client.send_message(chat_id,
            f"✅ {nickname} اختار {candidate}\n"
            f"بانتظار اللاعب الآخر...",
            circle_id)
    return True


# ── Phase 3: Moves ────────────────────────────────────────────────
async def c4_handle_move(user_id: str, nickname: str, col_num: int,
                         chat_id: str, circle_id: str) -> bool:
    """
    Process a column-number move from a player. Returns True if the
    message was consumed (whether move was accepted or rejected with
    a reply), False if the player isn't part of the game.
    """
    if connect4["state"] != C4State.PLAYING:
        return False
    if user_id not in (connect4["host_id"], connect4["opponent_id"]):
        return False

    not_your_turn = False
    bad_col       = False
    column_full   = False
    won           = False
    drawn         = False
    board_view    = ""
    placed_emoji  = ""
    next_name = next_emoji = host_name = opp_name = None

    async with connect4_lock:
        if connect4["state"] != C4State.PLAYING:
            return True
        if user_id != connect4["current_turn_id"]:
            not_your_turn = True
        elif col_num < 1 or col_num > C4_COLS:
            bad_col = True
        else:
            col     = col_num - 1
            is_host = (user_id == connect4["host_id"])
            placed_emoji = connect4["host_emoji"] if is_host else connect4["opponent_emoji"]

            row = c4_drop_piece(col, placed_emoji)
            if row is None:
                column_full = True
            else:
                won   = c4_check_win(row, col, placed_emoji)
                drawn = (not won) and c4_is_board_full()

                board_view = c4_render_board()
                host_name  = connect4["host_name"]
                opp_name   = connect4["opponent_name"]

                if won or drawn:
                    # Snapshot was already taken; clear state for a new game.
                    connect4_reset()
                else:
                    # Switch turns
                    connect4["current_turn_id"] = (
                        connect4["opponent_id"] if is_host else connect4["host_id"]
                    )
                    next_name  = opp_name if is_host else host_name
                    next_emoji = (connect4["opponent_emoji"] if is_host
                                  else connect4["host_emoji"])

    if not_your_turn:
        await client.send_message(chat_id,
            f"⚠️ مو دورك يا {nickname}، انتظر دورك.", circle_id)
        return True

    if bad_col:
        await client.send_message(chat_id,
            f"⚠️ اختر عمود من 1 إلى {C4_COLS}.", circle_id)
        return True

    if column_full:
        await client.send_message(chat_id,
            f"⚠️ العمود {col_num} ممتلئ، اختر عمود ثاني.", circle_id)
        return True

    if won:
        await client.send_message(chat_id,
            f"{board_view}\n\n"
            f"🎉 *{nickname}* {placed_emoji} فاز!\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"4 في صف! 🏆\n\n"
            f"للعبة جديدة، رد على شخص بـ *4 in a row*",
            circle_id)
        return True

    if drawn:
        await client.send_message(chat_id,
            f"{board_view}\n\n"
            f"🤝 *تعادل!* اللوحة ممتلئة ومحد فاز.\n\n"
            f"للعبة جديدة، رد على شخص بـ *4 in a row*",
            circle_id)
        return True

    # Normal continuation
    await client.send_message(chat_id,
        f"{board_view}\n\n"
        f"🎯 الدور لـ *{next_name}* {next_emoji}\n"
        f"اكتب رقم العمود (1–{C4_COLS}).",
        circle_id)
    return True


# ══════════════════════════════════════════════════════════════════
# BOT INSTANCES
# ══════════════════════════════════════════════════════════════════
client = Client(deviceId=Config.DEVICE_ID)
db     = UserDatabase(Config.USER_DATA_FILE)

# NEW: Persistent group memory and evolution
group_memory    = GroupMemory(Config.GROUP_MEMORY_FILE)
arabic_evolution = ArabicEvolution(Config.EVOLUTION_FILE)
hybrid          = HybridResponder(group_memory, arabic_evolution)

# Legacy conversation memory (kept for compatibility, but now backed by group_memory)
class ConversationMemory:
    def __init__(self):
        self.conversations: dict[str, list[dict]] = {}
    def add(self, user_id: str, user_msg: str, bot_response: str):
        bucket = self.conversations.setdefault(user_id, [])
        bucket.append({"user_msg": user_msg, "bot_response": bot_response})
        if len(bucket) > Config.CONVERSATION_MEMORY_LIMIT:
            del bucket[: len(bucket) - Config.CONVERSATION_MEMORY_LIMIT]
    def get(self, user_id: str) -> list[dict]:
        return self.conversations.get(user_id, [])[-6:]
    def clear(self, user_id: str):
        self.conversations.pop(user_id, None)

memory = ConversationMemory()
waiting: dict = {}

QUESTIONS = [
    "What is your name?",
    "How old are you?",
    "Which country are you from?",
    "Tell me a quote that describes you.",
]

YULIA_GREETINGS_AR = ["أهلاً، ماذا تريد؟", "نعم؟", "تفضل", "أيوه؟", "قل لي"]
YULIA_GREETINGS_EN = ["yeah?", "what's up", "go ahead", "mm?", "hey"]


def detect_yulia_trigger(content: str) -> tuple[bool, str, bool]:
    """
    Detect if message is a Yulia trigger.
    Returns: (triggered, message_without_trigger, is_arabic)
    Arabic triggers: يوليا / ي  → is_arabic=True
    English triggers: yulia / y   → is_arabic=False
    """
    s = content.strip()
    low = s.lower()
    words = s.split()
    if not words:
        return False, "", False

    # Arabic triggers
    if s.startswith("يوليا"):
        remainder = s[5:].strip()
        return True, remainder, True
    if words[0] == "ي":
        parts = s.split(None, 1)
        return True, (parts[1].strip() if len(parts) > 1 else ""), True

    # English triggers
    if low.startswith("yulia"):
        return True, s[5:].strip(), False
    if words[0].lower() == "y":
        parts = s.split(None, 1)
        return True, (parts[1].strip() if len(parts) > 1 else ""), False

    return False, "", False


# ══════════════════════════════════════════════════════════════════
# 17. EVENT HANDLERS
# ══════════════════════════════════════════════════════════════════

@client.middleware(EventType.ChatMessage)
async def user_filter(message: ChatMessage):
    if message.author.userId == client.userId:
        return False


@client.event(EventType.ChatMemberJoin)
async def on_join(message: ChatMessage):
    try:
        nickname   = message.author.nickname
        avatar_url = message.author.avatar_url
        if message.author.userId and nickname:
            members[nickname.lower()] = {
                "nickname":   nickname,
                "userId":     message.author.userId,
                "avatar_url": avatar_url or "",
            }
            save_members()
        if avatar_url:
            try:
                profile_img  = await download_image(avatar_url)
                welcome_path = create_welcome_image(profile_img, nickname)
                if welcome_path:
                    await send_photo_card(message.chatId, message.circleId, welcome_path)
                    with suppress(FileNotFoundError):
                        os.remove(welcome_path)
                    return
            except Exception as e:
                logger.warning(f"[welcome] image gen failed: {e}")
        await client.send_message(
            message.chatId, f"🌫️ أهلاً بك في المملكة, {nickname}", message.circleId
        )
    except Exception as e:
        logger.exception(f"[on_join] error: {e}")


@client.event(EventType.ChatMessage)
async def on_dm_message(message: ChatMessage):
    """Private DM handler — used for game role reveal (كشف)."""
    try:
        content = (message.content or "").strip()
        user_id = message.author.userId
        chat_id = message.chatId
        if chat_id == Config.CHAT_ID or user_id == client.userId:
            return
        dm_cache[user_id] = chat_id
        if content == "كشف":
            await barra_handle_kashf(user_id, chat_id)
    except Exception as e:
        logger.exception(f"[on_dm_message] error: {e}")


@client.event(EventType.ChatMessage)
async def on_message(message: ChatMessage):
    try:
        content    = (message.content or "").strip()
        user_id    = message.author.userId
        nickname   = message.author.nickname
        avatar_url = message.author.avatar_url
        chat_id    = message.chatId
        circle_id  = message.circleId
        msg_id     = message.messageId

        if not content:
            return
        if chat_id != Config.CHAT_ID:
            return  # DMs handled by on_dm_message

        content_low = content.lower()

        # ════════════════════════════════════════════════════
        # ■ RECORD EVERY MESSAGE IN GROUP MEMORY
        # ════════════════════════════════════════════════════
        group_memory.add_message(user_id, nickname, content, msg_type="chat")
        # If Arabic, feed to evolution engine
        if any("\u0600" <= c <= "\u06FF" for c in content):
            arabic_evolution.observe_message(user_id, nickname, content)

        # ════════════════════════════════════════════════════
        # ■ CONNECT FOUR — TRIGGER (must be a reply)
        # ════════════════════════════════════════════════════
        if _is_c4_trigger(content):
            if connect4["state"] != C4State.IDLE:
                await client.send_message(chat_id,
                    "⚠️ في لعبة 4 في صف شغالة بالفعل.\n"
                    "المضيف يكتب *انهاء اللعبة* أولاً.",
                    circle_id, reply_message_id=msg_id)
                return

            reply_obj = (
                getattr(message, "replyMessage", None) or
                getattr(message, "replyTo",      None) or
                getattr(message, "reply",        None)
            )
            if not reply_obj:
                await client.send_message(chat_id,
                    "⚠️ لازم ترد على رسالة الشخص اللي تبي تتحداه.",
                    circle_id, reply_message_id=msg_id)
                return

            target_uid  = None
            target_nick = None
            auth = getattr(reply_obj, "author", None)
            if auth:
                target_uid  = getattr(auth, "userId", None)
                target_nick = getattr(auth, "nickname", None)
            if not target_uid:
                target_uid = (getattr(reply_obj, "userId", None) or
                              getattr(reply_obj, "uid", None))

            if not target_uid:
                await client.send_message(chat_id,
                    "⚠️ ما قدرت أعرف اللاعب الثاني.",
                    circle_id, reply_message_id=msg_id)
                return
            if target_uid == user_id:
                await client.send_message(chat_id,
                    "⚠️ ما تقدر تتحدى نفسك! 😅",
                    circle_id, reply_message_id=msg_id)
                return
            if target_uid == client.userId:
                await client.send_message(chat_id,
                    "⚠️ تحدى لاعب حقيقي مو أنا.",
                    circle_id, reply_message_id=msg_id)
                return

            if not target_nick:
                for m in members.values():
                    if m["userId"] == target_uid:
                        target_nick = m["nickname"]
                        break
            if not target_nick:
                target_nick = "اللاعب 2"

            await c4_start_challenge(user_id, nickname, target_uid, target_nick,
                                     chat_id, circle_id)
            return

        # ════════════════════════════════════════════════════
        # ■ END GAME — works for whichever game is active
        # ════════════════════════════════════════════════════
        if content == "انهاء اللعبة" or content_low == "end game":
            # Connect4 first (only if it's actually running)
            if connect4["state"] != C4State.IDLE:
                if user_id != connect4["host_id"]:
                    await client.send_message(chat_id,
                        "⚠️ فقط مضيف اللعبة يقدر ينهيها.", circle_id)
                    return
                async with connect4_lock:
                    host_name_local = connect4["host_name"] or nickname
                    connect4_reset()
                await client.send_message(chat_id,
                    f"🛑 تم إنهاء لعبة 4 في صف بواسطة {host_name_local}.\n\n"
                    f"للعبة جديدة، رد على شخص بـ *4 in a row*",
                    circle_id)
                return
            # Otherwise fall through to barra
            if barra["state"] != BarraState.IDLE:
                if user_id != barra["host_id"]:
                    await client.send_message(chat_id,
                        "⚠️ فقط المضيف يقدر ينهي اللعبة.", circle_id)
                    return
                async with barra_lock:
                    barra_reset()
                await client.send_message(chat_id,
                    "🛑 تم إنهاء اللعبة وحذف كل البيانات.\n\n"
                    "لتبدأ لعبة جديدة اكتب: لعبة برا السالفة", circle_id)
                return
            # No game active — silent ignore
            return

        # ════════════════════════════════════════════════════
        # ■ CONNECT FOUR — emoji selection (consume emojis from players)
        # ════════════════════════════════════════════════════
        if connect4["state"] == C4State.EMOJI_SELECT and \
           user_id in (connect4["host_id"], connect4["opponent_id"]):
            if await c4_handle_emoji_pick(user_id, nickname, content, chat_id, circle_id):
                return

        # ════════════════════════════════════════════════════
        # ■ CONNECT FOUR — moves (consume column numbers from current player)
        # ════════════════════════════════════════════════════
        if connect4["state"] == C4State.PLAYING and \
           user_id in (connect4["host_id"], connect4["opponent_id"]) and \
           content.strip().isdigit():
            col_num = int(content.strip())
            if await c4_handle_move(user_id, nickname, col_num, chat_id, circle_id):
                return

        # ════════════════════════════════════════════════════
        # ■ برا السالفة COMMANDS (state-guarded)
        # ════════════════════════════════════════════════════

        if content == "لعبة برا السالفة":
            if barra["state"] != BarraState.IDLE:
                await client.send_message(chat_id,
                    "⚠️ في لعبة شغالة بالفعل.\nالمضيف يكتب *انهاء اللعبة* أولاً.", circle_id)
                return
            await barra_start_lobby(chat_id, circle_id, user_id, nickname)
            return

        if content == "مشاركة":
            if barra["state"] != BarraState.LOBBY:
                return
            async with barra_lock:
                if barra_get_player(user_id):
                    await client.send_message(chat_id,
                        f"⚠️ {nickname}، أنت مسجل بالفعل! 😄", circle_id)
                    return
                barra["players"].append({"userId": user_id, "nickname": nickname})
                count = len(barra["players"])
            await client.send_message(chat_id,
                f"✅ {nickname} انضم للعبة! إجمالي اللاعبين: {count}", circle_id)
            return

        if content == "اكتمل العدد":
            if barra["state"] != BarraState.LOBBY:
                return
            if user_id != barra["host_id"]:
                return
            if len(barra["players"]) < Config.MIN_PLAYERS:
                await client.send_message(chat_id,
                    f"⚠️ ما يكفي لاعبين! عندنا {len(barra['players'])} فقط، "
                    f"نحتاج {Config.MIN_PLAYERS} على الأقل.", circle_id)
                return
            await barra_start_reveal(chat_id, circle_id)
            return

        if content == "التالي":
            if barra["state"] != BarraState.ACTIVE:
                return
            if user_id != barra["host_id"]:
                return
            await barra_next_turn(chat_id, circle_id)
            return

        if content == "تصويت":
            if barra["state"] != BarraState.ACTIVE:
                return
            if user_id != barra["host_id"]:
                await client.send_message(chat_id,
                    "⚠️ فقط المضيف يقدر يبدأ التصويت.", circle_id)
                return
            await barra_start_voting(chat_id, circle_id)
            return

        if content.isdigit() and barra["state"] == BarraState.VOTING:
            if barra_get_player(user_id):
                await barra_cast_vote(user_id, nickname, int(content), chat_id, circle_id)
            return

        # ════════════════════════════════════════════════════
        # ■ PAINT (AI image generation)
        # ════════════════════════════════════════════════════
        is_paint, paint_prompt, paint_arabic = _detect_paint_trigger(content)
        if is_paint:
            asyncio.create_task(
                handle_paint_command(paint_prompt, paint_arabic,
                                     chat_id, circle_id, msg_id, user_id)
            )
            return

        # ════════════════════════════════════════════════════
        # ■ ADMIN COMMANDS
        # ════════════════════════════════════════════════════

        if content_low.startswith("kick "):
            if user_id != Config.SOR_ID:
                return
            query = content[5:].strip()
            if not query or not members:
                return
            matched_key = query.lower() if query.lower() in members else None
            if not matched_key:
                ai_result = await ai_match_name(query, list(members.keys()))
                if ai_result:
                    matched_key = ai_result.lower()
            if not matched_key or matched_key not in members:
                return
            target    = members[matched_key]
            target_id = target["userId"]
            if target_id == Config.SOR_ID:
                await client.send_message(chat_id, "Nice try.", circle_id, reply_message_id=msg_id)
                return
            try:
                await client.kick(chat_id, target_id, circle_id)
                if matched_key in members:
                    del members[matched_key]
                    save_members()
            except Exception as e:
                logger.warning(f"[kick] error: {e}")
            return

        if content_low == "yulia scan":
            if user_id != Config.SOR_ID:
                return
            await scan_members()
            return

        if content_low in ("ai remaining", "ai status"):
            now        = datetime.now(timezone.utc)
            reset_time = (now + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            remaining  = reset_time - now
            hours, rem        = divmod(remaining.seconds, 3600)
            minutes, seconds  = divmod(rem, 60)
            await client.send_message(chat_id,
                f"Yulia AI Status\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"UTC Now:    {now.strftime('%H:%M:%S')}\n"
                f"Resets At:  {reset_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                f"Remaining:  {hours}h {minutes}m {seconds}s\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Groq free tier resets daily at 00:00 UTC", circle_id)
            return

        _wprefix = None
        if content_low.startswith("yulia welcome "):
            _wprefix = "yulia welcome "
        elif content_low.startswith("y welcome "):
            _wprefix = "y welcome "
        if _wprefix and user_id == Config.SOR_ID:
            target_name = content[len(_wprefix):].strip()
            if not target_name:
                await client.send_message(chat_id, "who should I welcome?", circle_id)
                return
            matched_key = target_name.lower() if target_name.lower() in members else None
            if not matched_key:
                ai_result = await ai_match_name(target_name, list(members.keys()))
                if ai_result:
                    matched_key = ai_result.lower()
            if not matched_key or matched_key not in members:
                await client.send_message(chat_id,
                    f"couldn't find '{target_name}' in members.", circle_id)
                return
            member_info = members[matched_key]
            target_nick = member_info["nickname"]
            target_av   = member_info.get("avatar_url", "")
            try:
                if target_av:
                    profile_img  = await download_image(target_av)
                    welcome_path = create_welcome_image(profile_img, target_nick)
                    if welcome_path:
                        await send_photo_card(chat_id, circle_id, welcome_path)
                        with suppress(FileNotFoundError):
                            os.remove(welcome_path)
                        return
                await client.send_message(chat_id,
                    f"🌫️ أهلاً بك في المملكة, {target_nick}", circle_id)
            except Exception as e:
                logger.warning(f"[welcome_manual] error: {e}")
                await client.send_message(chat_id,
                    f"🌫️ أهلاً بك في المملكة, {target_nick}", circle_id)
            return

        # ─ barid:<n> — Sor only: assign code via reply ──────────
        if user_id == Config.SOR_ID:
            _bm = re.match(r"^barid\s*:\s*(\d+)$", content.strip(), re.IGNORECASE)
            if _bm:
                new_code = int(_bm.group(1))
                _reply_obj = (
                    getattr(message, "replyMessage", None) or
                    getattr(message, "replyTo",      None) or
                    getattr(message, "reply",        None)
                )
                _target_uid = None
                if _reply_obj:
                    _auth = getattr(_reply_obj, "author", None)
                    if _auth:
                        _target_uid = getattr(_auth, "userId", None)
                    if not _target_uid:
                        _target_uid = getattr(_reply_obj, "userId",
                                     getattr(_reply_obj, "uid", None))
                if not _target_uid:
                    await client.send_message(chat_id,
                        "ردّ على رسالة الشخص وبعدين اكتب الأمر",
                        circle_id, reply_message_id=msg_id)
                    return
                data = load_barid()
                taken_by = barid_find_user_by_code(data, new_code)
                if taken_by and taken_by != _target_uid:
                    taken_nick = data[taken_by].get("nickname", taken_by)
                    await client.send_message(chat_id,
                        f"الرقم {new_code} محجوز من قبل {taken_nick}",
                        circle_id, reply_message_id=msg_id)
                    return
                if _target_uid not in data:
                    _nick = next(
                        (m["nickname"] for m in members.values()
                         if m["userId"] == _target_uid), _target_uid)
                    data[_target_uid] = {"code": new_code, "nickname": _nick, "messages": []}
                else:
                    data[_target_uid]["code"] = new_code
                    _nick_fresh = next(
                        (m["nickname"] for m in members.values()
                         if m["userId"] == _target_uid), None)
                    if _nick_fresh:
                        data[_target_uid]["nickname"] = _nick_fresh
                save_barid(data)
                display_nick = data[_target_uid].get("nickname", _target_uid)
                await client.send_message(chat_id,
                    f"تم تغيير رقم بريد {display_nick} إلى {new_code}",
                    circle_id, reply_message_id=msg_id)
                return

        # ─ البريد المجهول commands ──────────────────────────────
        if await handle_barid_commands(content, user_id, nickname, chat_id, circle_id):
            return

        # ════════════════════════════════════════════════════
        # ■ YULIA TRIGGER (chat / intents) — HYBRID + AI
        # ════════════════════════════════════════════════════
        triggered, user_msg, is_arabic = detect_yulia_trigger(content)
        if triggered:
            await handle_yulia_intent(
                user_msg, is_arabic,
                user_id, nickname, avatar_url or "",
                chat_id, circle_id, msg_id,
            )
            return

        # Waiting for card question answers
        if user_id in waiting:
            await handle_answer(content, user_id, chat_id, circle_id)
            return

        # pfp
        if content_low == "pfp":
            if not avatar_url:
                await client.send_message(chat_id, "ما في صورة.", circle_id)
                return
            try:
                profile_img = await download_image(avatar_url)
                tmp = tempfile.mktemp(suffix=".jpg")
                profile_img.convert("RGB").save(tmp, "JPEG", quality=95)
                try:
                    await send_photo_card(chat_id, circle_id, tmp)
                finally:
                    with suppress(FileNotFoundError):
                        os.remove(tmp)
            except Exception as e:
                logger.warning(f"[pfp] error: {e}")
                await client.send_message(chat_id, "ما قدرت أجيب الصورة.", circle_id)
            return

        # remember me
        if content_low == "remember me":
            if db.get(user_id):
                await client.send_message(chat_id,
                    "You already have a saved card. "
                    "Say 'card' to view it or 'edit id card' to redo it.", circle_id)
            else:
                await client.send_message(chat_id,
                    "Welcome to the Gothic realm. Let me get to know you...", circle_id)
                await asyncio.sleep(1)
                await client.send_message(chat_id, QUESTIONS[0], circle_id)
                waiting[user_id] = {
                    "nickname": nickname, "avatar_url": avatar_url,
                    "step": "name", "name": "", "age": "",
                    "country": "", "quote": "",
                    "type": random.choice(Config.CREATURE_TYPES),
                }
            return

        # card / card @mention
        if content_low.startswith("card"):
            target_icon = avatar_url
            user_data   = db.get(user_id)
            if message.mentionedUids:
                target_id = message.mentionedUids[0]
                user_data = db.get(target_id)
                if not user_data:
                    await client.send_message(chat_id,
                        "This user doesn't have a saved card yet.", circle_id)
                    return
                target_icon = None
                for m in members.values():
                    if m["userId"] == target_id:
                        target_icon = m.get("avatar_url") or None
                        break
            if not user_data:
                await client.send_message(chat_id,
                    "You don't have a saved card yet. Say 'remember me' to create one.",
                    circle_id)
                return
            if not target_icon:
                for m in members.values():
                    if m["userId"] == user_id:
                        target_icon = m.get("avatar_url") or None
                        break
            if not target_icon:
                await client.send_message(chat_id, "ما قدرت أجيب صورتك.", circle_id)
                return
            try:
                profile_img = await download_image(target_icon)
                card_path   = create_id_card_image(
                    profile_img, user_data["name"], user_data["type"],
                    user_data["age"], user_data["country"], user_data["quote"])
                if card_path:
                    await send_photo_card(chat_id, circle_id, card_path)
                    with suppress(FileNotFoundError):
                        os.remove(card_path)
                    return
            except Exception as e:
                logger.warning(f"[card] error: {e}")
            await client.send_message(chat_id, "ما قدرت أعمل الكارت، جرب مرة ثانية.", circle_id)
            return

        # edit id card
        if content_low == "edit id card":
            if db.delete(user_id):
                await client.send_message(chat_id,
                    "Your card has been deleted. Say 'remember me' to create a new one.",
                    circle_id)
            else:
                await client.send_message(chat_id, "You don't have a saved card yet.", circle_id)
            return

        # members
        if content_low == "members":
            text = format_members_list()
            await client.send_message(chat_id, text or "No members loaded yet.", circle_id)
            return

        # /help
        if content_low in ("/help", "/commands"):
            await client.send_message(chat_id,
                "الأوامر:\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "yulia / يوليا / y / ي <رسالة> — تكلمي يوليا\n"
                "yulia paint <prompt> / يوليا ارسمي <وصف> — رسم بالذكاء\n"
                "yulia ابعثيلي صورة <شيء> — صورة من الإنترنت\n"
                "pfp — صورة البروفايل\n"
                "members — قائمة الأعضاء\n"
                "remember me — أنشئ كارت غوثيك\n"
                "card — شوف كارتك\n"
                "card @user — شوف كارت شخص ثاني\n"
                "edit id card — احذف وأعد الكارت\n"
                "ai remaining — وقت إعادة تعبئة الذكاء\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "البريد:\n"
                "مشاركة بريد — احصل على رقم بريد خاص\n"
                "مجهول <رقم> <رسالة> — رسالة مجهولة\n"
                "راسلي <رقم> <رسالة> — رسالة باسمك\n"
                "بريد — اعرض رسائلك (تُمسح بعد القراءة)\n"
                "رقم بريدي — اعرف رقم بريدك\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "لعبة برا السالفة — ابدأ اللعبة\n"
                "مشاركة — انضم للعبة\n"
                "اكتمل العدد — قفل اللاعبين (مضيف)\n"
                "التالي — الدور التالي (مضيف)\n"
                "تصويت — ابدأ التصويت (مضيف، بعد إكمال جولة)\n"
                "انهاء اللعبة — أوقف اللعبة (مضيف)\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🎮 4 في صف (Connect Four):\n"
                "رد على شخص واكتب: 4 in a row / أربعة على التوالي\n"
                "بعدها كل لاعب يختار إيموجي مختلف\n"
                "ثم يرسل رقم العمود (1–7) ليسقط إيموجيه\n"
                "أول من يرتب 4 في صف (أفقي/عمودي/قطري) يفوز\n"
                "انهاء اللعبة — لإلغاء اللعبة (المضيف فقط)\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "للمشرف فقط:\n"
                "yulia welcome <اسم> — كارت ترحيب يدوي\n"
                "yulia scan — تحديث قائمة الأعضاء\n"
                "kick <اسم> — طرد عضو", circle_id)
            return

    except Exception as e:
        logger.exception(f"[on_message] unhandled error: {e}")


# ── Yulia intent handler (called from on_message when triggered) ──
async def handle_yulia_intent(
    user_msg: str, is_arabic: bool,
    author_id: str, author_name: str, author_avatar_url: str,
    chat_id: str, circle_id: str, msg_id: str,
):
    language = "arabic" if is_arabic else "english"

    if not user_msg:
        pool = YULIA_GREETINGS_AR if is_arabic else YULIA_GREETINGS_EN
        await client.send_message(chat_id, random.choice(pool), circle_id, reply_message_id=msg_id)
        return

    # ═══════════════════════════════════════════════════════════════
    # ■ HYBRID RESPONSE LAYER — try local intelligence first
    # ═══════════════════════════════════════════════════════════════
    hybrid_reply = await hybrid.respond(user_msg, is_arabic, author_id, author_name)
    if hybrid_reply is not None:
        # Record the interaction
        group_memory.add_ai_interaction(author_id, author_name, user_msg, hybrid_reply, language)
        memory.add(author_id, user_msg, hybrid_reply)
        await client.send_message(chat_id, hybrid_reply, circle_id, reply_message_id=msg_id)
        return

    # ═══════════════════════════════════════════════════════════════
    # ■ AI INTENT DETECTION (for structured commands)
    # ═══════════════════════════════════════════════════════════════
    intent      = await detect_intent(user_msg)
    intent_type = intent.get("type", "chat")

    # KICK
    if intent_type == "kick":
        if author_id != Config.SOR_ID:
            return
        target_desc  = intent.get("target", "").strip()
        delay        = int(intent.get("delay") or 0)
        announcement = intent.get("announcement") or None
        do_countdown = bool(intent.get("countdown", False))
        if not target_desc:
            reply = "مين بدك أطرد؟" if is_arabic else "who do you want me to kick?"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        if not members:
            reply = "ما في أعضاء محملين بعد" if is_arabic else "no members loaded yet, try in a moment"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        matched_key = target_desc.lower() if target_desc.lower() in members else None
        if not matched_key:
            ai_result = await ai_match_name(target_desc, list(members.keys()))
            if ai_result:
                matched_key = ai_result.lower()
        if not matched_key or matched_key not in members:
            reply = "ما لقيت حدا هيك" if is_arabic else "couldn't find anyone matching that"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        target    = members[matched_key]
        target_id = target["userId"]
        if target_id == Config.SOR_ID:
            await client.send_message(chat_id, "Nice try.", circle_id, reply_message_id=msg_id)
            return
        if announcement:
            await client.send_message(chat_id, announcement, circle_id)
        if delay > 0:
            if do_countdown:
                for i in range(delay, 0, -1):
                    await client.send_message(chat_id, f"{i}...", circle_id)
                    await asyncio.sleep(1)
            else:
                await asyncio.sleep(delay)
        try:
            await client.kick(chat_id, target_id, circle_id)
            if matched_key in members:
                del members[matched_key]
                save_members()
        except Exception as e:
            logger.warning(f"[yulia_kick] error: {e}")
        return

    # REMEMBER
    if intent_type == "remember":
        if db.get(author_id):
            reply = ("عندك كارت محفوظ بالفعل، قل 'card' لتشوفه أو 'edit id card' لتعيد إنشاءه."
                     if is_arabic else
                     "you already have a saved card. say 'card' to view it or 'edit id card' to redo it.")
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        else:
            intro = ("أهلاً في عالم Silent Hill، دعني أتعرف عليك..."
                     if is_arabic else
                     "Welcome to the Gothic realm. Let me get to know you...")
            await client.send_message(chat_id, intro, circle_id)
            await asyncio.sleep(1)
            await client.send_message(chat_id, QUESTIONS[0], circle_id)
            waiting[author_id] = {
                "nickname": author_name, "avatar_url": author_avatar_url,
                "step": "name", "name": "", "age": "",
                "country": "", "quote": "",
                "type": random.choice(Config.CREATURE_TYPES),
            }
        return

    # MEMBERS
    if intent_type == "members":
        text = format_members_list()
        reply = text if text else (
            "ما في أعضاء محملين بعد" if is_arabic else "no members loaded yet, try again in a moment"
        )
        await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        return

    # PFP
    if intent_type == "pfp":
        target_desc = intent.get("target", "").strip().lower()
        SELF_REFS   = {"self", "me", "my", "mine", "myself", "i", "", "نفسي", "صورتي"}
        is_self     = target_desc in SELF_REFS
        if is_self:
            av = author_avatar_url or next(
                (m.get("avatar_url", "") for m in members.values() if m["userId"] == author_id), ""
            )
            if not av:
                reply = "ما قدرت أجيب صورتك" if is_arabic else "couldn't fetch your profile picture"
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
                return
            try:
                profile_img = await download_image(av)
                tmp = tempfile.mktemp(suffix=".jpg")
                profile_img.convert("RGB").save(tmp, "JPEG", quality=95)
                try:
                    await send_photo_card(chat_id, circle_id, tmp)
                finally:
                    with suppress(FileNotFoundError):
                        os.remove(tmp)
            except Exception as e:
                logger.warning(f"[yulia_pfp_self] error: {e}")
                reply = "ما قدرت أجيب الصورة" if is_arabic else "couldn't fetch the picture"
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        if not members:
            reply = "ما في أعضاء محملين بعد" if is_arabic else "no members loaded yet, try in a moment"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        matched_key = target_desc if target_desc in members else None
        if not matched_key:
            ai_result = await ai_match_name(target_desc, list(members.keys()))
            if ai_result:
                matched_key = ai_result.lower()
        if not matched_key or matched_key not in members:
            reply = "ما لقيت هالشخص بالمجموعة" if is_arabic else "couldn't find that person in the group"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        av = members[matched_key].get("avatar_url", "")
        if not av:
            name  = members[matched_key]["nickname"]
            reply = f"ما في صورة لـ {name}" if is_arabic else f"no profile picture available for {name}"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        try:
            profile_img = await download_image(av)
            tmp = tempfile.mktemp(suffix=".jpg")
            profile_img.convert("RGB").save(tmp, "JPEG", quality=95)
            try:
                await send_photo_card(chat_id, circle_id, tmp)
            finally:
                with suppress(FileNotFoundError):
                    os.remove(tmp)
        except Exception as e:
            logger.warning(f"[yulia_pfp_other] error: {e}")
            reply = "ما قدرت أجيب الصورة" if is_arabic else "couldn't fetch the picture"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        return

    # CARD
    if intent_type == "card":
        target_desc = intent.get("target", "").strip().lower()
        SELF_REFS   = {"self", "me", "my", "mine", "myself", "my card", "i",
                       "card", "", "كارتي", "كارت", "نفسي", "بطاقتي"}
        is_self = target_desc in SELF_REFS
        if is_self:
            user_data = db.get(author_id)
            if not user_data:
                reply = ("ما عندك كارت بعد، قول 'remember me' تعمله"
                         if is_arabic else
                         "you don't have a card yet, say 'remember me' to create one")
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
                return
            av = author_avatar_url or next(
                (m.get("avatar_url", "") for m in members.values() if m["userId"] == author_id), ""
            )
            if not av:
                reply = "ما قدرت أجيب صورتك" if is_arabic else "couldn't fetch your profile picture"
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
                return
            try:
                profile_img = await download_image(av)
                card_path   = create_id_card_image(
                    profile_img, user_data["name"], user_data["type"],
                    user_data["age"], user_data["country"], user_data["quote"])
                if card_path:
                    await send_photo_card(chat_id, circle_id, card_path)
                    with suppress(FileNotFoundError):
                        os.remove(card_path)
            except Exception as e:
                logger.warning(f"[yulia_card_self] error: {e}")
                reply = "ما قدرت أجيب الكارت" if is_arabic else "couldn't generate the card"
                await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        matched_key = target_desc if target_desc in members else None
        if not matched_key:
            ai_result = await ai_match_name(target_desc, list(members.keys()))
            if ai_result:
                matched_key = ai_result.lower()
        if not matched_key or matched_key not in members:
            reply = "ما لقيت هالشخص بالمجموعة" if is_arabic else "couldn't find that person in the group"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        member_info = members[matched_key]
        target_id   = member_info["userId"]
        user_data   = db.get(target_id)
        if not user_data:
            name  = member_info["nickname"]
            reply = f"{name} ما عندو كارت بعد" if is_arabic else f"{name} has no card yet"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        av = member_info.get("avatar_url", "") or (author_avatar_url if target_id == author_id else "")
        if not av:
            reply = "ما في صورة لهالشخص" if is_arabic else "no profile picture available for this user"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        try:
            profile_img = await download_image(av)
            card_path   = create_id_card_image(
                profile_img, user_data["name"], user_data["type"],
                user_data["age"], user_data["country"], user_data["quote"])
            if card_path:
                await send_photo_card(chat_id, circle_id, card_path)
                with suppress(FileNotFoundError):
                    os.remove(card_path)
        except Exception as e:
            logger.warning(f"[yulia_card] error: {e}")
            reply = "ما قدرت أجيب الكارت" if is_arabic else "couldn't generate the card"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        return

    # IMAGE (Pixabay)
    if intent_type == "image":
        keyword = intent.get("keyword", "").strip()
        is_nsfw = bool(intent.get("is_nsfw", False))

        if is_nsfw or _is_nsfw_request(keyword):
            reply = "اسفة مقدر" if is_arabic else "sorry I can't send that"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        if not keyword:
            reply = "أيش تبيني أجيب لك صورة؟" if is_arabic else "what should I find a picture of?"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return

        image_url = await fetch_pixabay_image(keyword)
        if not image_url:
            reply = "اسفة مقدر، ما لقيت صورة" if is_arabic else "sorry I can't find a picture for that"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
            return
        try:
            img = await download_image(image_url)
            tmp = tempfile.mktemp(suffix=".jpg")
            img.convert("RGB").save(tmp, "JPEG", quality=85)
            try:
                await send_photo_card(chat_id, circle_id, tmp)
            finally:
                with suppress(FileNotFoundError):
                    os.remove(tmp)
        except Exception as e:
            logger.warning(f"[yulia_image] error: {e}")
            reply = "اسفة مقدر، صار خطأ" if is_arabic else "sorry I can't, something went wrong"
            await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)
        return

    # ═══════════════════════════════════════════════════════════════
    # ■ CHAT (default) — AI with full memory context
    # ═══════════════════════════════════════════════════════════════
    reply = await get_ai_response(
        user_msg, author_name, author_id, is_arabic,
        group_memory, arabic_evolution
    )
    if reply:
        group_memory.add_ai_interaction(author_id, author_name, user_msg, reply, language)
        memory.add(author_id, user_msg, reply)
        # Learn from this interaction for evolution
        arabic_evolution.observe_ai_interaction(user_msg, reply, language)
    else:
        reply = "ما قدرت أرد، جرب بعد شوي" if is_arabic else "couldn't respond, try again"
    await client.send_message(chat_id, reply, circle_id, reply_message_id=msg_id)


async def handle_answer(content: str, user_id: str, chat_id: str, circle_id: str):
    data = waiting[user_id]
    step = data["step"]
    if step == "name":
        data["name"] = content
        data["step"] = "age"
        await client.send_message(chat_id, QUESTIONS[1], circle_id)
    elif step == "age":
        if not content.isdigit() or not (0 < int(content) < 150):
            await client.send_message(chat_id, "Please enter a valid age (1–149).", circle_id)
            return
        data["age"]  = content
        data["step"] = "country"
        await client.send_message(chat_id, QUESTIONS[2], circle_id)
    elif step == "country":
        data["country"] = content
        data["step"]    = "quote"
        await client.send_message(chat_id, QUESTIONS[3], circle_id)
    elif step == "quote":
        data["quote"] = content
        db.add(user_id, {
            "name":    data["name"],
            "age":     data["age"],
            "country": data["country"],
            "quote":   data["quote"],
            "type":    data["type"],
        })
        del waiting[user_id]
        await client.send_message(chat_id,
            f"Your card has been saved! You are a {data['type']}. Say 'card' to view it.",
            circle_id)


# ══════════════════════════════════════════════════════════════════
# SIGNAL HANDLING  (graceful shutdown for Render / Pydroid3)
# ══════════════════════════════════════════════════════════════════
_stop_requested = False

def _on_shutdown_signal(signum, frame):
    global _stop_requested
    logger.info(f"received signal {signum}, requesting graceful shutdown…")
    _stop_requested = True

signal.signal(signal.SIGTERM, _on_shutdown_signal)
signal.signal(signal.SIGINT, _on_shutdown_signal)


# ══════════════════════════════════════════════════════════════════
# 18. MAIN  (with auto-restart loop)
# ══════════════════════════════════════════════════════════════════
async def _run_session():
    """One full bot session: login, spawn background tasks, wait on socket."""
    refresh_task = None
    try:
        logger.info("logging in…")
        await client.login(Config.EMAIL, Config.PASSWORD)
        logger.success("✅ logged in — Silent Hill is alive!")

        refresh_task = asyncio.create_task(member_refresh_loop())
        asyncio.create_task(scan_members())
        asyncio.create_task(periodic_cleanup_loop())

        # Block here until socket dies / connection drops
        await client.socket_wait()
    finally:
        if refresh_task and not refresh_task.done():
            refresh_task.cancel()
            with suppress(Exception):
                await refresh_task


async def periodic_cleanup_loop():
    """Run every hour to force-save and trigger age-based cleanup on memory & evolution files."""
    await asyncio.sleep(120)  # wait 2 min after startup
    while True:
        try:
            logger.info("[cleanup] running periodic cleanup...")
            await group_memory._save()
            await arabic_evolution._save()
            logger.info("[cleanup] periodic cleanup complete")
        except Exception as e:
            logger.warning(f"[cleanup] error: {e}")
        await asyncio.sleep(3600)  # every hour


async def main():
    """
    Run the bot forever. If the session dies (network blip, server kick,
    socket close), wait with exponential backoff and restart automatically.
    Only KeyboardInterrupt or SystemExit will exit cleanly.
    """
    global _stop_requested
    ensure_data_files()
    load_members()
    backoff = Config.RESTART_BACKOFF_MIN_S

    try:
        while not _stop_requested:
            start_ts = time.time()
            try:
                await _run_session()
                logger.warning("[main] session ended cleanly — restarting")
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as e:
                logger.exception(f"[main] session crashed: {e}")

            if _stop_requested:
                break

            if time.time() - start_ts > 300:
                backoff = Config.RESTART_BACKOFF_MIN_S
            else:
                backoff = min(backoff * 2, Config.RESTART_BACKOFF_MAX_S)

            logger.info(f"[main] restarting in {backoff}s…")
            # Sleep in small chunks so a shutdown signal is respected quickly
            slept = 0.0
            while slept < backoff and not _stop_requested:
                await asyncio.sleep(1.0)
                slept += 1.0
    finally:
        await close_http()


if __name__ == "__main__":
    keep_alive()
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.error(f"Crash: {e}")
