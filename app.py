import asyncio
import os
import re
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from threading import Thread

from bs4 import BeautifulSoup
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from flask import Flask
import psycopg2
import requests

start_time = None

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
DATABASE_URL = os.getenv("DATABASE_URL")

# === OBSŁUGA BAZY DANYCH (POSTGRESQL / SQLITE FALLBACK) ===


def get_db_connection():
    """Łączy z PostgreSQL na Renderze lub z SQLite lokalnie."""
    if DATABASE_URL:
        # Render podaje URL zaczynający się od postgres://, psycopg2 wymaga postgresql://
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(url), "pg"
    else:
        return sqlite3.connect("bot_data.db"), "sqlite"


def init_db():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()

    if db_type == "pg":
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_frags (
                frag_hash TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS player_stats (
                player_name TEXT PRIMARY KEY,
                guild_name TEXT,
                kills INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS frag_history (
                id SERIAL PRIMARY KEY,
                player_name TEXT,
                entry_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_frags (
                frag_hash TEXT PRIMARY KEY
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                player_name TEXT PRIMARY KEY,
                guild_name TEXT,
                kills INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS frag_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT,
                entry_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    conn.commit()
    cursor.close()
    conn.close()


def is_frag_processed(frag_text):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    ph = "%s" if db_type == "pg" else "?"
    cursor.execute(
        f"SELECT 1 FROM processed_frags WHERE frag_hash = {ph}", (frag_text,)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row is not None


def mark_frags_processed_batch(frag_texts):
    """Szybki zapis zbiorczy wielu fragów w 1 zapytaniu do bazy."""
    if not frag_texts:
        return

    conn, db_type = get_db_connection()
    cursor = conn.cursor()

    try:
        if db_type == "pg":
            args_str = ",".join(
                cursor.mogrify("(%s)", (text,)).decode("utf-8")
                for text in frag_texts
            )
            cursor.execute(f"""
                INSERT INTO processed_frags (frag_hash) 
                VALUES {args_str} 
                ON CONFLICT (frag_hash) DO NOTHING
            """)
        else:
            cursor.executemany(
                "INSERT OR IGNORE INTO processed_frags (frag_hash) VALUES (?)",
                [(text,) for text in frag_texts],
            )
        conn.commit()
    except Exception as e:
        print(f"Błąd podczas zapisywania fragów: {e}")
    finally:
        cursor.close()
        conn.close()


def record_kill_and_death(killer, killer_guild, victim, victim_guild):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()

    if db_type == "pg":
        cursor.execute(
            """
            INSERT INTO player_stats (player_name, guild_name, kills, deaths)
            VALUES (%s, %s, 1, 0)
            ON CONFLICT(player_name) DO UPDATE SET
                guild_name = EXCLUDED.guild_name,
                kills = player_stats.kills + 1
        """,
            (killer, killer_guild),
        )
        cursor.execute(
            """
            INSERT INTO player_stats (player_name, guild_name, kills, deaths)
            VALUES (%s, %s, 0, 1)
            ON CONFLICT(player_name) DO UPDATE SET
                guild_name = EXCLUDED.guild_name,
                deaths = player_stats.deaths + 1
        """,
            (victim, victim_guild),
        )
        cursor.execute(
            """
            INSERT INTO frag_history (player_name, entry_text)
            VALUES (%s, %s)
        """,
            (killer, f"⚔️ Zabił {victim} ({victim_guild})"),
        )
        cursor.execute(
            """
            INSERT INTO frag_history (player_name, entry_text)
            VALUES (%s, %s)
        """,
            (victim, f"💀 Zginął od {killer} ({killer_guild})"),
        )
    else:
        cursor.execute(
            """
            INSERT INTO player_stats (player_name, guild_name, kills, deaths)
            VALUES (?, ?, 1, 0)
            ON CONFLICT(player_name) DO UPDATE SET
                guild_name = excluded.guild_name,
                kills = kills + 1
        """,
            (killer, killer_guild),
        )
        cursor.execute(
            """
            INSERT INTO player_stats (player_name, guild_name, kills, deaths)
            VALUES (?, ?, 0, 1)
            ON CONFLICT(player_name) DO UPDATE SET
                guild_name = excluded.guild_name,
                deaths = deaths + 1
        """,
            (victim, victim_guild),
        )
        cursor.execute(
            """
            INSERT INTO frag_history (player_name, entry_text)
            VALUES (?, ?)
        """,
            (killer, f"⚔️ Zabił {victim} ({victim_guild})"),
        )
        cursor.execute(
            """
            INSERT INTO frag_history (player_name, entry_text)
            VALUES (?, ?)
        """,
            (victim, f"💀 Zginął od {killer} ({killer_guild})"),
        )

    conn.commit()
    cursor.close()
    conn.close()


def get_top_guilds_data():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT guild_name, SUM(kills) as total_kills, SUM(deaths) as total_deaths
        FROM player_stats
        WHERE guild_name != 'Bez Gildii'
        GROUP BY guild_name
        ORDER BY total_kills DESC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def get_player_data(player_name):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    ph = "%s" if db_type == "pg" else "?"

    cursor.execute(
        f"""
        SELECT player_name, guild_name, kills, deaths
        FROM player_stats
        WHERE LOWER(player_name) = LOWER({ph})
    """,
        (player_name,),
    )
    player_row = cursor.fetchone()
    history_rows = []

    if player_row:
        exact_name = player_row[0]
        cursor.execute(
            f"""
            SELECT entry_text FROM frag_history
            WHERE player_name = {ph}
            ORDER BY id DESC LIMIT 5
        """,
            (exact_name,),
        )
        history_rows = [r[0] for r in cursor.fetchall()]

    cursor.close()
    conn.close()
    return player_row, history_rows


init_db()

# === SERWER HTTP DLA RENDER ===
web_app = Flask("")


@web_app.route("/")
def home():
    return "Bot is alive!"


def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port)


def keep_alive():
    t = Thread(target=run_web)
    t.daemon = True
    t.start()


keep_alive()

# === KONFIGURACJA BOTA ===
URL_FRAGS = "http://dblots.org.pl/lastfrags.php?lang=en&s=classic"
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
})
player_guild_cache = {}

# === ZMIENNE STANU BITKI ===
is_bitka_active = False  # Czy trwa obecnie bitka?
bitka_buffer = []  # Wszystkie fragi z obecnej bitki
pre_bitka_buffer = []  # Fragi przed osiągnięciem progu 5
last_frag_time = None  # Czas ostatniego fraga
bitka_start_time = None  # Czas rozpoczęcia bitki


def fetch_guild_from_profile(player_name):
    player_name = player_name.strip()
    if not player_name or len(player_name) > 25 or "->" in player_name:
        return "Bez Gildii"
    if player_name in player_guild_cache:
        return player_guild_cache[player_name]

    safe_name = urllib.parse.quote(player_name)
    url = (
        f"http://dblots.org.pl/characters.php?lang=en&s=classic&char={safe_name}"
    )

    try:
        res = session.get(url, timeout=5)
        if res.status_code != 200:
            return "Bez Gildii"

        soup = BeautifulSoup(res.text, "html.parser")
        guild_name = "Bez Gildii"

        for a in soup.find_all("a", href=True):
            if "guilds.php" in a["href"].lower():
                g_text = a.get_text(strip=True)
                if g_text and g_text.lower() not in ["guilds", "view", "back"]:
                    guild_name = g_text
                    break

        if guild_name == "Bez Gildii":
            for tr in soup.find_all("tr"):
                text = tr.get_text()
                if (
                    "member of" in text.lower()
                    or "leader of" in text.lower()
                    or "guild:" in text.lower()
                ):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        raw_val = tds[1].get_text(strip=True)
                        if " of " in raw_val:
                            guild_name = raw_val.split(" of ")[-1].strip()
                        elif raw_val and "none" not in raw_val.lower():
                            guild_name = raw_val
                        break

        if "->" in guild_name or len(guild_name) > 30 or "\n" in guild_name:
            guild_name = "Bez Gildii"

        player_guild_cache[player_name] = guild_name
        return guild_name
    except Exception:
        return "Bez Gildii"


def parse_frag_line(row_element):
    try:
        char_links = []
        for a in row_element.find_all("a", href=True):
            href = a["href"].lower()
            if "char=" in href or "name=" in href or "characters" in href:
                text = a.get_text(strip=True)
                if (
                    text
                    and len(text) < 25
                    and text.lower() not in ["back", "main", "view"]
                ):
                    char_links.append(text)

        if len(char_links) >= 2:
            return char_links[1], char_links[0]

        row_text = " ".join(row_element.text.split())
        if " killed by " in row_text.lower():
            parts = re.split(
                r"\s+killed\s+by\s+", row_text, flags=re.IGNORECASE
            )
            if len(parts) >= 2:
                killer_raw = parts[1].split("->")[0].strip()
                victim_words = parts[0].strip().split()
                clean_victim = []
                for w in reversed(victim_words):
                    if any(c.isdigit() for c in w) or w.endswith(":"):
                        break
                    clean_victim.insert(0, w)
                victim = " ".join(clean_victim).strip()
                if killer_raw and victim:
                    return killer_raw, victim
    except Exception:
        pass
    return None, None


async def send_bitka_start(channel):
    """Wysyła PIERWSZY i JEDYNY ping na start bitki."""
    role = discord.utils.get(channel.guild.roles, name="bitka")
    role_mention = role.mention if role else "@bitka"

    embed = discord.Embed(
        title="⚔️ ROZPOCZĘŁA SIĘ BITKA!",
        description="Wpadła seria zabójstw! Bot śledzi akcję – pełne podsumowanie pojawi się po zakończeniu starcia.",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc),
    )

    lines = [
        f"• **{k}** (*{kg}*) ⚔️ **{v}** (*{vg}*)"
        for k, kg, v, vg in bitka_buffer[:5]
    ]
    embed.add_field(
        name="Początkowe starcia", value="\n".join(lines), inline=False
    )

    await channel.send(
        content=f"🚨 {role_mention} Właśnie trwa bitka!", embed=embed
    )


async def send_bitka_summary(channel):
    """Wysyła podsumowanie KOŃCOWE w tradycyjnym formacie tekstowym."""
    global is_bitka_active, bitka_buffer, last_frag_time, bitka_start_time

    if not bitka_buffer:
        return

    total_frags = len(bitka_buffer)

    guild_stats = {}  # {guild: [frags, kills, deaths]}
    player_stats = {}  # {player: [guild, frags, kills, deaths]}
    guild_members = {}  # {guild: set(players)}

    for killer, killer_guild, victim, victim_guild in bitka_buffer:
        for g in [killer_guild, victim_guild]:
            if g not in guild_stats:
                guild_stats[g] = [0, 0, 0]
            if g not in guild_members:
                guild_members[g] = set()

        if killer not in player_stats:
            player_stats[killer] = [killer_guild, 0, 0, 0]
        if victim not in player_stats:
            player_stats[victim] = [victim_guild, 0, 0, 0]

        guild_members[killer_guild].add(killer)
        guild_members[victim_guild].add(victim)

        guild_stats[killer_guild][0] += 1
        guild_stats[killer_guild][1] += 1
        player_stats[killer][1] += 1
        player_stats[killer][2] += 1

        guild_stats[victim_guild][2] += 1
        player_stats[victim][3] += 1

    mvp_player = "Brak"
    max_kills = -1
    for p_name, data in player_stats.items():
        if data[2] > max_kills:
            max_kills = data[2]
            mvp_player = p_name

    start_str = (
        bitka_start_time.strftime("%Y.%m.%d %H:%M:%S")
        if bitka_start_time
        else "N/A"
    )
    end_str = datetime.now().strftime("%H:%M:%S")

    report = []
    report.append("LAST BATTLE REPORT")
    report.append("━" * 60)
    report.append(
        f"PODSUMOWANIE OSTATNIEJ BITKI {start_str} → {end_str} Fragi: {total_frags} MVP: {mvp_player}"
    )
    report.append("")
    report.append("━" * 60)
    report.append("GILDIE:")

    sorted_guilds = sorted(
        guild_stats.items(), key=lambda x: x[1][0], reverse=True
    )
    for g_name, stat in sorted_guilds:
        report.append(f"• {g_name:<20} |  {stat[0]}  {stat[1]}  {stat[2]}")

    report.append("")
    report.append("━" * 60)
    report.append("GRACZE:")

    sorted_players = sorted(
        player_stats.items(), key=lambda x: x[1][2], reverse=True
    )
    for p_name, data in sorted_players:
        report.append(
            f"{p_name} ({data[0]}) |  {data[1]}  {data[2]}  {data[3]}"
        )

    report.append("━" * 60)
    report.append("")
    report.append(" BATTLE — UCZESTNICY GILDII")
    report.append("━" * 60)
    report.append("SKŁADY GILDII — OSTATNIA BITWA:")
    report.append("")

    for g_name, members in sorted_guilds:
        m_list = guild_members[g_name]
        names_str = ", ".join(sorted(m_list))
        report.append(f"• {g_name} ({len(m_list)}): {names_str}")
        report.append("")

    report.append("━" * 60)

    full_text = "\n".join(report)

    if len(full_text) > 1900:
        chunks = [
            full_text[i : i + 1800] for i in range(0, len(full_text), 1800)
        ]
        for chunk in chunks:
            await channel.send(f"```text\n{chunk}\n```")
    else:
        await channel.send(f"```text\n{full_text}\n```")

    is_bitka_active = False
    bitka_buffer.clear()
    pre_bitka_buffer.clear()
    last_frag_time = None
    bitka_start_time = None


@tasks.loop(seconds=5)
async def check_frags():
    global is_bitka_active, bitka_buffer, pre_bitka_buffer, last_frag_time, bitka_start_time

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    try:
        # POBIERANIE STRONY W ODRĘBNYM WĄTKU (BEZ BLOKOWANIA ASYNCIO)
        res = await asyncio.to_thread(session.get, URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        rows = list(soup.find_all("tr"))

        now = datetime.now(timezone.utc)
        new_processed_frags = []

        for row in reversed(rows):
            row_text = row.get_text(strip=True)
            if not row_text or len(row_text) < 10:
                continue

            # SPRAWDZANIE W BAZIE W ODRĘBNYM WĄTKU
            processed = await asyncio.to_thread(is_frag_processed, row_text)
            if not processed:
                killer, victim = parse_frag_line(row)
                if killer and victim:
                    killer_guild = await asyncio.to_thread(
                        fetch_guild_from_profile, killer
                    )
                    victim_guild = await asyncio.to_thread(
                        fetch_guild_from_profile, victim
                    )

                    await asyncio.to_thread(
                        record_kill_and_death,
                        killer,
                        killer_guild,
                        victim,
                        victim_guild,
                    )

                    frag_data = (killer, killer_guild, victim, victim_guild)
                    last_frag_time = now

                    if is_bitka_active:
                        bitka_buffer.append(frag_data)
                    else:
                        pre_bitka_buffer.append(frag_data)

                        if len(pre_bitka_buffer) >= 5:
                            is_bitka_active = True
                            bitka_start_time = datetime.now()
                            bitka_buffer = list(pre_bitka_buffer)
                            pre_bitka_buffer.clear()
                            await send_bitka_start(channel)

                new_processed_frags.append(row_text)

        if new_processed_frags:
            await asyncio.to_thread(
                mark_frags_processed_batch, new_processed_frags
            )

        if not is_bitka_active and last_frag_time:
            if (now - last_frag_time).total_seconds() > 180:
                pre_bitka_buffer.clear()
                last_frag_time = None

        if is_bitka_active and last_frag_time:
            if (now - last_frag_time).total_seconds() >= 600:
                await send_bitka_summary(channel)

    except Exception as e:
        print(f"Błąd pętli: {e}")


@bot.event
async def on_ready():
    global start_time
    if start_time is None:
        start_time = datetime.now(timezone.utc)
    print(f"Zalogowano jako {bot.user.name}")

    try:
        res = await asyncio.to_thread(session.get, URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        initial_frags = [
            row.get_text(strip=True)
            for row in soup.find_all("tr")
            if row.get_text(strip=True)
        ]
        await asyncio.to_thread(mark_frags_processed_batch, initial_frags)
    except Exception as e:
        print(f"Błąd podczas inicjalizacji startowej: {e}")

    if not check_frags.is_running():
        check_frags.start()


@bot.command(name="top")
async def top_guilds(ctx):
    top_guilds_list = await asyncio.to_thread(get_top_guilds_data)
    if not top_guilds_list:
        await ctx.send("Brak zarejestrowanych zabójstw w bazie danych.")
        return

    embed = discord.Embed(title="🏆 Ranking Gildii", color=discord.Color.gold())
    for guild, kills, deaths in top_guilds_list:
        embed.add_field(
            name=f"🛡️ {guild}",
            value=f"Kills: `{kills}` | Deaths: `{deaths}`",
            inline=False,
        )

    await ctx.send(embed=embed)


@bot.command(name="gracz")
async def player_info(ctx, *, player_name: str):
    player_row, history = await asyncio.to_thread(get_player_data, player_name)
    if not player_row:
        await ctx.send(f"Nie znaleziono danych dla gracza **{player_name}**.")
        return

    exact_name, guild, kills, deaths = player_row
    embed = discord.Embed(
        title=f"👤 Statystyki: {exact_name}", color=discord.Color.blue()
    )
    embed.add_field(name="Gildia", value=guild, inline=True)
    embed.add_field(
        name="Kills / Deaths", value=f"`{kills}` / `{deaths}`", inline=True
    )

    if history:
        embed.add_field(
            name="Ostatnie starcia", value="\n".join(history), inline=False
        )
    else:
        embed.add_field(
            name="Ostatnie starcia", value="Brak wpisów", inline=False
        )

    await ctx.send(embed=embed)


@bot.command(name="status")
async def status(ctx):
    if start_time is None:
        await ctx.send("Bot dopiero się uruchamia...")
        return

    now = datetime.now(timezone.utc)
    uptime = now - start_time
    dni = uptime.days
    godziny, remainder = divmod(uptime.seconds, 3600)
    minuty, sekundy = divmod(remainder, 60)

    embed = discord.Embed(
        title="📊 Status Bota", color=discord.Color.green(), timestamp=now
    )
    embed.add_field(name="Stan", value="🟢 Online (24/7)", inline=False)
    embed.add_field(
        name="Czas działania (Uptime)",
        value=f"{dni}d {godziny}h {minuty}m {sekundy}s",
        inline=False,
    )
    embed.add_field(
        name="Opóźnienie (Ping)",
        value=f"{round(bot.latency * 1000)} ms",
        inline=False,
    )
    embed.set_footer(text=f"Wywołano przez {ctx.author.display_name}")

    await ctx.send(embed=embed)


bot.run(DISCORD_TOKEN)
