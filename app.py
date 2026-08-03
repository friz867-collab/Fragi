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
import requests

start_time = None

load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

# === BAZA DANYCH SQLITE ===
DB_NAME = "bot_data.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
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
    conn.close()


def is_frag_processed(frag_text):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_frags WHERE frag_hash = ?", (frag_text,))
    row = cursor.fetchone()
    conn.close()
    return row is not None


def mark_frag_processed(frag_text):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO processed_frags (frag_hash) VALUES (?)", (frag_text,))
    conn.commit()
    conn.close()


def record_kill_and_death(killer, killer_guild, victim, victim_guild):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO player_stats (player_name, guild_name, kills, deaths)
        VALUES (?, ?, 1, 0)
        ON CONFLICT(player_name) DO UPDATE SET
            guild_name = excluded.guild_name,
            kills = kills + 1
    """, (killer, killer_guild))
    cursor.execute("""
        INSERT INTO player_stats (player_name, guild_name, kills, deaths)
        VALUES (?, ?, 0, 1)
        ON CONFLICT(player_name) DO UPDATE SET
            guild_name = excluded.guild_name,
            deaths = deaths + 1
    """, (victim, victim_guild))
    cursor.execute("""
        INSERT INTO frag_history (player_name, entry_text)
        VALUES (?, ?)
    """, (killer, f"⚔️ Zabił {victim} ({victim_guild})"))
    cursor.execute("""
        INSERT INTO frag_history (player_name, entry_text)
        VALUES (?, ?)
    """, (victim, f"💀 Zginął od {killer} ({killer_guild})"))
    conn.commit()
    conn.close()


def get_top_guilds_data():
    conn = sqlite3.connect(DB_NAME)
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
    conn.close()
    return rows


def get_player_data(player_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT player_name, guild_name, kills, deaths
        FROM player_stats
        WHERE LOWER(player_name) = LOWER(?)
    """, (player_name,))
    player_row = cursor.fetchone()
    history_rows = []
    if player_row:
        exact_name = player_row[0]
        cursor.execute("""
            SELECT entry_text FROM frag_history
            WHERE player_name = ?
            ORDER BY id DESC LIMIT 5
        """, (exact_name,))
        history_rows = [r[0] for r in cursor.fetchall()]
    conn.close()
    return player_row, history_rows


init_db()

# === SERWER HTTP DLA RENDER ===
web_app = Flask('')


@web_app.route('/')
def home():
    return "Bot is alive!"


def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host='0.0.0.0', port=port)


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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})
player_guild_cache = {}

# Zmienne bufora
frag_buffer = []
buffer_start_time = None


def fetch_guild_from_profile(player_name):
    player_name = player_name.strip()
    if not player_name or len(player_name) > 25 or "->" in player_name:
        return "Bez Gildii"
    if player_name in player_guild_cache:
        return player_guild_cache[player_name]

    safe_name = urllib.parse.quote(player_name)
    url = f"http://dblots.org.pl/characters.php?lang=en&s=classic&char={safe_name}"

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
                if "member of" in text.lower() or "leader of" in text.lower() or "guild:" in text.lower():
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
                if text and len(text) < 25 and text.lower() not in ["back", "main", "view"]:
                    char_links.append(text)

        if len(char_links) >= 2:
            return char_links[1], char_links[0]

        row_text = " ".join(row_element.text.split())
        if " killed by " in row_text.lower():
            parts = re.split(r"\s+killed\s+by\s+", row_text, flags=re.IGNORECASE)
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


async def trigger_bitka_alert(channel):
    global frag_buffer, buffer_start_time
    if not frag_buffer:
        return

    role = discord.utils.get(channel.guild.roles, name="bitka")
    role_mention = role.mention if role else "@bitka"

    embed = discord.Embed(
        title=f"🔥 GORĄCA BITKA! ({len(frag_buffer)} fragów w krótkim czasie)",
        color=discord.Color.red(),
        timestamp=datetime.now(timezone.utc)
    )

    lines = []
    for killer, killer_guild, victim, victim_guild in frag_buffer:
        lines.append(f"• **{killer}** (*{killer_guild}*) ⚔️ **{victim}** (*{victim_guild}*)")

    content_text = "\n".join(lines)
    if len(content_text) > 1024:
        content_text = content_text[:1000] + "\n...i więcej."

    embed.add_field(name="Zabójstwa w akcji", value=content_text, inline=False)
    await channel.send(content=f"🚨 {role_mention} Właśnie trwa bitka!", embed=embed)

    # Reset bufora po pingu
    frag_buffer.clear()
    buffer_start_time = None


@tasks.loop(seconds=5)
async def check_frags():
    global frag_buffer, buffer_start_time
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    try:
        res = session.get(URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        rows = list(soup.find_all("tr"))

        for row in reversed(rows):
            row_text = row.get_text(strip=True)
            if not row_text or len(row_text) < 10:
                continue

            if not is_frag_processed(row_text):
                killer, victim = parse_frag_line(row)
                if killer and victim:
                    killer_guild = fetch_guild_from_profile(killer)
                    victim_guild = fetch_guild_from_profile(victim)

                    record_kill_and_death(killer, killer_guild, victim, victim_guild)

                    now = datetime.now(timezone.utc)

                    # Resetuj bufor, jeśli od pierwszego fraga minęły już 3 minuty
                    if buffer_start_time is not None and (now - buffer_start_time).total_seconds() > 180:
                        frag_buffer.clear()
                        buffer_start_time = None

                    if buffer_start_time is None:
                        buffer_start_time = now

                    frag_buffer.append((killer, killer_guild, victim, victim_guild))

                    # ODRADZA NATYCHMIAST: Jeśli dobiliśmy do 5 fragów w ciągu okna 3 minut
                    if len(frag_buffer) >= 5:
                        await trigger_bitka_alert(channel)

                mark_frag_processed(row_text)

        # Wyczyszczenie starego bufora, jeśli upłynęły 3 minuty i nie dobito do 5 fragów
        if buffer_start_time is not None:
            now = datetime.now(timezone.utc)
            if (now - buffer_start_time).total_seconds() > 180:
                frag_buffer.clear()
                buffer_start_time = None

    except Exception as e:
        print(f"Błąd pętli: {e}")


@bot.event
async def on_ready():
    global start_time
    if start_time is None:
        start_time = datetime.now(timezone.utc)
    print(f"Zalogowano jako {bot.user.name}")

    try:
        res = session.get(URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        for row in soup.find_all("tr"):
            row_text = row.get_text(strip=True)
            if row_text:
                mark_frag_processed(row_text)
    except Exception:
        pass

    check_frags.start()


@bot.command(name="top")
async def top_guilds(ctx):
    top_guilds_list = get_top_guilds_data()
    if not top_guilds_list:
        await ctx.send("Brak zarejestrowanych zabójstw w bazie danych.")
        return

    embed = discord.Embed(title="🏆 Ranking Gildii", color=discord.Color.gold())
    for guild, kills, deaths in top_guilds_list:
        embed.add_field(name=f"🛡️ {guild}", value=f"Kills: `{kills}` | Deaths: `{deaths}`", inline=False)

    await ctx.send(embed=embed)


@bot.command(name="gracz")
async def player_info(ctx, *, player_name: str):
    player_row, history = get_player_data(player_name)
    if not player_row:
        await ctx.send(f"Nie znaleziono danych dla gracza **{player_name}**.")
        return

    exact_name, guild, kills, deaths = player_row
    embed = discord.Embed(title=f"👤 Statystyki: {exact_name}", color=discord.Color.blue())
    embed.add_field(name="Gildia", value=guild, inline=True)
    embed.add_field(name="Kills / Deaths", value=f"`{kills}` / `{deaths}`", inline=True)

    if history:
        embed.add_field(name="Ostatnie starcia", value="\n".join(history), inline=False)
    else:
        embed.add_field(name="Ostatnie starcia", value="Brak wpisów", inline=False)

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

    embed = discord.Embed(title="📊 Status Bota", color=discord.Color.green(), timestamp=now)
    embed.add_field(name="Stan", value="🟢 Online (24/7)", inline=False)
    embed.add_field(name="Czas działania (Uptime)", value=f"{dni}d {godziny}h {minuty}m {sekundy}s", inline=False)
    embed.add_field(name="Opóźnienie (Ping)", value=f"{round(bot.latency * 1000)} ms", inline=False)
    embed.set_footer(text=f"Wywołano przez {ctx.author.display_name}")

    await ctx.send(embed=embed)


bot.run(DISCORD_TOKEN)
