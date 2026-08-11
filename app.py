import asyncio
import os
import re
import sqlite3
import urllib.parse
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, timedelta
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

# === KONFIGURACJA FILTRU ===
MAX_LEVEL_DIFF = 500  # Maksymalna różnica leveli

# === OBSŁUGA BAZY DANYCH (POSTGRESQL / SQLITE FALLBACK) ===

def get_db_connection():
    """Łączy z PostgreSQL na Renderze lub z SQLite lokalnie."""
    if DATABASE_URL:
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
            CREATE TABLE IF NOT EXISTS abuse_logs (
                id SERIAL PRIMARY KEY,
                killer TEXT,
                killer_lvl INTEGER,
                victim TEXT,
                victim_lvl INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS battle_history (
                id SERIAL PRIMARY KEY,
                report_text TEXT,
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS abuse_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                killer TEXT,
                killer_lvl INTEGER,
                victim TEXT,
                victim_lvl INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS battle_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_text TEXT,
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


def log_abuse(killer, killer_lvl, victim, victim_lvl):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    try:
        if db_type == "pg":
            cursor.execute(
                """
                INSERT INTO abuse_logs (killer, killer_lvl, victim, victim_lvl)
                VALUES (%s, %s, %s, %s)
            """,
                (killer, killer_lvl, victim, victim_lvl),
            )
        else:
            cursor.execute(
                """
                INSERT INTO abuse_logs (killer, killer_lvl, victim, victim_lvl)
                VALUES (?, ?, ?, ?)
            """,
                (killer, killer_lvl, victim, victim_lvl),
            )
        conn.commit()
    except Exception as e:
        print(f"Błąd logowania nadużycia: {e}")
    finally:
        cursor.close()
        conn.close()


def save_battle_report(report_text):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    try:
        if db_type == "pg":
            cursor.execute("INSERT INTO battle_history (report_text) VALUES (%s)", (report_text,))
        else:
            cursor.execute("INSERT INTO battle_history (report_text) VALUES (?)", (report_text,))
        conn.commit()
    except Exception as e:
        print(f"Błąd zapisu raportu bitwy: {e}")
    finally:
        cursor.close()
        conn.close()


def get_last_battle_report():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT report_text FROM battle_history ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"Błąd pobierania ostatniego raportu: {e}")
        return None
    finally:
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


def get_top_players_data():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT player_name, guild_name, kills, deaths
        FROM player_stats
        ORDER BY kills DESC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def get_top_players_24h_data():
    conn, db_type = get_db_connection()
    cursor = conn.cursor()

    if db_type == "pg":
        cursor.execute("""
            SELECT player_name, COUNT(*) as kills_24h
            FROM frag_history
            WHERE entry_text LIKE '⚔️ Zabił%'
              AND created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY player_name
            ORDER BY kills_24h DESC
            LIMIT 5
        """)
    else:
        cursor.execute("""
            SELECT player_name, COUNT(*) as kills_24h
            FROM frag_history
            WHERE entry_text LIKE '⚔️ Zabił%'
              AND created_at >= datetime('now', '-1 day')
            GROUP BY player_name
            ORDER BY kills_24h DESC
            LIMIT 5
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

    exact_name = player_name
    if player_row:
        exact_name = player_row[0]
    else:
        cursor.execute(
            f"""
            SELECT player_name FROM frag_history
            WHERE LOWER(player_name) = LOWER({ph})
            LIMIT 1
        """,
            (player_name,),
        )
        found_hist = cursor.fetchone()
        if found_hist:
            exact_name = found_hist[0]
        else:
            cursor.close()
            conn.close()
            return None, [], "Brak danych", "Brak danych"

    cursor.execute(
        f"""
        SELECT entry_text FROM frag_history
        WHERE LOWER(player_name) = LOWER({ph})
        ORDER BY id DESC LIMIT 5
    """,
        (exact_name,),
    )
    history_rows = [r[0] for r in cursor.fetchall()]

    cursor.execute(
        f"""
        SELECT entry_text FROM frag_history
        WHERE LOWER(player_name) = LOWER({ph})
    """,
        (exact_name,),
    )
    all_entries = cursor.fetchall()

    killers_count = {}
    victims_count = {}

    for (entry,) in all_entries:
        if "Zginął od" in entry:
            match = re.search(r"Zginął od (.+?)(?:\s*\(|$)", entry)
            if match:
                k_name = match.group(1).strip()
                killers_count[k_name] = killers_count.get(k_name, 0) + 1
        elif "Zabił" in entry:
            match = re.search(r"Zabił (.+?)(?:\s*\(|$)", entry)
            if match:
                v_name = match.group(1).strip()
                victims_count[v_name] = victims_count.get(v_name, 0) + 1

    nemesis_info = "Brak (nie zginął od nikogo)"
    if killers_count:
        top_killer = max(killers_count, key=killers_count.get)
        nemesis_info = f"**{top_killer}** ({killers_count[top_killer]} razy)"

    victim_info = "Brak (nikogo nie zabił)"
    if victims_count:
        top_victim = max(victims_count, key=victims_count.get)
        victim_info = f"**{top_victim}** ({victims_count[top_victim]} razy)"

    if not player_row:
        total_kills = sum(victims_count.values())
        total_deaths = sum(killers_count.values())
        player_row = (exact_name, "Bez Gildii", total_kills, total_deaths)

    cursor.close()
    conn.close()
    return player_row, history_rows, nemesis_info, victim_info


def get_player_data_detailed(player_name):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    ph = "%s" if db_type == "pg" else "?"

    cursor.execute(f"""
        SELECT player_name, guild_name, kills, deaths
        FROM player_stats
        WHERE LOWER(player_name) = LOWER({ph})
    """, (player_name,))
    player_row = cursor.fetchone()

    exact_name = player_name
    if player_row:
        exact_name = player_row[0]
    else:
        cursor.execute(f"SELECT player_name FROM frag_history WHERE LOWER(player_name) = LOWER({ph}) LIMIT 1", (player_name,))
        found_hist = cursor.fetchone()
        if found_hist:
            exact_name = found_hist[0]
        else:
            cursor.close()
            conn.close()
            return None

    cursor.execute(f"""
        SELECT entry_text FROM frag_history
        WHERE LOWER(player_name) = LOWER({ph})
    """, (exact_name,))
    all_entries = cursor.fetchall()

    killers_count = {}
    victims_count = {}

    for (entry,) in all_entries:
        if "Zginął od" in entry:
            match = re.search(r"Zginął od ([^(\n]+)", entry)
            if match:
                k_name = match.group(1).strip()
                killers_count[k_name] = killers_count.get(k_name, 0) + 1
        elif "Zabił" in entry:
            match = re.search(r"Zabił ([^(\n]+)", entry)
            if match:
                v_name = match.group(1).strip()
                victims_count[v_name] = victims_count.get(v_name, 0) + 1

    sorted_victims = sorted(victims_count.items(), key=lambda x: x[1], reverse=True)
    sorted_killers = sorted(killers_count.items(), key=lambda x: x[1], reverse=True)

    if not player_row:
        total_kills = sum(victims_count.values())
        total_deaths = sum(killers_count.values())
        player_row = (exact_name, "Bez Gildii", total_kills, total_deaths)

    cursor.close()
    conn.close()
    
    return {
        "profile": player_row,
        "victims": sorted_victims,
        "killers": sorted_killers
    }


def get_guild_confrontations_data(guild_name):
    conn, db_type = get_db_connection()
    cursor = conn.cursor()
    ph = "%s" if db_type == "pg" else "?"

    cursor.execute(
        f"""
        SELECT player_name, guild_name FROM player_stats
        WHERE LOWER(guild_name) = LOWER({ph})
    """,
        (guild_name,),
    )
    players = cursor.fetchall()

    if not players:
        cursor.close()
        conn.close()
        return None, {}, 0, 0

    exact_guild_name = players[0][1]
    player_names = [p[0] for p in players]

    confrontations = {}
    total_guild_kills = 0
    total_guild_deaths = 0

    for p_name in player_names:
        cursor.execute(
            f"""
            SELECT entry_text FROM frag_history
            WHERE LOWER(player_name) = LOWER({ph})
        """,
            (p_name,),
        )
        entries = cursor.fetchall()

        for (entry,) in entries:
            if "Zabił" in entry:
                match = re.search(r"Zabił .+? \((.+?)\)", entry)
                if match:
                    opp_guild = match.group(1).strip()
                    if opp_guild.lower() != exact_guild_name.lower():
                        if opp_guild not in confrontations:
                            confrontations[opp_guild] = {"kills": 0, "deaths": 0}
                        confrontations[opp_guild]["kills"] += 1
                        total_guild_kills += 1

            elif "Zginął od" in entry:
                match = re.search(r"Zginął od .+? \((.+?)\)", entry)
                if match:
                    opp_guild = match.group(1).strip()
                    if opp_guild.lower() != exact_guild_name.lower():
                        if opp_guild not in confrontations:
                            confrontations[opp_guild] = {"kills": 0, "deaths": 0}
                        confrontations[opp_guild]["deaths"] += 1
                        total_guild_deaths += 1

    cursor.close()
    conn.close()
    return (
        exact_guild_name,
        confrontations,
        total_guild_kills,
        total_guild_deaths,
    )


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
bot.remove_command("help")

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
})
player_guild_cache = {}

# === ZMIENNE STANU BITKI ===
is_bitka_active = False
bitka_buffer = []
pre_bitka_buffer = []
last_frag_time = None
bitka_start_time = None


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
        row_text = " ".join(row_element.text.split())
        
        # Oczyszczamy tekst z daty i godziny na początku linii (np. "2026.08.11 15:59:01")
        row_text_clean = re.sub(r'^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}', '', row_text).strip()
        
        killer, victim = None, None
        killer_lvl, victim_lvl = 0, 0

        char_links = []
        for a in row_element.find_all("a", href=True):
            href = a["href"].lower()
            if "char=" in href or "name=" in href or "characters" in href:
                text = a.get_text(strip=True)
                if (
                    text
                    and len(text) < 25
                    and text.lower()
                    not in ["back", "main", "view", "characters", "guilds"]
                ):
                    char_links.append(text)

        if len(char_links) >= 2:
            victim, killer = char_links[0], char_links[1]

        # Szukamy poziomów wyłącznie w tekście pozbawionym daty
        numbers = [int(s) for s in re.findall(r"\b\d+\b", row_text_clean)]
        
        if len(numbers) >= 2:
            victim_lvl = numbers[0]
            killer_lvl = numbers[1]
        else:
            levels_found = [
                int(s)
                for s in re.findall(r"(\d+)\s*(?:lvl|level)", row_text_clean, re.IGNORECASE)
            ]
            if len(levels_found) >= 2:
                victim_lvl = levels_found[0]
                killer_lvl = levels_found[1]

        if killer and victim:
            return killer, killer_lvl, victim, victim_lvl

    except Exception as e:
        print(f"Błąd parsowania linii: {e}")

    return None, 0, None, 0


async def send_bitka_start(channel):
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
    embed.set_footer(text=f"Filtr nadużyć włączony: max {MAX_LEVEL_DIFF} lvl różnicy")

    await channel.send(
        content=f"🚨 {role_mention} Właśnie trwa bitka!", embed=embed
    )


async def send_bitka_summary(channel):
    global is_bitka_active, bitka_buffer, last_frag_time, bitka_start_time

    if not bitka_buffer:
        return

    total_frags = len(bitka_buffer)

    guild_stats = {}
    player_stats = {}
    guild_members = {}

    for killer, killer_guild, victim, victim_guild in bitka_buffer:
        for g in [killer_guild, victim_guild]:
            if g not in guild_stats:
                guild_stats[g] = {"kills": 0, "deaths": 0}
            if g not in guild_members:
                guild_members[g] = set()

        if killer not in player_stats:
            player_stats[killer] = {"guild": killer_guild, "kills": 0, "deaths": 0}
        if victim not in player_stats:
            player_stats[victim] = {"guild": victim_guild, "kills": 0, "deaths": 0}

        guild_members[killer_guild].add(killer)
        guild_members[victim_guild].add(victim)

        # Naliczanie zabójstwa
        guild_stats[killer_guild]["kills"] += 1
        player_stats[killer]["kills"] += 1

        # Naliczanie zgonu
        guild_stats[victim_guild]["deaths"] += 1
        player_stats[victim]["deaths"] += 1

    mvp_player = "Brak"
    max_kills = -1
    for p_name, data in player_stats.items():
        if data["kills"] > max_kills:
            max_kills = data["kills"]
            mvp_player = p_name

    # --- KONWERSJA CZASU NA CZAS POLSKI ---
    tz_pl = ZoneInfo("Europe/Warsaw")
    
    if bitka_start_time:
        if bitka_start_time.tzinfo is None:
            bitka_start = bitka_start_time.replace(tzinfo=timezone.utc).astimezone(tz_pl)
        else:
            bitka_start = bitka_start_time.astimezone(tz_pl)
        start_str = bitka_start.strftime("%Y.%m.%d %H:%M:%S")
    else:
        start_str = "N/A"

    end_str = datetime.now(tz_pl).strftime("%H:%M:%S")

    report = []
    report.append("LAST BATTLE REPORT")
    report.append("━" * 60)
    report.append(
        f"PODSUMOWANIE OSTATNIEJ BITKI {start_str} → {end_str} Fragi: {total_frags} MVP: {mvp_player}"
    )
    report.append("")
    report.append("━" * 60)
    report.append("GILDIE (Zabójstwa | Zgony | Bilans):")

    sorted_guilds = sorted(
        guild_stats.items(), key=lambda x: x[1]["kills"], reverse=True
    )
    for g_name, stat in sorted_guilds:
        k = stat["kills"]
        d = stat["deaths"]
        diff = k - d
        diff_str = f"+{diff}" if diff > 0 else str(diff)
        report.append(f"• {g_name:<20} |  Zabójstwa: {k:<3} | Zgony: {d:<3} | Bilans: {diff_str}")

    report.append("")
    report.append("━" * 60)
    report.append("GRACZE (Zabójstwa | Zgony):")

    sorted_players = sorted(
        player_stats.items(), key=lambda x: x[1]["kills"], reverse=True
    )
    for p_name, data in sorted_players:
        k = data["kills"]
        d = data["deaths"]
        report.append(
            f"{p_name:<18} ({data['guild']}) | Fragi: {k:<2} | Zgony: {d:<2}"
        )

    report.append("━" * 60)
    report.append("")
    report.append(" BATTLE — UCZESTNICY GILDII")
    report.append("━" * 60)
    report.append("SKŁARDS GILDII — OSTATNIA BITWA:")
    report.append("")

    for g_name, _ in sorted_guilds:
        m_list = guild_members[g_name]
        names_str = ", ".join(sorted(m_list))
        report.append(f"• {g_name} ({len(m_list)}): {names_str}")
        report.append("")

    report.append("━" * 60)

    full_text = "\n".join(report)

    # Automatyczny zapis wygenerowanego raportu tekstowego do bazy danych
    await asyncio.to_thread(save_battle_report, full_text)

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
        res = await asyncio.to_thread(session.get, URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        rows = list(soup.find_all("tr"))

        now = datetime.now(timezone.utc)
        new_processed_frags = []

        for row in reversed(rows):
            row_text = row.get_text(strip=True)
            if not row_text or len(row_text) < 10:
                continue

            processed = await asyncio.to_thread(is_frag_processed, row_text)
            if not processed:
                killer, killer_lvl, victim, victim_lvl = parse_frag_line(row)
                if killer and victim:
                    
                    print(f"🔍 [PARSER LOG] Wykryto: {killer} ({killer_lvl} lvl) ⚔️ {victim} ({victim_lvl} lvl)")
                    
                    if killer_lvl > 0 and victim_lvl > 0:
                        lvl_diff = abs(killer_lvl - victim_lvl)
                        if lvl_diff > MAX_LEVEL_DIFF:
                            print(f"🛑 [ABUSE FILTER] ZABLOKOWANO FRAG: Różnica {lvl_diff} lvl (Max {MAX_LEVEL_DIFF})")
                            await asyncio.to_thread(log_abuse, killer, killer_lvl, victim, victim_lvl)
                            new_processed_frags.append(row_text)
                            continue
                            
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
                            bitka_start_time = datetime.now(timezone.utc)
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


# === KOMENDY BOTA ===


@bot.command(name="koniecbitki", aliases=["endbitka"])
async def end_bitka(ctx):
    global is_bitka_active

    if not is_bitka_active:
        await ctx.send("W tym momencie nie trwa żadna bitka.")
        return

    await ctx.send("🛑 Ręczne zamykanie bitki... Generuję podsumowanie.")
    await send_bitka_summary(ctx.channel)


@bot.command(name="lastbattle", aliases=["ostatniabitka"])
async def last_battle(ctx):
    """Pobiera i wyświetla z bazy danych raport z ostatniej zakończonej bitki."""
    report_text = await asyncio.to_thread(get_last_battle_report)
    
    if not report_text:
        await ctx.send("🫥 W bazie danych nie ma jeszcze żadnego zapisanego raportu z bitwy.")
        return

    if len(report_text) > 1900:
        chunks = [report_text[i : i + 1800] for i in range(0, len(report_text), 1800)]
        for chunk in chunks:
            await ctx.send(f"```text\n{chunk}\n```")
    else:
        await ctx.send(f"```text\n{report_text}\n```")


@bot.command(name="pomoc", aliases=["help"])
async def pomoc(ctx):
    embed = discord.Embed(
        title="📜 Lista Dostępnych Komend Bota",
        description="Wszystkie komendy rozpoczynają się od przedrostka `!`",
        color=discord.Color.blue(),
    )

    embed.add_field(
        name="🏆 Rankingi i Statystyki",
        value=(
            "`!top` — Wyświetla TOP 10 najlepszych gildii według zabójstw.\n"
            "`!topgracze` — Wyświetla TOP 10 graczy z największą liczbą zabić ogółem.\n"
            "`!top24h` — Wyświetla TOP 5 graczy z największą liczbą zabójstw w ostatnich 24h."
        ),
        inline=False,
    )

    embed.add_field(
        name="🛡️ Informacje o Gildiach i Graczach",
        value=(
            "`!gildia <nazwa>` — Wyświetla bilans konfrontacji danej gildii z przeciwnikami.\n"
            "`!gracz <nick>` — Wyświetla K/D ratio, Ostatnie starcia i Nemezis.\n"
            "`!gracz2 <nick>` — Wyświetla pełną analitykę (kogo zabił ile razy i od kogo padł)."
        ),
        inline=False,
    )

    embed.add_field(
        name="⚔️ Kontrola i Podgląd Bitki",
        value=(
            "`!battlelive` / `!blive` — Pokazuje bieżące statystyki i punktację trwającej walki.\n"
            "`!koniecbitki` — Ręcznie kończy aktywne starcie i generuje pełny raport końcowy.\n"
            "`!lastbattle` / `!ostatniabitka` — Wyświetla tekstowy raport z ostatniej odbytej bitwy."
        ),
        inline=False,
    )

    embed.add_field(
        name="⚙️ System i Informacje",
        value=(
            "`!status` — Sprawdza stan bota, czas działania (uptime) oraz opóźnienie (ping).\n"
            "`!pomoc` / `!help` — Wyświetla tę listę komend."
        ),
        inline=False,
    )

    embed.set_footer(text=f"Wywołano przez {ctx.author.display_name}")
    await ctx.send(embed=embed)


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


@bot.command(name="topgracze")
async def top_players(ctx):
    top_list = await asyncio.to_thread(get_top_players_data)
    if not top_list:
        await ctx.send("Brak zarejestrowanych zabójstw w bazie danych.")
        return

    embed = discord.Embed(
        title="🏆 Ranking TOP 10 Graczy", color=discord.Color.gold()
    )
    for idx, (player, guild, kills, deaths) in enumerate(top_list, 1):
        medal = (
            "🥇"
            if idx == 1
            else "🥈"
            if idx == 2
            else "🥉"
            if idx == 3
            else f"#{idx}"
        )
        embed.add_field(
            name=f"{medal} {player} ({guild})",
            value=f"Kills: `{kills}` | Deaths: `{deaths}`",
            inline=False,
        )

    await ctx.send(embed=embed)


@bot.command(name="top24h")
async def top_players_24h(ctx):
    top_list = await asyncio.to_thread(get_top_players_24h_data)
    if not top_list:
        await ctx.send("Brak fragów w ostatnich 24 godzinach.")
        return

    embed = discord.Embed(
        title="🔥 TOP Gracze Ostatnich 24h", color=discord.Color.orange()
    )
    for idx, (player, kills_24h) in enumerate(top_list, 1):
        medal = (
            "🥇"
            if idx == 1
            else "🥈"
            if idx == 2
            else "🥉"
            if idx == 3
            else f"#{idx}"
        )
        embed.add_field(
            name=f"{medal} {player}",
            value=f"Fragi w 24h: `{kills_24h}`",
            inline=False,
        )

    await ctx.send(embed=embed)


@bot.command(name="gracz")
async def player_info(ctx, *, player_name: str):
    player_row, history, nemesis, victim = await asyncio.to_thread(
        get_player_data, player_name
    )
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

    embed.add_field(name="👿 Nemezis", value=nemesis, inline=False)
    embed.add_field(name="🎯 Ulubiona Ofiara", value=victim, inline=False)

    if history:
        embed.add_field(
            name="Ostatnie starcia", value="\n".join(history), inline=False
        )
    else:
        embed.add_field(
            name="Ostatnie starcia", value="Brak wpisów", inline=False
        )

    await ctx.send(embed=embed)


@bot.command(name="gracz2", aliases=["szczegoly"])
async def player_info_detailed(ctx, *, player_name: str):
    data = await asyncio.to_thread(get_player_data_detailed, player_name)
    
    if not data:
        await ctx.send(f"❌ Nie znaleziono żadnych danych ani historii walk dla gracza **{player_name}**.")
        return

    exact_name, guild, kills, deaths = data["profile"]
    kd_ratio = kills / max(deaths, 1)

    embed = discord.Embed(
        title=f"📊 PEŁNA ANALITYKA WALKI: {exact_name}",
        description=f"🛡️ Aktualna gildia: **{guild}**",
        color=discord.Color.dark_purple()
    )
    
    embed.add_field(name="⚔️ Łącznie zabójstw", value=f"`{kills}`", inline=True)
    embed.add_field(name="💀 Łącznie zgonów", value=f"`{deaths}`", inline=True)
    embed.add_field(name="📈 Współczynnik K/D", value=f"`{kd_ratio:.2f}`", inline=True)

    if data["victims"]:
        victims_lines = [f"• **{name}** — zabił go `{count}x`" for name, count in data["victims"][:15]]
        if len(data["victims"]) > 15:
            victims_lines.append(f"*...oraz {len(data['victims']) - 15} innych pojedynczych ofiar.*")
        embed.add_field(name="🎯 KOGO ZABIJAŁ NAJCZĘŚCIEJ", value="\n".join(victims_lines), inline=False)
    else:
        embed.add_field(name="🎯 KOGO ZABIJAŁ NAJCZĘŚCIEJ", value="*Ten gracz nie posiada jeszcze zarejestrowanych zabójstw.*", inline=False)

    if data["killers"]:
        killers_lines = [f"• **{name}** — poległ `{count}x`" for name, count in data["killers"][:15]]
        if len(data["killers"]) > 15:
            killers_lines.append(f"*...oraz {len(data['killers']) - 15} innych oprawców.*")
        embed.add_field(name="🩸 OD KOGO GINĄŁ NAJCZĘŚCIEJ", value="\n".join(killers_lines), inline=False)
    else:
        embed.add_field(name="🩸 OD KOGO GINĄŁ NAJCZĘŚCIEJ", value="*Ten gracz posiada czyste konto (0 zgonów).*", inline=False)

    embed.set_footer(text=f"Szczegółowy wyciąg z bazy danych bota • Zgłoszone przez {ctx.author.display_name}")
    await ctx.send(embed=embed)


@bot.command(name="battlelive", aliases=["blive"])
async def battle_live(ctx):
    global is_bitka_active, bitka_buffer, bitka_start_time

    if not is_bitka_active or not bitka_buffer:
        await ctx.send("Status: 🫥 Aktualnie nie ma żadnej aktywnej bitki w toku.")
        return

    guild_stats = {}
    player_stats = {}

    for killer, killer_guild, victim, victim_guild in bitka_buffer:
        for g in [killer_guild, victim_guild]:
            if g not in guild_stats:
                guild_stats[g] = [0, 0]

        if killer not in player_stats:
            player_stats[killer] = [killer_guild, 0]

        guild_stats[killer_guild][0] += 1
        guild_stats[victim_guild][1] += 1
        player_stats[killer][1] += 1

    sorted_guilds = sorted(guild_stats.items(), key=lambda x: x[1][0], reverse=True)
    sorted_players = sorted(player_stats.items(), key=lambda x: x[1][1], reverse=True)

    now = datetime.now()
    duration = now - bitka_start_time if bitka_start_time else timedelta(0)
    minutes, seconds = divmod(duration.seconds, 60)

    embed = discord.Embed(
        title="⚔️ WYNIKI LIVE TRWAJĄCEJ BITKI",
        description=f"⏱️ Czas trwania starcia: **{minutes}m {seconds}s**\n📉 Łącznie fragów w buforze: `{len(bitka_buffer)}`",
        color=discord.Color.red()
    )

    guilds_lines = []
    for g_name, stat in sorted_guilds[:5]:
        guilds_lines.append(f"• **{g_name}**: `{stat[0]}` zabójstw / `{stat[1]}` zgonów")
    
    if guilds_lines:
        embed.add_field(name="🛡️ Klasyfikacja Gildii (Zabójstwa / Zgony)", value="\n".join(guilds_lines), inline=False)

    players_lines = []
    for p_name, data in sorted_players[:5]:
        players_lines.append(f"• **{p_name}** ({data[0]}) — `{data[1]}` Kills")
        
    if players_lines:
        embed.add_field(name="🔥 Najlepsi Fragerzy Starcia", value="\n".join(players_lines), inline=False)

    embed.set_footer(text=f"Stan na godzinę {now.strftime('%H:%M:%S')} • Użyj !koniecbitki aby zamknąć starcie")
    await ctx.send(embed=embed)


@bot.command(name="gildia")
async def guild_info(ctx, *, guild_name: str):
    exact_guild, conf_data, total_k, total_d = await asyncio.to_thread(
        get_guild_confrontations_data, guild_name
    )

    if not exact_guild:
        await ctx.send(f"Nie znaleziono danych dla gildii **{guild_name}**.")
        return

    embed = discord.Embed(
        title=f"🛡️ Statystyki Gildii: {exact_guild}", color=discord.Color.purple()
    )
    embed.add_field(
        name="Bilans Ogólny",
        value=f"Zabójstwa: `{total_k}` | Zgony: `{total_d}` | Bilans: `{total_k - total_d:+d}`",
        inline=False,
    )

    if not conf_data:
        embed.add_field(
            name="Konfrontacje z innymi gildiami",
            value="Brak zarejestrowanych konfrontacji.",
            inline=False,
        )
    else:
        sorted_conf = sorted(
            conf_data.items(),
            key=lambda item: item[1]["kills"] + item[1]["deaths"],
            reverse=True,
        )

        lines = []
        for opp_guild, stats in sorted_conf:
            k = stats["kills"]
            d = stats["deaths"]
            diff = k - d
            diff_str = f"+{diff}" if diff > 0 else str(diff)
            lines.append(
                f"• **{opp_guild}**: `{k}` zabójstw / `{d}` zgonów (Bilans: **{diff_str}**)"
            )

        embed.add_field(
            name="⚔️ Konfrontacje z gildiami",
            value="\n".join(lines[:15]),
            inline=False,
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


# Uruchomienie bota z zabezpieczeniem tekstowym
if DISCORD_TOKEN:
    bot.run(DISCORD_TOKEN)
else:
    print("❌ Błąd: Brak DISCORD_TOKEN w zmiennych środowiskowych (.env)!")
