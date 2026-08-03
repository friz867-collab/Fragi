import os
import re
import urllib.parse
from collections import defaultdict
import requests
from bs4 import BeautifulSoup
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Wczytanie zmiennych środowiskowych z pliku .env
load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

# ... reszta Twojego kodu ...

# === KONFIGURACJA ===
URL_FRAGS = "http://dblots.org.pl/lastfrags.php?lang=en&s=classic"

# Ustawienia bota
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

player_guild_cache = {}
guild_stats = defaultdict(lambda: {"kills": 0, "deaths": 0})
player_stats = defaultdict(lambda: {"guild": "Bez Gildii", "kills": 0, "deaths": 0})
player_history = defaultdict(list)
processed_frags = set()

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

# Pętla sprawdzająca nowe fragi w tle (co 5 sekund)
@tasks.loop(seconds=5)
async def check_frags():
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

            if row_text not in processed_frags:
                killer, victim = parse_frag_line(row)
                if killer and victim:
                    killer_guild = fetch_guild_from_profile(killer)
                    victim_guild = fetch_guild_from_profile(victim)

                    player_stats[killer]["guild"] = killer_guild
                    player_stats[killer]["kills"] += 1
                    player_stats[victim]["guild"] = victim_guild
                    player_stats[victim]["deaths"] += 1

                    guild_stats[killer_guild]["kills"] += 1
                    guild_stats[victim_guild]["deaths"] += 1

                    player_history[killer].append(f"⚔️ Zabił {victim} ({victim_guild})")
                    player_history[victim].append(f"💀 Zginął od {killer} ({killer_guild})")

                    # Wysyłanie wiadomości na Discord
                    embed = discord.Embed(title="⚔️ Nowy Frag!", color=discord.Color.red())
                    embed.add_field(name="Zabójca", value=f"**{killer}**\n*({killer_guild})*", inline=True)
                    embed.add_field(name="Ofiara", value=f"**{victim}**\n*({victim_guild})*", inline=True)
                    
                    await channel.send(embed=embed)

                processed_frags.add(row_text)
    except Exception as e:
        print(f"Błąd pętli: {e}")

@bot.event
async def on_ready():
    print(f"Zalogowano jako {bot.user.name}")
    
    # Inicjalizacja obecnych fragów (żeby nie wysyłać powiadomień o starych)
    try:
        res = session.get(URL_FRAGS, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        for row in soup.find_all("tr"):
            row_text = row.get_text(strip=True)
            if row_text:
                processed_frags.add(row_text)
    except Exception:
        pass

    check_frags.start()

# === KOMENDY DLA UŻYTKOWNIKÓW NA DISCORDZIE ===

@bot.command(name="top")
async def top_guilds(ctx):
    """Wyświetla ranking gildii po wpisaniu !top"""
    if not guild_stats:
        await ctx.send("Brak zarejestrowanych zabójstw od startu bota.")
        return

    embed = discord.Embed(title="🏆 Ranking Gildii", color=discord.Color.gold())
    sorted_guilds = sorted(guild_stats.items(), key=lambda x: x[1]["kills"], reverse=True)

    for guild, data in sorted_guilds[:10]:
        embed.add_field(
            name=f"🛡️ {guild}",
            value=f"Kills: `{data['kills']}` | Deaths: `{data['deaths']}`",
            inline=False
        )

    await ctx.send(embed=embed)

@bot.command(name="gracz")
async def player_info(ctx, *, player_name: str):
    """Wyświetla statystyki i historię gracza po wpisaniu np. !gracz Macro Tommy"""
    # Dopasowanie nazwy gracza bez względu na wielkość liter
    matched_player = None
    for p in player_stats.keys():
        if p.lower() == player_name.lower():
            matched_player = p
            break

    if not matched_player:
        await ctx.send(f"Nie znaleziono danych dla gracza **{player_name}**.")
        return

    data = player_stats[matched_player]
    history = player_history.get(matched_player, [])[-5:]  # Ostatnie 5 wpisów

    embed = discord.Embed(title=f"👤 Statystyki: {matched_player}", color=discord.Color.blue())
    embed.add_field(name="Gildia", value=data["guild"], inline=True)
    embed.add_field(name="Kills / Deaths", value=f"`{data['kills']}` / `{data['deaths']}`", inline=True)
    
    if history:
        embed.add_field(name="Ostatnie starcia", value="\n".join(history), inline=False)
    else:
        embed.add_field(name="Ostatnie starcia", value="Brak wpisów", inline=False)

    await ctx.send(embed=embed)
    @bot.command(name="status")
async def status(ctx):
    """Wyświetla status bota oraz czas jego ciągłego działania."""
    if start_time is None:
        await ctx.send("Bot dopiero się uruchamia...")
        return

    now = datetime.now(timezone.utc)
    uptime = now - start_time

    dni = uptime.days
    godziny, remainder = divmod(uptime.seconds, 3600)
    minuty, sekundy = divmod(remainder, 60)

    embed = discord.Embed(
        title="📊 Status Bota",
        color=discord.Color.green(),
        timestamp=now,
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
