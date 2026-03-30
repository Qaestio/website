"""
build_index.py — generates compact query-optimised index files from vct_data.json
Output: website/_index/
  teams.json       — team stats + players, no match detail (fast team/player lookup)
  players.json     — all players, pre-sorted by acs/rating/vflPts
  matches.json     — flattened match records (one entry per match, no veto)
  veto.json        — aggregated veto stats per map
  ratings.json     — team name + eloRating + ratingHistory only
  quick_ref.md     — human-readable tables for at-a-glance queries
"""

import json, os, re
from collections import defaultdict

ROOT = os.path.join(os.path.dirname(__file__), "..")
SRC  = os.path.join(ROOT, "vct_data.json")
OUT  = os.path.join(ROOT, "_index")
os.makedirs(OUT, exist_ok=True)

with open(SRC, encoding="utf-8") as f:
    data = json.load(f)

teams = data["teams"]
last_updated = data["lastUpdated"]

# ── 1. teams.json ──────────────────────────────────────────────────────────────
# Team stats + players, keyed by name. Omits raw match history.
teams_index = {}
for t in teams:
    map_total = (t.get("mapW", 0) + t.get("mapL", 0)) or 1
    match_total = (t.get("matchW", 0) + t.get("matchL", 0)) or 1
    teams_index[t["name"]] = {
        "region":   t["region"],
        "logo":     t.get("logo", ""),
        "matchW":   t.get("matchW", 0),
        "matchL":   t.get("matchL", 0),
        "matchWR":  round(t.get("matchW", 0) / match_total, 3),
        "mapW":     t.get("mapW", 0),
        "mapL":     t.get("mapL", 0),
        "mapWR":    round(t.get("mapW", 0) / map_total, 3),
        "eloRating": t.get("eloRating"),
        "players":  t.get("players", []),
    }

with open(os.path.join(OUT, "teams.json"), "w", encoding="utf-8") as f:
    json.dump({"lastUpdated": last_updated, "teams": teams_index}, f, ensure_ascii=False)

# ── 2. players.json ────────────────────────────────────────────────────────────
# All players flattened, pre-sorted by ACS then rating.
all_players = []
for t in teams:
    for p in t.get("players", []):
        all_players.append({
            "name":    p["name"],
            "team":    t["name"],
            "region":  t["region"],
            "acs":     p.get("acs", 0),
            "rating":  p.get("rating", 0),
            "fkfd":    p.get("fkfd", 0),
            "kpm":     p.get("kpm", 0),
            "vflPts":  p.get("vflPts", 0),
        })

all_players.sort(key=lambda p: (p["acs"], p["rating"]), reverse=True)

# Store players once; sorted rankings are just index arrays into the players list
def rank_indices(lst, key, reverse=True):
    return [i for i, _ in sorted(enumerate(lst), key=lambda x: x[1][key], reverse=reverse)]

by_acs    = rank_indices(all_players, "acs")
by_rating = rank_indices(all_players, "rating")
by_vfl    = rank_indices(all_players, "vflPts")
by_fkfd   = rank_indices(all_players, "fkfd")
by_kpm    = rank_indices(all_players, "kpm")

with open(os.path.join(OUT, "players.json"), "w", encoding="utf-8") as f:
    json.dump({
        "lastUpdated": last_updated,
        "count": len(all_players),
        "players": all_players,
        "ranks": {
            "acs":    by_acs,
            "rating": by_rating,
            "vflPts": by_vfl,
            "fkfd":   by_fkfd,
            "kpm":    by_kpm,
        }
    }, f, ensure_ascii=False)

# ── 3. matches.json ────────────────────────────────────────────────────────────
# Flattened match records. Each match appears once from the winning team's POV
# (de-duplicated by {event, sorted(team1, team2)}).
seen_matches = set()
match_list = []
for t in teams:
    for m in t.get("matches", []):
        key = (m["event"], tuple(sorted([t["name"], m["opponent"]])))
        if key in seen_matches:
            continue
        seen_matches.add(key)
        winner = t["name"] if m["result"] == "W" else m["opponent"]
        loser  = m["opponent"] if m["result"] == "W" else t["name"]
        match_list.append({
            "event":      m["event"],
            "winner":     winner,
            "loser":      loser,
            "score":      m.get("matchScore", []),
            "maps":       m.get("maps", []),
            "oppRating":  m.get("oppRating"),
        })

with open(os.path.join(OUT, "matches.json"), "w", encoding="utf-8") as f:
    json.dump({"lastUpdated": last_updated, "count": len(match_list), "matches": match_list}, f, ensure_ascii=False)

# ── 4. veto.json ───────────────────────────────────────────────────────────────
# Aggregated pick/ban counts per map.
veto_stats = defaultdict(lambda: {"picked": 0, "banned": 0, "decider": 0})
for t in teams:
    for m in t.get("matches", []):
        for v in m.get("veto", []):
            mp = v.get("map", "")
            action = v.get("action", "")
            if not mp:
                continue
            if action == "pick":
                veto_stats[mp]["picked"] += 1
            elif action == "ban":
                veto_stats[mp]["banned"] += 1
            elif action == "decider":
                veto_stats[mp]["decider"] += 1

# De-duplicate (each match veto appears in both teams' records)
for mp in veto_stats:
    for k in ("picked", "banned", "decider"):
        veto_stats[mp][k] //= 2

veto_sorted = dict(sorted(veto_stats.items(), key=lambda x: x[1]["picked"], reverse=True))

with open(os.path.join(OUT, "veto.json"), "w", encoding="utf-8") as f:
    json.dump({"lastUpdated": last_updated, "maps": veto_sorted}, f, ensure_ascii=False)

# ── 5. ratings.json ────────────────────────────────────────────────────────────
# Team Elo ratings + history only, sorted by current rating.
ratings = []
for t in teams:
    if t.get("eloRating") is not None:
        ratings.append({
            "name":    t["name"],
            "region":  t["region"],
            "elo":     t["eloRating"],
            "history": t.get("ratingHistory", []),
        })
ratings.sort(key=lambda r: r["elo"], reverse=True)

with open(os.path.join(OUT, "ratings.json"), "w", encoding="utf-8") as f:
    json.dump({"lastUpdated": last_updated, "teams": ratings}, f, ensure_ascii=False)

# ── 6. quick_ref.md ────────────────────────────────────────────────────────────
# Human-readable tables for at-a-glance queries.
def pct(w, l):
    t = w + l
    return f"{round(100*w/t if t else 0)}%"

regions = ["americas", "pacific", "emea", "china"]

lines = [
    f"# VCT 2026 Quick Reference",
    f"_Last updated: {last_updated}_",
    "",
]

# ── Elo ranking ──
lines += ["## Elo Rankings (all teams)", ""]
lines += ["| # | Team | Region | Elo |", "|---|------|--------|-----|"]
for i, r in enumerate(ratings, 1):
    lines.append(f"| {i} | {r['name']} | {r['region'].title()} | {r['elo']} |")
lines.append("")

# ── Team stats by region ──
for region in regions:
    rteams = [t for t in teams if t["region"] == region]
    rteams.sort(key=lambda t: t.get("matchW", 0), reverse=True)
    lines += [f"## {region.title()} Teams", ""]
    lines += ["| Team | W | L | Win% | Map W | Map L | Map% | Elo |",
              "|------|---|---|------|-------|-------|------|-----|"]
    for t in rteams:
        mw, ml = t.get("matchW", 0), t.get("matchL", 0)
        pw, pl = t.get("mapW", 0), t.get("mapL", 0)
        elo = t.get("eloRating", "—")
        lines.append(f"| {t['name']} | {mw} | {ml} | {pct(mw,ml)} | {pw} | {pl} | {pct(pw,pl)} | {elo} |")
    lines.append("")

# ── Top 20 players by ACS ──
lines += ["## Top 20 Players by ACS", ""]
lines += ["| # | Player | Team | Region | ACS | Rating | FKFD | KPM | VFL Pts |",
          "|---|--------|------|--------|-----|--------|------|-----|---------|"]
for i, idx in enumerate(by_acs[:20], 1):
    p = all_players[idx]
    lines.append(f"| {i} | {p['name']} | {p['team']} | {p['region'].title()} | {p['acs']} | {p['rating']} | {p['fkfd']} | {p['kpm']} | {p['vflPts']} |")
lines.append("")

# ── Top 20 players by VFL Pts ──
lines += ["## Top 20 Players by VFL Points", ""]
lines += ["| # | Player | Team | Region | VFL Pts | ACS | Rating |",
          "|---|--------|------|--------|---------|-----|--------|"]
for i, idx in enumerate(by_vfl[:20], 1):
    p = all_players[idx]
    lines.append(f"| {i} | {p['name']} | {p['team']} | {p['region'].title()} | {p['vflPts']} | {p['acs']} | {p['rating']} |")
lines.append("")

# ── Top 20 players by rating ──
lines += ["## Top 20 Players by Rating", ""]
lines += ["| # | Player | Team | Region | Rating | ACS | VFL Pts |",
          "|---|--------|------|--------|--------|-----|---------|"]
for i, idx in enumerate(by_rating[:20], 1):
    p = all_players[idx]
    lines.append(f"| {i} | {p['name']} | {p['team']} | {p['region'].title()} | {p['rating']} | {p['acs']} | {p['vflPts']} |")
lines.append("")

# ── Veto stats ──
lines += ["## Map Veto Statistics", ""]
lines += ["| Map | Picked | Banned | Decider |", "|-----|--------|--------|---------|"]
for mp, stats in veto_sorted.items():
    lines.append(f"| {mp} | {stats['picked']} | {stats['banned']} | {stats['decider']} |")
lines.append("")

# ── Match results ──
lines += [f"## All Match Results ({len(match_list)} total)", ""]
lines += ["| Event | Winner | Loser | Score |", "|-------|--------|-------|-------|"]
for m in match_list:
    score = "-".join(str(s) for s in m["score"])
    lines.append(f"| {m['event']} | {m['winner']} | {m['loser']} | {score} |")
lines.append("")

with open(os.path.join(OUT, "quick_ref.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

# ── Summary ───────────────────────────────────────────────────────────────────
sizes = {}
for fn in ("teams.json", "players.json", "matches.json", "veto.json", "ratings.json", "quick_ref.md"):
    path = os.path.join(OUT, fn)
    sizes[fn] = os.path.getsize(path)

print("Index files written to _index/:")
for fn, sz in sizes.items():
    print(f"  {fn:20s}  {sz//1024:>4} KB")
orig = os.path.getsize(SRC) // 1024
print(f"\nOriginal vct_data.json:     {orig} KB")
print(f"Total index size:           {sum(sizes.values())//1024} KB")
