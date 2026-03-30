import json
import sys
import urllib.request
from itertools import combinations
from collections import defaultdict, Counter

# ── Load vct_data vflPts ──────────────────────────────────────────────────────
with open('C:/Users/bearm/OneDrive/Documents/GitHub/website/vct_data.json', encoding='utf-8') as f:
    data = json.load(f)

vfl_pts_map = {}
for team in data['teams']:
    if team['region'] not in ('emea', 'pacific'):
        continue
    for p in team.get('players', []):
        if p.get('vflPts') is not None:
            key = p['name'].lower().replace(' ', '').replace('-', '').replace("'", '')
            vfl_pts_map[key] = p['vflPts']

# ── Fetch live prices from VFL API ────────────────────────────────────────────
def norm(s):
    return s.lower().replace(' ', '').replace('-', '').replace("'", '')

print('Fetching live prices from VFL API...')
sys.stdout.flush()

live_price_map = {}
try:
    url = 'https://api.valorantfantasyleague.net/api/Player/allplayers?eventId=8'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        api_players = json.load(r)
    for p in api_players:
        name = p.get('player', {}).get('name', '')
        price = p.get('price')
        if name and price is not None:
            live_price_map[norm(name)] = price
    print(f'  Loaded {len(live_price_map)} player prices from API.')
except Exception as e:
    print(f'  Warning: could not fetch live prices ({e}). Using hardcoded fallback.')
sys.stdout.flush()

# ── Hardcoded roles (API does not return reliable role data) ──────────────────
# role = the slot this player MUST fill (duelist/controller/initiator/sentinel)
vfl_roles = [
  # ── Gen.G ────────────────────────────────────────────────────────────────────
  {'name':'Lakia','team':'Gen.G','role':'initiator'},
  {'name':'ZynX','team':'Gen.G','role':'duelist'},
  {'name':'Ash','team':'Gen.G','role':'controller'},
  {'name':'Karon','team':'Gen.G','role':'sentinel'},
  {'name':'t3xture','team':'Gen.G','role':'duelist'},
  # ── Global Esports ───────────────────────────────────────────────────────────
  {'name':'PatMen','team':'Global Esports','role':'controller'},
  {'name':'UdoTan','team':'Global Esports','role':'sentinel'},
  {'name':'Kr1stal','team':'Global Esports','role':'initiator'},
  {'name':'xavi8k','team':'Global Esports','role':'controller'},
  {'name':'Autumn','team':'Global Esports','role':'duelist'},
  # ── T1 ───────────────────────────────────────────────────────────────────────
  {'name':'stax','team':'T1','role':'initiator'},
  {'name':'Meteor','team':'T1','role':'sentinel'},
  {'name':'BuZz','team':'T1','role':'duelist'},
  {'name':'iZu','team':'T1','role':'duelist'},
  {'name':'Munchkin','team':'T1','role':'controller'},
  # ── VARREL ───────────────────────────────────────────────────────────────────
  {'name':'C1ndeR','team':'VARREL','role':'controller'},
  {'name':'Zexy','team':'VARREL','role':'duelist'},
  {'name':'Klaus','team':'VARREL','role':'sentinel'},
  {'name':'XuNa','team':'VARREL','role':'initiator'},
  {'name':'oonzmlp','team':'VARREL','role':'controller'},
  # ── Nongshim RedForce ────────────────────────────────────────────────────────
  {'name':'Rb','team':'Nongshim RedForce','role':'controller'},
  {'name':'Dambi','team':'Nongshim RedForce','role':'duelist'},
  {'name':'Xross','team':'Nongshim RedForce','role':'initiator'},
  {'name':'Ivy','team':'Nongshim RedForce','role':'sentinel'},
  {'name':'Francis','team':'Nongshim RedForce','role':'duelist'},
  # ── Rex Regum Qeon ───────────────────────────────────────────────────────────
  {'name':'Jemkin','team':'Rex Regum Qeon','role':'duelist'},
  {'name':'xffero','team':'Rex Regum Qeon','role':'sentinel'},
  {'name':'Kushy','team':'Rex Regum Qeon','role':'initiator'},
  {'name':'Monyet','team':'Rex Regum Qeon','role':'controller'},
  {'name':'crazyguy','team':'Rex Regum Qeon','role':'controller'},
  # ── FULL SENSE ───────────────────────────────────────────────────────────────
  {'name':'primmie','team':'FULL SENSE','role':'duelist'},
  {'name':'Killua','team':'FULL SENSE','role':'initiator'},
  {'name':'Leviathan','team':'FULL SENSE','role':'controller'},
  {'name':'JitBoyS','team':'FULL SENSE','role':'sentinel'},
  {'name':'CRWS','team':'FULL SENSE','role':'controller'},
  # ── Paper Rex ────────────────────────────────────────────────────────────────
  {'name':'f0rsakeN','team':'Paper Rex','role':'controller'},
  {'name':'invy','team':'Paper Rex','role':'initiator'},
  {'name':'Jinggg','team':'Paper Rex','role':'duelist'},
  {'name':'something','team':'Paper Rex','role':'duelist'},
  {'name':'d4v41','team':'Paper Rex','role':'sentinel'},
  # ── DetonatioN FocusMe ───────────────────────────────────────────────────────
  {'name':'Meiy','team':'DetonatioN FocusMe','role':'duelist'},
  {'name':'Akame','team':'DetonatioN FocusMe','role':'initiator'},
  {'name':'yatsuka','team':'DetonatioN FocusMe','role':'duelist'},
  {'name':'SSeeS','team':'DetonatioN FocusMe','role':'controller'},
  {'name':'Caedye','team':'DetonatioN FocusMe','role':'sentinel'},
  # ── KIWOOM DRX ───────────────────────────────────────────────────────────────
  {'name':'MaKo','team':'KIWOOM DRX','role':'controller'},
  {'name':'HYUNMIN','team':'KIWOOM DRX','role':'duelist'},
  {'name':'free1ng','team':'KIWOOM DRX','role':'sentinel'},
  {'name':'BeYN','team':'KIWOOM DRX','role':'initiator'},
  {'name':'Hermes','team':'KIWOOM DRX','role':'initiator'},
  # ── ZETA DIVISION ────────────────────────────────────────────────────────────
  {'name':'SugarZ3ro','team':'ZETA DIVISION','role':'controller'},
  {'name':'eKo','team':'ZETA DIVISION','role':'duelist'},
  {'name':'Xdll','team':'ZETA DIVISION','role':'initiator'},
  {'name':'Absol','team':'ZETA DIVISION','role':'duelist'},
  {'name':'SyouTa','team':'ZETA DIVISION','role':'sentinel'},
  # ── Team Secret ──────────────────────────────────────────────────────────────
  {'name':'kellyS','team':'Team Secret','role':'duelist'},
  {'name':'JessieVash','team':'Team Secret','role':'initiator'},
  {'name':'BerserX','team':'Team Secret','role':'sentinel'},
  {'name':'Sylvan','team':'Team Secret','role':'controller'},
  {'name':'Rimuru','team':'Team Secret','role':'duelist'},
  # ── PCIFIC Esports ───────────────────────────────────────────────────────────
  {'name':'seven','team':'PCIFIC Esports','role':'initiator'},
  {'name':'al0rante','team':'PCIFIC Esports','role':'sentinel'},
  {'name':'NINJA','team':'PCIFIC Esports','role':'initiator'},
  {'name':'qpert','team':'PCIFIC Esports','role':'controller'},
  {'name':'cNed','team':'PCIFIC Esports','role':'duelist'},
  # ── BBL Esports ──────────────────────────────────────────────────────────────
  {'name':'lovers rock','team':'BBL Esports','role':'duelist'},
  {'name':'Loita','team':'BBL Esports','role':'controller'},
  {'name':'Lar0k','team':'BBL Esports','role':'duelist'},
  {'name':'Rose','team':'BBL Esports','role':'initiator'},
  {'name':'Crewen','team':'BBL Esports','role':'sentinel'},
  # ── FNATIC ───────────────────────────────────────────────────────────────────
  {'name':'Alfajer','team':'FNATIC','role':'sentinel'},
  {'name':'Veqaj','team':'FNATIC','role':'controller'},
  {'name':'crashies','team':'FNATIC','role':'initiator'},
  {'name':'kaajak','team':'FNATIC','role':'duelist'},
  {'name':'Boaster','team':'FNATIC','role':'controller'},
  # ── FUT Esports ──────────────────────────────────────────────────────────────
  {'name':'xeus','team':'FUT Esports','role':'duelist'},
  {'name':'MrFaliN','team':'FUT Esports','role':'controller'},
  {'name':'yetujey','team':'FUT Esports','role':'sentinel'},
  {'name':'KROSTALY','team':'FUT Esports','role':'initiator'},
  {'name':'S0PP','team':'FUT Esports','role':'duelist'},
  # ── Gentle Mates ─────────────────────────────────────────────────────────────
  {'name':'marteen','team':'Gentle Mates','role':'duelist'},
  {'name':'Minny','team':'Gentle Mates','role':'sentinel'},
  {'name':'bipo','team':'Gentle Mates','role':'duelist'},
  {'name':'starxo','team':'Gentle Mates','role':'initiator'},
  {'name':'GLYPH','team':'Gentle Mates','role':'controller'},
  # ── GIANTX ───────────────────────────────────────────────────────────────────
  {'name':'Flickless','team':'GIANTX','role':'initiator'},
  {'name':'Cloud','team':'GIANTX','role':'initiator'},
  {'name':'westside','team':'GIANTX','role':'sentinel'},
  {'name':'grubinho','team':'GIANTX','role':'controller'},
  {'name':'ara','team':'GIANTX','role':'duelist'},
  # ── Karmine Corp ─────────────────────────────────────────────────────────────
  {'name':'dos9','team':'Karmine Corp','role':'controller'},
  {'name':'SUYGETSU','team':'Karmine Corp','role':'sentinel'},
  {'name':'LewN','team':'Karmine Corp','role':'duelist'},
  {'name':'Avez','team':'Karmine Corp','role':'initiator'},
  {'name':'N4RRATE','team':'Karmine Corp','role':'initiator'},
  # ── Natus Vincere ────────────────────────────────────────────────────────────
  {'name':'hiro','team':'Natus Vincere','role':'sentinel'},
  {'name':'Ruxic','team':'Natus Vincere','role':'controller'},
  {'name':'Filu','team':'Natus Vincere','role':'duelist'},
  {'name':'Exit','team':'Natus Vincere','role':'duelist'},
  {'name':'Chloric','team':'Natus Vincere','role':'initiator'},
  # ── Team Heretics ────────────────────────────────────────────────────────────
  {'name':'benjyfishy','team':'Team Heretics','role':'sentinel'},
  {'name':'ComeBack','team':'Team Heretics','role':'duelist'},
  {'name':'Boo','team':'Team Heretics','role':'controller'},
  {'name':'RieNs','team':'Team Heretics','role':'initiator'},
  {'name':'Wo0t','team':'Team Heretics','role':'initiator'},
  # ── Team Liquid ──────────────────────────────────────────────────────────────
  {'name':'MiniBoo','team':'Team Liquid','role':'duelist'},
  {'name':'nAts','team':'Team Liquid','role':'sentinel'},
  {'name':'kamo','team':'Team Liquid','role':'duelist'},
  {'name':'purp0','team':'Team Liquid','role':'initiator'},
  {'name':'wayne','team':'Team Liquid','role':'controller'},
  # ── Team Vitality ────────────────────────────────────────────────────────────
  {'name':'Chronicle','team':'Team Vitality','role':'sentinel'},
  {'name':'Derke','team':'Team Vitality','role':'duelist'},
  {'name':'Jamppi','team':'Team Vitality','role':'initiator'},
  {'name':'PROFEK','team':'Team Vitality','role':'controller'},
  {'name':'Sayonara','team':'Team Vitality','role':'initiator'},
  # ── Eternal Fire ─────────────────────────────────────────────────────────────
  {'name':'audaz','team':'Eternal Fire','role':'controller'},
  {'name':'nekky','team':'Eternal Fire','role':'initiator'},
  {'name':'Favian','team':'Eternal Fire','role':'duelist'},
  {'name':'Izzy','team':'Eternal Fire','role':'duelist'},
  {'name':'Echo','team':'Eternal Fire','role':'sentinel'},
]

# ── Join: roles + live prices + vflPts ───────────────────────────────────────
players = []
missing_price = []
missing_pts = []
for p in vfl_roles:
    key = norm(p['name'])
    price = live_price_map.get(key)
    if price is None:
        missing_price.append(p['name'])
        continue
    pts = vfl_pts_map.get(key)
    if pts is None:
        missing_pts.append(p['name'])
        continue
    players.append({**p, 'price': price, 'vflPts': pts})

if missing_price:
    print(f'  No API price for: {missing_price}')
if missing_pts:
    print(f'  No vflPts for (excluded): {missing_pts}')
print(f'Total eligible players: {len(players)}')
sys.stdout.flush()

# ── Sort and group ────────────────────────────────────────────────────────────
players_sorted = sorted(players, key=lambda x: -x['vflPts'])

by_role = defaultdict(list)
for p in players_sorted:
    by_role[p['role']].append(p)

for role in ['duelist','controller','initiator','sentinel']:
    ps = by_role[role]
    print(f'  {role}: {len(ps)} eligible, best={ps[0]["name"]}({ps[0]["vflPts"]:.2f}pts,{ps[0]["price"]}VP)')
sys.stdout.flush()

BUDGET    = 100.0
MAX_PER_TEAM = 2
TOP_N     = 12   # top N per role to consider for role slots

role_pools = {r: by_role[r][:TOP_N] for r in ['duelist','controller','initiator','sentinel']}
min_price  = min(p['price'] for p in players)

# Wildcard candidates sorted by vflPts desc
wc_candidates = [(p['name'], p['team'], p['vflPts'], p['price']) for p in players_sorted]

def greedy_wc(role_set, team_counts, budget):
    """Greedily pick 3 wildcards: highest vflPts not already chosen,
       within budget, respecting MAX_PER_TEAM per team.
       Key: always reserve enough budget for remaining slots at min_price."""
    wc = []
    tc = dict(team_counts)
    rem = budget
    for name, team, pts, price in wc_candidates:
        if name in role_set:
            continue
        if tc.get(team, 0) >= MAX_PER_TEAM:
            continue
        slots_left = 3 - len(wc) - 1  # slots still needed after this pick
        if price > rem - slots_left * min_price:
            continue  # can't afford this + guarantee remaining slots
        wc.append((name, team, pts, price))
        tc[team] = tc.get(team, 0) + 1
        rem -= price
        if len(wc) == 3:
            return wc
    return None  # couldn't fill 3 wildcards

best_score = 0.0
best_combo = None
best_wc    = None

checked = 0
for d1, d2 in combinations(role_pools['duelist'], 2):
    cost_d = d1['price'] + d2['price']
    if cost_d > BUDGET - 6 * min_price: continue
    pts_d  = d1['vflPts'] + d2['vflPts']

    for c1, c2 in combinations(role_pools['controller'], 2):
        cost_dc = cost_d + c1['price'] + c2['price']
        if cost_dc > BUDGET - 4 * min_price: continue
        pts_dc  = pts_d + c1['vflPts'] + c2['vflPts']

        for i1, i2 in combinations(role_pools['initiator'], 2):
            cost_dci = cost_dc + i1['price'] + i2['price']
            if cost_dci > BUDGET - 2 * min_price: continue
            pts_dci  = pts_dc + i1['vflPts'] + i2['vflPts']

            for s1, s2 in combinations(role_pools['sentinel'], 2):
                role_cost = cost_dci + s1['price'] + s2['price']
                if role_cost > BUDGET - 3 * min_price: continue

                # Check team cap across all 8 role players
                role_players = [d1, d2, c1, c2, i1, i2, s1, s2]
                team_counts  = Counter(p['team'] for p in role_players)
                if any(v > MAX_PER_TEAM for v in team_counts.values()):
                    continue

                remaining = BUDGET - role_cost
                role_pts  = pts_dci + s1['vflPts'] + s2['vflPts']
                role_set  = {p['name'] for p in role_players}

                wc = greedy_wc(role_set, team_counts, remaining)
                if wc is None:
                    continue

                total = role_pts + sum(p[2] for p in wc)
                checked += 1
                if total > best_score:
                    best_score = total
                    best_combo = role_players
                    best_wc    = wc

print(f'\nEvaluated {checked:,} valid combinations.')
sys.stdout.flush()

if best_combo is None:
    print('No valid team found!')
    sys.exit(1)

# Reconstruct full team for display
best_team_roles = best_combo
best_team_wc    = [{'name': n, 'team': t, 'vflPts': pts, 'price': pr, 'role': 'wildcard'}
                   for n, t, pts, pr in best_wc]
best_team = best_team_roles + best_team_wc
total_cost = sum(p['price'] for p in best_team)

print(f'\n=== BEST TEAM  |  {best_score:.2f} total vflPts  |  {total_cost:.1f} VP ===')
for p in sorted(best_team_roles, key=lambda x: x['role']):
    print(f'  [{p["role"]:12s}]  {p["name"]:15s}  {p["team"]:25s}  {p["vflPts"]:.2f} pts  {p["price"]} VP')
print('  --- wildcards ---')
for p in best_team_wc:
    print(f'  [wildcard    ]  {p["name"]:15s}  {p["team"]:25s}  {p["vflPts"]:.2f} pts  {p["price"]} VP')
print(f'\n  Budget used: {total_cost:.1f} / {BUDGET} VP')
print(f'  Total vflPts: {best_score:.2f}')
print(f'  Avg per player: {best_score/11:.2f}')

# Verify team cap
all_teams = Counter(p['team'] for p in best_team)
print(f'\n  Team breakdown:')
for team, count in sorted(all_teams.items(), key=lambda x: -x[1]):
    print(f'    {team}: {count} player(s)')
