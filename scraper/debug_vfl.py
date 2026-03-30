import json, urllib.request, sys
from itertools import combinations
from collections import defaultdict, Counter

with open('C:/Users/bearm/OneDrive/Documents/GitHub/website/vct_data.json', encoding='utf-8') as f:
    data = json.load(f)

vfl_pts_map = {}
for team in data['teams']:
    if team['region'] not in ('emea', 'pacific'): continue
    for p in team.get('players', []):
        if p.get('vflPts') is not None:
            key = p['name'].lower().replace(' ','').replace('-','').replace("'",'')
            vfl_pts_map[key] = p['vflPts']

def norm(s): return s.lower().replace(' ','').replace('-','').replace("'",'')

url = 'https://api.valorantfantasyleague.net/api/Player/allplayers?eventId=8'
req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
with urllib.request.urlopen(req, timeout=15) as r:
    api_players = json.load(r)
live_price_map = {}
for p in api_players:
    n = p.get('player',{}).get('name','')
    price = p.get('price')
    if n and price is not None:
        live_price_map[norm(n)] = price

vfl_roles = [
  ('Lakia','Gen.G','duelist'),('t3xture','Gen.G','controller'),('Ash','Gen.G','initiator'),
  ('Karon','Gen.G','sentinel'),('ZynX','Gen.G','controller'),
  ('Kr1stal','Global Esports','duelist'),('PatMen','Global Esports','initiator'),
  ('xavi8k','Global Esports','initiator'),('UdoTan','Global Esports','sentinel'),('Autumn','Global Esports','controller'),
  ('stax','T1','duelist'),('Munchkin','T1','initiator'),('BuZz','T1','controller'),
  ('iZu','T1','controller'),('Meteor','T1','sentinel'),
  ('C1ndeR','VARREL','duelist'),('oonzmlp','VARREL','duelist'),('Klaus','VARREL','initiator'),
  ('Zexy','VARREL','controller'),('XuNa','VARREL','sentinel'),
  ('Dambi','Nongshim RedForce','duelist'),('Rb','Nongshim RedForce','controller'),
  ('Francis','Nongshim RedForce','initiator'),('Ivy','Nongshim RedForce','initiator'),
  ('Xross','Nongshim RedForce','sentinel'),
  ('crazyguy','Rex Regum Qeon','duelist'),('Kushy','Rex Regum Qeon','controller'),
  ('Jemkin','Rex Regum Qeon','initiator'),('Monyet','Rex Regum Qeon','initiator'),
  ('xffero','Rex Regum Qeon','sentinel'),
  ('primmie','FULL SENSE','duelist'),('Killua','FULL SENSE','controller'),
  ('JitBoyS','FULL SENSE','initiator'),('Leviathan','FULL SENSE','sentinel'),
  ('f0rsakeN','Paper Rex','duelist'),('Jinggg','Paper Rex','duelist'),
  ('invy','Paper Rex','controller'),('something','Paper Rex','initiator'),('d4v41','Paper Rex','sentinel'),
  ('Meiy','DetonatioN FocusMe','duelist'),('Caedye','DetonatioN FocusMe','duelist'),
  ('SSeeS','DetonatioN FocusMe','controller'),('Akame','DetonatioN FocusMe','initiator'),
  ('yatsuka','DetonatioN FocusMe','sentinel'),
  ('HYUNMIN','KIWOOM DRX','duelist'),('Hermes','KIWOOM DRX','duelist'),
  ('MaKo','KIWOOM DRX','controller'),('free1ng','KIWOOM DRX','initiator'),('BeYN','KIWOOM DRX','sentinel'),
  ('SugarZ3ro','ZETA DIVISION','duelist'),('eKo','ZETA DIVISION','duelist'),
  ('Xdll','ZETA DIVISION','controller'),('SyouTa','ZETA DIVISION','initiator'),('Absol','ZETA DIVISION','sentinel'),
  ('kellyS','Team Secret','duelist'),('BerserX','Team Secret','controller'),
  ('JessieVash','Team Secret','initiator'),('Sylvan','Team Secret','sentinel'),
  ('lovers rock','BBL Esports','duelist'),('Crewen','BBL Esports','controller'),
  ('Loita','BBL Esports','initiator'),('Lar0k','BBL Esports','sentinel'),
  ('Alfajer','FNATIC','duelist'),('Boaster','FNATIC','controller'),('kaajak','FNATIC','controller'),
  ('crashies','FNATIC','initiator'),('Veqaj','FNATIC','sentinel'),
  ('xeus','FUT Esports','duelist'),('yetujey','FUT Esports','controller'),
  ('MrFaliN','FUT Esports','initiator'),('KROSTALY','FUT Esports','sentinel'),
  ('marteen','Gentle Mates','duelist'),('starxo','Gentle Mates','duelist'),
  ('bipo','Gentle Mates','controller'),('GLYPH','Gentle Mates','initiator'),('Minny','Gentle Mates','sentinel'),
  ('Flickless','GIANTX','duelist'),('westside','GIANTX','controller'),('ara','GIANTX','sentinel'),
  ('grubinho','GIANTX','initiator'),('Cloud','GIANTX','sentinel'),
  ('dos9','Karmine Corp','duelist'),('LewN','Karmine Corp','controller'),
  ('Avez','Karmine Corp','initiator'),('N4RRATE','Karmine Corp','initiator'),('SUYGETSU','Karmine Corp','sentinel'),
  ('hiro','Natus Vincere','duelist'),('Filu','Natus Vincere','controller'),('Ruxic','Natus Vincere','sentinel'),
  ('benjyfishy','Team Heretics','duelist'),('Boo','Team Heretics','controller'),
  ('RieNs','Team Heretics','initiator'),('Wo0t','Team Heretics','sentinel'),('ComeBack','Team Heretics','sentinel'),
  ('MiniBoo','Team Liquid','duelist'),('kamo','Team Liquid','controller'),
  ('purp0','Team Liquid','initiator'),('wayne','Team Liquid','initiator'),('nAts','Team Liquid','sentinel'),
  ('Derke','Team Vitality','duelist'),('Jamppi','Team Vitality','controller'),
  ('PROFEK','Team Vitality','initiator'),('Chronicle','Team Vitality','sentinel'),
  ('cNed','PCIFIC Esports','duelist'),('seven','PCIFIC Esports','duelist'),
  ('NINJA','PCIFIC Esports','controller'),('al0rante','PCIFIC Esports','initiator'),('qpert','PCIFIC Esports','sentinel'),
  ('audaz','Eternal Fire','duelist'),('nekky','Eternal Fire','controller'),('Favian','Eternal Fire','initiator'),
]

players = []
for name, team, role in vfl_roles:
    key = norm(name)
    price = live_price_map.get(key)
    if price is None: continue
    pts = vfl_pts_map.get(key)
    if pts is None: continue
    players.append({'name':name,'team':team,'role':role,'price':price,'vflPts':pts})

players_sorted = sorted(players, key=lambda x: -x['vflPts'])
by_role = defaultdict(list)
for p in players_sorted: by_role[p['role']].append(p)

TOP_N = 12
BUDGET = 100.0
MAX_PER_TEAM = 2
min_price = min(p['price'] for p in players)
print(f'Total players: {len(players)}, min_price={min_price}')

role_pools = {r: by_role[r][:TOP_N] for r in ['duelist','controller','initiator','sentinel']}
print('Pool sizes:', {r: len(v) for r,v in role_pools.items()})
print('Top duelists:', [(p['name'],p['vflPts'],p['price']) for p in role_pools['duelist']])
print('Top controllers:', [(p['name'],p['vflPts'],p['price']) for p in role_pools['controller']])
print('Top initiators:', [(p['name'],p['vflPts'],p['price']) for p in role_pools['initiator']])
print('Top sentinels:', [(p['name'],p['vflPts'],p['price']) for p in role_pools['sentinel']])
sys.stdout.flush()

c_budget_d=c_budget_dc=c_budget_dci=c_budget_role=c_team=0
valid=0
for d1,d2 in combinations(role_pools['duelist'],2):
    cost_d = d1['price']+d2['price']
    if cost_d > BUDGET - 6*min_price: c_budget_d+=1; continue
    for cp1,cp2 in combinations(role_pools['controller'],2):
        cost_dc = cost_d+cp1['price']+cp2['price']
        if cost_dc > BUDGET - 4*min_price: c_budget_dc+=1; continue
        for i1,i2 in combinations(role_pools['initiator'],2):
            cost_dci = cost_dc+i1['price']+i2['price']
            if cost_dci > BUDGET - 2*min_price: c_budget_dci+=1; continue
            for s1,s2 in combinations(role_pools['sentinel'],2):
                role_cost = cost_dci+s1['price']+s2['price']
                if role_cost > BUDGET - 3*min_price: c_budget_role+=1; continue
                tc = Counter(p['team'] for p in [d1,d2,cp1,cp2,i1,i2,s1,s2])
                if any(v > MAX_PER_TEAM for v in tc.values()): c_team+=1; continue
                valid+=1

print(f'\nFilter counts:')
print(f'  budget_d:    {c_budget_d:,}')
print(f'  budget_dc:   {c_budget_dc:,}')
print(f'  budget_dci:  {c_budget_dci:,}')
print(f'  budget_role: {c_budget_role:,}')
print(f'  team_cap:    {c_team:,}')
print(f'  valid combos that pass all: {valid:,}')
