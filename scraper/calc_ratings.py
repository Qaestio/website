import json
from collections import defaultdict

with open('vct_data.json', encoding='utf-8') as f:
    data = json.load(f)

NAME_MAP = {
    'DRX':                'KIWOOM DRX',
    'ULF Esports':        'Eternal Fire',
    'JDG Esports':        'JD Mall JDG Esports',
    'Titan Esports Club': 'Wuxi Titan Esports Club',
    'Bilibili Gaming':    'Guangzhou Huadu Bilibili Gaming',
}
def R(n): return NAME_MAP.get(n, n)

all_teams = [t['name'] for t in data['teams']]

# Regional starting ratings — derived from finishing positions across the
# four most recent international tournaments (Bangkok, Toronto, Champs, Santiago).
# Pacific dominated (3 wins), Americas consistent top-3, EMEA consistent 4th,
# China strong in 2024 but fell off in 2025.
REGION_RATINGS = {
    'pacific':  1600.0,
    'americas': 1550.0,
    'emea':     1475.0,
    'china':    1450.0,
}
region_map   = {t['name']: t.get('region', 'americas') for t in data['teams']}
start_rating = {n: REGION_RATINGS.get(region_map.get(n, 'americas'), 1500.0)
                for n in all_teams}

ratings   = dict(start_rating)
tm_global = {t['name']: t.get('matches', []) for t in data['teams']}
cursors   = {n: {} for n in all_teams}
history   = {n: [{'event': 'Start', 'rnd': '', 'opp': '', 'result': '', 'rating': round(start_rating[n])}]
             for n in all_teams}

kru = next(k for k in all_teams if k.startswith('KR'))
lev = next(k for k in all_teams if 'LEVIAT' in k)
NAME_MAP['KRU'] = kru
NAME_MAP['LEV'] = lev

def get_match(ta, tb, ev):
    key = (tb, ev)
    idx = cursors[ta].get(key, 0)
    for i, m in enumerate(tm_global.get(ta, [])[idx:], idx):
        if m.get('opponent') == tb and m.get('event') == ev:
            cursors[ta][key] = i + 1
            return m
    return None

X         = 200   # rating_diff divisor (upset scaling)
CAP       = 175   # base delta cap (before round multiplier)
WIN_FLOOR = 35    # minimum transfer per match (symmetric — winner gains, loser loses same)

# Deep rounds use a flat base (not map-diff) so a 3-2 series is equal to a 3-0 sweep
# in terms of "you won this stage". Map margin still matters in earlier rounds.
FINALS_ROUNDS = {'UF', 'MF', 'LF', 'GF', 'SF'}
FINALS_BASE   = 80   # flat base for finals-stage matches

# All rounds use a flat 1.0 multiplier; Bo5 matches get a small bonus
# detected dynamically from the match score (winner reached 3 maps).
ROUND_MULT = {}
BO5_MULT   = 1.5   # applied on top of ROUND_MULT for Bo5 series

def apply_match(ta_r, tb_r, ev, flat=False, rnd=''):
    ta, tb = R(ta_r), R(tb_r)
    m = get_match(ta, tb, ev)
    if not m:
        m = get_match(tb, ta, ev)
        if m: ta, tb = tb, ta
    if not m or not m.get('maps'):
        return None

    # Advance the other team's cursor in sync so neither team re-uses this
    # match entry if the two teams meet again later in the same event.
    # Guard with `in cursors` in case a NAME_MAP alias doesn't match the stored
    # team name exactly (the look-up would be a silent no-op, which is safe).
    m_tb = get_match(tb, ta, ev) if tb in cursors else None

    ms       = m['matchScore']
    # Use the stored result flag (from ta's perspective) rather than comparing
    # matchScore — avoids wrong winner when the cursor lands on the wrong entry.
    if m.get('result') == 'W':
        w, l = ta, tb
    else:
        w, l = tb, ta
    map_diff = abs(ms[0] - ms[1])

    rating_diff = ratings[l] - ratings[w]   # positive = upset

    # Deep finals rounds use a fixed base so a 3-2 win ≈ a 3-0 win in bracket value
    use_flat_base = (rnd in FINALS_ROUNDS)

    if flat:
        # Seed round: map margin only, no rating weighting
        delta = 50 * map_diff
    elif use_flat_base:
        base = FINALS_BASE
        if rating_diff <= 0:
            # Favourite wins: reward scales with opponent quality
            delta = base * (ratings[l] / 1500)
        else:
            # Upset: same quality base + bonus proportional to gap size
            delta = base * (ratings[l] / 1500) * (1 + rating_diff / X)
    elif rating_diff <= 0:
        # Favourite wins: scale by opponent quality (weaker opp = less reward)
        delta = 50 * map_diff * (ratings[l] / 1500)
    else:
        # Upset: same quality base + bonus proportional to gap size
        # Ensures underdog always earns more than the favourite would have
        delta = 50 * map_diff * (ratings[l] / 1500) * (1 + rating_diff / X)

    delta = max(0, min(CAP, delta))

    bo5      = max(ms) >= 3   # winner needed 3 maps → Bo5 series
    mult     = ROUND_MULT.get(rnd, 1.0) * (BO5_MULT if bo5 else 1.0)
    transfer = max(delta * mult, WIN_FLOOR)

    # Store each team's pre-match opponent rating before the transfer
    m['oppRating'] = round(ratings[tb])
    if m_tb:
        m_tb['oppRating'] = round(ratings[ta])

    ratings[w] += transfer
    ratings[l] -= transfer

    return w, l, max(ms), min(ms)

# ── Abbreviations ────────────────────────────────────────────
SHORT = {
    '100 Thieves':'100T', 'Cloud9':'C9',    'ENVY':'ENVY',  'Evil Geniuses':'EG',
    'FURIA':'FUR',        'G2 Esports':'G2','LOUD':'LOUD',  'MIBR':'MIBR',
    'NRG':'NRG',          'Sentinels':'SEN',
    'Natus Vincere':'NAVI','Karmine Corp':'KC', 'FUT Esports':'FUT',
    'Gentle Mates':'GM',  'PCIFIC Esports':'PCF','BBL Esports':'BBL',
    'Team Vitality':'VIT','Team Heretics':'TH', 'GIANTX':'GX',
    'FNATIC':'FNC',       'Team Liquid':'TL',   'Eternal Fire':'EF',
    'Nongshim RedForce':'NRF','Team Secret':'TS','ZETA DIVISION':'ZETA',
    'FULL SENSE':'FS',    'VARREL':'VRL',   'Global Esports':'GE',
    'DetonatioN FocusMe':'DFM','Gen.G':'GEN','T1':'T1',
    'KIWOOM DRX':'DRX',   'Paper Rex':'PRX','Rex Regum Qeon':'RRQ',
    'Trace Esports':'TRC','Wolves Esports':'WLV','FunPlus Phoenix':'FPX',
    'TYLOO':'TYL',        'All Gamers':'AG', 'Nova Esports':'NOV',
    'JD Mall JDG Esports':'JDG','Wuxi Titan Esports Club':'TEC',
    'Xi Lai Gaming':'XLG','EDward Gaming':'EDG',
    'Guangzhou Huadu Bilibili Gaming':'BLG',
    'Dragon Ranger Gaming':'DRG',
}
SHORT[kru] = 'KRU'
SHORT[lev] = 'LEV'
def sn(t): return SHORT.get(t, t[:5])

# ── Events ────────────────────────────────────────────────────
events = [
  ('Americas Kickoff', 'UR1', [
    ('UR1','ENVY','Evil Geniuses'), ('UR1','LOUD','Cloud9'),
    ('UR1','KRU','FURIA'),          ('UR1','100 Thieves','LEV'),
    ('UR2','NRG','Cloud9'),         ('UR2','MIBR','ENVY'),
    ('UR2','Sentinels','FURIA'),    ('UR2','G2 Esports','100 Thieves'),
    ('MR1','LOUD','100 Thieves'),   ('MR1','Evil Geniuses','Sentinels'),
    ('MR1','KRU','ENVY'),           ('MR1','LEV','Cloud9'),
    ('MR2','100 Thieves','Sentinels'),('MR2','ENVY','Cloud9'),
    ('UR3','NRG','MIBR'),           ('UR3','FURIA','G2 Esports'),
    ('LR1','LOUD','Evil Geniuses'), ('LR1','KRU','LEV'),
    ('MR3','NRG','100 Thieves'),    ('MR3','G2 Esports','Cloud9'),
    ('LR2','ENVY','Evil Geniuses'), ('LR2','Sentinels','LEV'),
    ('LR3','100 Thieves','Evil Geniuses'),('LR3','Cloud9','LEV'),
    ('MR4','NRG','G2 Esports'),     ('LR4','100 Thieves','Cloud9'),
    ('UF','MIBR','FURIA'),          ('MF','MIBR','G2 Esports'),
    ('LR5','NRG','100 Thieves'),    ('LF','MIBR','NRG'),
  ]),
  ('EMEA Kickoff', 'UR1', [
    ('UR1','Natus Vincere','Karmine Corp'),  ('UR1','FUT Esports','Gentle Mates'),
    ('UR1','PCIFIC Esports','BBL Esports'), ('UR1','ULF Esports','Team Vitality'),
    ('UR2','Team Heretics','Natus Vincere'),('UR2','GIANTX','Gentle Mates'),
    ('UR2','FNATIC','BBL Esports'),         ('UR2','Team Liquid','Team Vitality'),
    ('MR1','Karmine Corp','Team Liquid'),   ('MR1','FUT Esports','FNATIC'),
    ('MR1','PCIFIC Esports','GIANTX'),      ('MR1','ULF Esports','Team Heretics'),
    ('UR3','Natus Vincere','Gentle Mates'), ('UR3','BBL Esports','Team Vitality'),
    ('MR2','Team Liquid','FNATIC'),         ('MR2','GIANTX','Team Heretics'),
    ('LR1','Karmine Corp','FUT Esports'),   ('LR1','PCIFIC Esports','ULF Esports'),
    ('MR3','Natus Vincere','FNATIC'),       ('MR3','Team Vitality','GIANTX'),
    ('LR2','Team Heretics','Karmine Corp'), ('LR2','Team Liquid','ULF Esports'),
    ('LR3','Natus Vincere','Team Heretics'),('LR3','GIANTX','Team Liquid'),
    ('LR4','Team Heretics','Team Liquid'),  ('MR4','FNATIC','Team Vitality'),
    ('UF','Gentle Mates','BBL Esports'),    ('LR5','Team Vitality','Team Liquid'),
    ('MF','Gentle Mates','FNATIC'),         ('LF','FNATIC','Team Liquid'),
  ]),
  ('Pacific Kickoff', 'UR1', [
    ('UR1','Nongshim RedForce','Team Secret'),('UR1','ZETA DIVISION','FULL SENSE'),
    ('UR1','VARREL','Global Esports'),        ('UR1','DetonatioN FocusMe','Gen.G'),
    ('UR2','T1','Nongshim RedForce'),          ('UR2','DRX','FULL SENSE'),
    ('UR2','Paper Rex','Global Esports'),      ('UR2','Rex Regum Qeon','DetonatioN FocusMe'),
    ('MR1','Team Secret','DetonatioN FocusMe'),('MR1','ZETA DIVISION','Global Esports'),
    ('MR1','VARREL','DRX'),                    ('MR1','Gen.G','T1'),
    ('UR3','Nongshim RedForce','FULL SENSE'),  ('UR3','Paper Rex','Rex Regum Qeon'),
    ('MR2','DetonatioN FocusMe','Global Esports'),('MR2','DRX','T1'),
    ('LR1','Team Secret','ZETA DIVISION'),    ('LR1','VARREL','Gen.G'),
    ('MR3','FULL SENSE','DetonatioN FocusMe'),('MR3','Paper Rex','T1'),
    ('LR2','DRX','ZETA DIVISION'),            ('LR2','Global Esports','Gen.G'),
    ('LR3','FULL SENSE','DRX'),               ('LR3','Paper Rex','Global Esports'),
    ('LR4','DRX','Paper Rex'),                ('MR4','DetonatioN FocusMe','T1'),
    ('UF','Nongshim RedForce','Rex Regum Qeon'),('LR5','DetonatioN FocusMe','Paper Rex'),
    ('MF','Rex Regum Qeon','T1'),             ('LF','Rex Regum Qeon','Paper Rex'),
  ]),
  ('China Kickoff', 'UR1', [
    ('UR1','Trace Esports','Wolves Esports'), ('UR1','FunPlus Phoenix','TYLOO'),
    ('UR1','All Gamers','Nova Esports'),       ('UR1','JDG Esports','Titan Esports Club'),
    ('UR2','Xi Lai Gaming','Wolves Esports'), ('UR2','EDward Gaming','TYLOO'),
    ('UR2','Bilibili Gaming','All Gamers'),   ('UR2','Dragon Ranger Gaming','JDG Esports'),
    ('MR1','Nova Esports','TYLOO'),           ('MR1','Titan Esports Club','Wolves Esports'),
    ('MR1','Trace Esports','JDG Esports'),    ('MR1','FunPlus Phoenix','Bilibili Gaming'),
    ('MR2','TYLOO','Titan Esports Club'),     ('MR2','Trace Esports','Bilibili Gaming'),
    ('LR1','Nova Esports','Wolves Esports'),  ('UR3','All Gamers','Dragon Ranger Gaming'),
    ('LR1','JDG Esports','FunPlus Phoenix'),  ('UR3','Xi Lai Gaming','EDward Gaming'),
    ('LR2','Trace Esports','Wolves Esports'), ('MR3','Dragon Ranger Gaming','TYLOO'),
    ('LR2','Titan Esports Club','JDG Esports'),('MR3','EDward Gaming','Bilibili Gaming'),
    ('LR3','TYLOO','Trace Esports'),          ('LR3','EDward Gaming','JDG Esports'),
    ('MR4','Dragon Ranger Gaming','Bilibili Gaming'),('LR4','TYLOO','EDward Gaming'),
    ('LR5','Dragon Ranger Gaming','EDward Gaming'),  ('UF','All Gamers','Xi Lai Gaming'),
    ('MF','Xi Lai Gaming','Bilibili Gaming'),         ('LF','Bilibili Gaming','EDward Gaming'),
  ]),
  ('China Stage 1', None, [
    ('SR1','All Gamers','Titan Esports Club'), ('SR1','EDward Gaming','Nova Esports'),
    # SR1 (remaining), SR2–SR5, and playoff rounds to be added as results come in
  ]),
  ('Masters Santiago', None, [
    ('SR1','Gentle Mates','EDward Gaming'),  ('SR1','Xi Lai Gaming','NRG'),
    ('SR1','G2 Esports','Paper Rex'),        ('SR1','T1','Team Liquid'),
    ('SR2','Paper Rex','NRG'),               ('SR2','Gentle Mates','Team Liquid'),
    ('SR2','EDward Gaming','T1'),            ('SR2','Xi Lai Gaming','G2 Esports'),
    ('SR3','NRG','Team Liquid'),             ('SR3','G2 Esports','T1'),
    ('QF','FURIA','Paper Rex'),              ('QF','Nongshim RedForce','Gentle Mates'),
    ('QF','BBL Esports','NRG'),              ('QF','All Gamers','G2 Esports'),
    ('LR1','FURIA','BBL Esports'),           ('LR1','Gentle Mates','All Gamers'),
    ('SF','Nongshim RedForce','G2 Esports'), ('SF','Paper Rex','NRG'),
    ('LR2','G2 Esports','BBL Esports'),      ('LR2','Paper Rex','All Gamers'),
    ('UF','Nongshim RedForce','NRG'),        ('LR3','Paper Rex','G2 Esports'),
    ('LF','NRG','Paper Rex'),                ('GF','Nongshim RedForce','Paper Rex'),
  ]),
]

# ── Display helpers ───────────────────────────────────────────
BAR_MAX = 30

def print_global(label):
    ranked = sorted(all_teams, key=lambda x: -ratings[x])
    mn = min(ratings.values())
    mx = max(ratings.values())
    rng = mx - mn if mx != mn else 1
    cols = 3
    rows = (len(ranked) + cols - 1) // cols
    print(f'\n  {"="*64}')
    print(f'  Global Rankings  —  after {label}')
    print(f'  {"="*64}')
    for r in range(rows):
        parts = []
        for c in range(cols):
            idx = c * rows + r
            if idx < len(ranked):
                t   = ranked[idx]
                bar = '#' * int((ratings[t] - mn) / rng * 12)
                parts.append(f'{idx+1:2}. {sn(t):<5} {ratings[t]:5.0f} {bar:<12}')
            else:
                parts.append(' ' * 26)
        print('  ' + '   '.join(parts))

def run_event(ev_name, flat_rnd, schedule):
    print(f'\n{"="*64}')
    print(f'  {ev_name}')
    print(f'{"="*64}')

    rounds_order = []
    rounds_dict  = defaultdict(list)
    for rnd, ta_r, tb_r in schedule:
        if rnd not in rounds_dict:
            rounds_order.append(rnd)
        rounds_dict[rnd].append((ta_r, tb_r))

    for rnd in rounds_order:
        pairs = rounds_dict[rnd]
        results = []
        for ta_r, tb_r in pairs:
            res = apply_match(ta_r, tb_r, ev_name, flat=(rnd == flat_rnd), rnd=rnd)
            if res:
                w, l, ws, ls = res
                history[w].append({'event': ev_name, 'rnd': rnd, 'opp': sn(l), 'result': 'W', 'rating': round(ratings[w])})
                history[l].append({'event': ev_name, 'rnd': rnd, 'opp': sn(w), 'result': 'L', 'rating': round(ratings[l])})
                results.append((ta_r, tb_r, res))

        print(f'\n  -- {rnd} --')
        for ta_r, tb_r, (w, l, ws, ls) in results:
            delta_str = f'{ratings[w] - 1500:+.0f}' if False else ''
            print(f'    {sn(R(w)):<5} def. {sn(R(l)):<5}  {ws}-{ls}'
                  f'   ({sn(R(w))}: {ratings[w]:.0f}  {sn(R(l))}: {ratings[l]:.0f})')

    print_global(ev_name)

# ── Run all events ────────────────────────────────────────────
for ev_name, flat_rnd, schedule in events:
    run_event(ev_name, flat_rnd, schedule)

# ── Write ratings back to vct_data.json ───────────────────────
for team in data['teams']:
    team['eloRating']     = round(ratings.get(team['name'], 1500))
    team['ratingHistory'] = history.get(team['name'], [])

with open('vct_data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print('\n  Ratings written to vct_data.json')

import subprocess, sys
from pathlib import Path
subprocess.run([sys.executable, str(Path(__file__).parent / 'build_index.py')], check=True)
