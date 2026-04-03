#!/usr/bin/env python3
"""
VCT 2026 data scraper for vlr.gg
Outputs  ../vct_data.json  (read by valorant.html before falling back to the API)

Usage:
    cd scraper
    python scrape_vct.py

Requirements:  pip install requests beautifulsoup4
"""

import json, time, re, sys, subprocess
from pathlib import Path
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# -- Config --------------------------------------------------------------------

BASE    = 'https://www.vlr.gg'
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}
DELAY = 1.1   # seconds between requests — be polite to vlr.gg

# VCT 2026 franchise events.
# Format: (event_id, slug, region_key, display_label)
# region_key = None for international events (Masters / Champions)
# Add new events here as the season progresses.
# Teams that have rebranded mid-season.
# Maps old lowercase team name → new display name.
# The scraper merges historical data from the old entry into the new name.
TEAM_RENAMES = {
    'drx':        'Kiwoom DRX',
    'ulf esports': 'Eternal Fire',
    # China: Kickoff uses short names, Stage 1 overview uses sponsor-prefixed names.
    # The short-name entries get registered without franchise=True (overview selector
    # fails on the Kickoff page), so we rename them into the franchise entries here.
    'bilibili gaming':   'Guangzhou Huadu Bilibili Gaming',
    'jdg esports':       'JD Mall JDG Esports',
    'titan esports club': 'Wuxi Titan Esports Club',
}

# vlr.gg abbreviations for renamed teams (add new abbrevs if needed)
# 'kdrx' or 'ef' may be used once vlr.gg updates their pages.

VCT_EVENTS = [
    # -- Kickoffs --------------------------------------------------------------
    (2682, 'vct-2026-americas-kickoff',  'americas', 'Americas Kickoff'),
    (2683, 'vct-2026-pacific-kickoff',   'pacific',  'Pacific Kickoff'),
    (2684, 'vct-2026-emea-kickoff',      'emea',     'EMEA Kickoff'),
    (2685, 'vct-2026-china-kickoff',     'china',    'China Kickoff'),
    # -- Stage 1 ---------------------------------------------------------------
    (2775, 'vct-2026-pacific-stage-1',   'pacific',  'Pacific Stage 1'),
    (2776, 'vct-2026-pacific-stage-2',   'pacific',  'Pacific Stage 2'),
    (2860, 'vct-2026-americas-stage-1',  'americas', 'Americas Stage 1'),
    (2863, 'vct-2026-emea-stage-1',      'emea',     'EMEA Stage 1'),
    (2864, 'vct-2026-china-stage-1',     'china',    'China Stage 1'),
    # -- International ---------------------------------------------------------
    (2760, 'valorant-masters-santiago-2026', None, 'Masters Santiago'),
]

# Region metadata (mirrors valorant.html)
REGION_META = {
    'americas': {'label': 'Americas', 'color': '#f0923b'},
    'emea':     {'label': 'EMEA',     'color': '#f0c040'},
    'pacific':  {'label': 'Pacific',  'color': '#5b9cf6'},
    'china':    {'label': 'China',    'color': '#e05252'},
}

OUT_FILE    = Path(__file__).parent.parent / 'vct_data.json'
CACHE_FILE  = Path(__file__).parent / 'match_cache.json'

# -- Ratings -------------------------------------------------------------------

_REGION_START = {'pacific': 1600., 'americas': 1550., 'emea': 1475., 'china': 1450.}
_X            = 200    # rating-diff divisor (upset scaling)
_CAP          = 175    # base delta cap before multiplier
_WIN_FLOOR    = 35     # minimum rating transfer per match
_FINALS_BASE  = 80     # flat base for deep-bracket rounds
_BO5_MULT     = 1.5    # multiplier for best-of-5 series
_FINALS_RNDS  = {'UF', 'MF', 'LF', 'GF', 'SF'}

# Regional kickoff events use flat (seed-based) scoring for their first upper round
_FLAT_EV_RND  = {label: ('UR1' if region else None)
                 for _, _, region, label in VCT_EVENTS}

# Map vlr.gg .match-item-event-series text → abbreviated round ID
_RND_MAP = {
    'Upper Round 1': 'UR1', 'Upper Round 2': 'UR2', 'Upper Round 3': 'UR3',
    'Upper Round 4': 'UR4', 'Upper Round 5': 'UR5',
    'Middle Round 1': 'MR1', 'Middle Round 2': 'MR2', 'Middle Round 3': 'MR3',
    'Middle Round 4': 'MR4', 'Middle Round 5': 'MR5',
    'Lower Round 1': 'LR1', 'Lower Round 2': 'LR2', 'Lower Round 3': 'LR3',
    'Lower Round 4': 'LR4', 'Lower Round 5': 'LR5',
    'Upper Final': 'UF', 'Middle Final': 'MF', 'Lower Final': 'LF',
    'Grand Final': 'GF', 'Upper Semifinals': 'SF', 'Semifinal': 'SF',
    'Upper Quarterfinals': 'QF', 'Quarterfinal': 'QF',
    'Round 1': 'SR1', 'Round 2 (1-0)': 'SR2', 'Round 2 (0-1)': 'SR2',
    'Round 3': 'SR3', 'Round 4': 'SR4',
    'Week 1': 'W1', 'Week 2': 'W2', 'Week 3': 'W3',
    'Week 4': 'W4', 'Week 5': 'W5', 'Week 6': 'W6', 'Week 7': 'W7',
}

# Short name used in ratingHistory 'opp' field (mirrors calc_ratings.py SHORT dict)
_SHORT = {
    '100 Thieves': '100T',       'Cloud9': 'C9',
    'ENVY': 'ENVY',              'Evil Geniuses': 'EG',
    'FURIA': 'FUR',              'G2 Esports': 'G2',
    'LOUD': 'LOUD',              'MIBR': 'MIBR',
    'NRG': 'NRG',                'Sentinels': 'SEN',
    'Natus Vincere': 'NAVI',     'Karmine Corp': 'KC',
    'FUT Esports': 'FUT',        'Gentle Mates': 'GM',
    'PCIFIC Esports': 'PCF',     'BBL Esports': 'BBL',
    'Team Vitality': 'VIT',      'Team Heretics': 'TH',
    'GIANTX': 'GX',              'FNATIC': 'FNC',
    'Team Liquid': 'TL',         'Eternal Fire': 'EF',
    'Nongshim RedForce': 'NRF',  'Team Secret': 'TS',
    'ZETA DIVISION': 'ZETA',     'FULL SENSE': 'FS',
    'VARREL': 'VRL',             'Global Esports': 'GE',
    'DetonatioN FocusMe': 'DFM', 'Gen.G': 'GEN',
    'T1': 'T1',                  'KIWOOM DRX': 'DRX',
    'Paper Rex': 'PRX',          'Rex Regum Qeon': 'RRQ',
    'Trace Esports': 'TRC',      'Wolves Esports': 'WLV',
    'FunPlus Phoenix': 'FPX',    'TYLOO': 'TYL',
    'All Gamers': 'AG',          'Nova Esports': 'NOV',
    'JD Mall JDG Esports': 'JDG','Wuxi Titan Esports Club': 'TEC',
    'Xi Lai Gaming': 'XLG',      'EDward Gaming': 'EDG',
    'Guangzhou Huadu Bilibili Gaming': 'BLG',
    'Dragon Ranger Gaming': 'DRG',
}

def _sn(name):
    """Return the short display name for a team."""
    return _SHORT.get(name, name[:4].upper())

def _apply_ratings(teams_out):
    """Incremental Elo update — only processes matches not yet rated (no oppRating).

    Loads current ratings from the existing vct_data.json, finds any new
    completed matches, applies the rating delta for each in event/round order,
    then stamps eloRating and ratingHistory onto every team in teams_out.
    """
    # Load existing ratings, history, and already-rated match fingerprints
    cur_ratings, cur_history, old_opp = {}, {}, {}
    if OUT_FILE.exists():
        try:
            old = json.loads(OUT_FILE.read_text(encoding='utf-8'))
            for t in old.get('teams', []):
                n = t['name']
                if t.get('eloRating'):
                    cur_ratings[n] = float(t['eloRating'])
                if t.get('ratingHistory'):
                    cur_history[n] = t['ratingHistory']
                for m in t.get('matches', []):
                    if m.get('oppRating') is not None:
                        key = (n, m['event'],
                               m.get('opponent', '').lower(),
                               tuple(m.get('matchScore', [])))
                        old_opp[key] = m['oppRating']
        except Exception:
            pass

    by_name = {t['name']: t for t in teams_out}

    # Initialise current ratings (stored value, or regional seed for new teams)
    ratings = {t['name']: cur_ratings.get(t['name'],
               _REGION_START.get(t.get('region', 'americas'), 1500.))
               for t in teams_out}

    history = {t['name']: (cur_history.get(t['name']) or
               [{'event': 'Start', 'rnd': '', 'opp': '', 'result': '',
                 'rating': round(ratings[t['name']])}])
               for t in teams_out}

    # Restore oppRating for matches already processed in a previous run
    for t in teams_out:
        for m in t.get('matches', []):
            key = (t['name'], m['event'],
                   m.get('opponent', '').lower(),
                   tuple(m.get('matchScore', [])))
            if key in old_opp:
                m['oppRating'] = old_opp[key]

    # Collect new matches (deduplicated canonical pairs, sorted by event then position)
    ev_order = {label: i for i, (_, _, _, label) in enumerate(VCT_EVENTS)}
    seen, to_rate = set(), []
    for t in teams_out:
        ta = t['name']
        for pos, m in enumerate(t.get('matches', [])):
            if m.get('oppRating') is not None:
                continue                              # already rated
            if not m.get('maps') or not m.get('matchScore'):
                continue                              # incomplete / upcoming
            tb  = m.get('opponent', '')
            ev  = m['event']
            pair = tuple(sorted([ta, tb])) + (ev,)
            if pair in seen:
                continue
            seen.add(pair)
            # Find opponent's corresponding match record
            m_tb = None
            tb_t = by_name.get(tb)
            if tb_t:
                for mo in tb_t.get('matches', []):
                    if (mo.get('event') == ev
                            and mo.get('opponent', '').lower() == ta.lower()
                            and mo.get('matchScore') is not None
                            and mo.get('oppRating') is None):
                        m_tb = mo
                        break
            to_rate.append((ev_order.get(ev, 999), pos, ta, tb, m, m_tb, ev))

    to_rate.sort(key=lambda x: (x[0], x[1]))

    # Apply rating delta for each new match
    for _, _, ta, tb, m_ta, m_tb, ev in to_rate:
        if ta not in ratings or tb not in ratings:
            continue
        ms          = m_ta['matchScore']
        w, l        = (ta, tb) if m_ta['result'] == 'W' else (tb, ta)
        map_diff    = abs(ms[0] - ms[1])
        rating_diff = ratings[l] - ratings[w]        # positive = upset
        rnd         = m_ta.get('rnd', '')
        flat        = (_FLAT_EV_RND.get(ev) == rnd) and bool(rnd)
        use_flat    = rnd in _FINALS_RNDS

        if flat:
            delta = 50 * map_diff
        elif use_flat:
            delta = _FINALS_BASE * (ratings[l] / 1500) * (1 + max(0, rating_diff) / _X)
        elif rating_diff <= 0:
            delta = 50 * map_diff * (ratings[l] / 1500)
        else:
            delta = 50 * map_diff * (ratings[l] / 1500) * (1 + rating_diff / _X)

        delta    = max(0, min(_CAP, delta))
        transfer = max(delta * (_BO5_MULT if max(ms) >= 3 else 1.0), _WIN_FLOOR)

        m_ta['oppRating'] = round(ratings[tb])
        if m_tb is not None:
            m_tb['oppRating'] = round(ratings[ta])

        ratings[w] += transfer
        ratings[l] -= transfer

        history[w].append({'event': ev, 'rnd': rnd, 'opp': _sn(l),
                            'result': 'W', 'rating': round(ratings[w])})
        history[l].append({'event': ev, 'rnd': rnd, 'opp': _sn(w),
                            'result': 'L', 'rating': round(ratings[l])})

    # Stamp final ratings onto each team
    for t in teams_out:
        n = t['name']
        t['eloRating']     = round(ratings.get(n, 1500))
        t['ratingHistory'] = history.get(n, [])

# -- Cache ---------------------------------------------------------------------

def load_cache() -> dict:
    """Load the match detail cache from disk. Returns empty dict if not found."""
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text(encoding='utf-8'))
            cached = len(data.get('matches', {}))
            print(f'Cache loaded: {cached} match(es) already stored in {CACHE_FILE.name}')
            return data
        except (json.JSONDecodeError, OSError) as exc:
            print(f'! Could not read cache ({exc}), starting fresh')
    return {'matches': {}}


def save_cache(cache: dict):
    """Persist the match detail cache to disk."""
    cache['last_run'] = datetime.now(timezone.utc).isoformat()
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Cache saved: {len(cache["matches"])} match(es) -> {CACHE_FILE.name}')


# -- HTTP ----------------------------------------------------------------------

_session = requests.Session()
_session.headers.update(HEADERS)

def fetch(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            r = _session.get(url, timeout=20)
            r.raise_for_status()
            time.sleep(DELAY)
            return BeautifulSoup(r.text, 'html.parser')
        except requests.RequestException as exc:
            wait = 2 ** attempt
            print(f'    !  fetch error ({exc}), retrying in {wait}s…')
            time.sleep(wait)
    print(f'    X  Failed to fetch {url}')
    return None

# -- Helpers -------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalise a team/org name for fuzzy matching."""
    import unicodedata
    s = unicodedata.normalize('NFD', s.lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')  # strip diacritics
    return re.sub(r'[^a-z0-9]', '', s)


# Maps the vlr.gg stats-page abbreviation (stats-player-country) → canonical
# normalised team name, so short codes like "FNC" resolve to "fnatic".
ORG_ABBREV = {
    # Americas
    '100t': '100thieves',  'c9': 'cloud9',       'eg': 'evilgeniuses',
    'nrg':  'nrg',         'sen': 'sentinels',   'kru': 'kruesports',
    'loud': 'loud',        'fur': 'furia',        '2g': '2gameesports',
    'mibr': 'mibr',        'lev': 'leviatan',
    # EMEA
    'bbl':  'bblesports',  'fnc': 'fnatic',       'fut': 'futesports',
    'gx':   'giantx',      'gm':  'gentlemates',  'm8': 'gentlemates',
    'navi': 'natusvincere','kc':  'karminecorp',  'th': 'teamheretics',
    'tl':   'teamliquid',  'ulf': 'ulfesports',   'ef':  'eternalfire',  'vit': 'teamvitality',
    # Pacific
    'drx':  'kiwoomdrx',   'kdrx': 'kiwoomdrx',   't1':  't1',            'gen': 'geng',
    'ns':   'nongshimredforce', 'nrf': 'nongshimredforce', 'prx': 'paperrex',
    'rrq':  'rexregumqeon','ge':  'globalesports', 'fs':  'fullsense',
    'ts':   'teamsecret',  'dfm': 'detonationfocusme',
    'zeta': 'zetadivision','vl':  'varrel',        'vrl': 'varrel',   'var': 'varrel',
    # Americas (international)
    'g2':   'g2esports',
    # China
    'pcf':  'pcificesports',                          # PCIFIC Esports (EMEA)
    'ag':   'allgamers',   'blg': 'bilibiligaming', 'drg': 'dragonrangergaming',
    'edg':  'edwardgaming', 'fpx': 'funplusphoenix', 'jdg': 'jdgesports',
    'nova': 'novaesports', 'te':  'traceesports',  'tec': 'titanesportsclub',
    'tyl':  'tyloo',       'we':  'wolvesesports',  'xlg': 'xilaigaming',
}


def find_team_key(abbr: str, team_map: dict) -> str | None:
    """
    Resolve a vlr.gg org abbreviation (e.g. 'FNC') to a team_map key.
    Resolution order:
      1. Direct lowercase key lookup (full names already in team_map)
      2. ORG_ABBREV canonical lookup
      3. Normalised full-name match
      4. Prefix match  (e.g. 'bbl' → 'bblesports')
    """
    key = abbr.lower()
    if key in team_map:
        return key

    n = norm(abbr)

    # Step 2 — check abbreviation table with substring containment
    # Handles sponsored names: "jdgesports" IN "jdmalljdgesports" → JDG match
    canonical = ORG_ABBREV.get(n)
    if canonical:
        for k, t in team_map.items():
            tn = norm(t['name'])
            if tn == canonical or canonical in tn or tn in canonical:
                return k

    # Step 3 — normalised full-name exact match
    for k, t in team_map.items():
        if norm(t['name']) == n:
            return k

    # Step 4 — prefix match (handles "ulfesports".startswith("ulf"))
    for k, t in team_map.items():
        tn = norm(t['name'])
        if len(n) >= 2 and tn.startswith(n):
            return k

    return None

def col_indices(table) -> dict:
    """Return a dict mapping header text -> column index (0-based) for a stats table."""
    headers = [th.get_text(strip=True) for th in table.select('thead th')]
    return {h: i for i, h in enumerate(headers)}

# -- Scraping ------------------------------------------------------------------

def scrape_teams_from_overview(event_id: int, slug: str, region: str, team_map: dict):
    """
    Scrape the event overview page to register all participating teams + logos.
    This captures teams even before any matches are played.
    """
    url  = f'{BASE}/event/{event_id}/{slug}'
    soup = fetch(url)
    if not soup:
        return

    container = soup.select_one('div.event-teams-container')
    if not container:
        print(f'    !  No teams container found on overview page')
        return

    found = 0
    for card in container.select('div.event-team'):
        name_el = card.select_one('a.event-team-name')
        logo_el = card.select_one('img.event-team-players-mask-team')
        if not name_el:
            continue

        name    = name_el.get_text(strip=True)
        logo    = ('https:' + logo_el['src']) if logo_el else ''
        key     = name.lower()

        if key not in team_map:
            team_map[key] = {
                'name':          name,
                'region':        region,
                'logo':          logo,
                'matchW': 0, 'matchL': 0,
                'mapW':   0, 'mapL':   0,
                'players':       [],
                'matches':       [],
                'franchise':     True,   # confirmed franchise team from event overview
            }
        else:
            team_map[key]['franchise'] = True  # mark existing entry as confirmed
            if logo and not team_map[key]['logo']:
                team_map[key]['logo'] = logo

        found += 1

    print(f'    -> {found} teams registered from overview')


# Known VCT map pool — used for veto text-parsing fallback
VCT_MAPS = frozenset({
    'Abyss', 'Ascent', 'Bind', 'Breeze', 'Corrode', 'Fracture',
    'Haven', 'Icebox', 'Lotus', 'Pearl', 'Split', 'Sunset',
})


def scrape_match_detail(match_url: str, t1_name: str = '', t2_name: str = '') -> dict:
    """
    Visit a single match page and return map scores + veto sequence + player rosters
    + per-map per-player kill/rating stats.
    Returns {'maps': [...], 'veto': [...], 't1_players': [...], 't2_players': [...],
             'map_player_stats': [[{name, kills, rating}, ...], ...]}
    t1/t2 correspond to the left/right team on the match listing page.
    """
    soup = fetch(match_url)
    if not soup:
        return {'maps': [], 'veto': [], 't1_players': [], 't2_players': [], 'map_player_stats': []}

    roster = _scrape_roster(soup)
    return {
        'maps':             _scrape_maps(soup),
        'veto':             _scrape_veto(soup, t1_name, t2_name),
        't1_players':       roster['t1_players'],
        't2_players':       roster['t2_players'],
        'map_player_stats': _scrape_map_player_stats(soup),
    }


def _scrape_roster(soup) -> dict:
    """
    Extract the 5-player lineup for each team from a vlr.gg match page.
    Uses the 'all' (overall) stats game block; falls back to the first map block.
    Returns {'t1_players': [name, ...], 't2_players': [name, ...]}.
    The first 5 player names encountered are t1; the next 5 are t2.
    """
    game_block = (
        soup.select_one('.vm-stats-game[data-game-id="all"]') or
        next(iter(soup.select('.vm-stats-game[data-game-id]')), None)
    )
    if not game_block:
        return {'t1_players': [], 't2_players': []}

    names = []
    for td in game_block.select('td.mod-player'):
        name_el = td.select_one('div.text-of')
        if name_el:
            name = name_el.get_text(strip=True)
            if name:
                names.append(name)

    return {
        't1_players': names[:5],
        't2_players': names[5:10],
    }


def _scrape_map_player_stats(soup) -> list[list[dict]]:
    """
    Return per-map per-player kills and VLR rating, in map order (excluding the 'all' block).
    Result: [ [{'name': str, 'kills': int, 'rating': float|None}, ...], ... ]
    One inner list per map played.
    """
    result = []
    for game in soup.select('.vm-stats-game[data-game-id]'):
        if game.get('data-game-id') == 'all':
            continue
        tables = game.select('table')
        if not tables:
            result.append([])
            continue

        players = []
        for table in tables:
            headers = [th.get_text(strip=True) for th in table.select('thead th')]
            cols    = {h: i for i, h in enumerate(headers)}
            k_col   = cols.get('K')
            r_col   = next((cols[h] for h in ('R2.0', 'Rating', 'R', 'Rtg') if h in cols), None)

            if k_col is None:
                continue

            for row in table.select('tbody tr'):
                tds       = row.select('td')
                player_td = row.select_one('td.mod-player')
                if not player_td:
                    continue
                name_el = player_td.select_one('div.text-of')
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue

                try:
                    k_td = tds[k_col]
                    both = k_td.select_one('.mod-both')
                    kills = int((both or k_td).get_text(strip=True))
                except (ValueError, IndexError):
                    kills = 0

                rating = None
                if r_col is not None and r_col < len(tds):
                    try:
                        r_td = tds[r_col]
                        both_r = r_td.select_one('.mod-both')
                        rating = float((both_r or r_td).get_text(strip=True))
                    except (ValueError, IndexError):
                        pass

                players.append({'name': name, 'kills': kills, 'rating': rating})

        result.append(players)
    return result


def _scrape_maps(soup) -> list[dict]:
    maps = []

    # Primary: parse the game nav tabs — each tab shows map name + score
    for nav in soup.select('.vm-stats-gamesnav-item[data-game-id]'):
        gid = nav.get('data-game-id', 'all')
        if gid == 'all':
            continue

        map_el = nav.select_one('.map')
        if not map_el:
            continue
        map_name_el = map_el.select_one('span:not(.mod-side):not(.mod-ver)')
        map_name = (map_name_el or map_el).get_text(strip=True)
        map_name = map_name.splitlines()[-1].strip()
        if not map_name or map_name.lower() in ('tbd', ''):
            continue

        scores = nav.select('.score')
        if len(scores) >= 2:
            try:
                s1 = int(scores[0].get_text(strip=True))
                s2 = int(scores[-1].get_text(strip=True))
                maps.append({'map': _clean_map_name(map_name), 't1_score': s1, 't2_score': s2})
            except ValueError:
                pass

    if maps:
        return maps

    # Fallback: parse individual vm-stats-game blocks
    for game in soup.select('.vm-stats-game[data-game-id]'):
        gid = game.get('data-game-id', 'all')
        if gid == 'all':
            continue

        map_el = (
            game.select_one('.map span:not(.mod-ver):not(.mod-side)') or
            game.select_one('.map-name') or
            game.select_one('.map')
        )
        if not map_el:
            continue
        map_name = map_el.get_text(strip=True).splitlines()[-1].strip()
        if not map_name or map_name.lower() == 'tbd':
            continue

        scores = game.select('.score')
        if len(scores) >= 2:
            try:
                s1 = int(scores[0].get_text(strip=True))
                s2 = int(scores[-1].get_text(strip=True))
                maps.append({'map': _clean_map_name(map_name), 't1_score': s1, 't2_score': s2})
            except ValueError:
                pass

    return maps


def _clean_map_name(name: str) -> str:
    """Strip pick/ban/decider labels that vlr.gg concatenates with map names."""
    for suffix in ('PICK', 'BAN', 'DECIDER', 'Pick', 'Ban', 'Decider', 'pick', 'ban', 'decider'):
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    return name


def _resolve_team(raw: str, t1_name: str, t2_name: str) -> str:
    """Map a raw team string (full name or abbreviation) to t1/t2 name, or '' if unresolved.

    Priority order (to avoid false-positive substring matches like 'GE' → 'Gen.G'):
      1. Exact case-insensitive match
      2. ORG_ABBREV lookup  (explicit, highest confidence)
      3. Prefix substring fallback  (raw must be a prefix of the team name, ≥ 3 chars)
    """
    rl = raw.lower().strip()
    if not rl:
        return ''

    # 1. Exact full-name match
    for name in (t1_name, t2_name):
        if name and name.lower() == rl:
            return name

    # 2. ORG_ABBREV — checked BEFORE substring to prevent e.g. 'GE' matching 'Gen.G'
    canonical = ORG_ABBREV.get(norm(raw))
    if canonical:
        for name in (t1_name, t2_name):
            if name and (norm(name) == canonical or canonical in norm(name) or norm(name) in canonical):
                return name
        return ''  # abbreviation is known but neither team matches — don't guess

    # 3. Prefix fallback: raw must be a prefix of the team name (≥ 3 chars).
    #    Handles full-name usage like "CLOUD9" → "Cloud9".
    #    Avoids false positives like "LOUD" inside "cloud9" or "SENSE" inside "FULL SENSE".
    for name in (t1_name, t2_name):
        if name and len(rl) >= 3 and name.lower().startswith(rl):
            return name

    return ''


def _scrape_veto(soup, t1_name: str, t2_name: str) -> list[dict]:
    """
    Try to extract the map pick/ban sequence from a vlr.gg match page.
    Returns a list of steps ordered by step number.

    vlr.gg wraps the veto in elements like .match-veto / .match-header-note.
    If selector-based parsing finds nothing, falls back to scanning short text
    nodes that contain a known map name alongside 'ban'/'pick'/'decider'.
    """
    veto = []

    # ── match-header-note (most common vlr.gg format) ─────────────────────────
    # vlr.gg renders the veto inline as:
    #   "TS ban Corrode; NS ban Pearl; TS pick Haven; ...; Breeze remains"
    # inside a .match-header-note element.
    _NOTE_RE = re.compile(
        r'(\S+)\s+(ban|pick|decider)\s+(\w+)'   # "ABBR ban/pick Map"
        r'|(\w+)\s+remains',                     # "Map remains" → decider
        re.IGNORECASE
    )
    note_el = soup.select_one('.match-header-note')
    if note_el:
        note_text = note_el.get_text(' ', strip=True)
        seen: set[str] = set()
        step = 0
        for m_obj in _NOTE_RE.finditer(note_text):
            if m_obj.group(1):  # "ABBR action Map" form
                abbr, action, map_word = (
                    m_obj.group(1), m_obj.group(2).lower(), m_obj.group(3).title()
                )
                team_name = _resolve_team(abbr, t1_name, t2_name)
            else:               # "Map remains" form → decider
                map_word  = m_obj.group(4).title()
                action    = 'decider'
                team_name = ''
            if map_word not in VCT_MAPS or map_word in seen:
                continue
            seen.add(map_word)
            step += 1
            veto.append({'step': step, 'action': action, 'map': map_word, 'team': team_name})
        if veto:
            return veto

    # ── Selector-based (secondary) ─────────────────────────────────────────────
    container = (
        soup.select_one('.match-veto') or
        soup.select_one('[class*="veto"]') or
        soup.select_one('.map-picks')
    )
    if container:
        step = 0
        for item in container.select(
            '.item, .veto-item, .pick-item, .ban-item, tr, li, .row, div'
        ):
            text = item.get_text(' ', strip=True)
            tl   = text.lower()

            action = (
                'ban'     if 'ban'      in tl else
                'pick'    if 'pick'     in tl else
                'decider' if any(w in tl for w in ('decider', 'remaining', 'left over')) else
                None
            )
            if not action:
                continue

            # Map name from child element or text search
            map_el = item.select_one('.map, [class*="map"]')
            map_name = map_el.get_text(strip=True).splitlines()[-1].strip() if map_el else ''
            if not map_name or map_name.title() not in VCT_MAPS:
                for word in text.split():
                    if word.title() in VCT_MAPS:
                        map_name = word.title()
                        break
            if not map_name:
                continue

            # Team name
            team_el = item.select_one('.team, [class*="team"]')
            raw_team = team_el.get_text(strip=True) if team_el else ''
            team_name = _resolve_team(raw_team, t1_name, t2_name) if raw_team else ''

            step += 1
            veto.append({'step': step, 'action': action, 'map': map_name, 'team': team_name})

        if veto:
            return veto

    # ── Text-scan fallback ─────────────────────────────────────────────────────
    # Walk every element; look for "ABBR ban/pick MapName" patterns.
    # vlr.gg sometimes packs multiple steps into one text node, e.g.:
    #   "NRF ban Pearl RRQ ban Abyss"
    # so we extract all triples via regex rather than one-map-per-element.
    _NAV_TAB_RE  = re.compile(r'^\S+\s+(?:PICK|BAN|DECIDER)\s+\d+:\d+$', re.IGNORECASE)
    _STEP_RE     = re.compile(
        r'(\S+)\s+(ban|pick|decider)\s+(\S+)'   # "ABBR action Map"
        r'|(\S+)\s+remains',                     # "Map remains" → decider
        re.IGNORECASE
    )
    seen_maps: set[str] = set()
    step = 0
    for el in soup.find_all(True):
        if len(el.find_all(True)) > 4:
            continue
        text = el.get_text(' ', strip=True)
        if len(text) > 120 or not text:
            continue

        # Skip map navigation tab elements: "MapName PICK/BAN mm:ss"
        if _NAV_TAB_RE.match(text):
            continue

        for m_obj in _STEP_RE.finditer(text):
            if m_obj.group(1):  # "ABBR action Map" form
                map_word  = m_obj.group(3).title()
                act_word  = m_obj.group(2).lower()
                team_name = _resolve_team(m_obj.group(1), t1_name, t2_name)
            else:               # "Map remains" form
                map_word  = m_obj.group(4).title()
                act_word  = 'decider'
                team_name = ''
            if map_word not in VCT_MAPS or map_word in seen_maps:
                continue
            seen_maps.add(map_word)
            step += 1
            veto.append({'step': step, 'action': act_word, 'map': map_word, 'team': team_name})

    return veto


def calc_vfl_for_match(match_score, map_details, map_player_stats, t1_players, t2_players):
    """
    Compute VFL fantasy points per player for a single match.

    Scoring implemented:
      Kills per map: 0k=-3, 1-4k=-1, 5-9k=0, 10k=+1 (+1 per 5 above 10)
      Map win: +1; 13-0 win: +5; 0-13 loss: -5
      Win margin: +2 (10+), +1 (5-9); Loss margin: -1 (10+)
      Series win bonus: 2-0=+2, 3-0=+4, 3-1=+1
      VLR rating rank: #1=+3, #2=+2, #3=+1 (avg across match)
      VLR rating threshold: 1.5+=+1, 1.75+=+2, 2.0+=+3
    Not scored (no per-round data): multi-kill round bonuses (4K/5K/6K/7K).

    Returns: {player_name_lower: {'vfl': float, 'maps': int}}
    """
    if not map_player_stats or not t1_players or not t2_players:
        return {}

    t1_lower = {n.lower() for n in t1_players}
    t2_lower = {n.lower() for n in t2_players}
    name_canon = {n.lower(): n for n in t1_players + t2_players}

    vfl        = {}   # canonical_name -> vfl total
    maps_count = {}   # canonical_name -> maps played
    rating_sum = {}   # canonical_name -> sum of per-map ratings
    rating_cnt = {}   # canonical_name -> maps with a rating value

    def add(name, pts):
        vfl[name] = vfl.get(name, 0.0) + pts

    t1_map_wins, t2_map_wins = match_score

    # ── Per-map scoring ───────────────────────────────────────────────────────
    for i, players in enumerate(map_player_stats):
        if i >= len(map_details):
            break
        md   = map_details[i]
        t1s  = md['t1_score']
        t2s  = md['t2_score']
        t1_won = t1s > t2s
        margin = abs(t1s - t2s)

        for p in players:
            nl = p['name'].lower()
            if nl not in name_canon:
                continue
            canon = name_canon[nl]
            maps_count[canon] = maps_count.get(canon, 0) + 1

            # Kill points
            k = p['kills']
            if k == 0:      kpts = -3
            elif k <= 4:    kpts = -1
            elif k < 10:    kpts =  0
            else:           kpts =  1 + (k - 10) // 5
            add(canon, kpts)

            p_t1  = nl in t1_lower
            p_won = (p_t1 and t1_won) or (not p_t1 and not t1_won)

            if p_won:
                add(canon, 1)   # map win
                # Perfect-map bonus
                own_s, opp_s = (t1s, t2s) if p_t1 else (t2s, t1s)
                if own_s == 13 and opp_s == 0:
                    add(canon, 5)
                # Win margin
                if margin >= 10:   add(canon,  2)
                elif margin >= 5:  add(canon,  1)
            else:
                # Perfect-map loss penalty
                own_s, opp_s = (t1s, t2s) if p_t1 else (t2s, t1s)
                if own_s == 0 and opp_s == 13:
                    add(canon, -5)
                # Loss margin
                if margin >= 10:   add(canon, -1)

            r = p.get('rating')
            if r is not None:
                rating_sum[canon] = rating_sum.get(canon, 0.0) + r
                rating_cnt[canon] = rating_cnt.get(canon, 0)  + 1

    if not vfl:
        return {}

    # ── Series win bonus ──────────────────────────────────────────────────────
    def series_bonus(won, lost):
        if won == 2 and lost == 0: return 2
        if won == 3 and lost == 0: return 4
        if won == 3 and lost == 1: return 1
        return 0

    t1_bonus = series_bonus(t1_map_wins, t2_map_wins)
    t2_bonus = series_bonus(t2_map_wins, t1_map_wins)

    for canon in list(vfl):
        add(canon, t1_bonus if canon.lower() in t1_lower else t2_bonus)

    # ── Rating-based bonuses ──────────────────────────────────────────────────
    avg_ratings = {n: rating_sum[n] / rating_cnt[n]
                   for n in rating_sum if rating_cnt.get(n, 0) > 0}

    # Rank bonuses: #1=+3, #2=+2, #3=+1
    for rank, (canon, _) in enumerate(
            sorted(avg_ratings.items(), key=lambda x: x[1], reverse=True)[:3], 1):
        add(canon, 4 - rank)

    # Threshold bonuses
    for canon, avg_r in avg_ratings.items():
        if avg_r >= 2.0:       add(canon, 3)
        elif avg_r >= 1.75:    add(canon, 2)
        elif avg_r >= 1.5:     add(canon, 1)

    return {n: {'vfl': vfl[n], 'maps': maps_count.get(n, 0)} for n in vfl}


def scrape_matches(event_id: int, slug: str, region: str, team_map: dict, label: str,
                   vfl_map: dict, match_cache: dict):
    """
    Scrape the event matches page.
    • Registers any new teams (with correct region).
    • Accumulates match W/L and map W/L for completed matches.
    • Stores per-match history (opponent, result, map scores) on each team.
    • Accumulates per-player VFL points into vfl_map.
    """
    url  = f'{BASE}/event/matches/{event_id}/{slug}/'
    soup = fetch(url)
    if not soup:
        return

    completed = upcoming = new_count = 0

    for match in soup.select('a.match-item'):
        teams_els = match.select('div.match-item-vs-team')
        if len(teams_els) != 2:
            continue

        t1_el, t2_el = teams_els
        t1_name_el   = t1_el.select_one('div.text-of') or t1_el.select_one('.match-item-vs-team-name')
        t2_name_el   = t2_el.select_one('div.text-of') or t2_el.select_one('.match-item-vs-team-name')
        t1_score_el  = t1_el.select_one('div.match-item-vs-team-score')
        t2_score_el  = t2_el.select_one('div.match-item-vs-team-score')

        if not all([t1_name_el, t2_name_el, t1_score_el, t2_score_el]):
            continue

        t1_name  = t1_name_el.get_text(strip=True)
        t2_name  = t2_name_el.get_text(strip=True)
        t1_score = t1_score_el.get_text(strip=True)
        t2_score = t2_score_el.get_text(strip=True)

        # Skip TBD placeholders
        if not t1_name or not t2_name or t1_name == 'TBD' or t2_name == 'TBD':
            continue

        # Register teams (upcoming + completed)
        for name in (t1_name, t2_name):
            k = name.lower()
            if k not in team_map:
                team_map[k] = {
                    'name': name, 'region': region, 'logo': '',
                    'matchW': 0, 'matchL': 0, 'mapW': 0, 'mapL': 0,
                    'players': [], 'matches': [],
                }

        # Upcoming match — no score to process
        if t1_score == '–' or t2_score == '–' or not t1_score or not t2_score:
            upcoming += 1
            continue

        try:
            s1, s2 = int(t1_score), int(t2_score)
        except ValueError:
            continue

        # Skip matches already in the cache — their data is in vct_data.json
        match_href = match.get('href', '')
        cached_matches = match_cache.setdefault('matches', {})
        if match_href and match_href in cached_matches:
            completed += 1
            continue

        k1, k2 = t1_name.lower(), t2_name.lower()
        if s1 > s2:
            team_map[k1]['matchW'] += 1
            team_map[k2]['matchL'] += 1
        elif s2 > s1:
            team_map[k2]['matchW'] += 1
            team_map[k1]['matchL'] += 1

        team_map[k1]['mapW'] += s1;  team_map[k1]['mapL'] += s2
        team_map[k2]['mapW'] += s2;  team_map[k2]['mapL'] += s1

        # Fetch per-map details + veto sequence + rosters from the match page
        map_details, veto, t1_players, t2_players, map_player_stats = [], [], [], [], []
        if match_href:
            detail = scrape_match_detail(BASE + match_href, t1_name, t2_name)
            cached_matches[match_href] = detail
            map_details      = detail['maps']
            veto             = detail['veto']
            t1_players       = detail.get('t1_players', [])
            t2_players       = detail.get('t2_players', [])
            map_player_stats = detail.get('map_player_stats', [])

        # Track the most recent lineup for each team (later matches overwrite earlier)
        if t1_players:
            team_map[k1]['last_lineup'] = t1_players
        if t2_players:
            team_map[k2]['last_lineup'] = t2_players

        # Accumulate per-player VFL points (keyed by lowercase player name)
        match_vfl = calc_vfl_for_match(
            match_score      = [s1, s2],
            map_details      = map_details,
            map_player_stats = map_player_stats,
            t1_players       = t1_players,
            t2_players       = t2_players,
        )
        for canon, stats in match_vfl.items():
            key = canon.lower()
            if key in vfl_map:
                vfl_map[key]['vfl_total']  += stats['vfl']
                vfl_map[key]['maps_total'] += stats['maps']
            else:
                vfl_map[key] = {'vfl_total': stats['vfl'], 'maps_total': stats['maps']}

        # Extract round label (e.g. 'Upper Round 1' → 'UR1')
        series_el = match.select_one('.match-item-event-series')
        rnd = _RND_MAP.get(series_el.get_text(strip=True), '') if series_el else ''

        # Store match history on both teams (flip map scores for t2; veto is shared)
        team_map[k1].setdefault('matches', []).append({
            'event':      label,
            'rnd':        rnd,
            'opponent':   t2_name,
            'result':     'W' if s1 > s2 else 'L',
            'matchScore': [s1, s2],
            'maps': [
                {'map': m['map'], 'score': [m['t1_score'], m['t2_score']]}
                for m in map_details
            ],
            'veto': veto,
        })
        team_map[k2].setdefault('matches', []).append({
            'event':      label,
            'rnd':        rnd,
            'opponent':   t1_name,
            'result':     'W' if s2 > s1 else 'L',
            'matchScore': [s2, s1],
            'maps': [
                {'map': m['map'], 'score': [m['t2_score'], m['t1_score']]}
                for m in map_details
            ],
            'veto': veto,
        })

        completed += 1
        new_count += 1

    skipped = completed - new_count
    print(f'    -> {new_count} new  |  {skipped} already stored  |  {upcoming} upcoming')
    return new_count


def scrape_stats(event_id: int, slug: str, team_map: dict, player_map: dict):
    """
    Scrape the event stats page and aggregate player ACS / Rating / FKPR / FDPR.
    player_map: { 'PlayerName|teamkey' -> {name, acs, rating, fkpr, fdpr, rounds} }
    Uses a weighted average when the same player appears across multiple events.
    """
    url  = f'{BASE}/event/stats/{event_id}/{slug}'
    soup = fetch(url)
    if not soup:
        return

    table = soup.select_one('table.wf-table.mod-stats')
    if not table:
        print(f'    !  Stats table not found')
        return

    cols = col_indices(table)
    # Locate required columns; use fallbacks for header text variants
    def ci(name, *alts):
        for n in (name, *alts):
            if n in cols:
                return cols[n]
        return None

    i_rnd    = ci('Rnd')
    i_rating = ci('R2.0', 'Rating', 'R')
    i_acs    = ci('ACS')
    i_fkpr   = ci('FKPR', 'FK%')
    i_fdpr   = ci('FDPR', 'FD%')
    i_kpr    = ci('KPR')          # kills per round — optional
    i_k      = ci('K')            # total kills    — fallback for kpr

    if None in (i_rnd, i_rating, i_acs, i_fkpr, i_fdpr):
        print(f'    !  Could not find all required stat columns in: {list(cols.keys())}')
        return

    rows_added = 0
    for row in table.select('tbody tr'):
        tds = row.select('td')
        if len(tds) < max(i_rnd, i_rating, i_acs, i_fkpr, i_fdpr) + 1:
            continue

        # Player name and team abbreviation
        player_td = row.select_one('td.mod-player')
        if not player_td:
            continue
        name_el = player_td.select_one('div.text-of')
        org_el  = player_td.select_one('div.stats-player-country')
        if not name_el or not org_el:
            continue

        player_name = name_el.get_text(strip=True)
        org_abbr    = org_el.get_text(strip=True)

        if not player_name:
            continue

        # Find team in team_map by org abbreviation
        team_key = find_team_key(org_abbr, team_map)
        if not team_key:
            continue  # player's team not a VCT franchise team

        # Parse stats
        def floatval(td):
            txt = tds[td].get_text(strip=True).rstrip('%').replace('–', '').strip()
            try:
                return float(txt)
            except ValueError:
                return 0.0

        rnd    = int(floatval(i_rnd))
        rating = floatval(i_rating)
        acs    = floatval(i_acs)
        fkpr   = floatval(i_fkpr)
        fdpr   = floatval(i_fdpr)

        # Kills per round: prefer KPR column; derive from K/Rnd if available
        if i_kpr is not None:
            kpr = floatval(i_kpr)
        elif i_k is not None and rnd > 0:
            kpr = floatval(i_k) / rnd
        else:
            kpr = 0.0

        if rnd == 0:
            continue

        composite_key = f'{player_name}|{team_key}'
        if composite_key in player_map:
            # Weighted average across events
            ex     = player_map[composite_key]
            total  = ex['rounds'] + rnd
            def wavg(a, b): return (a * ex['rounds'] + b * rnd) / total
            ex['acs']    = wavg(ex['acs'],    acs)
            ex['rating'] = wavg(ex['rating'], rating)
            ex['fkpr']   = wavg(ex['fkpr'],   fkpr)
            ex['fdpr']   = wavg(ex['fdpr'],   fdpr)
            ex['kpr']    = wavg(ex['kpr'],    kpr)
            ex['rounds'] = total
        else:
            player_map[composite_key] = {
                'name':     player_name,
                'team_key': team_key,
                'acs':      acs,
                'rating':   rating,
                'fkpr':     fkpr,
                'fdpr':     fdpr,
                'kpr':      kpr,
                'rounds':   rnd,
            }
        rows_added += 1

    print(f'    -> {rows_added} player-event rows scraped')


# -- Assembly ------------------------------------------------------------------

def assign_players(team_map: dict, player_map: dict, vfl_map: dict):
    """
    Attach the top-5 players to each team in team_map.
    If a 'last_lineup' exists (scraped from the most recent match page) only
    players in that lineup are included — this handles mid-season roster changes
    such as benchings and substitutions.  Players from the lineup who have no
    aggregate stats (brand-new signings) are appended with null stats.
    Computes fkfd = fkpr / fdpr, kpm = kills per map (kpr × 22 rounds avg),
    and vflPts = total VFL pts / maps played from per-match data.
    """
    from collections import defaultdict
    by_team = defaultdict(list)
    for p in player_map.values():
        by_team[p['team_key']].append(p)

    for key, players in by_team.items():
        if key not in team_map:
            continue

        last_lineup = team_map[key].get('last_lineup', [])

        if last_lineup:
            # Only keep players who appeared in the most recent match
            lineup_norm = {norm(n) for n in last_lineup}
            active = [p for p in players if norm(p['name']) in lineup_norm]

            # Append any lineup players with no recorded stats (e.g. new signings)
            known_norm = {norm(p['name']) for p in active}
            for player_name in last_lineup:
                if norm(player_name) not in known_norm:
                    active.append({
                        'name': player_name, 'team_key': key,
                        'acs': 0.0, 'rating': 0.0,
                        'fkpr': 0.0, 'fdpr': 0.0, 'kpr': 0.0, 'rounds': 0,
                    })
        else:
            active = players

        active.sort(key=lambda p: p['rounds'], reverse=True)

        player_list = []
        for p in active[:5]:
            vfl_data = vfl_map.get(p['name'].lower())
            vfl_pts  = (round(vfl_data['vfl_total'] / vfl_data['maps_total'], 2)
                        if vfl_data and vfl_data['maps_total'] > 0 else None)
            player_list.append({
                'name':   p['name'],
                'acs':    round(p['acs'])          if p['rounds'] > 0 else None,
                'rating': round(p['rating'], 2)    if p['rounds'] > 0 else None,
                'fkfd':   round(p['fkpr'] / p['fdpr'], 2)
                          if p.get('fdpr', 0) > 0 else None,
                'kpm':    round(p.get('kpr', 0.0) * 22, 1)
                          if p['rounds'] > 0 and p.get('kpr', 0.0) > 0 else None,
                'vflPts': vfl_pts,
            })
        team_map[key]['players'] = player_list


# -- Main ----------------------------------------------------------------------

def main():
    print('-' * 60)
    print('VCT 2026 scraper  ->  vlr.gg')
    print('-' * 60)

    match_cache = load_cache()

    # Seed team_map from existing data — avoids re-processing old matches
    team_map       = {}
    existing_events = set()
    old_vfl_pts    = {}   # (team_key, player_name_lower) -> vflPts, for restoration below
    if OUT_FILE.exists():
        try:
            old = json.loads(OUT_FILE.read_text(encoding='utf-8'))
            existing_events = set(old.get('events', []))
            for t in old.get('teams', []):
                key = t['name'].lower()
                team_map[key] = {
                    'name':      t['name'],
                    'region':    t['region'],
                    'logo':      t['logo'],
                    'matchW':    t['matchW'],
                    'matchL':    t['matchL'],
                    'mapW':      t['mapW'],
                    'mapL':      t['mapL'],
                    'players':   list(t.get('players', [])),
                    'matches':   list(t.get('matches', [])),
                    'franchise': True,
                }
                for p in t.get('players', []):
                    if p.get('vflPts') is not None:
                        old_vfl_pts[(key, p['name'].lower())] = p['vflPts']
        except Exception:
            pass

    player_map     = {}
    vfl_map        = {}
    events_scraped = sorted(existing_events, key=lambda e: next(
        (i for i, (_, _, _, l) in enumerate(VCT_EVENTS) if l == e), 999))
    events_with_new = set()

    for (event_id, slug, region, label) in VCT_EVENTS:
        print(f'\n[{label}]  event/{event_id}')

        if region and label not in existing_events:
            # Only hit the overview page for events we haven't seen before
            print('  Overview …')
            scrape_teams_from_overview(event_id, slug, region, team_map)

        print('  Matches …')
        new_count = scrape_matches(event_id, slug, region or 'americas', team_map, label, vfl_map, match_cache)

        if new_count > 0:
            events_with_new.add(label)
            print('  Stats …')
            scrape_stats(event_id, slug, team_map, player_map)

        if label not in existing_events:
            events_scraped.append(label)

    if not events_with_new:
        print('\nNo new matches — database is up to date.')
        return

    # Persist match detail cache so future runs skip already-scraped matches
    save_cache(match_cache)

    # Attach updated player stats to teams that had new matches
    assign_players(team_map, player_map, vfl_map)

    # Restore vflPts for any player whose value was lost (stats page lacks per-match VFL)
    for key, t in team_map.items():
        for p in t.get('players', []):
            if p.get('vflPts') is None:
                stored = old_vfl_pts.get((key, p['name'].lower()))
                if stored is not None:
                    p['vflPts'] = stored

    # Apply mid-season team renames (e.g. DRX → Kiwoom DRX).
    # Merges old entry into new name, preserving all historical stats/matches.
    for old_key, new_name in TEAM_RENAMES.items():
        if old_key not in team_map:
            continue
        entry = team_map.pop(old_key)
        entry['name'] = new_name
        new_key = new_name.lower()
        if new_key in team_map:
            # New-name entry already exists (scraped under new name) — merge
            existing = team_map[new_key]
            existing['matchW'] += entry['matchW']
            existing['matchL'] += entry['matchL']
            existing['mapW']   += entry['mapW']
            existing['mapL']   += entry['mapL']
            existing['matches'] = entry['matches'] + existing['matches']
            if not existing['logo'] and entry['logo']:
                existing['logo'] = entry['logo']
            if not existing.get('players') and entry.get('players'):
                existing['players'] = entry['players']
        else:
            entry['franchise'] = True  # all explicitly renamed teams are franchise
            team_map[new_key] = entry
        print(f'    -> Renamed "{old_key}" -> "{new_name}"')

    # Also rename the team name in any opponent/veto references inside match history
    rename_map = {old: new for old, new in TEAM_RENAMES.items()}
    for t in team_map.values():
        for m in t.get('matches', []):
            opp_l = m.get('opponent', '').lower()
            if opp_l in rename_map:
                m['opponent'] = rename_map[opp_l]
            for step in m.get('veto', []):
                if step.get('team', '').lower() in rename_map:
                    step['team'] = rename_map[step['team'].lower()]

    # Build output — only confirmed franchise teams in known regions
    teams_out = []
    for t in team_map.values():
        if not t.get('franchise'):
            continue  # non-franchise wildcard/invitee team
        if t['region'] not in REGION_META:
            continue
        teams_out.append({
            'name':    t['name'],
            'region':  t['region'],
            'logo':    t['logo'],
            'matchW':  t['matchW'],
            'matchL':  t['matchL'],
            'mapW':    t['mapW'],
            'mapL':    t['mapL'],
            'players': t['players'],
            'matches': t.get('matches', []),
        })

    # Update ratings for any new completed matches before writing output
    _apply_ratings(teams_out)

    output = {
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'events':      events_scraped,
        'teams':       teams_out,
        'source':      'vlr.gg scraper',
    }

    OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')

    print(f'\n{"-" * 60}')
    print(f'OK  {len(teams_out)} teams written to {OUT_FILE.name}')

    by_region = {}
    for t in teams_out:
        by_region[t['region']] = by_region.get(t['region'], 0) + 1
    for r, n in sorted(by_region.items()):
        print(f'   {REGION_META[r]["label"]:12s}  {n} teams')

    players_with_stats = sum(1 for t in teams_out if t['players'])
    matches_with_maps  = sum(
        1 for t in teams_out
        for m in t['matches'] if m.get('maps')
    )
    print(f'   {players_with_stats} / {len(teams_out)} teams have player stats')
    print(f'   {matches_with_maps} match entries have map detail')
    print('-' * 60)

    print('\nBuilding query index …')
    subprocess.run([sys.executable, str(Path(__file__).parent / 'build_index.py')], check=True)


if __name__ == '__main__':
    main()
