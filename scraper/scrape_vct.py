#!/usr/bin/env python3
"""
VCT 2026 data scraper for vlr.gg
Outputs  ../vct_data.json  (read by valorant.html before falling back to the API)

Usage:
    cd scraper
    python scrape_vct.py

Requirements:  pip install requests beautifulsoup4
"""

import json, time, re, sys
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

OUT_FILE = Path(__file__).parent.parent / 'vct_data.json'

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
    'tl':   'teamliquid',  'ulf': 'ulfesports',   'vit': 'teamvitality',
    # Pacific
    'drx':  'drx',         't1':  't1',            'gen': 'geng',
    'ns':   'nongshimredforce',                    'prx': 'paperrex',
    'rrq':  'rexregumqeon','ge':  'globalesports', 'fs':  'fullsense',
    'ts':   'teamsecret',  'dfm': 'detonationfocusme',
    'zeta': 'zetadivision','vrl': 'varrel',
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
    Visit a single match page and return map scores + veto sequence.
    Returns {'maps': [...], 'veto': [...]}
      maps: [{'map': str, 't1_score': int, 't2_score': int}, ...]
      veto: [{'step': int, 'action': 'ban'|'pick'|'decider', 'map': str, 'team': str}, ...]
    t1/t2 correspond to the left/right team on the match listing page.
    """
    soup = fetch(match_url)
    if not soup:
        return {'maps': [], 'veto': []}

    return {
        'maps': _scrape_maps(soup),
        'veto': _scrape_veto(soup, t1_name, t2_name),
    }


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
    """Map a raw team string (full name or abbreviation) to t1/t2 name, or '' if unresolved."""
    rl = raw.lower().strip()
    if not rl:
        return ''
    # Full-name substring match
    if t1_name and (t1_name.lower() in rl or rl in t1_name.lower()):
        return t1_name
    if t2_name and (t2_name.lower() in rl or rl in t2_name.lower()):
        return t2_name
    # Abbreviation match via ORG_ABBREV
    canonical = ORG_ABBREV.get(norm(raw))
    if canonical:
        for name in (t1_name, t2_name):
            if name and (norm(name) == canonical or canonical in norm(name) or norm(name) in canonical):
                return name
    return ''  # unresolved — don't pollute with raw text


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


def scrape_matches(event_id: int, slug: str, region: str, team_map: dict, label: str):
    """
    Scrape the event matches page.
    • Registers any new teams (with correct region).
    • Accumulates match W/L and map W/L for completed matches.
    • Stores per-match history (opponent, result, map scores) on each team.
    """
    url  = f'{BASE}/event/matches/{event_id}/{slug}/'
    soup = fetch(url)
    if not soup:
        return

    completed = upcoming = 0

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

        k1, k2 = t1_name.lower(), t2_name.lower()
        if s1 > s2:
            team_map[k1]['matchW'] += 1
            team_map[k2]['matchL'] += 1
        elif s2 > s1:
            team_map[k2]['matchW'] += 1
            team_map[k1]['matchL'] += 1

        team_map[k1]['mapW'] += s1;  team_map[k1]['mapL'] += s2
        team_map[k2]['mapW'] += s2;  team_map[k2]['mapL'] += s1

        # Fetch per-map details + veto sequence from the match page
        match_href = match.get('href', '')
        map_details, veto = [], []
        if match_href:
            detail     = scrape_match_detail(BASE + match_href, t1_name, t2_name)
            map_details = detail['maps']
            veto        = detail['veto']

        # Store match history on both teams (flip map scores for t2; veto is shared)
        team_map[k1].setdefault('matches', []).append({
            'event':      label,
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

    print(f'    -> {completed} completed matches, {upcoming} upcoming')


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
            ex['rounds'] = total
        else:
            player_map[composite_key] = {
                'name':     player_name,
                'team_key': team_key,
                'acs':      acs,
                'rating':   rating,
                'fkpr':     fkpr,
                'fdpr':     fdpr,
                'rounds':   rnd,
            }
        rows_added += 1

    print(f'    -> {rows_added} player-event rows scraped')


# -- Assembly ------------------------------------------------------------------

def assign_players(team_map: dict, player_map: dict):
    """
    Attach the top-5 players (by rounds played) to each team in team_map.
    Computes fkfd = fkpr / fdpr.
    """
    from collections import defaultdict
    by_team = defaultdict(list)
    for p in player_map.values():
        by_team[p['team_key']].append(p)

    for key, players in by_team.items():
        if key not in team_map:
            continue
        players.sort(key=lambda p: p['rounds'], reverse=True)
        team_map[key]['players'] = [
            {
                'name':   p['name'],
                'acs':    round(p['acs']),
                'rating': round(p['rating'], 2),
                'fkfd':   round(p['fkpr'] / p['fdpr'], 2) if p['fdpr'] > 0 else 0.0,
            }
            for p in players[:5]
        ]


# -- Main ----------------------------------------------------------------------

def main():
    print('-' * 60)
    print('VCT 2026 scraper  ->  vlr.gg')
    print('-' * 60)

    team_map   = {}   # lower-case name -> team dict
    player_map = {}   # 'PlayerName|team_key' -> stat dict

    events_scraped = []

    for (event_id, slug, region, label) in VCT_EVENTS:
        print(f'\n[{label}]  event/{event_id}')

        if region:
            # Register all franchise teams from the overview page (incl. pre-season)
            print('  Overview …')
            scrape_teams_from_overview(event_id, slug, region, team_map)

        print('  Matches …')
        scrape_matches(event_id, slug, region or 'americas', team_map, label)

        print('  Stats …')
        scrape_stats(event_id, slug, team_map, player_map)

        events_scraped.append(label)

    # Attach player stats to teams
    assign_players(team_map, player_map)

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


if __name__ == '__main__':
    main()
