"""
extract_data.py
Ekstrak data dari Google Sheets → update HTML dashboard insentif.

Dijalankan oleh GitHub Actions. Butuh env vars:
  GDRIVE_CREDENTIALS : JSON service account key (dari GitHub Secrets)
  SHEET_ID_2026      : ID spreadsheet 2026
  SHEET_ID_2025      : ID spreadsheet 2025
"""

import os, json, re
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials

# ── Config ──────────────────────────────────────────────────────────────────

SITES_26 = [
    'JBBK','CKP','SDA',
    'Hub Bogor','Hub Tangerang','Hub Utara','Hub Bandung',
    'Hub Yogya','Hub Semarang','Hub Lampung','Hub Palembang','Hub Kediri'
]
SITES_25 = ['JBBK','CKP','SDA']   # 2025 hanya NDC
MONTHS   = ['January','February','March','April','May']

# Bulan parsial — update setiap bulan baru masuk
# key: nama bulan, value: hari cutoff MTD
PARTIAL_MONTHS = {'May': 4}

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]

HTML_PATH = 'dashboard_insentif_2026.html'

# ── Auth Google Sheets ───────────────────────────────────────────────────────

def get_gc():
    creds_json = os.environ['GDRIVE_CREDENTIALS']
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Helpers ──────────────────────────────────────────────────────────────────

def col_idx(headers, name):
    """Return 0-based index of column by name, -1 if not found."""
    for i, h in enumerate(headers):
        if str(h).strip() == name:
            return i
    return -1

def to_num(v):
    if v in (None, '', 'None'): return 0.0
    try:
        return float(str(v).replace(',', '').replace(' ', ''))
    except:
        return 0.0

def empty_month():
    return {
        'trips': 0, 'do_': 0, 'dp': 0, 'ujp': 0, 'ins': 0,
        'mpp_low': 0, 'mpp_mid': 0, 'mpp_high': 0
    }


# ── Extraction ───────────────────────────────────────────────────────────────

def extract_sheet(ws, site, months, sm, mpp_raw):
    """Extract one worksheet into sm[site][month] dict."""
    all_rows = ws.get_all_values()
    if not all_rows:
        print(f"  [SKIP] {site} — sheet kosong")
        return

    headers = all_rows[0]
    ci = {
        'lc'     : col_idx(headers, 'LC NUM'),
        'month'  : col_idx(headers, 'Month Rev'),
        'do'     : col_idx(headers, 'Jumlah_do'),
        'dp'     : col_idx(headers, 'jumlah_titik'),
        'ujp'    : col_idx(headers, 'UJP'),
        'ins'    : col_idx(headers, 'Insentif Ref'),
        'insmpp' : col_idx(headers, 'Insentif per MPP'),
        'driver' : col_idx(headers, 'driver'),
        'nik1'   : col_idx(headers, 'NIK1'),
        'nik2'   : col_idx(headers, 'nik2'),
        'kenek'  : col_idx(headers, 'kenek1'),
    }

    monthly   = defaultdict(empty_month)
    mpp_month = defaultdict(lambda: defaultdict(float))
    mpp_info  = {}

    for row in all_rows[1:]:
        def g(c): return row[c] if c >= 0 and c < len(row) else ''

        m = str(g(ci['month'])).strip()
        if m not in months:
            continue

        drv = str(g(ci['driver'])).strip()
        is_dummy  = 'DUMMY' in drv.upper()
        lc_raw    = str(g(ci['lc'])).strip()
        lc_empty  = not lc_raw or lc_raw in ('', 'None', '#N/A')
        has_driver = bool(drv and drv.upper() not in ('', 'NONE'))

        # Row inclusion rule
        if lc_empty and not has_driver:
            continue

        monthly[m]['trips'] += 1
        monthly[m]['do_']   += to_num(g(ci['do']))
        monthly[m]['dp']    += to_num(g(ci['dp']))
        monthly[m]['ujp']   += to_num(g(ci['ujp']))
        monthly[m]['ins']   += to_num(g(ci['ins']))

        if is_dummy or lc_empty:
            continue

        ins_mpp = to_num(g(ci['insmpp']))
        if ins_mpp <= 0:
            continue

        for nik_ci, name_ci in [(ci['nik1'], ci['driver']), (ci['nik2'], ci['kenek'])]:
            nik  = str(g(nik_ci)).strip()
            name = str(g(name_ci)).strip()
            if not nik or nik in ('None', '999999', ''):
                continue
            if 'DUMMY' in name.upper():
                continue
            mpp_month[nik][m] += ins_mpp
            if nik not in mpp_info:
                mpp_info[nik] = {'name': name, 'site': site}

    # Finalize monthly
    sm[site] = {m: dict(v) for m, v in monthly.items()}

    # Aggregate mpp_raw for mpp_low/mid/high
    for nik, info in mpp_info.items():
        if nik not in mpp_raw:
            mpp_raw[nik] = {'name': info['name'], 'site': site, 'months': {}}
        for mo, ins in mpp_month[nik].items():
            mpp_raw[nik]['months'][mo] = mpp_raw[nik]['months'].get(mo, 0) + ins

    print(f"  [OK] {site} — {dict({m: sm[site][m]['trips'] for m in sm[site]})}")


def compute_mpp_categories(sm, mpp_raw):
    """Fill mpp_low/mid/high in sm from mpp_raw."""
    for nik, d in mpp_raw.items():
        for mo, ins in d['months'].items():
            site = d['site']
            if site in sm and mo in sm[site]:
                if ins < 500_000:
                    sm[site][mo]['mpp_low']  += 1
                elif ins > 1_500_000:
                    sm[site][mo]['mpp_high'] += 1
                else:
                    sm[site][mo]['mpp_mid']  += 1


def add_period_subkeys(sm, partial_months):
    """
    Add .period and .mom_period sub-keys for partial months.
    For now, .period = full month data (will be overridden if you filter by date).
    .mom_period = same-window data from previous month (must be in SITE_MONTHLY already).

    NOTE: If your sheet already has a 'Period' or 'Cutoff' column, filter here.
    Otherwise, .period mirrors the full month totals (extraction already filtered by date
    if you set your sheet to only include MTD rows).
    """
    pass  # Sub-keys sudah dihitung di extraction script terpisah jika diperlukan


def build_insight_data(sm26, sm25, sites_ndc):
    """Build INSIGHT_DATA dict from sm26 and sm25."""
    def agg(sm, month, sites, key=None):
        r = {'trips': 0, 'do_': 0, 'dp': 0, 'ujp': 0, 'ins': 0}
        for sk in sites:
            d = sm.get(sk, {}).get(month, {})
            if key and isinstance(d.get(key), dict):
                d = d[key]
            for k in r:
                r[k] += d.get(k, 0)
        return r

    def metrics(d):
        t  = d['trips'] or 1
        do_ = d['do_']  or 1
        dp_ = d['dp']   or 1
        return {
            'DO'       : d['do_'],
            'DP'       : d['dp'],
            'Trip'     : d['trips'],
            'UJP'      : d['ujp'],
            'Insentif' : d['ins'],
            'DO/Trip'  : round(d['do_'] / t,  2),
            'DP/Trip'  : round(d['dp']  / t,  2),
            'UJP/Trip' : round(d['ujp'] / t),
            'UJP/DO'   : round(d['ujp'] / do_),
            'UJP/DP'   : round(d['ujp'] / dp_),
            'DO/DP'    : round(d['do_'] / dp_, 2),
        }

    ALL_SITES = list(sm26.keys())
    prev_map  = {
        'February': 'January', 'March': 'February', 'April': 'March',
        'May': 'April', 'June': 'May'
    }

    insight = {}
    for m in MONTHS:
        prev = prev_map.get(m)
        is_partial = m in PARTIAL_MONTHS

        cur_key  = 'period'    if is_partial else None
        prev_key = 'mom_period' if is_partial else None

        cur26  = agg(sm26, m, ALL_SITES, cur_key)
        cur25  = agg(sm25, m, sites_ndc)
        prev26 = agg(sm26, prev, ALL_SITES, prev_key) if prev else None

        insight[m] = {
            'cur26'  : metrics(cur26),
            'cur25'  : metrics(cur25),
            'prev26' : metrics(prev26) if prev26 else None,
            'cutoff_day'     : PARTIAL_MONTHS.get(m),
            'prev_cutoff_day': PARTIAL_MONTHS.get(prev) if prev else None,
        }

    return insight


# ── HTML Replace ─────────────────────────────────────────────────────────────

def replace_section(html, const_name, new_js, next_const):
    start = html.find(f'const {const_name}=')
    end   = html.find(f'const {next_const}=')
    if start == -1 or end == -1:
        raise ValueError(f'replace_section: tidak ditemukan {const_name} atau {next_const}')
    return html[:start] + f'const {const_name}={new_js};\n' + html[end:]

def jd(obj):
    return json.dumps(obj, separators=(',', ':'), ensure_ascii=False)

def update_html(sm26, sm25, all_mpp, top20, insight):
    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    # Update LAST_MONTH di nav()
    last_month = MONTHS[-1]
    html = re.sub(
        r"const LAST_MONTH='[A-Za-z]+'",
        f"const LAST_MONTH='{last_month}'",
        html
    )

    # Update PERIOD_CONFIG
    partial_list = json.dumps(list(PARTIAL_MONTHS.keys()))
    cutoff_dict  = json.dumps(PARTIAL_MONTHS)
    html = re.sub(
        r'const PERIOD_CONFIG=\{[^;]+\};',
        f'const PERIOD_CONFIG={{partial_months:{partial_list},cutoff:{cutoff_dict}}};',
        html
    )

    # Update MONTHS array
    months_js = json.dumps(MONTHS)
    html = re.sub(r'const MONTHS=\[[^\]]+\]', f'const MONTHS={months_js}', html)

    # Replace data consts
    html = replace_section(html, 'SITE_MONTHLY_2025', jd(sm25), 'SITE_MONTHLY')
    html = replace_section(html, 'SITE_MONTHLY',      jd(sm26), 'ALL_MPP')
    html = replace_section(html, 'ALL_MPP',           jd(all_mpp), 'TOP_MPP')
    html = replace_section(html, 'TOP_MPP',           jd(top20),   'INSIGHT_DATA')

    id_start = html.find('const INSIGHT_DATA=')
    id_end   = html.find("const NK=['JBBK'")
    html = html[:id_start] + f'const INSIGHT_DATA={jd(insight)};\n' + html[id_end:]

    with open(HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f'\n✅ HTML updated: {HTML_PATH}')


# ── Build ALL_MPP & TOP_MPP ──────────────────────────────────────────────────

def build_mpp_tables(mpp_raw):
    """Build ALL_MPP and TOP_MPP from mpp_raw."""
    all_mpp = []
    for nik, d in mpp_raw.items():
        total = sum(d['months'].values())
        row = {
            'nik'  : nik,
            'name' : d['name'],
            'site' : d['site'],
            'total': total,
        }
        for m in MONTHS:
            row[m[:3].lower()] = d['months'].get(m, 0)
        all_mpp.append(row)

    all_mpp.sort(key=lambda x: -x['total'])

    top20 = [
        {'name': r['name'], 'site': r['site'], 'total': r['total'],
         **{m[:3].lower(): r[m[:3].lower()] for m in MONTHS}}
        for r in all_mpp[:20]
    ]

    return all_mpp, top20


# ── Verification ─────────────────────────────────────────────────────────────

def verify(sm26):
    checks = [('JBBK', 'April', 1340), ('CKP', 'April', 1211), ('SDA', 'April', 823)]
    ok = True
    for sk, m, exp in checks:
        actual = sm26.get(sk, {}).get(m, {}).get('trips', 0)
        status = '✅' if actual == exp else f'⚠️  expected {exp}'
        print(f'  {sk} {m} trips={actual} {status}')
        if actual != exp: ok = False
    return ok


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print('=== Dashboard Insentif — Auto Update ===\n')

    gc = get_gc()

    sheet_id_26 = os.environ['SHEET_ID_2026']
    sheet_id_25 = os.environ['SHEET_ID_2025']

    sm26    = {s: {} for s in SITES_26}
    sm25    = {s: {} for s in SITES_25}
    mpp_raw = {}

    # ── Extract 2026 ──
    print('📥 Extracting 2026...')
    wb26 = gc.open_by_key(sheet_id_26)
    for site in SITES_26:
        try:
            ws = wb26.worksheet(site)
            extract_sheet(ws, site, MONTHS, sm26, mpp_raw)
        except gspread.exceptions.WorksheetNotFound:
            print(f'  [MISS] {site} — tab tidak ditemukan')

    # ── Extract 2025 ──
    print('\n📥 Extracting 2025...')
    wb25 = gc.open_by_key(sheet_id_25)
    mpp_raw_25 = {}
    for site in SITES_25:
        try:
            ws = wb25.worksheet(site)
            extract_sheet(ws, site, MONTHS, sm25, mpp_raw_25)
        except gspread.exceptions.WorksheetNotFound:
            print(f'  [MISS] {site} — tab tidak ditemukan')

    # ── MPP categories ──
    compute_mpp_categories(sm26, mpp_raw)
    all_mpp, top20 = build_mpp_tables(mpp_raw)

    # ── INSIGHT_DATA ──
    insight = build_insight_data(sm26, sm25, SITES_25)

    # ── Verify ──
    print('\n🔍 Verifikasi trip count April:')
    verify(sm26)

    # ── Update HTML ──
    print('\n✏️  Updating HTML...')
    update_html(sm26, sm25, all_mpp, top20, insight)


if __name__ == '__main__':
    main()
