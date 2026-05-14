"""
extract_data.py
Ekstrak data dari Google Sheets → update HTML dashboard insentif.
Auto-detect bulan aktif dan partial months dari data sheet.

Dijalankan oleh GitHub Actions. Butuh env vars:
  GDRIVE_CREDENTIALS : JSON service account key (dari GitHub Secrets)
  SHEET_ID_2026      : ID spreadsheet 2026
  SHEET_ID_2025      : ID spreadsheet 2025
"""

import os, json, re, time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials

# ── Config ───────────────────────────────────────────────────────────────────

SITES_26 = [
    'JBBK','CKP','SDA',
    'Hub Bogor','Hub Tangerang','Hub Utara','Hub Bandung',
    'Hub Yogya','Hub Semarang','Hub Lampung','Hub Palembang','Hub Kediri'
]
SITES_25 = ['JBBK','CKP','SDA']

# Tab names di sheet 2025 pakai HURUF KAPITAL — mapping site name → tab name 2025
SITES_25_TAB_MAP = {
    'JBBK'          : 'JBBK',
    'CKP'           : 'CKP',
    'SDA'           : 'SDA',
    'Hub Bogor'     : 'HUB BOGOR',
    'Hub Tangerang' : 'HUB TANGERANG',
    'Hub Utara'     : 'HUB UTARA',
    'Hub Bandung'   : 'HUB BANDUNG',
    'Hub Yogya'     : 'HUB YOGYA',
    'Hub Semarang'  : 'HUB SEMARANG',
    'Hub Lampung'   : 'HUB LAMPUNG',
    'Hub Palembang' : 'HUB PALEMBANG',
    'Hub Kediri'    : 'HUB KEDIRI',
}

# 2025: extract semua bulan yang tersedia termasuk Q4
MONTHS_2025 = [
    'January','February','March','April','May','June',
    'July','August','September','October','November','December'
]

MONTH_ORDER = [
    'January','February','March','April','May','June',
    'July','August','September','October','November','December'
]

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]

HTML_PATH = 'dashboard_insentif_2026.html'

# ── Auto-detect months dari sheet ────────────────────────────────────────────

def detect_months_and_partial(wb, sites):
    """
    Baca semua nilai kolom 'Month Rev' dari semua sheet,
    lalu tentukan:
      - MONTHS: semua bulan yang ada (sorted by MONTH_ORDER)
      - PARTIAL_MONTHS: bulan terakhir + cutoff day (hari max yang ada di data)
    """
    wib = timezone(timedelta(hours=7))
    today = datetime.now(wib)
    current_month = today.strftime('%B')   # e.g. 'May'
    current_day   = today.day

    month_set = set()
    # Untuk partial: cari max tanggal di bulan terakhir
    # Kolom tanggal bisa 'Tanggal', 'Date', 'date', 'tgl' — coba semua
    date_col_candidates = ['Tanggal','tanggal','Date','date','Tgl','tgl','Transaction Date']

    month_maxday = defaultdict(int)

    for site in sites:
        try:
            ws = wb.worksheet(site)
            all_rows = ws.get_all_values()
            if not all_rows: continue
            headers = all_rows[0]

            ci_month = -1
            ci_date  = -1
            for i, h in enumerate(headers):
                if str(h).strip() == 'Month Rev':
                    ci_month = i
                for dc in date_col_candidates:
                    if str(h).strip() == dc:
                        ci_date = i

            for row in all_rows[1:]:
                m = row[ci_month].strip() if ci_month >= 0 and ci_month < len(row) else ''
                if m in MONTH_ORDER:
                    month_set.add(m)
                    # Coba extract hari dari kolom tanggal
                    if ci_date >= 0 and ci_date < len(row):
                        raw = str(row[ci_date]).strip()
                        for fmt in ('%d/%m/%Y','%Y-%m-%d','%d-%m-%Y','%m/%d/%Y'):
                            try:
                                d = datetime.strptime(raw, fmt)
                                month_maxday[m] = max(month_maxday[m], d.day)
                                break
                            except: pass
        except Exception as e:
            print(f'  [detect] {site} skip: {e}')
            continue

    if not month_set:
        # Fallback: pakai bulan saat ini
        month_set = {current_month}

    months = sorted(month_set, key=lambda m: MONTH_ORDER.index(m))

    # Tentukan partial months:
    # Bulan dianggap partial kalau == bulan sekarang di tahun ini
    partial_months = {}
    last_month = months[-1]
    if last_month == current_month:
        # Cutoff = max day yang ada di data, atau hari ini kalau tidak ada kolom tanggal
        cutoff = month_maxday.get(last_month, current_day)
        partial_months[last_month] = cutoff

    print(f'\n📅 Auto-detect: MONTHS={months}')
    print(f'📅 PARTIAL_MONTHS={partial_months}')
    return months, partial_months


# ── Auth ─────────────────────────────────────────────────────────────────────

def get_gc():
    creds_json = os.environ['GDRIVE_CREDENTIALS']
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Helpers ──────────────────────────────────────────────────────────────────

def col_idx(headers, name):
    for i, h in enumerate(headers):
        if str(h).strip() == name:
            return i
    return -1

def to_num(v):
    if v in (None, '', 'None'): return 0.0
    try:
        return float(str(v).replace(',','').replace(' ',''))
    except:
        return 0.0

def empty_month():
    return {'trips':0,'do_':0,'dp':0,'ujp':0,'ins':0,
            'mpp_low':0,'mpp_mid':0,'mpp_high':0}


# ── Extraction ───────────────────────────────────────────────────────────────

def extract_sheet(ws, site, months, sm, mpp_raw, partial_months=None, is_2025=False):
    """
    Extract sheet data.
    Kalau is_2025=True dan partial_months ada, generate sub-key .yoy_period
    berisi data dengan cutoff hari yang sama (untuk YoY apple-to-apple).
    """
    DATE_FMTS = ['%d/%m/%Y','%Y-%m-%d','%d-%m-%Y','%m/%d/%Y']
    DATE_COLS  = ['Tanggal','tanggal','Date','date','Tgl','tgl','Transaction Date']

    all_rows = ws.get_all_values()
    if not all_rows:
        print(f'  [SKIP] {site} — kosong')
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
        'date'   : next((col_idx(headers, dc) for dc in DATE_COLS if col_idx(headers, dc) >= 0), -1),
    }

    monthly     = defaultdict(empty_month)
    yoy_partial = defaultdict(empty_month)  # untuk YoY apple-to-apple di 2025
    mpp_month   = defaultdict(lambda: defaultdict(float))
    mpp_info    = {}

    for row in all_rows[1:]:
        def g(c): return row[c] if 0 <= c < len(row) else ''

        m = str(g(ci['month'])).strip()
        if m not in months: continue

        drv        = str(g(ci['driver'])).strip()
        is_dummy   = 'DUMMY' in drv.upper()
        lc_raw     = str(g(ci['lc'])).strip()
        lc_empty   = not lc_raw or lc_raw in ('','None','#N/A')
        has_driver = bool(drv and drv.upper() not in ('','NONE'))

        if lc_empty and not has_driver: continue

        # Parse tanggal untuk yoy_period filter
        row_day = None
        if ci['date'] >= 0:
            raw_date = str(g(ci['date'])).strip()
            for fmt in DATE_FMTS:
                try:
                    row_day = datetime.strptime(raw_date, fmt).day
                    break
                except: pass

        monthly[m]['trips'] += 1
        monthly[m]['do_']   += to_num(g(ci['do']))
        monthly[m]['dp']    += to_num(g(ci['dp']))
        monthly[m]['ujp']   += to_num(g(ci['ujp']))
        monthly[m]['ins']   += to_num(g(ci['ins']))

        # Accumulate yoy_period — filter by cutoff day (sama dengan 2026 partial)
        if is_2025 and partial_months and m in partial_months:
            cutoff = partial_months[m]
            if row_day is not None and row_day <= cutoff:
                yoy_partial[m]['trips'] += 1
                yoy_partial[m]['do_']   += to_num(g(ci['do']))
                yoy_partial[m]['dp']    += to_num(g(ci['dp']))
                yoy_partial[m]['ujp']   += to_num(g(ci['ujp']))
                yoy_partial[m]['ins']   += to_num(g(ci['ins']))

        if is_dummy or lc_empty: continue
        ins_mpp = to_num(g(ci['insmpp']))
        if ins_mpp <= 0: continue

        for nik_ci, name_ci in [(ci['nik1'], ci['driver']), (ci['nik2'], ci['kenek'])]:
            nik  = str(g(nik_ci)).strip()
            name = str(g(name_ci)).strip()
            if not nik or nik in ('None','999999',''): continue
            if 'DUMMY' in name.upper(): continue
            mpp_month[nik][m] += ins_mpp
            if nik not in mpp_info:
                mpp_info[nik] = {'name': name, 'site': site}

    sm[site] = {m: dict(v) for m, v in monthly.items()}

    # Inject yoy_period sub-key ke sm[site][month]
    for m, d in yoy_partial.items():
        if m in sm[site]:
            sm[site][m]['yoy_period'] = dict(d)

    for nik, info in mpp_info.items():
        if nik not in mpp_raw:
            mpp_raw[nik] = {'name': info['name'], 'site': site, 'months': {}}
        for mo, ins in mpp_month[nik].items():
            mpp_raw[nik]['months'][mo] = mpp_raw[nik]['months'].get(mo, 0) + ins

    print(f'  [OK] {site} — {dict({m: sm[site][m]["trips"] for m in sm[site]})}')


def compute_mpp_categories(sm, mpp_raw):
    for nik, d in mpp_raw.items():
        for mo, ins in d['months'].items():
            site = d['site']
            if site in sm and mo in sm[site]:
                if ins < 500_000:    sm[site][mo]['mpp_low']  += 1
                elif ins > 1_500_000: sm[site][mo]['mpp_high'] += 1
                else:                 sm[site][mo]['mpp_mid']  += 1


def build_mpp_tables(mpp_raw, months):
    all_mpp = []
    for nik, d in mpp_raw.items():
        total = sum(d['months'].values())
        row = {'nik': nik, 'name': d['name'], 'site': d['site'], 'total': total}
        for m in months:
            row[m[:3].lower()] = d['months'].get(m, 0)
        all_mpp.append(row)
    all_mpp.sort(key=lambda x: -x['total'])
    top20 = all_mpp[:20]
    return all_mpp, top20


def build_insight_data(sm26, sm25, sites_ndc, months, partial_months):
    def agg(sm, month, sites, key=None):
        r = {'trips':0,'do_':0,'dp':0,'ujp':0,'ins':0}
        for sk in sites:
            d = sm.get(sk,{}).get(month,{})
            if key and isinstance(d.get(key), dict): d = d[key]
            for k in r: r[k] += d.get(k, 0)
        return r

    def metrics(d):
        t=d['trips'] or 1; do_=d['do_'] or 1; dp_=d['dp'] or 1
        return {
            'DO':d['do_'],'DP':d['dp'],'Trip':d['trips'],
            'UJP':d['ujp'],'Insentif':d['ins'],
            'DO/Trip':round(d['do_']/t,2),'DP/Trip':round(d['dp']/t,2),
            'UJP/Trip':round(d['ujp']/t),'UJP/DO':round(d['ujp']/do_),
            'UJP/DP':round(d['ujp']/dp_),'DO/DP':round(d['do_']/dp_,2),
        }

    ALL_SITES = list(sm26.keys())
    prev_map = {MONTH_ORDER[i]: MONTH_ORDER[i-1] for i in range(1, len(MONTH_ORDER))}

    insight = {}
    for m in months:
        prev = prev_map.get(m)
        is_partial = m in partial_months
        cur_key  = 'period'     if is_partial else None
        prev_key = 'mom_period' if is_partial else None

        cur26  = agg(sm26, m, ALL_SITES, cur_key)
        cur25  = agg(sm25, m, ALL_SITES, 'yoy_period' if is_partial else None)
        prev26 = agg(sm26, prev, ALL_SITES, prev_key) if prev and prev in months else None

        insight[m] = {
            'cur26'          : metrics(cur26),
            'cur25'          : metrics(cur25),
            'prev26'         : metrics(prev26) if prev26 else None,
            'cutoff_day'     : partial_months.get(m),
            'prev_cutoff_day': partial_months.get(prev) if prev else None,
        }
    return insight


# ── HTML Update ──────────────────────────────────────────────────────────────

def replace_section(html, const_name, new_js, next_const):
    start = html.find(f'const {const_name}=')
    end   = html.find(f'const {next_const}=')
    if start == -1 or end == -1:
        raise ValueError(f'Tidak ditemukan: {const_name} atau {next_const}')
    return html[:start] + f'const {const_name}={new_js};\n' + html[end:]

def jd(obj):
    return json.dumps(obj, separators=(',',':'), ensure_ascii=False)

def update_html(sm26, sm25, all_mpp, top20, insight, months, partial_months):
    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        html = f.read()

    # Auto-generate tanggal update
    wib = timezone(timedelta(hours=7))
    now = datetime.now(wib)
    MONTH_ID = ['','Jan','Feb','Mar','Apr','Mei','Jun','Jul','Agu','Sep','Okt','Nov','Des']
    tgl_update = f"{now.day} {MONTH_ID[now.month]} {now.year}"

    # Update chip tanggal di HTML
    html = re.sub(r'Update: \d+ \w+ \d{4}', f'Update: {tgl_update}', html)

    # Update MTD chip di setView()
    last_m = months[-1]
    last_cutoff = partial_months.get(last_m)
    if last_cutoff:
        MONTH_EN_TO_IDX = {m:i for i,m in enumerate(['','January','February','March','April','May','June','July','August','September','October','November','December'])}
        m_idx = MONTH_EN_TO_IDX.get(last_m, 0)
        mtd_text = f'MTD s/d {last_cutoff} {MONTH_ID[m_idx]} {now.year}'
        html = re.sub(r'MTD s/d \d+ \w+ \d{4}', mtd_text, html)

    # Update MONTHS
    html = re.sub(r'const MONTHS=\[[^\]]+\]', f'const MONTHS={jd(months)}', html)

    # Update LAST_MONTH
    html = re.sub(r"const LAST_MONTH='[A-Za-z]+'", f"const LAST_MONTH='{months[-1]}'", html)

    # Update PERIOD_CONFIG
    html = re.sub(
        r'const PERIOD_CONFIG=\{[^;]+\};',
        f'const PERIOD_CONFIG={{partial_months:{jd(list(partial_months.keys()))},cutoff:{jd(partial_months)}}};',
        html
    )

    # Update data consts
    html = replace_section(html, 'SITE_MONTHLY_2025', jd(sm25), 'SITE_MONTHLY')
    html = replace_section(html, 'SITE_MONTHLY',      jd(sm26), 'ALL_MPP')
    html = replace_section(html, 'ALL_MPP',           jd(all_mpp), 'TOP_MPP')
    html = replace_section(html, 'TOP_MPP',           jd(top20),   'INSIGHT_DATA')

    id_start = html.find('const INSIGHT_DATA=')
    id_end   = html.find("const NK=['JBBK'")
    html = html[:id_start] + f'const INSIGHT_DATA={jd(insight)};\n' + html[id_end:]

    with open(HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    wib = timezone(timedelta(hours=7))
    now = datetime.now(wib).strftime('%d %b %Y %H:%M WIB')
    print(f'\n✅ HTML updated: {HTML_PATH} [{now}]')


# ── Verify ───────────────────────────────────────────────────────────────────

def verify(sm26):
    checks = [('JBBK','April',1340),('CKP','April',1211),('SDA','April',823)]
    for sk, m, exp in checks:
        actual = sm26.get(sk,{}).get(m,{}).get('trips',0)
        status = '✅' if actual == exp else f'⚠️  expected {exp}'
        print(f'  {sk} {m} trips={actual} {status}')


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('=== Dashboard Insentif — Auto Update ===\n')
    gc = get_gc()

    wb26 = gc.open_by_key(os.environ['SHEET_ID_2026'])
    wb25 = gc.open_by_key(os.environ['SHEET_ID_2025'])

    # Auto-detect months dari sheet 2026
    months, partial_months = detect_months_and_partial(wb26, SITES_26)

    sm26 = {s:{} for s in SITES_26}
    sm25 = {s:{} for s in SITES_26}  # semua site, bukan cuma NDC
    mpp_raw = {}

    print('\n📥 Extracting 2026...')
    for i, site in enumerate(SITES_26):
        try:
            extract_sheet(wb26.worksheet(site), site, months, sm26, mpp_raw)
            if i < len(SITES_26)-1: time.sleep(10)  # hindari quota exceeded
        except gspread.exceptions.WorksheetNotFound:
            print(f'  [MISS] {site}')

    print('\n📥 Extracting 2025...')
    mpp_raw_25 = {}
    for i, site in enumerate(SITES_26):
        tab_name = SITES_25_TAB_MAP.get(site, site)  # pakai nama tab 2025
        try:
            extract_sheet(wb25.worksheet(tab_name), site, MONTHS_2025, sm25, mpp_raw_25,
                         partial_months=partial_months, is_2025=True)
            if i < len(SITES_26)-1: time.sleep(10)  # hindari quota exceeded
        except gspread.exceptions.WorksheetNotFound:
            print(f'  [MISS] {site} (tab: {tab_name})')

    compute_mpp_categories(sm26, mpp_raw)
    all_mpp, top20 = build_mpp_tables(mpp_raw, months)
    insight = build_insight_data(sm26, sm25, SITES_26, months, partial_months)

    print('\n🔍 Verifikasi:')
    verify(sm26)

    print('\n✏️  Updating HTML...')
    update_html(sm26, sm25, all_mpp, top20, insight, months, partial_months)

if __name__ == '__main__':
    main()
