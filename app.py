# -*- coding: utf-8 -*-
"""
================================================================================
 온라인팀 미니 ERP & 매출 분석 대시보드
================================================================================
회사 ERP '매출 로우데이터'(엑셀/CSV)를 DB에 누적 적재하고, 팀원이 브라우저에서
연차·아이템·시즌·브랜드별 전년비교(대표님 보고 프레임)를 조회하는 팀 전용 미니 ERP.

데이터 저장소는 두 가지를 자동 지원한다.
  · Streamlit secrets에 [postgres] 가 있으면  → Supabase(Postgres)  (배포용, 영속)
  · 없으면                                     → SQLite 파일          (로컬 개발용)
================================================================================
"""

import io
import os
import hashlib
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

DB_PATH = "sales_data.db"
TABLE = "sales"
ROW_KEY = "_row_key"

# ==============================================================================
# SECTION A. ETL
# ==============================================================================
BRAND_MAP = {
    "0": "ZERO LOUNGE", "9": "QUIKSILVER", "A": "CODI GALLERY", "C": "이월",
    "D": "DIEMS", "J": "GENTLEMENS PHILOSOPHY", "L": "GENDERLESS", "M": "맞춤",
    "N": "NORATED", "P": "PAUL&LOUIS", "R": "ROOM,ET", "S": "STCO",
    "T": "MARKET_TEST", "U": "UMEORA",
}
BRAND_CODE_MAP = dict(BRAND_MAP); BRAND_CODE_MAP["C"] = "이월"

ITEM_MAP = {
    "PA": ("바지-바지", "팬츠", "이너웨어"), "DM": ("바지-청바지", "팬츠", "이너웨어"),
    "GP": ("골프-바지", "팬츠", "이너웨어"), "WP": ("RENEW WORK 팬츠", "팬츠", "이너웨어"),
    "HP": ("바지-하프팬츠", "팬츠", "이너웨어"),
    "DS": ("셔츠-드레스셔츠", "셔츠류", "이너웨어"), "WD": ("RENEW WORK 셔츠", "셔츠류", "이너웨어"),
    "KT": ("스웨터-니트", "니트/티셔츠류", "이너웨어"), "TS": ("티셔츠-티셔츠", "니트/티셔츠류", "이너웨어"),
    "KG": ("스웨터-가디건", "니트/티셔츠류", "이너웨어"), "KV": ("스웨터-조끼", "니트/티셔츠류", "이너웨어"),
    "IT": ("이너웨어 티셔츠", "니트/티셔츠류", "이너웨어"), "GT": ("골프-티셔츠", "니트/티셔츠류", "이너웨어"),
    "GK": ("골프-스웨터니트", "니트/티셔츠류", "이너웨어"), "WI": ("RENEW WORK 스웨터", "니트/티셔츠류", "이너웨어"),
    "WS": ("RENEW WORK 티셔츠", "니트/티셔츠류", "이너웨어"),
    "CT": ("코트-코트", "아우터", "아우터"), "JP": ("점퍼-점퍼", "아우터", "아우터"),
    "JA": ("자켓-자켓", "아우터", "아우터"), "DJ": ("데님 점퍼", "아우터", "아우터"),
    "WO": ("RENEW WORK 코트", "아우터", "아우터"), "PV": ("베스트-패딩 베스트", "아우터", "아우터"),
    "WJ": ("RENEW WORK 점퍼", "아우터", "아우터"), "WK": ("RENEW WORK 자켓", "아우터", "아우터"),
    "GE": ("골프-패딩베스트", "아우터", "아우터"), "GJ": ("점퍼-기타", "아우터", "아우터"),
    "SJ": ("정장-수트상의", "수트류", "수트류"), "SL": ("정장-수트하의", "수트류", "수트류"),
    "SP": ("정장-단품정장", "수트류", "수트류"),
    "EJ": ("세트-셋업 자켓", "수트류", "수트류"), "EP": ("세트-셋업 팬츠", "수트류", "수트류"),
    "JV": ("베스트-우븐조끼", "수트류", "수트류"),
    "LJ": ("점퍼-가죽점퍼", "아우터", "아우터"), "TR": ("팬티", "이너웨어", "이너웨어"),
    "WA": ("지갑", "ACC", "액세서리"), "HA": ("모자", "ACC", "액세서리"),
    "FW": ("신발-신발", "신발", "슈즈"),
    "NT": ("넥타이", "ACC", "액세서리"), "BE": ("벨트-벨트", "ACC", "액세서리"),
    "BA": ("가방-가방", "ACC", "액세서리"), "MF": ("머플러", "ACC", "액세서리"),
    "SC": ("양말", "ACC", "액세서리"), "GL": ("장갑", "ACC", "액세서리"),
    "MU": ("머플러", "ACC", "액세서리"),
}
YEAR_MAP = {"O": 2017, "P": 2018, "Q": 2019, "R": 2020, "S": 2021, "T": 2022,
            "U": 2023, "V": 2024, "W": 2025, "X": 2026, "Y": 2027, "Z": 2028}
SEASON_MAP = {"A": "봄", "B": "여름", "C": "가을", "D": "겨울", "E": "RUNNING", "Z": "공통"}
SEASON_GROUP = {"봄": "S/S", "여름": "S/S", "가을": "F/W", "겨울": "F/W",
                "공통": "상시/ACC", "RUNNING": "상시/ACC"}

_ITEMGROUP_RAW = {
    "수트류": ["SJ", "SL", "EJ", "EP", "JV", "SP"],
    "셔츠":   ["DS", "WD"],
    "팬츠":   ["PA", "HP", "DM", "GP", "WP"],
    "아우터": ["DJ", "JA", "JP", "CT", "WJ", "GJ", "WO", "PV", "GE", "LJ"],
    "니트류": ["KT", "GK", "KG", "KV", "WK", "WI"],
    "티셔츠": ["TS", "GT", "WS", "IT"],
    "신발":   ["FW"], "넥타이": ["NT"], "벨트": ["BE"], "양말": ["SC"],
    "ACC":    ["BA", "WA", "HA", "MF", "GL", "MU", "TR"],
}
ITEMGROUP_MAP = {c: g for g, codes in _ITEMGROUP_RAW.items() for c in codes}
ITEMGROUP_ORDER = ["수트류", "아우터", "셔츠", "팬츠", "니트류", "티셔츠",
                   "신발", "넥타이", "벨트", "양말", "ACC", "기타"]

NUMERIC_COLS = ["매장수수료율", "할인율", "최초가", "현판가", "판매수량",
                "최초판매금액", "현판매금액", "실판매금액", "공급금액", "판가율",
                "원가(VAT+)", "판매원가(실판가)", "배수(실판가)", "계",
                "SKT", "상품권", "사용포인트", "마일리지상품권", "임의할인"]
REVENUE_CANDIDATES = ["실판매금액", "현판매금액", "최초판매금액"]
QTY_COL = "판매수량"


def year_age_label(sale_year, product_year):
    try:
        n = int(sale_year) - int(product_year)
    except (TypeError, ValueError):
        return None
    if n <= -1:
        return "내년신상"
    if n == 0:
        return "신상"
    return f"{n}년차"


def _make_columns_unique(cols):
    seen, out = {}, []
    for c in cols:
        c = str(c).strip()
        if c in seen:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 1
            out.append(c)
    return out


def _find_header_row(raw):
    for i in range(min(10, len(raw))):
        vals = [str(v).strip() for v in raw.iloc[i].tolist()]
        if "판매일자" in vals and "품번" in vals:
            return i
    return 0


def read_raw_file(uploaded_file):
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
    else:
        raw = pd.read_excel(uploaded_file, header=None, dtype=str)
    hrow = _find_header_row(raw)
    header = _make_columns_unique(raw.iloc[hrow].tolist())
    df = raw.iloc[hrow + 1:].copy()
    df.columns = header
    df = df.reset_index(drop=True).dropna(how="all")
    df = df[~df.apply(lambda r: all((str(v).strip() == "" or str(v) == "nan") for v in r), axis=1)]
    return df


def _brand_name(col_val, code):
    if col_val and str(col_val).strip() in BRAND_MAP:
        return BRAND_MAP[str(col_val).strip()]
    if code:
        return BRAND_CODE_MAP.get(str(code)[0].upper())
    return None


def decode_stco(code, cols=None):
    code = str(code).strip().upper()
    cols = cols or {}
    res = {"브랜드명": None, "아이템명": None, "중카테고리": None, "대카테고리": None,
           "연도": None, "시즌명": None, "시즌그룹": None, "순번": None}
    res["브랜드명"] = _brand_name(cols.get("브랜드"), code)
    item_code = str(cols.get("아이템") or (code[1:3] if len(code) >= 3 else "")).strip().upper()
    item = ITEM_MAP.get(item_code)
    if item:
        res["아이템명"], res["중카테고리"], res["대카테고리"] = item
    year_code = str(cols.get("년도") or (code[3] if len(code) >= 4 else "")).strip().upper()
    res["연도"] = YEAR_MAP.get(year_code)
    season_code = str(cols.get("시즌") or (code[4] if len(code) >= 5 else "")).strip().upper()
    season = SEASON_MAP.get(season_code)
    res["시즌명"] = season
    res["시즌그룹"] = SEASON_GROUP.get(season)
    res["순번"] = str(cols.get("순번") or (code[5:7] if len(code) >= 7 else "")).strip()
    return res


def enrich(df):
    df = df.copy()
    for col in NUMERIC_COLS:
        if col in df.columns:
            s = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            df[col] = pd.to_numeric(s, errors="coerce")
    if "품번" in df.columns:
        def _decode(row):
            cols = {k: row.get(k) for k in ["브랜드", "아이템", "년도", "시즌", "순번"]}
            return decode_stco(row.get("품번", ""), cols)
        decoded = df.apply(_decode, axis=1).apply(pd.Series)
        for c in decoded.columns:
            df[c] = decoded[c]
    if "판매일자" in df.columns:
        dt = pd.to_datetime(df["판매일자"], errors="coerce")
        df["_판매일"] = dt
        df["판매연도"] = dt.dt.year
        df["년월"] = dt.dt.strftime("%Y-%m")
        df["주차"] = dt.dt.strftime("%G-W%V")
    if "아이템" in df.columns:
        item_series = df["아이템"].astype(str).str.strip().str.upper()
    elif "품번" in df.columns:
        item_series = df["품번"].astype(str).str.strip().str.upper().str[1:3]
    else:
        item_series = None
    if item_series is not None:
        df["아이템그룹"] = item_series.map(ITEMGROUP_MAP).fillna("기타")
    if "판매연도" in df.columns and "연도" in df.columns:
        df["연차"] = [year_age_label(sy, py) for sy, py in zip(df["판매연도"], df["연도"])]
    rev = next((c for c in REVENUE_CANDIDATES if c in df.columns), None)
    df["_매출액"] = df[rev] if rev else 0
    df["_최초가매출"] = df["최초판매금액"] if "최초판매금액" in df.columns else 0
    df["_수량"] = pd.to_numeric(df["판매수량"], errors="coerce") if "판매수량" in df.columns else 0
    df["_채널"] = df["매장명"] if "매장명" in df.columns else df.get("매장코드", "기타")
    return df


def add_row_key(df):
    df = df.copy()
    key_cols = [c for c in ["판매일자", "매장코드", "판매번호", "판매연번", "품번"] if c in df.columns]

    def _key(row):
        base = "|".join(str(row.get(c, "")) for c in key_cols)
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    df[ROW_KEY] = df.apply(_key, axis=1)
    return df


# ==============================================================================
# SECTION B. DATABASE (SQLAlchemy: Postgres/SQLite 공용)
# ==============================================================================
@st.cache_resource
def get_engine():
    try:
        pg = st.secrets.get("postgres", None)
    except Exception:
        pg = None
    if pg:
        url = (f"postgresql+psycopg2://{pg['user']}:{quote_plus(str(pg['password']))}"
               f"@{pg['host']}:{pg.get('port',5432)}/{pg.get('dbname','postgres')}?sslmode=require")
        return create_engine(url, pool_pre_ping=True, pool_recycle=300)
    return create_engine(f"sqlite:///{DB_PATH}")


def backend_name():
    try:
        return "Supabase(Postgres)" if st.secrets.get("postgres", None) else "SQLite(로컬)"
    except Exception:
        return "SQLite(로컬)"


def _table_columns(conn):
    insp = conn.exec_driver_sql(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s"
        if conn.engine.dialect.name == "postgresql" else
        f'PRAGMA table_info("{TABLE}")',
        (TABLE,) if conn.engine.dialect.name == "postgresql" else ()
    )
    rows = insp.fetchall()
    if conn.engine.dialect.name == "postgresql":
        return [r[0] for r in rows]
    return [r[1] for r in rows]


def ensure_table(conn, df):
    cols = _table_columns(conn)
    q = '"'
    if not cols:
        defs = ", ".join([f'{q}{c}{q} TEXT' for c in df.columns if c != ROW_KEY])
        conn.exec_driver_sql(f'CREATE TABLE {q}{TABLE}{q} ({q}{ROW_KEY}{q} TEXT PRIMARY KEY, {defs})')
        return
    for c in df.columns:
        if c not in cols:
            conn.exec_driver_sql(f'ALTER TABLE {q}{TABLE}{q} ADD COLUMN {q}{c}{q} TEXT')


def append_to_db(df):
    save = [c for c in df.columns if not c.startswith("_") or c == ROW_KEY]
    df = df[save].astype(object).where(pd.notnull(df[save]), None)
    eng = get_engine()
    with eng.begin() as conn:
        ensure_table(conn, df)
        before = conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{TABLE}"').scalar()
        existing = set(r[0] for r in conn.exec_driver_sql(f'SELECT "{ROW_KEY}" FROM "{TABLE}"').fetchall())
        new = df[~df[ROW_KEY].isin(existing)].copy()
        if len(new):
            new = new.astype(str).where(pd.notnull(new), None)
            chunk = max(1, 30000 // max(1, len(new.columns)))
            new.to_sql(TABLE, conn, if_exists="append", index=False, method="multi", chunksize=chunk)
        after = before + len(new)
    return {"inserted": len(new), "skipped": len(df) - len(new), "total_after": after}


@st.cache_data(ttl=120)
def load_db():
    eng = get_engine()
    try:
        with eng.connect() as conn:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.tables WHERE table_name=%s"
                if eng.dialect.name == "postgresql" else
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (TABLE,)).fetchone()
            if not exists:
                return pd.DataFrame()
            df = pd.read_sql(f'SELECT * FROM "{TABLE}"', conn)
    except Exception:
        return pd.DataFrame()
    for col in NUMERIC_COLS + ["판매연도", "연도"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
    rev = next((c for c in REVENUE_CANDIDATES if c in df.columns), None)
    df["_매출액"] = pd.to_numeric(df[rev].astype(str).str.replace(",", "", regex=False), errors="coerce") if rev else 0
    df["_최초가매출"] = pd.to_numeric(df["최초판매금액"].astype(str).str.replace(",", "", regex=False), errors="coerce") if "최초판매금액" in df.columns else 0
    df["_수량"] = pd.to_numeric(df[QTY_COL].astype(str).str.replace(",", "", regex=False), errors="coerce") if QTY_COL in df.columns else 0
    df["_채널"] = df["매장명"] if "매장명" in df.columns else df.get("매장코드", "기타")
    if "판매일자" in df.columns:
        df["_판매일"] = pd.to_datetime(df["판매일자"], errors="coerce")
    if "아이템" in df.columns:
        df["아이템그룹"] = df["아이템"].astype(str).str.strip().str.upper().map(ITEMGROUP_MAP).fillna("기타")
    if "판매연도" in df.columns and "연도" in df.columns:
        df["연차"] = [year_age_label(sy, py) for sy, py in zip(df["판매연도"], df["연도"])]
    return df


def db_row_count():
    try:
        with get_engine().connect() as conn:
            return conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{TABLE}"').scalar()
    except Exception:
        return 0


# ==============================================================================
# SECTION C. ANALYSIS UI
# ==============================================================================
def _won(n):
    try:
        return f"{int(round(float(n))):,} 원"
    except Exception:
        return "-"


def _mm(v):
    try:
        return float(v) / 1e6
    except Exception:
        return 0.0


AGE_RANK = {"내년신상": -1, "신상": 0}


def _age_sort_key(a):
    if a in AGE_RANK:
        return AGE_RANK[a]
    try:
        return int(str(a).replace("년차", ""))
    except Exception:
        return 99


def yoy_frame(cur, prev, dim, order_list=None):
    def agg(f):
        if f is None or f.empty:
            return pd.DataFrame(columns=[dim, "rev", "orig", "qty"]).set_index(dim)
        return f.groupby(dim).agg(rev=("_매출액", "sum"), orig=("_최초가매출", "sum"),
                                  qty=("_수량", "sum"))
    c, p = agg(cur), agg(prev)
    keys = list(dict.fromkeys(list(c.index) + list(p.index)))
    if order_list:
        keys = [k for k in order_list if k in keys] + [k for k in keys if k not in order_list]
    else:
        keys = sorted(keys, key=lambda k: -float(c["rev"].get(k, 0)))
    tot_c, tot_p = float(c["rev"].sum()), float(p["rev"].sum())

    def metrics(r26, r25, o26, o25, q26, q25, den_c, den_p):
        return {
            ("실판매금액(백만)", "25년"): r25 / 1e6, ("실판매금액(백만)", "26년"): r26 / 1e6,
            ("실판매금액(백만)", "증감율"): ((r26 - r25) / r25) if r25 else None,
            ("판가율", "25년"): (r25 / o25) if o25 else 0, ("판가율", "26년"): (r26 / o26) if o26 else 0,
            ("판가율", "증감"): ((r26 / o26 if o26 else 0) - (r25 / o25 if o25 else 0)),
            ("비중", "25년"): (r25 / den_p) if den_p else 0, ("비중", "26년"): (r26 / den_c) if den_c else 0,
            ("비중", "증감"): ((r26 / den_c if den_c else 0) - (r25 / den_p if den_p else 0)),
            ("평균단가(원)", "25년"): (r25 / q25) if q25 else 0, ("평균단가(원)", "26년"): (r26 / q26) if q26 else 0,
            ("평균단가(원)", "증감"): ((r26 / q26 if q26 else 0) - (r25 / q25 if q25 else 0)),
        }

    rows, index = [], []
    rows.append(metrics(tot_c, tot_p, float(c["orig"].sum()), float(p["orig"].sum()),
                        float(c["qty"].sum()), float(p["qty"].sum()), tot_c, tot_p))
    index.append("G.TOTAL")
    for k in keys:
        rows.append(metrics(float(c["rev"].get(k, 0)), float(p["rev"].get(k, 0)),
                            float(c["orig"].get(k, 0)), float(p["orig"].get(k, 0)),
                            float(c["qty"].get(k, 0)), float(p["qty"].get(k, 0)), tot_c, tot_p))
        index.append(k)
    D = pd.DataFrame(rows, index=index)
    D.columns = pd.MultiIndex.from_tuples(D.columns)
    D.index.name = dim
    return D


def _fmt_cell(col, v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "–"
    top, sub = col
    if top == "실판매금액(백만)":
        return f"{v:,.0f}" if sub != "증감율" else f"{v*100:+.0f}%"
    if top == "판가율":
        return f"{v*100:.0f}%" if sub != "증감" else f"{v*100:+.1f}%p"
    if top == "비중":
        return f"{v*100:.1f}%" if sub != "증감" else f"{v*100:+.1f}%p"
    if top == "평균단가(원)":
        return f"{v:,.0f}" if sub != "증감" else f"{v:+,.0f}"
    return v


def style_yoy(D):
    disp = D.copy()
    for col in disp.columns:
        disp[col] = [_fmt_cell(col, v) for v in disp[col]]
    delta_cols = [c for c in D.columns if c[1] in ("증감율", "증감")]

    def color(col):
        vals = D[col]
        return ["color:#c62828;font-weight:600" if (pd.notnull(v) and v < 0)
                else ("color:#1f8a4c;font-weight:600" if pd.notnull(v) and v > 0 else "")
                for v in vals]
    sty = disp.style
    for col in delta_cols:
        sty = sty.apply(lambda s, c=col: color(c), subset=pd.IndexSlice[:, [col]])
    sty = sty.set_properties(**{"text-align": "right"})
    return sty


def yoy_excel_bytes(D, sheet="분석"):
    disp = D.copy()
    for col in disp.columns:
        disp[col] = [_fmt_cell(col, v) for v in disp[col]]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        disp.to_excel(w, sheet_name=sheet)
    return buf.getvalue()


def perf_table(cur, prev, dim, order_list, title, key):
    D = yoy_frame(cur, prev, dim, order_list)
    h1, h2 = st.columns([4, 1])
    h1.markdown(f"**{title}**")
    h2.download_button("⬇ 엑셀", yoy_excel_bytes(D, title[:28]),
                       file_name=f"{title[:24]}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key=f"dl_{key}", use_container_width=True)
    st.dataframe(style_yoy(D), use_container_width=True)


def render_flagship(df):
    st.subheader("📅 연차 · 아이템별 전년 대비 분석")
    if df.empty or "_판매일" not in df.columns or df["_판매일"].notna().sum() == 0:
        st.info("데이터를 먼저 적재하세요.")
        return
    d = df[df["_판매일"].notna()].copy()
    years = sorted(d["_판매일"].dt.year.dropna().astype(int).unique(), reverse=True)
    st.caption("올해 vs 전년 '동기간'(같은 날짜범위) 비교 · 금액 단위 백만원 · 판가율=실판가÷최초가(가중)")
    f1, f2, f3, f4 = st.columns([1, 1.7, 1.2, 1.2])
    with f1:
        cy = st.selectbox("기준연도", years, index=0)
    cur_all = d[d["_판매일"].dt.year == cy]
    dmin, dmax = cur_all["_판매일"].min().date(), cur_all["_판매일"].max().date()
    with f2:
        rng = st.date_input(f"기준기간 (전년 {cy-1} 동기간 자동)", value=(dmin, dmax),
                            min_value=d["_판매일"].min().date(), max_value=d["_판매일"].max().date())
    with f3:
        brands = sorted([b for b in d["브랜드명"].dropna().unique()]) if "브랜드명" in d.columns else []
        selb = st.multiselect("브랜드", brands, default=brands)
    with f4:
        seasons = sorted([s for s in d["시즌명"].dropna().unique()]) if "시즌명" in d.columns else []
        sels = st.multiselect("시즌", seasons, default=seasons)
    chans = sorted([c for c in d["_채널"].dropna().unique()]) if "_채널" in d.columns else []
    selc = st.multiselect("매장/채널", chans, default=chans)
    if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
        st.info("기간(시작~끝)을 선택하세요.")
        return
    s, e = pd.to_datetime(rng[0]), pd.to_datetime(rng[1])
    base = d.copy()
    if selb and "브랜드명" in base:
        base = base[base["브랜드명"].isin(selb)]
    if sels and "시즌명" in base:
        base = base[base["시즌명"].isin(sels)]
    if selc and "_채널" in base:
        base = base[base["_채널"].isin(selc)]
    cur = base[(base["_판매일"] >= s) & (base["_판매일"] <= e)]
    prev = base[(base["_판매일"] >= s - pd.DateOffset(years=1)) & (base["_판매일"] <= e - pd.DateOffset(years=1))]
    tot_c = cur["_매출액"].sum()
    tot_p = prev["_매출액"].sum()
    k1, k2, k3 = st.columns(3)
    k1.metric(f"{cy} 매출(백만)", f"{_mm(tot_c):,.0f}")
    k2.metric(f"{cy-1} 매출(백만)", f"{_mm(tot_p):,.0f}")
    g = ((tot_c - tot_p) / tot_p) if tot_p else None
    k3.metric("전년비 성장률", "신규/–" if g is None else f"{g*100:+.1f}%")
    if not tot_p:
        st.warning(f"전년({cy-1}) 동기간 데이터가 없어요. {cy-1}년 로우데이터를 적재하면 채워집니다.")
    age_order = sorted([a for a in base["연차"].dropna().unique()], key=_age_sort_key)
    st.markdown("### 연차별 성과표")
    perf_table(cur, prev, "연차", age_order, "연차별 성과표", "age")
    st.markdown("### 아이템그룹별 성과표 (전연차 토탈 + 연차별)")
    perf_table(cur, prev, "아이템그룹", ITEMGROUP_ORDER, "아이템그룹별 성과표 (전연차)", "grp_all")
    buckets = []
    sinsang = [a for a in ["신상", "내년신상"] if a in age_order]
    if sinsang:
        buckets.append(("신상+내년신상", sinsang))
    for a in age_order:
        if a.endswith("년차"):
            buckets.append((a, [a]))
    for name, ages in buckets:
        curb = cur[cur["연차"].isin(ages)]
        prevb = prev[prev["연차"].isin(ages)]
        perf_table(curb, prevb, "아이템그룹", ITEMGROUP_ORDER,
                   f"아이템그룹별 성과표 ({name})", f"grp_{name}")


def render_dashboard(q, df):
    if q is None or q.empty:
        st.warning("선택한 조건에 데이터가 없습니다.")
        return
    rev = q["_매출액"].sum(); qty = q["_수량"].sum()
    orig = q["_최초가매출"].sum() if "_최초가매출" in q else 0
    k = st.columns(5)
    k[0].metric("총 매출액(실판가)", _won(rev))
    k[1].metric("총 판매수량", f"{int(qty):,} 개")
    k[2].metric("평균 단가", _won(rev / qty) if qty else "-")
    k[3].metric("판가율", f"{rev/orig*100:.1f}%" if orig else "-")
    k[4].metric("거래 건수", f"{len(q):,} 건")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**월별 매출 추이**")
        if "년월" in q:
            t = q.groupby("년월", as_index=False)["_매출액"].sum().sort_values("년월")
            fig = px.line(t, x="년월", y="_매출액", markers=True, labels={"_매출액": "매출액"})
            fig.update_layout(height=320, margin=dict(t=10, b=0)); st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.markdown("**아이템그룹별 매출 비중**")
        if "아이템그룹" in q:
            comp = q.groupby("아이템그룹", as_index=False)["_매출액"].sum().sort_values("_매출액", ascending=False)
            fig = px.pie(comp, names="아이템그룹", values="_매출액", hole=0.5)
            fig.update_layout(height=320, margin=dict(t=10, b=0)); st.plotly_chart(fig, use_container_width=True)
    st.markdown("**채널별 매출 TOP 10**")
    if "_채널" in q:
        ch = q.groupby("_채널", as_index=False)["_매출액"].sum().sort_values("_매출액", ascending=False).head(10)
        fig = px.bar(ch, x="_매출액", y="_채널", orientation="h", labels={"_매출액": "매출액", "_채널": "채널"})
        fig.update_layout(height=340, margin=dict(t=10, b=0), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)


def main():
    st.set_page_config(page_title="온라인팀 미니 ERP", page_icon="📊", layout="wide")
    st.title("📊 온라인팀 미니 ERP · 매출 분석")
    with st.sidebar:
        st.header("⚙️ 데이터 관리")
        st.caption(f"저장소: **{backend_name()}**")
        ups = st.file_uploader("① 로우데이터 업로드 (여러 개 한 번에 가능)",
                               type=["xlsx", "xls", "csv"], accept_multiple_files=True)
        if ups:
            st.caption(f"{len(ups)}개 파일 선택됨")
            if st.button("② DB에 적재하기", type="primary", use_container_width=True):
                tn = ts = 0; last = 0
                prog = st.progress(0.0)
                for i, f in enumerate(ups):
                    try:
                        clean = add_row_key(enrich(read_raw_file(f)))
                        res = append_to_db(clean)
                        tn += res["inserted"]; ts += res["skipped"]; last = res["total_after"]
                    except Exception as ex:
                        st.error(f"{f.name} 오류: {ex}")
                    prog.progress((i + 1) / len(ups))
                load_db.clear()
                st.success(f"적재 완료 ✅ 신규 {tn:,} / 중복 {ts:,} · DB 총 {last:,}건")
        st.divider()
        st.metric("현재 DB 누적", f"{db_row_count():,} 건")
        if st.button("🔄 새로고침(캐시 비우기)", use_container_width=True):
            load_db.clear(); st.rerun()
    df = load_db()
    if df.empty:
        st.info("👈 사이드바에서 매출 로우데이터를 업로드하고 [DB에 적재하기]를 눌러 시작하세요.")
        return
    tab1, tab2 = st.tabs(["📅 연차·아이템 세부분석 (플래그십)", "📊 종합 대시보드"])
    with tab1:
        render_flagship(df)
    with tab2:
        render_dashboard(df, df)


if __name__ == "__main__":
    main()
