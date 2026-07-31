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

  [A] ETL      : 로우데이터 정제 + STCO 품번코드 해독
  [B] DATABASE : SQLAlchemy 누적적재 + 스키마 자동확장 + 중복방지 (Postgres/SQLite 공용)
  [C] ANALYSIS : 종합 대시보드 + 플래그십(연차·아이템별 전년비교)

공통룰(2026-07-31 추가 · 룰11): 모든 조회 표에는 엑셀 다운로드 버튼을 기본 제공한다.
  · 예외: 주간회의 보고자료 '메인 표'는 팀 주간보고 양식(weekly_template.xlsx)
    특별 템플릿 다운로드를 그대로 유지(변경 금지).

실행:  streamlit run app.py
================================================================================
"""

import io
import os
import gc
import hmac
import hashlib
from datetime import datetime
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from sqlalchemy import create_engine, text

DB_PATH = "sales_data.db"
TABLE = "sales"
ROW_KEY = "_row_key"

# ==============================================================================
# SECTION A. ETL  ─ 로우데이터 정제 + STCO 품번코드 해독
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

# 아이템 → 아이템그룹 (구분자 기준 + 팀 요청: ACC에서 신발·넥타이·벨트·양말 분리)
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
    """연차: (기준=판매연도) − 상품년도. -1↓=내년신상, 0=신상, 1↑=N년차."""
    try:
        n = int(sale_year) - int(product_year)
    except (TypeError, ValueError):
        return None
    if n <= -1:
        return "내년신상"
    if n == 0:
        return "신상"
    return f"{n}년차"


def year_age_series(sale_year, product_year):
    """year_age_label의 벡터화 버전. 대용량에서 행별 호출 대신 사용(결과 동일).

    비교는 numpy float(NaN→False)로 수행하고, 유효하지 않은 행은 마지막에 None 처리.
    """
    sy = pd.to_numeric(sale_year, errors="coerce")
    py = pd.to_numeric(product_year, errors="coerce")
    n = sy - py
    nyoncha = (n.astype("Int64").astype(str) + "년차").to_numpy()   # "1년차" … (결측은 뒤에서 마스킹)
    nf = n.to_numpy(dtype="float64")
    lab = np.where(nf <= -1, "내년신상",
                   np.where(nf == 0, "신상", nyoncha))
    out = pd.Series(lab, index=sy.index, dtype="object")
    return out.where(sy.notna() & py.notna(), None)


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


# ── 보안: 고객 개인정보 컬럼 차단 ─────────────────────────────────────
# 고객코드 등 '고객' 관련 컬럼은 개인정보라 DB 적재·외부반출 금지.
# 로우데이터를 읽는 즉시 제거하여, 실수로 포함돼 올라와도 저장소·화면에 절대 남지 않게 한다.
PII_KEYWORDS = ("고객",)


def _drop_pii_cols(df):
    """컬럼명에 PII 키워드('고객' 등)가 들어간 컬럼을 모두 제거."""
    pii = [c for c in df.columns if any(k in str(c) for k in PII_KEYWORDS)]
    if pii:
        df = df.drop(columns=pii)
    return df


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
    # ERP 다운로드 맨 아래 '전체 합계' 행 제거 — 날짜·품번 없이 숫자만 있는 행은 실제 거래가 아님
    if "판매일자" in df.columns:
        dd = df["판매일자"].astype(str).str.strip().str.lower()
        df = df[dd.ne("") & ~dd.isin(["nan", "none", "nat"])]
    df = _drop_pii_cols(df)   # 보안: 고객코드 등 개인정보 컬럼은 읽는 즉시 제거
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


def _col_or_code(df, colname, code, idx):
    """해당 컬럼값이 있으면 그것을, 비었거나 컬럼이 없으면 품번코드의 자리(idx)를 사용(벡터화)."""
    code_part = code.str[idx]
    if colname in df.columns:
        s = df[colname].astype(str).str.strip().str.upper()
        return s.where(s.ne("") & s.ne("NAN") & s.ne("NONE"), code_part)
    return code_part


def enrich(df):
    """로우데이터 정제 + STCO 품번코드 해독 (벡터화 · 대용량 메모리 최적화)."""
    for col in NUMERIC_COLS:
        if col in df.columns:
            s = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            df[col] = pd.to_numeric(s, errors="coerce")

    if "품번" in df.columns:
        code = df["품번"].astype(str).str.strip().str.upper()
        if "브랜드" in df.columns:
            bname = df["브랜드"].astype(str).str.strip().map(BRAND_MAP)
        else:
            bname = pd.Series(np.nan, index=df.index, dtype="object")
        df["브랜드명"] = bname.fillna(code.str[0].map(BRAND_CODE_MAP))
        ic = _col_or_code(df, "아이템", code, slice(1, 3))
        df["아이템명"] = ic.map({k: v[0] for k, v in ITEM_MAP.items()})
        df["중카테고리"] = ic.map({k: v[1] for k, v in ITEM_MAP.items()})
        df["대카테고리"] = ic.map({k: v[2] for k, v in ITEM_MAP.items()})
        df["연도"] = _col_or_code(df, "년도", code, 3).map(YEAR_MAP)
        season = _col_or_code(df, "시즌", code, 4).map(SEASON_MAP)
        df["시즌명"] = season
        df["시즌그룹"] = season.map(SEASON_GROUP)
        df["순번"] = _col_or_code(df, "순번", code, slice(5, 7))
        df["_아이템코드"] = ic

    if "판매일자" in df.columns:
        dt = pd.to_datetime(df["판매일자"], errors="coerce")
        df["_판매일"] = dt
        df["판매연도"] = dt.dt.year
        df["년월"] = dt.dt.strftime("%Y-%m")
        df["주차"] = dt.dt.strftime("%G-W%V")

    if "_아이템코드" in df.columns:
        item_code = df["_아이템코드"]
    elif "아이템" in df.columns:
        item_code = df["아이템"].astype(str).str.strip().str.upper()
    elif "품번" in df.columns:
        item_code = df["품번"].astype(str).str.strip().str.upper().str[1:3]
    else:
        item_code = None
    if item_code is not None:
        df["아이템그룹"] = item_code.map(ITEMGROUP_MAP).fillna("기타")

    if "판매연도" in df.columns and "연도" in df.columns:
        df["연차"] = year_age_series(df["판매연도"], df["연도"])

    rev = next((c for c in REVENUE_CANDIDATES if c in df.columns), None)
    df["_매출액"] = df[rev] if rev else 0
    df["_최초가매출"] = df["최초판매금액"] if "최초판매금액" in df.columns else 0
    df["_수량"] = pd.to_numeric(df["판매수량"], errors="coerce") if "판매수량" in df.columns else 0
    df["_채널"] = df["매장명"] if "매장명" in df.columns else df.get("매장코드", "기타")
    if "_아이템코드" in df.columns:
        df.drop(columns=["_아이템코드"], inplace=True)
    return df


def add_row_key(df):
    """중복 방지용 행 키(md5) 생성 — 벡터화로 문자열 결합 후 해시.

    결측은 "nan"으로 통일(기존 apply 방식의 str(nan)과 동일)하여, 이미 적재된
    DB의 키와 완전히 같은 값을 생성한다(누적/중복방지 호환).
    """
    key_cols = [c for c in ["판매일자", "매장코드", "판매번호", "판매연번", "품번"] if c in df.columns]
    if not key_cols:
        df[ROW_KEY] = [hashlib.md5(str(i).encode()).hexdigest() for i in range(len(df))]
        return df

    def _col_str(c):
        return df[c].astype("string").fillna("nan").astype(str)

    base = _col_str(key_cols[0])
    for c in key_cols[1:]:
        base = base.str.cat(_col_str(c), sep="|")
    df[ROW_KEY] = [hashlib.md5(s.encode("utf-8")).hexdigest() for s in base]
    return df


# ==============================================================================
# SECTION B. DATABASE  ─ SQLAlchemy (Postgres/SQLite 공용)
# ==============================================================================
@st.cache_resource
def get_engine():
    """secrets에 [postgres] 있으면 Supabase, 없으면 SQLite."""
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
    """정제·키 생성된 df를 누적 적재. 중복(ROW_KEY)은 건너뜀. 파생(_) 컬럼 제외.

    메모리 최적화: 저장 대상 컬럼만 추린 뒤 파일 내 중복부터 제거하고,
    실제 신규 행에 대해서만 문자열화/적재를 수행한다.
    """
    save = [c for c in df.columns
            if (not c.startswith("_") or c == ROW_KEY)
            and not any(k in str(c) for k in PII_KEYWORDS)]   # 보안: 고객 개인정보 컬럼 적재 제외
    out = df[save].drop_duplicates(subset=[ROW_KEY])
    eng = get_engine()
    with eng.begin() as conn:
        ensure_table(conn, out)
        before = conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{TABLE}"').scalar()
        existing = set(r[0] for r in conn.exec_driver_sql(f'SELECT "{ROW_KEY}" FROM "{TABLE}"').fetchall())
        new = out[~out[ROW_KEY].isin(existing)]
        n_new = len(new)
        if n_new:
            new = new.astype(object).where(new.notna(), None)  # 결측→None (신규 행에만)
            # DB별 파라미터 한도(Postgres 65535 / SQLite 32766) 안전하게: chunk×cols < 30000
            chunk = max(1, 30000 // max(1, len(new.columns)))
            new.to_sql(TABLE, conn, if_exists="append", index=False, method="multi", chunksize=chunk)
        after = before + n_new
    return {"inserted": n_new, "skipped": len(out) - n_new, "total_after": after}


def delete_dates(date_isos):
    """덮어쓰기 모드용: 지정한 날짜(['YYYY-MM-DD', ...])의 기존 행을 sales에서 삭제하고 삭제 건수 반환.

    저장된 '판매일자' 텍스트의 구분자(-, /, .)를 제거해 'YYYYMMDD'로 정규화한 뒤 매칭하므로,
    2026-07-27 / 2026/07/27 / 20260727 / 뒤에 시간이 붙은 형태까지 모두 같은 날로 잡아 삭제한다.
    """
    if not date_isos:
        return 0
    eng = get_engine()
    total = 0
    with eng.begin() as conn:
        exists = conn.exec_driver_sql(
            "SELECT 1 FROM information_schema.tables WHERE table_name=%s"
            if eng.dialect.name == "postgresql" else
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (TABLE,)).fetchone()
        if not exists:
            return 0
        for d in date_isos:
            compact = str(d).replace("-", "") + "%"   # 'YYYYMMDD%'
            r = conn.execute(
                text(f'DELETE FROM "{TABLE}" '
                     f'WHERE replace(replace(replace("판매일자", \'-\', \'\'), \'/\', \'\'), \'.\', \'\') LIKE :c'),
                {"c": compact})
            total += (r.rowcount or 0)
    return total


# 분석 화면이 실제로 쓰는 컬럼만 로드 (49만 행 × 60여 컬럼 전체 로드 시 메모리 초과 → OOM)
LOAD_COLS = ["판매일자", "브랜드명", "시즌명", "시즌그룹", "아이템", "아이템명",
             "연도", "판매연도", "년월", "최초판매금액", "실판매금액", "현판매금액",
             "판매수량", "매장명", "매장코드", "품번"]
LOAD_NUM = ["최초판매금액", "실판매금액", "현판매금액", "판매수량", "판매연도", "연도"]
LOAD_CAT = ["브랜드명", "시즌명", "시즌그룹", "아이템", "아이템명", "년월", "매장명", "매장코드"]


def _existing_columns(conn, eng):
    if eng.dialect.name == "postgresql":
        rows = conn.exec_driver_sql(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s", (TABLE,)).fetchall()
    else:
        rows = conn.exec_driver_sql(f'PRAGMA table_info("{TABLE}")').fetchall()
        return [r[1] for r in rows]
    return [r[0] for r in rows]


@st.cache_data(ttl=120)
def load_db():
    """필요한 컬럼만 청크 단위로 읽어 category/downcast로 적재 (대용량 메모리 최적화)."""
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
            have = _existing_columns(conn, eng)
            use = [c for c in LOAD_COLS if c in have]
            if not use:
                use = have
            q = "SELECT " + ", ".join(f'"{c}"' for c in use) + f' FROM "{TABLE}"'
            parts = []
            for ch in pd.read_sql(q, conn, chunksize=50000):
                for c in LOAD_NUM:
                    if c in ch.columns:
                        ch[c] = pd.to_numeric(
                            ch[c].astype(str).str.replace(",", "", regex=False),
                            errors="coerce", downcast="float")
                for c in LOAD_CAT:
                    if c in ch.columns:
                        ch[c] = ch[c].astype("category")
                parts.append(ch)
            if not parts:
                return pd.DataFrame()
            df = pd.concat(parts, ignore_index=True)
            del parts
            gc.collect()
    except Exception:
        return pd.DataFrame()

    # concat 후 category 재정리(청크별 카테고리 합집합)
    for c in LOAD_CAT:
        if c in df.columns and str(df[c].dtype) != "category":
            df[c] = df[c].astype("category")

    rev = next((c for c in REVENUE_CANDIDATES if c in df.columns), None)
    df["_매출액"] = df[rev] if rev else 0.0
    df["_최초가매출"] = df["최초판매금액"] if "최초판매금액" in df.columns else 0.0
    df["_수량"] = df[QTY_COL] if QTY_COL in df.columns else 0.0
    df["_채널"] = df["매장명"] if "매장명" in df.columns else df.get("매장코드", "기타")
    if "판매일자" in df.columns:
        df["_판매일"] = pd.to_datetime(df["판매일자"], errors="coerce")
        df = df[df["_판매일"].notna()].copy()   # 합계행 등 날짜 없는 행 제외 (대시보드 총액 정합성)
    # 비즈니스 규칙(아이템그룹·연차)은 저장값 대신 항상 최신 기준으로 재계산
    #  → 그룹 정의를 바꿔도 재적재 없이 즉시 반영됨
    if "아이템" in df.columns:
        df["아이템그룹"] = df["아이템"].astype(str).str.strip().str.upper().map(ITEMGROUP_MAP).fillna("기타")
    if "판매연도" in df.columns and "연도" in df.columns:
        df["연차"] = year_age_series(df["판매연도"], df["연도"])
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


# ---- 전년비교 성과표 (연차 / 아이템그룹 공용) ----
GROUPS = [("실판매금액(백만)", "실판매"), ("판가율", "판가율"), ("비중", "비중"), ("평균단가(원)", "평균단가")]

# 시즌 7행 (2026-07-31 중태님 지시 · 연차별 성과표 전용): G.TOTAL 바로 아래.
#  1~2행 = 시즌그룹 합계(S/S=공통+봄+여름 · F/W=가을+겨울), 3~7행 = 개별 시즌 Z→A→B→C→D.
#  E(RUNNING)는 별도 행 없이 G.TOTAL에만 포함. 비중 분모는 전체(G.TOTAL).
SEASON_ROW_DEFS = [
    ("S/S TOTAL", ["공통", "봄", "여름"]),
    ("F/W TOTAL", ["가을", "겨울"]),
    ("Z (공통)", ["공통"]),
    ("A (봄)", ["봄"]),
    ("B (여름)", ["여름"]),
    ("C (가을)", ["가을"]),
    ("D (겨울)", ["겨울"]),
]


def yoy_frame(cur, prev, dim, order_list=None, season_rows=False):
    """올해(cur)·전년(prev)을 dim으로 묶어 전년비교 numeric DataFrame(멀티헤더) 반환. G.TOTAL 상단."""
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

    def metrics(r26, r25, o26, o25, q26, q25, share_den_c, share_den_p):
        return {
            ("실판매금액(백만)", "25년"): r25 / 1e6, ("실판매금액(백만)", "26년"): r26 / 1e6,
            ("실판매금액(백만)", "증감율"): ((r26 - r25) / r25) if r25 else None,
            ("판가율", "25년"): (r25 / o25) if o25 else 0, ("판가율", "26년"): (r26 / o26) if o26 else 0,
            ("판가율", "증감"): ((r26 / o26 if o26 else 0) - (r25 / o25 if o25 else 0)),
            ("비중", "25년"): (r25 / share_den_p) if share_den_p else 0,
            ("비중", "26년"): (r26 / share_den_c) if share_den_c else 0,
            ("비중", "증감"): ((r26 / share_den_c if share_den_c else 0) - (r25 / share_den_p if share_den_p else 0)),
            ("평균단가(원)", "25년"): (r25 / q25) if q25 else 0, ("평균단가(원)", "26년"): (r26 / q26) if q26 else 0,
            ("평균단가(원)", "증감"): ((r26 / q26 if q26 else 0) - (r25 / q25 if q25 else 0)),
        }

    rows, index = [], []
    # G.TOTAL 먼저
    rows.append(metrics(tot_c, tot_p, float(c["orig"].sum()), float(p["orig"].sum()),
                        float(c["qty"].sum()), float(p["qty"].sum()), tot_c, tot_p))
    index.append("G.TOTAL")
    # 시즌 7행 (연차별 성과표 전용) — G.TOTAL 다음, 연차 행들 앞
    if season_rows:
        def _ssum(f, sns):
            if f is None or f.empty or "시즌명" not in f.columns:
                return 0.0, 0.0, 0.0
            sub = f[f["시즌명"].astype(str).isin(sns)]
            return (float(sub["_매출액"].sum()), float(sub["_최초가매출"].sum()),
                    float(sub["_수량"].sum()))
        for lbl, sns in SEASON_ROW_DEFS:
            r26, o26, q26 = _ssum(cur, sns)
            r25, o25, q25 = _ssum(prev, sns)
            rows.append(metrics(r26, r25, o26, o25, q26, q25, tot_c, tot_p))
            index.append(lbl)
    for k in keys:
        rows.append(metrics(float(c["rev"].get(k, 0)), float(p["rev"].get(k, 0)),
                            float(c["orig"].get(k, 0)), float(p["orig"].get(k, 0)),
                            float(c["qty"].get(k, 0)), float(p["qty"].get(k, 0)), tot_c, tot_p))
        index.append(k)
    D = pd.DataFrame(rows, index=index)
    D.columns = pd.MultiIndex.from_tuples(D.columns)
    D.index.name = dim
    return D


def yoy_frame2(cur_m, prev_m, cur_y, prev_y, dim, order_list=None, season_rows=False,
               blk_labels=("당월누계", "연간누계")):
    """플래그십 2블록 프레임 (2026-07-31 목업 v2 컨펌): 당월누계 + 연간누계.

    현재 헤더 12개 컬럼(실판매금액·판가율·비중·평균단가 × 25/26/증감)을 기간별로 복제해
    최상단에 기간 블록(당월누계·연간누계)을 얹는다. 비중은 각 블록 안에서 행÷전체.
    행 순서는 연간누계 기준(당월에만 있는 행은 뒤에 추가, 없는 칸은 '–').
    """
    Dm = yoy_frame(cur_m, prev_m, dim, order_list, season_rows=season_rows)
    Dy = yoy_frame(cur_y, prev_y, dim, order_list, season_rows=season_rows)
    idx = list(Dy.index) + [k for k in Dm.index if k not in Dy.index]
    Dm = Dm.reindex(idx)
    Dy = Dy.reindex(idx)
    D = pd.concat([Dm, Dy], axis=1, keys=list(blk_labels))
    D.index.name = dim
    return D


def _fmt_cell(col, v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "–"
    top, sub = col[-2], col[-1]   # 2단(그룹·항목)/3단(기간·그룹·항목) 컬럼 모두 지원
    if top == "실판매금액(백만)":
        return f"{v:,.1f}" if sub != "증감율" else f"{v*100:+.0f}%"
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
    delta_cols = [c for c in D.columns if c[-1] in ("증감율", "증감")]

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
        disp.to_excel(w, sheet_name=_safe_name(sheet)[:28] or "분석")   # 시트명 금지문자('/' 등) 치환
    return buf.getvalue()


# ── 공통(룰11 · 2026-07-31): 모든 조회 표 엑셀 다운로드 기본 제공 ──────────────
# 주간회의 보고자료 '메인 표'만 예외(팀 주간보고 양식 템플릿 다운로드 유지 — weekly_excel_bytes).
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _safe_name(s):
    """엑셀 시트명·파일명에 못 쓰는 문자를 '-'로 치환."""
    s = str(s)
    for ch in '\\/:*?"<>|[]':
        s = s.replace(ch, "-")
    return s.strip()


def table_excel_bytes(disp, sheet="표"):
    """화면 표시용(포맷된) DataFrame을 그대로 엑셀로 변환 — 일반 표 공용 다운로드(룰11)."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        disp.to_excel(w, sheet_name=_safe_name(sheet)[:28] or "표")
    return buf.getvalue()


def _money_note():
    """룰1: 표 오른쪽 상단 [금액: 백만원 / VAT+] 표기. (레거시 — 현재는 _NOTE_FLOAT를 제목 줄에 붙임)"""
    st.markdown(
        "<div style='text-align:right;color:#888;font-size:0.78rem;margin:8px 0 3px 0;'>"
        "[금액: 백만원 / VAT+]</div>", unsafe_allow_html=True)


# 룰1 표기를 '표 제목 줄 오른쪽'에 붙여, 제목과 표 사이에 별도 줄이 안 생기게 함(여백 축소).
_NOTE_FLOAT = ("<span style='float:right;color:#888;font-weight:400;font-size:0.78rem;"
               "white-space:nowrap;'>[금액: 백만원 / VAT+]</span>")


# 공통 표 CSS: 옵션A 여백(3px 9px) + 헤더·구분 검정 + G.TOTAL(첫 행) 노란 강조 + 증감 색 유지
_TBL_CSS = """
<style>
.erp-wrap{overflow-x:auto;margin:0 0 8px;}
table.erp-tbl{border-collapse:collapse;font-size:0.82rem;}
table.erp-tbl th, table.erp-tbl td{padding:3px 6px;border:1px solid #e6e6e6;white-space:nowrap;}
table.erp-tbl thead th{color:#111;font-weight:700;background:#f4f4f6;text-align:center;}
table.erp-tbl tbody th{color:#111;font-weight:600;text-align:left;background:#fafafa;}
table.erp-tbl td{color:#111;text-align:right;}
table.erp-tbl tbody tr:first-child th, table.erp-tbl tbody tr:first-child td{
    background:#fff2b8 !important;font-weight:700;}
</style>
"""


# 시즌 7행 강조 CSS (연차별 성과표 전용 — erp-season 클래스가 붙은 표에만 적용)
#  2~3행(S/S·F/W TOTAL)=진한 블루그레이+볼드 · 4~8행(개별 시즌)=연한 톤+들여쓰기
_SEASON_ROW_CSS = """
<style>
table.erp-season tbody tr:nth-child(2) th, table.erp-season tbody tr:nth-child(2) td,
table.erp-season tbody tr:nth-child(3) th, table.erp-season tbody tr:nth-child(3) td{
    background:#e3ecf7 !important;font-weight:700;}
table.erp-season tbody tr:nth-child(n+4):nth-child(-n+8) th,
table.erp-season tbody tr:nth-child(n+4):nth-child(-n+8) td{background:#f4f8fc !important;}
table.erp-season tbody tr:nth-child(n+4):nth-child(-n+8) th{padding-left:18px;font-weight:500;}
</style>
"""


def render_styled_table(sty, extra_class="", extra_css=""):
    """Styler를 HTML 표로 렌더(가로여백 축소·헤더검정·G.TOTAL 노란강조). 증감 빨강/초록은 Styler가 유지."""
    cls = ("erp-tbl " + extra_class).strip()
    sty = sty.set_table_attributes(f'class="{cls}"')
    st.markdown(_TBL_CSS + extra_css + f'<div class="erp-wrap">{sty.to_html()}</div>',
                unsafe_allow_html=True)


def perf_table(cur, prev, dim, order_list, title, key, extra=None, season_rows=False,
               month=None, blk_labels=("당월누계", "연간누계")):
    """제목 + 우측 엑셀버튼 + 전년비교 표 렌더.

    extra=(컬럼명, {행라벨: 값})이면 표 맨 앞(행이름 바로 옆)에 텍스트 컬럼을 삽입
    — 예: 유통채널별 표의 '담당자'. 엑셀 다운로드에도 그대로 포함된다. (month와 병용 불가)
    season_rows=True면 G.TOTAL 아래 시즌 7행(S/S·F/W TOTAL + Z·A·B·C·D) 삽입.
    month=(cur_m, prev_m)이면 당월누계+연간누계 2블록 표(플래그십 탭 전용, 2026-07-31).
    """
    if month is not None:
        cur_m, prev_m = month
        D = yoy_frame2(cur_m, prev_m, cur, prev, dim, order_list,
                       season_rows=season_rows, blk_labels=blk_labels)
    else:
        D = yoy_frame(cur, prev, dim, order_list, season_rows=season_rows)
    if extra:
        _name, _map = extra
        D.insert(0, (_name, ""),
                 ["" if k == "G.TOTAL" else str(_map.get(str(k), "") or "") for k in D.index])
    h1, h2 = st.columns([4, 1])
    h1.markdown(f"**{title}**{_NOTE_FLOAT}", unsafe_allow_html=True)
    h2.download_button("⬇ 엑셀", yoy_excel_bytes(D, title[:28]),
                       file_name=f"{_safe_name(title)[:24]}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key=f"dl_{key}", use_container_width=True)
    if season_rows:
        render_styled_table(style_yoy(D), extra_class="erp-season", extra_css=_SEASON_ROW_CSS)
    else:
        render_styled_table(style_yoy(D))   # 룰3·4 + 헤더검정 + G.TOTAL 노란강조


def render_flagship(df):
    st.subheader("📅 연차 · 아이템별 전년 대비 분석")
    if df.empty or "_판매일" not in df.columns or df["_판매일"].notna().sum() == 0:
        st.info("데이터를 먼저 적재하세요.")
        return
    d = df[df["_판매일"].notna()].copy()
    years = sorted(d["_판매일"].dt.year.dropna().astype(int).unique(), reverse=True)

    st.caption("올해 vs 전년 '동기간'(같은 날짜범위) 비교 · 금액 단위 백만원 · 판가율=실판가÷최초가(가중)")
    f1, f2 = st.columns([1, 2.4])
    with f1:
        cy = st.selectbox("기준연도", years, index=0)
    cur_all = d[d["_판매일"].dt.year == cy]
    dmin, dmax = cur_all["_판매일"].min().date(), cur_all["_판매일"].max().date()
    with f2:
        rng = st.date_input(f"기준기간 (전년 {cy-1} 동기간 자동)", value=(dmin, dmax),
                            min_value=d["_판매일"].min().date(), max_value=d["_판매일"].max().date())
    # 공통 필터 (주간보고 방식) — 브랜드별 → 연차별 → 시즌별 · 빈칸=전체
    fb1, fb2, fb3 = st.columns(3)
    brands = sorted(d["브랜드명"].dropna().unique()) if "브랜드명" in d.columns else []
    ages = sorted(d["연차"].dropna().unique(), key=_age_sort_key) if "연차" in d.columns else []
    seasons = sorted(d["시즌명"].dropna().unique()) if "시즌명" in d.columns else []
    selb = fb1.multiselect("브랜드별", brands, default=[], placeholder="전체", key="fs_b")
    sela = fb2.multiselect("연차별", ages, default=[], placeholder="전체", key="fs_a")
    sels = fb3.multiselect("시즌별", seasons, default=[], placeholder="전체", key="fs_s")
    chans = sorted(d["_채널"].dropna().unique()) if "_채널" in d.columns else []
    selc = st.multiselect("매장/채널", chans, default=[], placeholder="전체", key="fs_c")

    if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
        st.info("기간(시작~끝)을 선택하세요.")
        return
    s, e = pd.to_datetime(rng[0]), pd.to_datetime(rng[1])
    base = d.copy()
    if selb and "브랜드명" in base:
        base = base[base["브랜드명"].isin(selb)]
    if sela and "연차" in base:
        base = base[base["연차"].isin(sela)]
    if sels and "시즌명" in base:
        base = base[base["시즌명"].isin(sels)]
    if selc and "_채널" in base:
        base = base[base["_채널"].isin(selc)]
    cur = base[(base["_판매일"] >= s) & (base["_판매일"] <= e)]
    prev = base[(base["_판매일"] >= s - pd.DateOffset(years=1)) & (base["_판매일"] <= e - pd.DateOffset(years=1))]
    # 당월누계 (2026-07-31 목업 v2 컨펌): 기준기간 끝날짜가 속한 달의 1일 → 끝날짜 · 전년 동범위 비교
    ms = e.replace(day=1)
    cur_m = base[(base["_판매일"] >= ms) & (base["_판매일"] <= e)]
    prev_m = base[(base["_판매일"] >= ms - pd.DateOffset(years=1)) & (base["_판매일"] <= e - pd.DateOffset(years=1))]
    blk = (f"당월누계 ({ms.month:02d}/{ms.day:02d}→{e.month:02d}/{e.day:02d})",
           f"연간누계 ({s.month:02d}/{s.day:02d}→{e.month:02d}/{e.day:02d})")

    tot_c = cur["_매출액"].sum()
    tot_p = prev["_매출액"].sum()
    k1, k2, k3 = st.columns(3)
    k1.metric(f"{cy} 매출(백만)", f"{_mm(tot_c):,.0f}")
    k2.metric(f"{cy-1} 매출(백만)", f"{_mm(tot_p):,.0f}")
    g = ((tot_c - tot_p) / tot_p) if tot_p else None
    k3.metric("전년비 성장률", "신규/–" if g is None else f"{g*100:+.1f}%")
    if not tot_p:
        st.warning(f"전년({cy-1}) 동기간 데이터가 없어요. {cy-1}년 로우데이터를 적재하면 채워집니다.")

    # 연차 순서
    age_order = sorted([a for a in base["연차"].dropna().unique()], key=_age_sort_key)
    # 표 이름 변경 (2026-07-31 컨펌): "연차별 성과표" → "시즌별/연차별 한눈에 보기"
    # 시즌 7행 포함 (목업 v3 컨펌): S/S TOTAL · F/W TOTAL + Z·A·B·C·D
    st.markdown("### 시즌별/연차별 한눈에 보기")
    perf_table(cur, prev, "연차", age_order, "시즌별/연차별 한눈에 보기", "age",
               season_rows=True, month=(cur_m, prev_m), blk_labels=blk)

    st.markdown("### 아이템그룹별 성과표 (전연차 토탈 + 연차별)")
    perf_table(cur, prev, "아이템그룹", ITEMGROUP_ORDER, "아이템그룹별 성과표 (전연차)", "grp_all",
               month=(cur_m, prev_m), blk_labels=blk)
    # 연차별 버킷
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
        curb_m = cur_m[cur_m["연차"].isin(ages)]
        prevb_m = prev_m[prev_m["연차"].isin(ages)]
        perf_table(curb, prevb, "아이템그룹", ITEMGROUP_ORDER,
                   f"아이템그룹별 성과표 ({name})", f"grp_{name}",
                   month=(curb_m, prevb_m), blk_labels=blk)


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


def render_channel_brand(df):
    """매주 대표님 보고 B: 유통채널별 · 브랜드별 매출현황 (전년 동기간 비교)."""
    st.subheader("📈 유통채널 · 브랜드별 매출현황 (전년 동기간 비교)")
    if df.empty or "_판매일" not in df.columns or df["_판매일"].notna().sum() == 0:
        st.info("데이터를 먼저 적재하세요.")
        return
    d = df[df["_판매일"].notna()].copy()
    # 매장 담당자 매핑 (매장 마스터의 담당자 기준) — 담당자 컬럼·담당 필터용
    master = load_master()
    if not master.empty and "담당자" in master.columns:
        _mgr_map = dict(zip(master["매장코드"].astype(str).str.strip(),
                            master["담당자"].astype(str).str.strip()))
        d["_담당자"] = d["매장코드"].astype(str).str.strip().map(_mgr_map)
    else:
        d["_담당자"] = None
    dmin, dmax = d["_판매일"].min().date(), d["_판매일"].max().date()
    default_start = (pd.to_datetime(dmax) - pd.Timedelta(days=6)).date()

    st.caption("올해 vs 전년 '동기간'(같은 날짜범위) 비교 · 금액 백만원 · 판가율=실판가÷최초가(가중) · 기본기간=최근 1주")
    rng = st.date_input("조회기간 (기본: 최근 1주)", value=(default_start, dmax),
                        min_value=dmin, max_value=dmax, key="cb_rng")
    # 공통 필터 (주간보고 방식) — 브랜드별 → 연차별 → 시즌별 → 매장 담당 · 빈칸=전체
    cb1, cb2, cb3, cb4 = st.columns(4)
    brands = sorted(d["브랜드명"].dropna().unique()) if "브랜드명" in d.columns else []
    ages = sorted(d["연차"].dropna().unique(), key=_age_sort_key) if "연차" in d.columns else []
    seasons = sorted(d["시즌명"].dropna().unique()) if "시즌명" in d.columns else []
    _mans = sorted({str(m).strip() for m in d["_담당자"].dropna().astype(str)
                    if str(m).strip() and str(m).strip().lower() not in ("nan", "none")})
    selb = cb1.multiselect("브랜드별", brands, default=[], placeholder="전체", key="cb_brand")
    sela = cb2.multiselect("연차별", ages, default=[], placeholder="전체", key="cb_age")
    sels = cb3.multiselect("시즌별", seasons, default=[], placeholder="전체", key="cb_season")
    selm = cb4.multiselect("매장 담당", _mans, default=[], placeholder="전체", key="cb_mgr")
    if not _mans:
        st.caption("※ 매장 기준정보(담당자)가 없어 담당자 컬럼·필터가 비어 있어요 — 사이드바에서 매장 기준정보를 업로드하면 채워져요.")

    if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
        st.info("기간(시작~끝)을 선택하세요.")
        return
    s, e = pd.to_datetime(rng[0]), pd.to_datetime(rng[1])
    base = d
    if selb and "브랜드명" in base.columns:
        base = base[base["브랜드명"].isin(selb)]
    if sela and "연차" in base.columns:
        base = base[base["연차"].isin(sela)]
    if sels and "시즌명" in base.columns:
        base = base[base["시즌명"].isin(sels)]
    if selm and "_담당자" in base.columns:
        base = base[base["_담당자"].astype(str).str.strip().isin(selm)]
    cur = base[(base["_판매일"] >= s) & (base["_판매일"] <= e)]
    prev = base[(base["_판매일"] >= s - pd.DateOffset(years=1)) & (base["_판매일"] <= e - pd.DateOffset(years=1))]

    tot_c, tot_p = cur["_매출액"].sum(), prev["_매출액"].sum()
    k1, k2, k3 = st.columns(3)
    k1.metric("기간 매출(백만)", f"{_mm(tot_c):,.0f}")
    k2.metric("전년 동기(백만)", f"{_mm(tot_p):,.0f}")
    g = ((tot_c - tot_p) / tot_p) if tot_p else None
    k3.metric("전년비 신장률", "신규/–" if g is None else f"{g*100:+.1f}%")
    if not tot_p:
        st.warning("전년 동기간 데이터가 없어요. 기간을 조정하거나 전년 로우데이터를 적재하세요.")

    st.markdown("### A. 유통채널별 (매출 순)")
    # 매장명(행) → 담당자 매핑: 표 맨 앞 '담당자' 컬럼으로 표시
    _cm = d[["_채널", "_담당자"]].astype(str).drop_duplicates(subset=["_채널"])
    chan_mgr = {c: ("" if m.strip().lower() in ("nan", "none") else m.strip())
                for c, m in zip(_cm["_채널"], _cm["_담당자"])}
    perf_table(cur, prev, "_채널", None, "유통채널별 매출현황", "cb_ch", extra=("담당자", chan_mgr))
    st.caption("※ 채널을 자사몰/외부몰 등 그룹으로 묶으려면 '채널 기준정보(매핑)'가 필요해요 — 준비되면 그룹 집계도 추가해드릴게요.")

    st.markdown("### B. 브랜드별")
    perf_table(cur, prev, "브랜드명", None, "브랜드별 매출현황", "cb_br")


# ==============================================================================
# 매장(채널) 기준정보 마스터  ─ 업로드 시 전체 교체
# ==============================================================================
MASTER_TABLE = "channel_master"
MASTER_COLS = ["매장코드", "매장명", "담당자", "유통성격", "채널소유", "채널스토리"]


def read_master_file(uploaded_file):
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        raw = pd.read_csv(uploaded_file, header=None, dtype=str, keep_default_na=False)
    else:
        raw = pd.read_excel(uploaded_file, header=None, dtype=str)
    hrow = 0
    for i in range(min(10, len(raw))):
        vals = [str(v).strip() for v in raw.iloc[i].tolist()]
        if "매장코드" in vals:
            hrow = i
            break
    header = [str(v).strip() for v in raw.iloc[hrow].tolist()]
    m = raw.iloc[hrow + 1:].copy()
    m.columns = header
    m = m.dropna(how="all")
    keep = [c for c in MASTER_COLS if c in m.columns]
    m = m[keep].copy()
    for c in keep:
        m[c] = m[c].astype(str).str.strip()
    m = m[m["매장코드"].ne("") & ~m["매장코드"].str.lower().isin(["nan", "none"])]
    if "유통성격" in m.columns:
        m["유통성격"] = m["유통성격"].replace({"벤더": "밴더"})  # 표기 통일
    return m.reset_index(drop=True)


def replace_master(m):
    eng = get_engine()
    with eng.begin() as conn:
        m.astype(str).to_sql(MASTER_TABLE, conn, if_exists="replace", index=False)
    return len(m)


@st.cache_data(ttl=120)
def load_master():
    eng = get_engine()
    try:
        with eng.connect() as conn:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.tables WHERE table_name=%s"
                if eng.dialect.name == "postgresql" else
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (MASTER_TABLE,)).fetchone()
            if not exists:
                return pd.DataFrame()
            m = pd.read_sql(f'SELECT * FROM "{MASTER_TABLE}"', conn)
    except Exception:
        return pd.DataFrame()
    for c in m.columns:
        m[c] = m[c].astype(str).str.strip()
    return m


def master_row_count():
    try:
        with get_engine().connect() as conn:
            return conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{MASTER_TABLE}"').scalar()
    except Exception:
        return 0


# ── 사업계획(월별 목표) 마스터 : 매장별 + 브랜드별(연차 포함) ──
PLAN_TABLE = "plan_master"


def read_plan_file(uploaded_file):
    """사업계획 엑셀(매장별 시트 + 브랜드별 시트) → long DF[dim, code, sub, month, amount]."""
    import re
    def _num(v):
        try:
            f = float(v)
            return 0.0 if pd.isna(f) else f
        except Exception:
            return 0.0
    xls = pd.read_excel(uploaded_file, sheet_name=None, header=None, dtype=object)
    out = []
    for _, raw in xls.items():
        if raw is None or len(raw) == 0:
            continue
        hrow = kind = None
        for i in range(min(10, len(raw))):
            vals = [str(v).strip() for v in raw.iloc[i].tolist()]
            if "매장코드" in vals:
                hrow, kind = i, "store"; break
            if "브랜드" in vals:
                hrow, kind = i, "brand"; break
        if hrow is None:
            continue
        header = [str(v).strip() for v in raw.iloc[hrow].tolist()]
        mcols = {}
        for ci, h in enumerate(header):
            mm = re.match(r"(\d{1,2})\s*월", h)
            if mm:
                mcols[int(mm.group(1))] = ci
        body = raw.iloc[hrow + 1:]
        if kind == "store":
            ci_code = header.index("매장코드")
            for _, r in body.iterrows():
                code = str(r.iloc[ci_code]).strip()
                if code == "" or code.lower() in ("nan", "none"):
                    continue
                for m, ci in mcols.items():
                    out.append(("store", code, "", m, _num(r.iloc[ci])))
        else:
            ci_b = header.index("브랜드")
            ci_a = header.index("연차") if "연차" in header else None
            for _, r in body.iterrows():
                b = str(r.iloc[ci_b]).strip()
                if b == "" or b.lower() in ("nan", "none"):
                    continue
                sub = str(r.iloc[ci_a]).strip() if ci_a is not None else "합계"
                for m, ci in mcols.items():
                    out.append(("brand", b, sub, m, _num(r.iloc[ci])))
    return pd.DataFrame(out, columns=["dim", "code", "sub", "month", "amount"])


def replace_plan(p):
    eng = get_engine()
    with eng.begin() as conn:
        p.to_sql(PLAN_TABLE, conn, if_exists="replace", index=False)
    return len(p)


@st.cache_data(ttl=120)
def load_plan():
    eng = get_engine()
    try:
        with eng.connect() as conn:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.tables WHERE table_name=%s"
                if eng.dialect.name == "postgresql" else
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (PLAN_TABLE,)).fetchone()
            if not exists:
                return pd.DataFrame()
            p = pd.read_sql(f'SELECT * FROM "{PLAN_TABLE}"', conn)
    except Exception:
        return pd.DataFrame()
    if not p.empty:
        p["month"] = pd.to_numeric(p["month"], errors="coerce")
        p["amount"] = pd.to_numeric(p["amount"], errors="coerce").fillna(0.0)
        p["code"] = p["code"].astype(str).str.strip()
        p["sub"] = p["sub"].astype(str).str.strip()
    return p


def plan_row_count():
    try:
        with get_engine().connect() as conn:
            return conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{PLAN_TABLE}"').scalar()
    except Exception:
        return 0


# 사업계획 → 주간보고 행 매핑
_PLAN_BRAND = {"S/D/L": "S/D/L", "A (CODI GALLERY)": "A", "0 (ZERO LOUNGE)": "0",
               "J (GENTLEMENS)": "J", "N (NORATED)": "N"}
_PLAN_SUB = {"합계": "합계", "신상": "신상+내년신상", "1년차": "1년차", "2년차": "2년차", "3년차": "3년차"}
_PLAN_CH_KW = {"원래직입점": "원래", "웹뜰이관": "웹뜰", "웍스바이이관": "웍스"}


def _plan_cum(plan, master):
    """연간 전체(1~12월) 사업계획. dict{store, brand, cs} 반환. 없으면 None."""
    if plan is None or plan.empty:
        return None
    p = plan
    store = p[p["dim"] == "store"].groupby("code")["amount"].sum().to_dict()
    brand = p[p["dim"] == "brand"].groupby(["code", "sub"])["amount"].sum().to_dict()
    cs = {"원래": [], "웹뜰": [], "웍스": []}
    if master is not None and not master.empty and "채널스토리" in master.columns:
        for _, mr in master.iterrows():
            story = str(mr.get("채널스토리", ""))
            code = str(mr.get("매장코드", "")).strip()
            for kw in cs:
                if kw in story:
                    cs[kw].append(code)
    return {"store": store, "brand": brand, "cs": cs}


def _plan_for(key, cum):
    if cum is None:
        return None
    sec, mid, sub = key
    store, brand, cs = cum["store"], cum["brand"], cum["cs"]
    if mid == "G.TOTAL":
        return store.get("G.TOTAL")
    if sec == "유통별":
        if mid == "통합몰":
            return store.get("SD065")
        if mid == "네이버스토어":
            return (store.get("SD165") or 0) + (store.get("SD174") or 0)
        kw = _PLAN_CH_KW.get(mid)
        if kw:
            codes = cs.get(kw, [])
            return sum(store.get(c, 0) for c in codes) if codes else None
    if sec == "브랜드별":
        b = _PLAN_BRAND.get(mid); s = _PLAN_SUB.get(sub)
        if b and s:
            return brand.get((b, s))
    return None


def _store_annual():
    """매장코드 → 연간 사업계획 dict."""
    plan = load_plan()
    if plan is None or plan.empty:
        return {}
    s = plan[plan["dim"] == "store"]
    return s.groupby("code")["amount"].sum().to_dict()


def inject_plan(by, idx, master):
    """누계 블록 by[key]에 '사업계획'(연간)·'진도율' 주입. 진도율 = 26누계실적 ÷ 연간계획."""
    cum = _plan_cum(load_plan(), master)
    for key in idx:
        p = _plan_for(key, cum)
        r = by.get(key)
        if r is None:
            continue
        r["사업계획"] = p
        act = r.get("cy실판가")
        r["진도율"] = (act / p) if (p and act is not None) else None


def inject_plan_manager(by, idx, master):
    """담당별 표 by[key]에 '사업계획'(담당자 매장 연간계획 합)·'진도율' 주입."""
    store = _store_annual()
    mgr_codes = {}
    if master is not None and not master.empty and "담당자" in master.columns:
        for _, mr in master.iterrows():
            m = str(mr.get("담당자", "")).strip()
            c = str(mr.get("매장코드", "")).strip()
            if m:
                mgr_codes.setdefault(m, []).append(c)
    for key in idx:
        _, mid, _ = key
        if mid == "G.TOTAL":
            p = store.get("G.TOTAL")
        else:
            codes = mgr_codes.get(mid, [])
            p = sum(store.get(c, 0) for c in codes) if (codes and store) else None
        r = by.get(key)
        if r is None:
            continue
        r["사업계획"] = p
        act = r.get("cy실판가")
        r["진도율"] = (act / p) if (p and act is not None) else None


# ==============================================================================
# 주간회의 보고자료  ─ 당월실적 / 연간누계 (전년 동기간 비교)
# ==============================================================================
SDL_BRANDS = ["STCO", "DIEMS", "GENDERLESS"]
WK_MONEY = ["실판가", "사업계획"]

# 유통별 5개 분류 기준 (요약행 + 매장 드릴다운 공용 · 단일 소스)
_CHANNEL_MASKS = {
    "통합몰":      lambda x: x["매장코드"].astype(str).str.strip().isin(["SD065"]),
    "네이버스토어": lambda x: x["매장코드"].astype(str).str.strip().isin(["SD165", "SD174"]),
    "원래직입점":   lambda x: x["_채널스토리"].astype(str).str.contains("원래", na=False),
    "웹뜰이관":     lambda x: x["_채널스토리"].astype(str).str.contains("웹뜰", na=False),
    "웍스바이이관": lambda x: x["_채널스토리"].astype(str).str.contains("웍스", na=False),
}


def _wk_metrics(cur_sub, prev_sub, total_c):
    r26 = float(cur_sub["_매출액"].sum()); r25 = float(prev_sub["_매출액"].sum())
    o26 = float(cur_sub["_최초가매출"].sum()); o25 = float(prev_sub["_최초가매출"].sum())
    pg26 = (r26 / o26) if o26 else 0.0; pg25 = (r25 / o25) if o25 else 0.0
    return {
        "py실판가": r25, "py판가율": pg25,
        "cy실판가": r26, "증감율": ((r26 - r25) / r25) if r25 else None,
        "비중": (r26 / total_c) if total_c else 0.0, "cy판가율": pg26,
        "편차": pg26 - pg25,
    }


def _wk_block(cur, prev, rows):
    total_c = float(cur["_매출액"].sum())
    out = {}
    for key, mask in rows:
        cs = cur[mask(cur)] if len(cur) else cur
        ps = prev[mask(prev)] if len(prev) else prev
        out[key] = _wk_metrics(cs, ps, total_c)
    return out


def _wk_rows():
    def code(x, cs): return x["매장코드"].astype(str).str.strip().isin(cs)
    def story(x, kw): return x["_채널스토리"].astype(str).str.contains(kw, na=False)  # 핵심단어 유연매칭
    def brand(x, ns): return x["브랜드명"].isin(ns)
    def age(x, a): return x["연차"].isin(a)
    return [
        (("전체", "G.TOTAL", "합계"), lambda x: pd.Series(True, index=x.index)),
        (("유통별", "통합몰", "합계"), _CHANNEL_MASKS["통합몰"]),
        (("유통별", "네이버스토어", "합계"), _CHANNEL_MASKS["네이버스토어"]),
        (("유통별", "원래직입점", "합계"), _CHANNEL_MASKS["원래직입점"]),
        (("유통별", "웹뜰이관", "합계"), _CHANNEL_MASKS["웹뜰이관"]),
        (("유통별", "웍스바이이관", "합계"), _CHANNEL_MASKS["웍스바이이관"]),
        (("브랜드별", "S/D/L", "합계"), lambda x: brand(x, SDL_BRANDS)),
        (("브랜드별", "S/D/L", "신상"), lambda x: brand(x, SDL_BRANDS) & age(x, ["신상", "내년신상"])),
        (("브랜드별", "S/D/L", "1년차"), lambda x: brand(x, SDL_BRANDS) & age(x, ["1년차"])),
        (("브랜드별", "S/D/L", "2년차"), lambda x: brand(x, SDL_BRANDS) & age(x, ["2년차"])),
        (("브랜드별", "S/D/L", "3년차"), lambda x: brand(x, SDL_BRANDS) & age(x, ["3년차"])),
        (("브랜드별", "A (CODI GALLERY)", "합계"), lambda x: brand(x, ["CODI GALLERY"])),
        (("브랜드별", "0 (ZERO LOUNGE)", "합계"), lambda x: brand(x, ["ZERO LOUNGE"])),
        (("브랜드별", "J (GENTLEMENS)", "합계"), lambda x: brand(x, ["GENTLEMENS PHILOSOPHY"])),
        (("브랜드별", "N (NORATED)", "합계"), lambda x: brand(x, ["NORATED"])),
    ]


def _wk_fmt(block, sub, v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "–"
    if sub == "사업계획":
        return f"{v/1e6:,.1f}"   # 백만원 · 소수1자리
    if sub == "진도율":
        return f"{v*100:.0f}%"
    if "실판가" in sub:
        return f"{v/1e6:,.1f}"   # 룰1: 백만원 · 소수1자리
    if "판가율" in sub:
        return f"{v*100:.1f}%"
    if sub == "증감율":
        return f"{v*100:+.1f}%"
    if sub == "비중":
        return f"{v*100:.1f}%"
    if sub == "편차":
        return f"{v*100:+.1f}%p"
    return v


def weekly_excel_bytes(rows, bm, by, asof, cy, py):
    """팀 주간보고 양식(weekly_template.xlsx)을 템플릿으로 열어 매출현황·마감일·특이사항만 채워 반환."""
    import os
    from openpyxl import load_workbook
    tpl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_template.xlsx")
    wb = load_workbook(tpl)
    ws = wb["주간보고"] if "주간보고" in wb.sheetnames else wb.active

    ws["P1"] = f"마감일: {str(cy)[-2:]}년 {asof.month:02d}월 {asof.day:02d}일"

    row_map = {
        ("전체", "G.TOTAL", "합계"): 12,
        ("유통별", "통합몰", "합계"): 13, ("유통별", "네이버스토어", "합계"): 14,
        ("유통별", "원래직입점", "합계"): 15, ("유통별", "웹뜰이관", "합계"): 16,
        ("유통별", "웍스바이이관", "합계"): 17,
        ("브랜드별", "S/D/L", "합계"): 18, ("브랜드별", "S/D/L", "신상"): 19,
        ("브랜드별", "S/D/L", "1년차"): 20, ("브랜드별", "S/D/L", "2년차"): 21,
        ("브랜드별", "S/D/L", "3년차"): 22,
        ("브랜드별", "A (CODI GALLERY)", "합계"): 23, ("브랜드별", "0 (ZERO LOUNGE)", "합계"): 24,
        ("브랜드별", "J (GENTLEMENS)", "합계"): 25, ("브랜드별", "N (NORATED)", "합계"): 26,
    }

    def setc(r, col, v):
        c = ws.cell(r, col)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            c.value = "–"
        else:
            c.value = float(v)

    for key, r in row_map.items():
        m = bm.get(key, {}); y = by.get(key, {})
        setc(r, 4, m.get("py실판가")); setc(r, 5, m.get("py판가율")); setc(r, 6, m.get("cy실판가"))
        setc(r, 7, m.get("증감율")); setc(r, 8, m.get("비중")); setc(r, 9, m.get("cy판가율"))
        setc(r, 10, m.get("편차"))
        setc(r, 11, y.get("py실판가")); setc(r, 12, y.get("py판가율")); setc(r, 14, y.get("cy실판가"))
        setc(r, 16, y.get("증감율")); setc(r, 17, y.get("비중")); setc(r, 18, y.get("cy판가율"))
        setc(r, 19, y.get("편차"))

    def pc(v):
        return "N/A" if (v is None or (isinstance(v, float) and pd.isna(v))) else f"{v*100:+.0f}%"
    G = bm.get(("전체", "G.TOTAL", "합계"), {})
    TM = bm.get(("유통별", "통합몰", "합계"), {}); NV = bm.get(("유통별", "네이버스토어", "합계"), {})
    j26 = (TM.get("cy실판가") or 0) + (NV.get("cy실판가") or 0)
    j25 = (TM.get("py실판가") or 0) + (NV.get("py실판가") or 0)
    jasa = ((j26 - j25) / j25) if j25 else None
    OW = bm.get(("유통별", "원래직입점", "합계"), {}); WT = bm.get(("유통별", "웹뜰이관", "합계"), {})
    WK = bm.get(("유통별", "웍스바이이관", "합계"), {})
    gt = G.get("증감율")
    trend = "상승" if (gt or 0) >= 0 else "하락"
    ws["A29"] = (f"1. 당월실적 전년대비 {pc(gt)} {trend} 추세\n"
                 f"2. 통합몰은 {pc(TM.get('증감율'))} , 네이버스토어 {pc(NV.get('증감율'))}. "
                 f"자사채널 전체는 {pc(jasa)} 추세\n"
                 f"3. 원래직입점 {pc(OW.get('증감율'))} , 웹뜰이관 {pc(WT.get('증감율'))}, "
                 f"웍스바이이관 {pc(WK.get('증감율'))} 추세")

    buf = io.BytesIO(); wb.save(buf); return buf.getvalue()


def _wk_style_table(bm, by, idx, cy, py):
    """주간보고 프레임(당월+누계 · 동일 컬럼)으로 (bm,by,idx)를 스타일 표로 변환. 메인표·담당별표 공용.

    반환: (Styler, 표시용 DataFrame) — 표시용 DF는 엑셀 다운로드(룰11)에 사용.
    """
    MON, YTD = "당월 실적", "연간누계"
    sy, sc = str(py)[-2:], str(cy)[-2:]   # 룰2: 연도 2자리
    # 컬럼 순서 (2026-07-31 팀장님 지정): 실판가 25→26 → 증감율 → 비중 → (누계: 사업계획→진도율) → 판가율 25→26 → 편차
    mcols = [(MON, f"{sy}실판가"), (MON, f"{sc}실판가"), (MON, "증감율"), (MON, "비중"),
             (MON, f"{sy}판가율"), (MON, f"{sc}판가율"), (MON, "편차")]
    ycols = [(YTD, f"{sy}실판가"), (YTD, f"{sc}실판가"), (YTD, "증감율"), (YTD, "비중"),
             (YTD, "사업계획"), (YTD, "진도율"),
             (YTD, f"{sy}판가율"), (YTD, f"{sc}판가율"), (YTD, "편차")]

    def cellval(block_res, key, sub):
        r = block_res[key]
        if "실판가" in sub:
            return r["py실판가"] if sub.startswith(sy) else r["cy실판가"]
        if "판가율" in sub:
            return r["py판가율"] if sub.startswith(sy) else r["cy판가율"]
        if sub in ("사업계획", "진도율"):
            return r.get(sub)
        return r[sub]

    data = [[cellval(bm, k, s[1]) for s in mcols] + [cellval(by, k, s[1]) for s in ycols] for k in idx]
    D = pd.DataFrame(data, index=pd.MultiIndex.from_tuples(idx),
                     columns=pd.MultiIndex.from_tuples(mcols + ycols))
    disp = D.copy()
    for col in disp.columns:
        disp[col] = [_wk_fmt(col[0], col[1], v) for v in D[col]]

    def _color(col):
        if col[1] not in ("증감율", "편차"):
            return ["" for _ in D[col]]
        return ["color:#c62828;font-weight:600" if (pd.notnull(v) and v < 0)
                else ("color:#1f8a4c;font-weight:600" if (pd.notnull(v) and v > 0) else "") for v in D[col]]
    sty = disp.style
    for col in D.columns:
        if col[1] in ("증감율", "편차"):
            sty = sty.apply(lambda s, c=col: _color(c), subset=pd.IndexSlice[:, [col]])
    return sty.set_properties(**{"text-align": "right"}), disp


def render_weekly_drilldown(cur_m, prev_m, cur_y, prev_y, label, mask, cy, py, show_plan=True):
    """선택한 그룹(유통 또는 담당자)의 매장별 상세표 — 주간보고와 동일 형식(당월+누계). 비중=해당 그룹 내."""
    cm, pm = cur_m[mask(cur_m)], prev_m[mask(prev_m)]
    cyd, pyd = cur_y[mask(cur_y)], prev_y[mask(prev_y)]
    if cm.empty and cyd.empty and pm.empty and pyd.empty:
        st.info(f"'{label}'에 해당하는 매장 데이터가 없어요.")
        return

    tot_m = float(cm["_매출액"].sum())    # 당월 유통 total (비중 분모)
    tot_y = float(cyd["_매출액"].sum())   # 누계 유통 total

    codes = pd.Index(pd.concat([cyd["매장코드"], pyd["매장코드"]])
                     .astype(str).str.strip().replace({"nan": None, "none": None}).dropna().unique())
    lbl_src = pd.concat([cur_y, prev_y])
    name_map = {}
    if "매장명" in lbl_src.columns:
        tmp = lbl_src[["매장코드", "매장명"]].astype(str)
        name_map = dict(zip(tmp["매장코드"].str.strip(), tmp["매장명"]))

    def sub(frame, c):
        return frame[frame["매장코드"].astype(str).str.strip() == c]

    store_ann = _store_annual() if show_plan else {}   # 매장코드 → 연간 사업계획 (필터 시 생략)

    def _addplan(y, pl):
        y["사업계획"] = pl
        act = y.get("cy실판가")
        y["진도율"] = (act / pl) if (pl and act is not None) else None
        return y

    store_rows = []
    for c in codes:
        m = _wk_metrics(sub(cm, c), sub(pm, c), tot_m)
        y = _addplan(_wk_metrics(sub(cyd, c), sub(pyd, c), tot_y),
                     store_ann.get(str(c).strip()) if store_ann else None)
        rev_y = float(sub(cyd, c)["_매출액"].sum())
        store_rows.append((name_map.get(c, c) or c, m, y, rev_y))
    store_rows.sort(key=lambda t: -t[3])   # 누계 올해 매출 큰 순

    plan_head = sum(store_ann.get(str(c).strip(), 0) for c in codes) if store_ann else None
    head_y = _addplan(_wk_metrics(cyd, pyd, tot_y), plan_head)
    entries = [(f"{label} (합계)", _wk_metrics(cm, pm, tot_m), head_y)]
    entries += [(r[0], r[1], r[2]) for r in store_rows]

    sy, sc = str(py)[-2:], str(cy)[-2:]
    MON, YTD = "당월 실적", "연간누계"
    # 컬럼 순서 (2026-07-31 팀장님 지정): 실판가 25→26 → 증감율 → 비중 → (누계: 사업계획→진도율) → 판가율 25→26 → 편차
    mcols = [(MON, f"{sy}실판가"), (MON, f"{sc}실판가"), (MON, "증감율"), (MON, "비중"),
             (MON, f"{sy}판가율"), (MON, f"{sc}판가율"), (MON, "편차")]
    ycols = [(YTD, f"{sy}실판가"), (YTD, f"{sc}실판가"), (YTD, "증감율"), (YTD, "비중"),
             (YTD, "사업계획"), (YTD, "진도율"),
             (YTD, f"{sy}판가율"), (YTD, f"{sc}판가율"), (YTD, "편차")]

    def val(metrics, s):
        if "실판가" in s:
            return metrics["py실판가"] if s.startswith(sy) else metrics["cy실판가"]
        if "판가율" in s:
            return metrics["py판가율"] if s.startswith(sy) else metrics["cy판가율"]
        return metrics.get(s)

    idx, data = [], []
    for _row_lbl, m, y in entries:   # ← label(선택 그룹명)을 덮어쓰지 않도록 별도 변수 사용
        idx.append(_row_lbl)
        data.append([val(m, s[1]) for s in mcols] + [val(y, s[1]) for s in ycols])
    D = pd.DataFrame(data, index=idx, columns=pd.MultiIndex.from_tuples(mcols + ycols))

    disp = D.copy()
    for col in disp.columns:
        disp[col] = [_wk_fmt(col[0], col[1], v) for v in D[col]]

    def _color(col):
        if col[1] not in ("증감율", "편차"):
            return ["" for _ in D[col]]
        return ["color:#c62828;font-weight:600" if (pd.notnull(v) and v < 0)
                else ("color:#1f8a4c;font-weight:600" if (pd.notnull(v) and v > 0) else "") for v in D[col]]
    sty = disp.style
    for col in D.columns:
        if col[1] in ("증감율", "편차"):
            sty = sty.apply(lambda s, c=col: _color(c), subset=pd.IndexSlice[:, [col]])
    sty = sty.set_properties(**{"text-align": "right"})

    # 룰11: 제목 + 우측 일반 엑셀 다운로드 버튼 (2026-07-31)
    d1, d2 = st.columns([5, 1])
    d1.markdown(f"**🔍 {label} · 매장별 상세**  "
                f"<span style='color:#888;font-size:0.8rem;'>(매장 {len(store_rows)}개 · 비중=해당 그룹 내 · 매출 큰 순)</span>"
                f"{_NOTE_FLOAT}", unsafe_allow_html=True)
    d2.download_button("⬇ 엑셀", table_excel_bytes(disp, f"{label} 매장별"),
                       file_name=f"{_safe_name(label)}_매장별상세.xlsx", mime=XLSX_MIME,
                       key=f"wk_dl_drill_{label}", use_container_width=True)
    render_styled_table(sty)


def render_weekly_item_drilldown(cur_m, prev_m, cur_y, prev_y, label, mask, cy, py):
    """선택한 매장(또는 담당자 전체매장)의 아이템그룹별 상세표 — 주간보고 동일 프레임. 비중=해당 그룹 내."""
    cm, pm = cur_m[mask(cur_m)], prev_m[mask(prev_m)]
    cyd, pyd = cur_y[mask(cur_y)], prev_y[mask(prev_y)]
    if cm.empty and cyd.empty and pm.empty and pyd.empty:
        st.info(f"'{label}' 데이터가 없어요.")
        return
    present = set(pd.concat([cyd["아이템그룹"], pyd["아이템그룹"], cm["아이템그룹"], pm["아이템그룹"]]).astype(str))
    groups = [g for g in ITEMGROUP_ORDER if g in present] + [g for g in present if g not in ITEMGROUP_ORDER]
    rows = [(("전체", "G.TOTAL", "합계"), lambda x: pd.Series(True, index=x.index))]
    for g in groups:
        rows.append((("아이템", g, "합계"),
                     (lambda gg: (lambda x: x["아이템그룹"].astype(str) == gg))(g)))
    bm = _wk_block(cm, pm, rows)
    by = _wk_block(cyd, pyd, rows)
    sty, disp = _wk_style_table(bm, by, [k for k, _ in rows], cy, py)
    # 룰11: 제목 + 우측 일반 엑셀 다운로드 버튼 (2026-07-31)
    i1, i2 = st.columns([5, 1])
    i1.markdown(f"**🔍 {label} · 아이템그룹별 상세**  "
                f"<span style='color:#888;font-size:0.8rem;'>(비중=해당 그룹 내 · G.TOTAL=선택 전체)</span>"
                f"{_NOTE_FLOAT}", unsafe_allow_html=True)
    i2.download_button("⬇ 엑셀", table_excel_bytes(disp, f"{label} 아이템"),
                       file_name=f"{_safe_name(label)}_아이템그룹별상세.xlsx", mime=XLSX_MIME,
                       key=f"wk_dl_item_{label}", use_container_width=True)
    render_styled_table(sty)


def render_weekly_report(df):
    st.subheader("📋 주간회의 보고자료 (당월 · 연간누계, 전년 동기간 비교)")
    if df.empty or "_판매일" not in df.columns or df["_판매일"].notna().sum() == 0:
        st.info("데이터를 먼저 적재하세요.")
        return
    d = df[df["_판매일"].notna()].copy()
    master = load_master()
    if not master.empty and "채널스토리" in master.columns:
        cs_map = dict(zip(master["매장코드"].astype(str).str.strip(), master["채널스토리"]))
        d["_채널스토리"] = d["매장코드"].astype(str).str.strip().map(cs_map)
    else:
        d["_채널스토리"] = None
        st.warning("매장 기준정보(채널스토리)가 없어 유통별 3개(원래직입점·웹뜰이관·웍스바이이관)는 0으로 나와요. "
                   "사이드바 **매장 기준정보 업로드**에 마스터 파일을 올리면 채워져요.")
    # 담당자 매핑 (드릴다운 담당별용)
    if not master.empty and "담당자" in master.columns:
        mgr_map = dict(zip(master["매장코드"].astype(str).str.strip(), master["담당자"].astype(str).str.strip()))
        d["_담당자"] = d["매장코드"].astype(str).str.strip().map(mgr_map)
    else:
        d["_담당자"] = None

    dmin, dmax = d["_판매일"].min().date(), d["_판매일"].max().date()
    asof = st.date_input("조회 기준일 (당월·누계의 끝 날짜)", value=dmax, min_value=dmin, max_value=dmax, key="wk_asof")
    asof = pd.to_datetime(asof)
    cy, py = asof.year, asof.year - 1
    st.caption(f"올해({cy}) vs 전년({py}) 동기간 · 실판가=실매출(백만원) · 판가율=실판가÷최초가(가중) · 비중=행÷전체")

    # ── 공통 필터 (이 페이지 모든 표에 적용) — 브랜드별 → 연차별 → 시즌별 ──
    fc1, fc2, fc3 = st.columns(3)
    _brands = sorted(d["브랜드명"].dropna().unique()) if "브랜드명" in d.columns else []
    _ages = sorted(d["연차"].dropna().unique(), key=_age_sort_key) if "연차" in d.columns else []
    _seasons = sorted(d["시즌명"].dropna().unique()) if "시즌명" in d.columns else []
    selb = fc1.multiselect("브랜드별", _brands, default=[], placeholder="전체", key="wk_fb")
    sela = fc2.multiselect("연차별", _ages, default=[], placeholder="전체", key="wk_fa")
    sels = fc3.multiselect("시즌별", _seasons, default=[], placeholder="전체", key="wk_fs")
    fd = d
    if selb:
        fd = fd[fd["브랜드명"].isin(selb)]
    if sela:
        fd = fd[fd["연차"].isin(sela)]
    if sels:
        fd = fd[fd["시즌명"].isin(sels)]
    _filtered = bool(selb or sela or sels)
    if _filtered:
        st.caption("🔎 필터 적용 중 — 실적·판가율·증감·비중은 선택 조건 기준. "
                   "**사업계획·진도율은 시즌/연차 세분화가 없어 필터 시 '–'로 표시**(전체일 때만 계획 표시).")

    m_start = asof.replace(day=1)
    y_start = asof.replace(month=1, day=1)
    cur_m = fd[(fd["_판매일"] >= m_start) & (fd["_판매일"] <= asof)]
    prev_m = fd[(fd["_판매일"] >= m_start - pd.DateOffset(years=1)) & (fd["_판매일"] <= asof - pd.DateOffset(years=1))]
    cur_y = fd[(fd["_판매일"] >= y_start) & (fd["_판매일"] <= asof)]
    prev_y = fd[(fd["_판매일"] >= y_start - pd.DateOffset(years=1)) & (fd["_판매일"] <= asof - pd.DateOffset(years=1))]

    rows = _wk_rows()
    bm = _wk_block(cur_m, prev_m, rows)   # 당월
    by = _wk_block(cur_y, prev_y, rows)   # 누계

    # 표 구성: 행(섹션/구분/세부) × 열(블록×지표) — 공용 프레임 함수
    idx = [k for k, _ in rows]
    if not _filtered:
        inject_plan(by, idx, master)   # 연간 사업계획·진도율 주입 (필터 없을 때만)
    sty, _disp_main = _wk_style_table(bm, by, idx, cy, py)

    h1, h2 = st.columns([5, 1])
    h1.markdown(f"**주간보고 · 기준일 {asof.date()}**  (당월 {m_start.date()} → {asof.date()} · 누계 {y_start.date()} → {asof.date()})"
                f"{_NOTE_FLOAT}", unsafe_allow_html=True)
    # 엑셀 다운로드 — ⚠️ 메인 표 전용 '특별 조건': 팀 주간보고 양식(weekly_template.xlsx) 템플릿에
    # 값을 채워 내려받는 방식. 룰11(일반 엑셀 버튼)의 예외이므로 절대 일반 방식으로 바꾸지 말 것.
    xls_bytes = weekly_excel_bytes(rows, bm, by, asof, cy, py)
    h2.download_button("⬇ 엑셀", xls_bytes, file_name=f"주간보고_{asof.date()}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="wk_dl", use_container_width=True)
    render_styled_table(sty)   # 룰3·4 + 헤더검정 + G.TOTAL 노란강조
    st.caption("※ 유통별 5개는 주요 채널만 (직영몰·특수채널·K2K이관 등은 G.TOTAL엔 포함, 유통 행엔 미표기). "
               "S/D/L 신상=신상+내년신상, 4년차↑는 합계엔 포함되나 별도 행 없음. 사업계획·진도율은 목표 입력 후 채워짐.")

    # ── 매장 담당별 분석 (위 표와 동일 프레임, 행만 담당자) ──
    _MGR_BOTTOM = ["없음", "26년 미운영", "직원구매"]   # 비담당 라벨 → 맨 아래(이 순서)
    managers = []
    if "_담당자" in d.columns:
        _mset = {m for m in d["_담당자"].dropna().astype(str).str.strip()
                 if m and m.lower() not in ("nan", "none")}
        managers = sorted(m for m in _mset if m not in _MGR_BOTTOM) + [m for m in _MGR_BOTTOM if m in _mset]
    if managers:
        st.divider()
        mrows = [(("전체", "G.TOTAL", "합계"), lambda x: pd.Series(True, index=x.index))]
        for nm in managers:
            mrows.append((("담당별", nm, "합계"),
                          (lambda name: (lambda x: x["_담당자"].astype(str).str.strip() == name))(nm)))
        bm2 = _wk_block(cur_m, prev_m, mrows)
        by2 = _wk_block(cur_y, prev_y, mrows)
        if not _filtered:
            inject_plan_manager(by2, [k for k, _ in mrows], master)   # 담당자 매장 연간계획 합
        sty2, disp2 = _wk_style_table(bm2, by2, [k for k, _ in mrows], cy, py)
        # 룰11: 제목 + 우측 일반 엑셀 다운로드 버튼 (2026-07-31)
        g1, g2 = st.columns([5, 1])
        g1.markdown("##### 👤 매장 담당별 분석" + _NOTE_FLOAT, unsafe_allow_html=True)
        g2.download_button("⬇ 엑셀", table_excel_bytes(disp2, "매장 담당별 분석"),
                           file_name=f"매장담당별분석_{asof.date()}.xlsx", mime=XLSX_MIME,
                           key="wk_dl_mgr", use_container_width=True)
        render_styled_table(sty2)
        st.caption("※ 담당별 = 매장 마스터의 담당자 기준. 담당 미지정 매장은 담당 행엔 미포함(G.TOTAL엔 포함). 비중=행÷전체.")

    st.divider()
    st.markdown("##### 🔍 (드릴다운 1) 유통별/담당별 매장 상세 보기")
    NONE, HEAD_C, HEAD_M = "(선택 안 함)", "─ 유통별 ─", "─ 담당별 ─"
    opts = [NONE, HEAD_C] + list(_CHANNEL_MASKS.keys())
    if managers:
        opts += [HEAD_M] + managers
    sel = st.selectbox("유통 또는 담당자를 선택하면 그 그룹의 매장별 지표가 같은 형식으로 펼쳐져요.",
                       opts, key="wk_drill")
    if sel not in (NONE, HEAD_C, HEAD_M):
        if sel in _CHANNEL_MASKS:
            _mask = _CHANNEL_MASKS[sel]
        else:
            _mask = (lambda nm: (lambda x: x["_담당자"].astype(str).str.strip() == nm))(sel)
        render_weekly_drilldown(cur_m, prev_m, cur_y, prev_y, sel, _mask, cy, py, show_plan=not _filtered)

    st.divider()
    st.markdown("##### 🔍 (드릴다운 2) 매장별/담당별 아이템분석")
    sv = cur_y.assign(_c=cur_y["매장코드"].astype(str).str.strip()).groupby("_c")["_매출액"].sum().sort_values(ascending=False)
    nmap = dict(zip(d["매장코드"].astype(str).str.strip(), d["매장명"].astype(str)))
    labels = [f"{nmap.get(c, c)} ({c})" for c in sv.index]
    code_of = dict(zip(labels, sv.index))
    I_NONE, I_HM, I_HS = "(선택 안 함)", "─ 담당별 ─", "─ 매장별 ─"
    iopts = [I_NONE]
    if managers:
        iopts += [I_HM] + managers
    iopts += [I_HS] + labels
    isel = st.selectbox("담당자(담당 전체매장 기준) 또는 매장을 선택하면 아이템그룹별 지표가 같은 형식으로 펼쳐져요.",
                        iopts, key="wk_item_drill")
    if isel not in (I_NONE, I_HM, I_HS):
        if isel in managers:
            imask = (lambda nm: (lambda x: x["_담당자"].astype(str).str.strip() == nm))(isel)
            render_weekly_item_drilldown(cur_m, prev_m, cur_y, prev_y, f"{isel} (담당 전체매장)", imask, cy, py)
        else:
            code = code_of[isel]
            imask = (lambda c: (lambda x: x["매장코드"].astype(str).str.strip() == c))(code)
            render_weekly_item_drilldown(cur_m, prev_m, cur_y, prev_y, isel, imask, cy, py)


# ==============================================================================
# 로그인 / 사용자 관리  ─ 개인 계정 + 역할(admin=관리자 / viewer=뷰어)
# ==============================================================================
USERS_TABLE = "app_users"


def _make_hash(password):
    """PBKDF2-SHA256 해시 문자열 'pbkdf2$iter$salt$hash' 생성. 평문 비번은 저장하지 않는다."""
    salt = os.urandom(16).hex()
    it = 200_000
    h = hashlib.pbkdf2_hmac("sha256", str(password).encode("utf-8"), bytes.fromhex(salt), it).hex()
    return f"pbkdf2${it}${salt}${h}"


def _verify_pw(password, stored):
    try:
        _algo, it, salt, h = str(stored).split("$")
        calc = hashlib.pbkdf2_hmac("sha256", str(password).encode("utf-8"), bytes.fromhex(salt), int(it)).hex()
        return hmac.compare_digest(calc, h)
    except Exception:
        return False


def ensure_users_table():
    eng = get_engine()
    with eng.begin() as conn:
        conn.exec_driver_sql(
            f'CREATE TABLE IF NOT EXISTS "{USERS_TABLE}" ('
            '"username" TEXT PRIMARY KEY, "display_name" TEXT, "pw" TEXT, '
            '"role" TEXT DEFAULT \'viewer\', "active" TEXT DEFAULT \'Y\', "created_at" TEXT)')


def get_user(username):
    eng = get_engine()
    with eng.connect() as conn:
        r = conn.execute(text(
            f'SELECT "username","display_name","pw","role","active" FROM "{USERS_TABLE}" WHERE "username"=:u'),
            {"u": username}).fetchone()
    if not r:
        return None
    return {"username": r[0], "display_name": r[1], "pw": r[2], "role": r[3], "active": r[4]}


def list_users():
    eng = get_engine()
    try:
        with eng.connect() as conn:
            rows = conn.execute(text(
                f'SELECT "username","display_name","role","active" FROM "{USERS_TABLE}" ORDER BY "role","username"')).fetchall()
    except Exception:
        return []
    return [{"username": r[0], "display_name": r[1], "role": r[2], "active": r[3]} for r in rows]


def user_count():
    ensure_users_table()
    with get_engine().connect() as conn:
        return conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{USERS_TABLE}"').scalar()


def upsert_user(username, display_name, password, role, active="Y"):
    """계정 생성/수정. password가 비어 있으면(None) 비번은 그대로 두고 나머지만 갱신."""
    ensure_users_table()
    eng = get_engine()
    exists = get_user(username) is not None
    with eng.begin() as conn:
        if exists:
            if password:
                conn.execute(text(f'UPDATE "{USERS_TABLE}" SET "display_name"=:d,"pw"=:p,"role"=:r,"active"=:a WHERE "username"=:u'),
                             {"d": display_name, "p": _make_hash(password), "r": role, "a": active, "u": username})
            else:
                conn.execute(text(f'UPDATE "{USERS_TABLE}" SET "display_name"=:d,"role"=:r,"active"=:a WHERE "username"=:u'),
                             {"d": display_name, "r": role, "a": active, "u": username})
        else:
            conn.execute(text(f'INSERT INTO "{USERS_TABLE}" ("username","display_name","pw","role","active","created_at") '
                              'VALUES (:u,:d,:p,:r,:a,:c)'),
                         {"u": username, "d": display_name, "p": _make_hash(password or os.urandom(8).hex()),
                          "r": role, "a": active, "c": datetime.now().strftime("%Y-%m-%d")})


def delete_user(username):
    with get_engine().begin() as conn:
        conn.execute(text(f'DELETE FROM "{USERS_TABLE}" WHERE "username"=:u'), {"u": username})


# ── 로그인 유지(쿠키 세션) ────────────────────────────────────────────
# 로그인 상태를 브라우저 메모리(st.session_state)에만 두면 새로고침·잠깐 방치로
# 바로 재로그인이 필요했음 → 로그인 시 토큰을 발급해 브라우저 쿠키 + DB(app_sessions,
# 해시만 저장)에 두고, '마지막 사용 후 2시간'까지는 자동으로 로그인을 이어준다.
SESSIONS_TABLE = "app_sessions"
AUTH_COOKIE = "erp_auth"
IDLE_LIMIT_HOURS = 2          # 이 시간 동안 사용이 없으면 자동 만료 → 재로그인
_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _hash_token(token):
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def ensure_sessions_table():
    with get_engine().begin() as conn:
        conn.exec_driver_sql(
            f'CREATE TABLE IF NOT EXISTS "{SESSIONS_TABLE}" ('
            '"token_hash" TEXT PRIMARY KEY, "username" TEXT, "last_seen" TEXT)')


def create_session(username):
    """새 로그인 토큰 발급. DB에는 해시만 저장(유출 대비). 오래된 세션은 청소."""
    ensure_sessions_table()
    token = os.urandom(32).hex()
    now = datetime.now()
    with get_engine().begin() as conn:
        cutoff = (now - pd.Timedelta(days=7)).strftime(_TS_FMT)
        conn.execute(text(f'DELETE FROM "{SESSIONS_TABLE}" WHERE "last_seen" < :c'), {"c": cutoff})
        conn.execute(text(f'INSERT INTO "{SESSIONS_TABLE}" ("token_hash","username","last_seen") '
                          'VALUES (:t,:u,:s)'),
                     {"t": _hash_token(token), "u": username, "s": now.strftime(_TS_FMT)})
    return token


def _session_user(token):
    """쿠키 토큰 검증: 마지막 사용이 2시간 이내면 사용자명 반환(+시간 갱신), 아니면 None."""
    if not token:
        return None
    th = _hash_token(token)
    now = datetime.now()
    try:
        ensure_sessions_table()
        with get_engine().begin() as conn:
            r = conn.execute(text(
                f'SELECT "username","last_seen" FROM "{SESSIONS_TABLE}" WHERE "token_hash"=:t'),
                {"t": th}).fetchone()
            if not r:
                return None
            try:
                last = datetime.strptime(str(r[1]), _TS_FMT)
            except Exception:
                last = None
            if last is None or (now - last).total_seconds() > IDLE_LIMIT_HOURS * 3600:
                conn.execute(text(f'DELETE FROM "{SESSIONS_TABLE}" WHERE "token_hash"=:t'), {"t": th})
                return None
            conn.execute(text(f'UPDATE "{SESSIONS_TABLE}" SET "last_seen"=:s WHERE "token_hash"=:t'),
                         {"s": now.strftime(_TS_FMT), "t": th})
        return r[0]
    except Exception:
        return None


def touch_session():
    """화면을 쓸 때마다 마지막 사용시간 갱신 → 2시간 카운트가 계속 리셋됨."""
    tok = st.session_state.get("auth_token")
    if not tok:
        return
    try:
        with get_engine().begin() as conn:
            conn.execute(text(f'UPDATE "{SESSIONS_TABLE}" SET "last_seen"=:s WHERE "token_hash"=:t'),
                         {"s": datetime.now().strftime(_TS_FMT), "t": _hash_token(tok)})
    except Exception:
        pass


def drop_session():
    """로그아웃: DB 세션 즉시 삭제(쿠키가 남아 있어도 더 이상 못 씀)."""
    tok = st.session_state.get("auth_token")
    if not tok:
        return
    try:
        with get_engine().begin() as conn:
            conn.execute(text(f'DELETE FROM "{SESSIONS_TABLE}" WHERE "token_hash"=:t'), {"t": _hash_token(tok)})
    except Exception:
        pass


def _write_cookie(value, max_age):
    """브라우저 쿠키 기록/삭제(로그인 유지용). 0높이 컴포넌트로 JS만 실행 — 화면·로그인 흐름에 영향 없음.

    ※ 쿠키 기록이 실패해도 로그인은 st.session_state로 이미 성공한 상태라 사용엔 지장 없음
      (그 경우 하드새로고침 때만 재로그인 — 기존과 동일).
    """
    components.html(
        "<script>"
        f'document.cookie = "{AUTH_COOKIE}={value}; path=/; max-age={int(max_age)}; SameSite=Lax";'
        "</script>", height=0)


def _cookie_token():
    """브라우저가 보낸 로그인 유지 쿠키 읽기(구버전 Streamlit이면 None)."""
    try:
        return st.context.cookies.get(AUTH_COOKIE)
    except Exception:
        return None


def _render_login():
    st.title("🔐 온라인팀 미니 ERP")
    if user_count() == 0:
        st.info("최초 관리자 계정을 만들어 주세요. (계정이 하나도 없을 때만 나오는 화면이에요)")
        with st.form("bootstrap_admin"):
            u = st.text_input("관리자 ID")
            dn = st.text_input("이름 (표시용)")
            p1 = st.text_input("비밀번호", type="password")
            p2 = st.text_input("비밀번호 확인", type="password")
            ok = st.form_submit_button("관리자 계정 만들기", type="primary")
        if ok:
            if not u.strip() or not p1:
                st.error("ID와 비밀번호를 입력하세요.")
            elif p1 != p2:
                st.error("비밀번호가 일치하지 않아요.")
            else:
                upsert_user(u.strip(), dn.strip() or u.strip(), p1, "admin")
                st.success("관리자 계정 생성 완료! 이제 아래에서 로그인하세요.")
                st.rerun()
        return
    with st.form("login"):
        u = st.text_input("ID")
        p = st.text_input("비밀번호", type="password")
        ok = st.form_submit_button("로그인", type="primary")
    if ok:
        rec = get_user(u.strip())
        if rec and str(rec["active"]).upper() != "N" and _verify_pw(p, rec["pw"]):
            # 로그인은 세션에 즉시 반영(쿠키 성공 여부와 무관하게 항상 성공).
            # 로그인 유지 쿠키는 로그인 후 화면(사이드바)에서 매 실행 기록/갱신.
            token = create_session(rec["username"])
            st.session_state["auth_user"] = rec["username"]
            st.session_state["auth_name"] = rec["display_name"] or rec["username"]
            st.session_state["auth_role"] = rec["role"]
            st.session_state["auth_token"] = token
            st.rerun()
        else:
            st.error("ID·비밀번호가 올바르지 않거나 비활성화된 계정이에요.")
    st.caption("계정이 필요하면 관리자(팀장)에게 요청하세요.")


def render_user_admin():
    """관리자용 사용자 관리: 목록/활성토글/삭제 + 추가·비번 재설정."""
    me = st.session_state.get("auth_user")
    users = list_users()
    n_admin_active = sum(1 for u in users
                         if u["role"] == "admin" and str(u["active"]).upper() != "N")
    for us in users:
        act = str(us["active"]).upper() != "N"
        is_adm = us["role"] == "admin"
        c = st.columns([2.5, 1.2, 1.1, 0.9])
        c[0].caption(f"{'🟢' if act else '⚪'} **{us['display_name']}** ({us['username']}) · "
                     f"{'관리자' if is_adm else '뷰어'}")
        # 역할 전환 버튼 (2026-07-31 추가): 목록에서 바로 관리자↔뷰어 변경
        if c[1].button("뷰어로" if is_adm else "관리자로", key=f"role_{us['username']}", use_container_width=True):
            if is_adm and us["username"] == me:
                st.warning("본인 계정의 역할은 스스로 낮출 수 없어요. (관리자 잠금 방지)")
            elif is_adm and n_admin_active <= 1:
                st.warning("마지막 관리자는 뷰어로 바꿀 수 없어요.")
            else:
                upsert_user(us["username"], us["display_name"], None,
                            "viewer" if is_adm else "admin", us["active"])
                st.rerun()
        if c[2].button("비활성" if act else "활성화", key=f"tgl_{us['username']}", use_container_width=True):
            # 잠금 사고 방지: 본인 계정·마지막 관리자 계정은 비활성 불가 (2026-07-31 잠금 사고 재발 방지)
            if act and us["username"] == me:
                st.warning("본인 계정은 비활성화할 수 없어요. (잠금 사고 방지)")
            elif act and is_adm and n_admin_active <= 1:
                st.warning("마지막 관리자 계정은 비활성화할 수 없어요.")
            else:
                upsert_user(us["username"], us["display_name"], None, us["role"], "N" if act else "Y")
                st.rerun()
        if c[3].button("삭제", key=f"del_{us['username']}", use_container_width=True):
            if us["username"] == me:
                st.warning("본인 계정은 삭제할 수 없어요.")
            else:
                delete_user(us["username"])
                st.rerun()
    st.caption("— 계정 추가 / 비밀번호 재설정 (기존 ID면 갱신) —")
    with st.form("add_user", clear_on_submit=True):
        nu = st.text_input("ID")
        nn = st.text_input("이름")
        npw = st.text_input("비밀번호", type="password")
        nr = st.selectbox("역할", ["viewer", "admin"],
                          format_func=lambda x: "관리자" if x == "admin" else "뷰어")
        ok = st.form_submit_button("저장", use_container_width=True)
    if ok:
        if not nu.strip():
            st.error("ID를 입력하세요.")
        elif get_user(nu.strip()) is None and not npw:
            st.error("새 계정은 비밀번호가 필요해요.")
        elif nu.strip() == me and nr != "admin":
            st.error("본인 계정의 역할은 뷰어로 바꿀 수 없어요. (관리자 잠금 방지)")
        else:
            # 기존 계정 갱신 시 이름을 비워 두면 기존 표시 이름 유지 (ID로 덮어쓰지 않음)
            prev = get_user(nu.strip())
            keep_name = nn.strip() or (prev["display_name"] if prev else nu.strip())
            upsert_user(nu.strip(), keep_name, npw or None, nr)
            st.success(f"저장 완료 ✅ {nu.strip()}")
            st.rerun()


def main():
    st.set_page_config(page_title="온라인팀 미니 ERP", page_icon="📊", layout="wide")
    # 전역 여백 축소: 요소 간격·헤더 하단여백을 줄여 타이틀을 표에 바짝 붙임
    st.markdown("""
        <style>
        [data-testid="stVerticalBlock"]{gap:0.4rem;}
        [data-testid="stMarkdownContainer"] h3,
        [data-testid="stMarkdownContainer"] h5{margin-bottom:0.35rem;padding-bottom:0;}
        </style>""", unsafe_allow_html=True)
    # ── 로그인 게이트 ──────────────────────────────────────────────
    ensure_users_table()
    if not st.session_state.get("auth_user"):
        # 1) 쿠키(로그인 유지 토큰)로 자동 로그인 — 마지막 사용 2시간 이내면 유지
        tok = _cookie_token()
        uname = _session_user(tok) if tok else None
        rec = get_user(uname) if uname else None
        if rec and str(rec["active"]).upper() != "N":
            st.session_state["auth_user"] = rec["username"]
            st.session_state["auth_name"] = rec["display_name"] or rec["username"]
            st.session_state["auth_role"] = rec["role"]
            st.session_state["auth_token"] = tok
        else:
            # 2) 유효한 토큰이 없으면 로그인 화면
            _render_login()
            return
    touch_session()   # 사용 중엔 매 동작마다 2시간 카운트 리셋
    is_admin = st.session_state.get("auth_role") == "admin"

    st.title("📊 온라인팀 미니 ERP · 매출 분석")
    fresh_slot = st.container()   # 타이틀 바로 아래: 매출 데이터 최종 업데이트 일자 표기 자리

    with st.sidebar:
        # 로그인 유지 쿠키를 매 실행 기록/갱신 (수명 30일 — 실제 만료는 서버가 '마지막 사용 2시간'으로 판정)
        if st.session_state.get("auth_token"):
            _write_cookie(st.session_state["auth_token"], 30 * 24 * 3600)
        st.caption(f"👋 **{st.session_state.get('auth_name','')}**님 "
                   f"· {'관리자' if is_admin else '뷰어'}")
        if st.button("🚪 로그아웃", use_container_width=True):
            drop_session()   # DB 세션 삭제 — 쿠키가 브라우저에 남아 있어도 즉시 무효
            for _k in ("auth_user", "auth_name", "auth_role", "auth_token"):
                st.session_state.pop(_k, None)
            _write_cookie("", 0)   # 쿠키 삭제 시도
            st.rerun()
        st.metric("현재 DB 누적", f"{db_row_count():,} 건")
        if st.button("🔄 새로고침(캐시 비우기)", use_container_width=True):
            load_db.clear(); load_master.clear(); st.rerun()

        if not is_admin:
            st.divider()
            st.caption("🔒 조회 전용(뷰어) 계정이에요. 데이터 업로드·적재·삭제는 관리자만 할 수 있어요.")

        if is_admin:
            st.divider()
            st.header("⚙️ 데이터 관리")
            st.caption(f"저장소: **{backend_name()}**")
            ups = st.file_uploader("① 로우데이터 업로드 (여러 개 한 번에 가능)",
                                   type=["xlsx", "xls", "csv"], accept_multiple_files=True)
            if ups:
                st.caption(f"{len(ups)}개 파일 선택됨")
                overwrite = st.checkbox(
                    "♻️ 덮어쓰기 모드 (파일에 있는 날짜는 먼저 삭제 후 적재)",
                    help="당일 매출처럼 ERP 값이 바뀌는 경우 켜세요. 업로드한 파일에 포함된 '날짜'의 "
                         "기존 데이터를 먼저 지우고 새로 넣습니다(수정·취소분까지 정확히 교체). "
                         "파일에 없는 다른 날짜는 그대로 둡니다. 끄면 기존 방식(중복 건너뛰고 추가만).")
                if st.button("② DB에 적재하기", type="primary", use_container_width=True):
                    tn = ts = dn = 0; last = db_row_count()
                    deleted_dates = set()
                    prog = st.progress(0.0)
                    status = st.empty()
                    for i, f in enumerate(ups):
                        try:
                            status.caption(f"⏳ ({i+1}/{len(ups)}) {f.name} 처리 중…")
                            clean = add_row_key(enrich(read_raw_file(f)))
                            if overwrite and "_판매일" in clean.columns:
                                fdates = sorted(clean["_판매일"].dropna().dt.strftime("%Y-%m-%d").unique())
                                todo = [d for d in fdates if d not in deleted_dates]
                                if todo:
                                    dn += delete_dates(todo)
                                    deleted_dates.update(todo)
                            res = append_to_db(clean)
                            tn += res["inserted"]; ts += res["skipped"]; last = res["total_after"]
                            del clean, res            # 파일별 메모리 즉시 해제 (OOM 방지)
                            gc.collect()
                        except Exception as ex:
                            st.error(f"{f.name} 오류: {ex}")
                            gc.collect()
                        prog.progress((i + 1) / len(ups))
                    status.empty()
                    load_db.clear()
                    if overwrite:
                        st.success(f"덮어쓰기 적재 완료 ✅ 삭제 {dn:,} · 신규 {tn:,} / 중복 {ts:,} · DB 총 {last:,}건")
                        if deleted_dates:
                            st.caption("교체된 날짜: " + ", ".join(sorted(deleted_dates)))
                    else:
                        st.success(f"적재 완료 ✅ 신규 {tn:,} / 중복 {ts:,} · DB 총 {last:,}건")

            st.divider()
            st.caption(f"🏬 매장 기준정보(태그): 현재 **{master_row_count():,}개** 매장")
            mup = st.file_uploader("매장 기준정보 업로드 (담당자·유통성격·채널소유·채널스토리)",
                                   type=["xlsx", "xls", "csv"], accept_multiple_files=False, key="master_up")
            if mup is not None:
                if st.button("🏬 매장 기준정보 적용(전체 교체)", use_container_width=True):
                    try:
                        n = replace_master(read_master_file(mup))
                        load_master.clear()
                        st.success(f"매장 기준정보 갱신 완료 ✅ {n}개 매장")
                    except Exception as ex:
                        st.error(f"매장 기준정보 오류: {ex}")

            st.divider()
            st.caption(f"🎯 사업계획(월별 목표): 현재 **{plan_row_count():,}행**")
            pup = st.file_uploader("사업계획 업로드 (매장별·브랜드별 월별 목표)",
                                   type=["xlsx", "xls"], accept_multiple_files=False, key="plan_up")
            if pup is not None:
                if st.button("🎯 사업계획 적용(전체 교체)", use_container_width=True):
                    try:
                        n = replace_plan(read_plan_file(pup))
                        load_plan.clear()
                        st.success(f"사업계획 갱신 완료 ✅ {n:,}행")
                    except Exception as ex:
                        st.error(f"사업계획 오류: {ex}")

            st.divider()
            with st.expander("👤 사용자 관리 (계정 추가·권한·비활성)"):
                render_user_admin()

    df = load_db()
    # 타이틀 아래 최종 업데이트 일자 (매출 로우데이터의 마지막 판매일자 = 데이터가 채워진 마지막 날)
    if not df.empty and "_판매일" in df.columns and df["_판매일"].notna().any():
        _last = df["_판매일"].max()
        fresh_slot.caption(
            f"🗓️ **매출 로우데이터 최종 업데이트 일자 : {_last.year}년 {_last.month:02d}월 {_last.day:02d}일**"
            "  (이 날짜까지의 매출이 입력되어 있어요)")
    if df.empty:
        st.info("👈 사이드바에서 매출 로우데이터를 업로드하고 [DB에 적재하기]를 눌러 시작하세요.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["📋 주간회의 보고자료",
                                      "📅 연차·아이템 세부분석 (플래그십)",
                                      "📈 유통채널·브랜드 주간현황",
                                      "📊 종합 대시보드"])
    with tab1:
        render_weekly_report(df)
    with tab2:
        render_flagship(df)
    with tab3:
        render_channel_brand(df)
    with tab4:
        render_dashboard(df, df)


if __name__ == "__main__":
    main()
