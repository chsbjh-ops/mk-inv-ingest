#!/usr/bin/env python3
# =====================================================================
# File        : mk_nyfed_daily_ingest.py
# Purpose     : MK_NYFED — 뉴욕 연준(NY Fed) 무료 공식 데이터를 수집하여
#               MK_NYFED_DATA(MAST_ID, SERIES_CD, SERIES_NM, TD, VALUE)로 적재.
#               * 라이선스 제약 없는 공공 데이터(ACM·Reference Rates·CMDI).
# 출력        : data/MK_NYFED_DATA_<today>.xlsx (일자별) + mk_nyfed_data_master.xlsx (누적)
#               시트명 'MK_NYFED_DATA_DB', (MAST_ID, TD) 기준 중복제거.
# 컨벤션      : mk_fred_daily_ingest.py 와 동일(env MK_NYFED_OUT / MK_NYFED_START, run()).
# 소스(3종)   : 1) ACM Term Premia  .xls (시트 'ACM Daily') → MAST 1~3 (일별)
#               2) Markets Reference Rates search.json       → MAST 4~6 (일별)
#               3) CMDI 데이터 파일(URL 환경변수 주입)        → MAST 7~9 (월별, 선택)
# =====================================================================
import os, io, sys, logging, warnings
from datetime import date, datetime
from pathlib import Path
import requests
import pandas as pd

warnings.filterwarnings("ignore", message="Could not infer format")

log = logging.getLogger("MK_NYFED")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

START_DEFAULT = "2020-01-01"      # 1회 백필 시작일. 매 실행마다 이 날짜~오늘 재조회(멱등).
HTTP_TIMEOUT  = 60
_UA = {"User-Agent": "Mozilla/5.0 (BIGSET MK_NYFED ingest)"}
DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "TD", "VALUE"]

# ── 엔드포인트 ───────────────────────────────────────────────────────
#   ACM 은 .xls(이진 Excel) — 시트 'ACM Daily'.  (구 .csv 아님)
ACM_XLS = "https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls"
#   Reference Rates 는 last/N 이 아니라 날짜범위 search.json (secured=SOFR/BGCR/TGCR, unsecured=EFFR/OBFR)
REF_SECURED   = "https://markets.newyorkfed.org/api/rates/secured/all/search.json?startDate={s}&endDate={e}"
REF_UNSECURED = "https://markets.newyorkfed.org/api/rates/unsecured/all/search.json?startDate={s}&endDate={e}"
#   CMDI: 인터랙티브 다운로드 파일(시장·IG·HY 포함). 환경변수로 교체 가능.
#   ※ 환경변수가 빈 문자열("")이어도(워크플로에서 미설정 변수 주입 시) 기본 URL로 폴백.
CMDI_URL = (os.environ.get("MK_NYFED_CMDI_URL") or
    "https://www.newyorkfed.org/medialibrary/research/interactives/cmdi/downloads/Market%20CMDI.xlsx").strip()

# ── MAST 매핑 ────────────────────────────────────────────────────────
ACM_MAP = [  # (MAST_ID, SERIES_CD, SERIES_NM, ACM Daily 컬럼)
    (1, "ACM_TP_10Y",  "텀프리미엄(ACM 10Y)",       "ACMTP10"),
    (2, "ACM_RNY_10Y", "기대 단기금리(ACM RN 10Y)", "ACMRNY10"),
    (3, "ACM_Y_10Y",   "적합수익률(ACM 10Y)",       "ACMY10"),
]
REF_MAP = {  # Markets API 'type' → (MAST_ID, SERIES_CD, SERIES_NM)
    "EFFR": (4, "EFFR", "실효 연방기금금리(EFFR)"),
    "BGCR": (5, "BGCR", "광의 GC 레포금리(BGCR)"),
    "TGCR": (6, "TGCR", "삼자간 GC 레포금리(TGCR)"),
}
#   (MAST_ID, SERIES_CD, SERIES_NM, 별도파일명(없으면 시장파일에서), 값열 키워드)
CMDI_MAP = [
    (7, "CMDI_MKT", "회사채 시장 디스트레스(CMDI 전체)", None,                            ["market cmdi", "market", "cmdi", "index"]),
    (8, "CMDI_IG",  "회사채 시장 디스트레스(CMDI IG)",   "Investment%20Grade%20CMDI.xlsx", ["investment", "ig", "cmdi", "index"]),
    (9, "CMDI_HY",  "회사채 시장 디스트레스(CMDI HY)",   "High%20Yield%20CMDI.xlsx",       ["high", "hy", "cmdi", "index"]),
]

def _td(d): return pd.to_datetime(d).strftime("%Y%m%d")

# ── 1) ACM Term Premia (.xls, 시트 'ACM Daily') ──────────────────────
def fetch_acm(start):
    rows = []
    try:
        r = requests.get(ACM_XLS, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        try:
            df = pd.read_excel(io.BytesIO(r.content), sheet_name="ACM Daily", engine="xlrd")
        except Exception:
            df = pd.read_excel(io.BytesIO(r.content), sheet_name=0)   # 폴백: 첫 시트
        dcol = next((c for c in df.columns if "DATE" in str(c).upper()), df.columns[0])
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
        df = df[df[dcol] >= pd.to_datetime(start)]
        for mid, scd, snm, col in ACM_MAP:
            if col not in df.columns:
                log.warning("[ACM] 컬럼 없음 %s (가용:%s)", col, list(df.columns)[:6]); continue
            for _, x in df[[dcol, col]].dropna().iterrows():
                rows.append({"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
                             "TD": _td(x[dcol]), "VALUE": float(x[col])})
        log.info("[ACM] %d행", len(rows))
    except Exception as e:
        log.error("[ACM] 실패: %s", e)
    return rows

# ── 2) Reference Rates (search.json, 연 단위 청크) ───────────────────
def _ref_call(url):
    out = []
    try:
        r = requests.get(url, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        for rec in r.json().get("refRates", []):
            t = (rec.get("type") or "").upper()
            if t not in REF_MAP or rec.get("percentRate") is None:
                continue
            mid, scd, snm = REF_MAP[t]
            out.append({"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
                        "TD": _td(rec["effectiveDate"]), "VALUE": float(rec["percentRate"])})
    except Exception as e:
        log.error("[REF] 실패 %s: %s", url, e)
    return out

def fetch_refrates(start, end):
    rows = []
    y0, y1 = int(start[:4]), int(end[:4])
    for y in range(y0, y1 + 1):                       # 연 단위 청크(범위 제한 회피)
        s = max(start, f"{y}-01-01"); e = min(end, f"{y}-12-31")
        rows += _ref_call(REF_SECURED.format(s=s, e=e))
        rows += _ref_call(REF_UNSECURED.format(s=s, e=e))
    log.info("[REF] %d행", len(rows))
    return rows

# ── 3) CMDI (월별, URL 환경변수 주입 시에만) ─────────────────────────
_HDR_KW = ("date", "month", "cmdi", "market", "investment", "grade",
           "high", "yield", "index", "aggregate")

def _read_table(content, preview_tag=None):
    """헤더가 상단 설명행에 가려진 NY Fed 인터랙티브 파일 대응:
       ① 날짜값이 가장 많은 열을 찾아 그 첫 데이터행의 윗행을 헤더로(값 기반).
       ② 실패 시 키워드 포함 행으로 폴백."""
    raw = pd.read_excel(io.BytesIO(content), header=None)
    if preview_tag:                                        # 구조 진단용(넓게)
        log.info("[CMDI] %s 원시 상위행(최대 20행×16열):", preview_tag)
        for i in range(min(20, len(raw))):
            vals = [str(v)[:14] for v in raw.iloc[i].tolist()[:16]]
            log.info("   r%d: %s", i, vals)

    hdr = None
    # ① 값 기반: 날짜 파싱이 가장 많은 열 → 첫 날짜행 바로 위가 헤더
    best_j, best_cnt = None, 0
    for j in range(raw.shape[1]):
        cnt = pd.to_datetime(raw[j], errors="coerce").notna().sum()
        if cnt > best_cnt:
            best_cnt, best_j = cnt, j
    if best_j is not None and best_cnt >= 12:
        parsed = pd.to_datetime(raw[best_j], errors="coerce")
        first_data = int(parsed.notna().idxmax())
        hdr = max(first_data - 1, 0)
    # ② 키워드 폴백
    if hdr is None:
        hdr = 0
        for i in range(min(40, len(raw))):
            cells = [str(v).strip().lower() for v in raw.iloc[i].tolist()]
            hits = sum(any(k in c for k in _HDR_KW) for c in cells)
            if any("date" in c or c == "month" for c in cells) or hits >= 2:
                hdr = i; break

    df = pd.read_excel(io.BytesIO(content), header=hdr)
    df = df.dropna(axis=1, how="all")                      # 좌측 빈 열 제거
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _date_col(df):
    for c in df.columns:
        if "date" in c or c in ("month", "observation_date"):
            return c
    for c in df.columns:                                   # 폴백: 날짜 파싱률 높은 열
        if pd.to_datetime(df[c], errors="coerce").notna().mean() > 0.8:
            return c
    return df.columns[0]

def _extract(df, mid, scd, snm, keys, start, used=None):
    used = used if used is not None else set()
    dcol = _date_col(df)
    col = None
    for k in keys:
        col = next((c for c in df.columns if c != dcol and c not in used and k in c), None)
        if col:
            break
    if not col:
        log.warning("[CMDI] 열매칭 실패 %s (가용:%s)", scd, list(df.columns)); return []
    used.add(col)
    sub = df[[dcol, col]].copy()
    sub[dcol] = pd.to_datetime(sub[dcol], errors="coerce")
    sub = sub[sub[dcol].notna() & (sub[dcol] >= pd.to_datetime(start))].dropna()
    log.info("[CMDI] %s ← '%s' %d행", scd, col, len(sub))
    return [{"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
             "TD": _td(r[dcol]), "VALUE": float(r[col])} for _, r in sub.iterrows()]

def fetch_cmdi(start):
    if not CMDI_URL:
        log.warning("[CMDI] URL 미설정 — 건너뜀"); return []
    rows = []
    base = CMDI_URL.rsplit("/", 1)[0]
    # 1) 시장 파일(기본 URL): 가능한 열은 모두 추출(시장·IG·HY가 한 파일에 있을 수도 있음)
    mkt_df = None
    try:
        r = requests.get(CMDI_URL, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        mkt_df = _read_table(r.content, preview_tag="시장파일")
        log.info("[CMDI] 시장파일 열: %s", list(mkt_df.columns))
        used = set()
        for mid, scd, snm, _fn, keys in CMDI_MAP:
            rows += _extract(mkt_df, mid, scd, snm, keys, start, used)
    except Exception as e:
        log.error("[CMDI] 시장파일 실패: %s", e)
    # 2) 시장 파일에서 못 구한 IG/HY 는 형제 파일에서 보강
    got = {x["MAST_ID"] for x in rows}
    for mid, scd, snm, fn, keys in CMDI_MAP:
        if mid in got or not fn:
            continue
        url = f"{base}/{fn}"
        try:
            r = requests.get(url, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
            df = _read_table(r.content)
            rows += _extract(df, mid, scd, snm, keys, start)
        except Exception as e:
            log.warning("[CMDI] 형제파일 건너뜀 %s (%s): %s", scd, fn, e)
    log.info("[CMDI] 합계 %d행", len(rows))
    return rows

def run(start: str = START_DEFAULT, out_dir: str = "./MK_NYFED") -> dict:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    end = date.today().strftime("%Y-%m-%d")

    rows = fetch_acm(start) + fetch_refrates(start, end) + fetch_cmdi(start)
    if not rows:
        log.error("수집 0행 — 종료"); return {"new_rows": 0}
    df_new = pd.DataFrame(rows, columns=DATA_COLS)

    daily_path = out / f"MK_NYFED_DATA_{today}.xlsx"
    df_new.to_excel(daily_path, sheet_name="MK_NYFED_DATA_DB", index=False)

    master_path = out / "mk_nyfed_data_master.xlsx"
    if master_path.exists():
        old = pd.read_excel(master_path, sheet_name="MK_NYFED_DATA_DB", dtype={"TD": str})
        df_new = pd.concat([old, df_new], ignore_index=True)
    merged = (df_new.drop_duplicates(subset=["MAST_ID", "TD"], keep="last")
                    .sort_values(["MAST_ID", "TD"]).reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_NYFED_DATA_DB", index=False)

    log.info("완료 : 신규 %d행 → %s (누적 %d행)", len(rows), master_path, len(merged))
    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(rows), "master_rows": len(merged)}

if __name__ == "__main__":
    run(start=os.environ.get("MK_NYFED_START", START_DEFAULT),
        out_dir=os.environ.get("MK_NYFED_OUT", "./MK_NYFED"))
