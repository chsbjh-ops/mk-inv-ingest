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
#   (MAST_ID, SERIES_CD, SERIES_NM, 시트명 키워드)  — 한 워크북의 여러 시트로 구성
CMDI_MAP = [
    (7, "CMDI_MKT", "회사채 시장 디스트레스(CMDI 전체)", ["market", "aggregate", "agg", "cmdi"]),
    (8, "CMDI_IG",  "회사채 시장 디스트레스(CMDI IG)",   ["investment", "grade", "ig"]),
    (9, "CMDI_HY",  "회사채 시장 디스트레스(CMDI HY)",   ["high", "yield", "hy"]),
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
def _extract_single(raw, start):
    """헤더 없는/오프셋된 시트에서 (날짜열, 값열)을 값 기반으로 자동 식별.
       - 날짜열 = 날짜 파싱 최다 열
       - 값열   = 날짜열 제외, 숫자 최다 열"""
    if raw is None or raw.shape[1] < 2:
        return []
    dcounts = {j: pd.to_datetime(raw[j], errors="coerce").notna().sum() for j in raw.columns}
    dcol = max(dcounts, key=dcounts.get)
    if dcounts[dcol] < 12:
        return []
    vcol, vcnt = None, 0
    for j in raw.columns:
        if j == dcol:
            continue
        cnt = pd.to_numeric(raw[j], errors="coerce").notna().sum()
        if cnt > vcnt:
            vcnt, vcol = cnt, j
    if vcol is None:
        return []
    dates = pd.to_datetime(raw[dcol], errors="coerce")
    vals = pd.to_numeric(raw[vcol], errors="coerce")
    s0 = pd.to_datetime(start)
    out = []
    for d, v in zip(dates, vals):
        if pd.notna(d) and pd.notna(v) and d >= s0:
            out.append((_td(d), float(v)))
    return out

def fetch_cmdi(start):
    if not CMDI_URL:
        log.warning("[CMDI] URL 미설정 — 건너뜀"); return []
    rows = []
    try:
        r = requests.get(CMDI_URL, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        sheets = pd.read_excel(io.BytesIO(r.content), sheet_name=None, header=None)  # 전체 시트
        log.info("[CMDI] 시트 목록: %s", list(sheets.keys()))
    except Exception as e:
        log.error("[CMDI] 다운로드/열기 실패: %s", e); return []

    names = list(sheets.keys())
    used_sheets = set()
    for mid, scd, snm, kw in CMDI_MAP:
        sh = next((n for n in names if n not in used_sheets
                   and any(k in n.lower() for k in kw)), None)
        if sh is None and mid == 7 and len(names) == 1:       # 단일 시트면 시장으로
            sh = names[0]
        if sh is None:
            log.warning("[CMDI] 시트 매칭 실패 %s (시트:%s)", scd, names); continue
        used_sheets.add(sh)
        pairs = _extract_single(sheets[sh], start)
        log.info("[CMDI] %s ← 시트 '%s' %d행", scd, sh, len(pairs))
        rows += [{"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm, "TD": td, "VALUE": v}
                 for td, v in pairs]
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
