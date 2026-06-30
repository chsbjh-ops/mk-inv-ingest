#!/usr/bin/env python3
# =====================================================================
# File        : mk_nyfed_daily_ingest.py
# Purpose     : MK_NYFED — 뉴욕 연준(NY Fed) 무료 공식 데이터 9종을 수집하여
#               MK_NYFED_DATA(MAST_ID, SERIES_CD, SERIES_NM, TD, VALUE)로 적재.
#               * 라이선스 제약 없는 공공 데이터(ACM·CMDI·Reference Rates).
# 출력        : data/MK_NYFED_DATA_<today>.xlsx (일자별) + mk_nyfed_data_master.xlsx (누적)
#               시트명 'MK_NYFED_DATA_DB', (MAST_ID, TD) 기준 중복제거.
# 컨벤션      : mk_fred_daily_ingest.py 와 동일(env MK_NYFED_OUT / MK_NYFED_START, run()).
# 소스(3종)   : 1) ACM Term Premia CSV   → MAST 1~3 (텀프리미엄·금리분해, 일별)
#               2) Markets Reference Rates API → MAST 4~6 (EFFR/BGCR/TGCR, 일별)
#               3) CMDI 데이터 파일       → MAST 7~9 (회사채 시장 디스트레스, 월별)
# =====================================================================
import os, io, sys, logging
from datetime import date, datetime
from pathlib import Path
import requests
import pandas as pd

log = logging.getLogger("MK_NYFED")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

START_DEFAULT = "2020-01-01"      # 1회 백필 시작일. 매 실행마다 이 날짜~오늘 재조회(멱등).
HTTP_TIMEOUT  = 60
_UA = {"User-Agent": "Mozilla/5.0 (BIGSET MK_NYFED ingest)"}
DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "TD", "VALUE"]

# ── 엔드포인트(최초 1회 확인 권장) ───────────────────────────────────
ACM_CSV       = "https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.csv"
REF_SECURED   = "https://markets.newyorkfed.org/api/rates/secured/all/last/{n}.json"     # SOFR/BGCR/TGCR
REF_UNSECURED = "https://markets.newyorkfed.org/api/rates/unsecured/effr/last/{n}.json"   # EFFR
CMDI_XLSX     = "https://www.newyorkfed.org/medialibrary/media/research/policy/cmdi/CMDI_data.xlsx"  # 파일명 확인 권장

# ── MAST 매핑 ────────────────────────────────────────────────────────
ACM_MAP = [  # (MAST_ID, SERIES_CD, SERIES_NM, ACM CSV 컬럼)
    (1, "ACM_TP_10Y",  "텀프리미엄(ACM 10Y)",       "ACMTP10"),
    (2, "ACM_RNY_10Y", "기대 단기금리(ACM RN 10Y)", "ACMRNY10"),
    (3, "ACM_Y_10Y",   "적합수익률(ACM 10Y)",       "ACMY10"),
]
REF_MAP = {  # Markets API 'type' → (MAST_ID, SERIES_CD, SERIES_NM)
    "EFFR": (4, "EFFR", "실효 연방기금금리(EFFR)"),
    "BGCR": (5, "BGCR", "광의 GC 레포금리(BGCR)"),
    "TGCR": (6, "TGCR", "삼자간 GC 레포금리(TGCR)"),
}
CMDI_MAP = [  # (MAST_ID, SERIES_CD, SERIES_NM, 컬럼 후보 키워드)
    (7, "CMDI_MKT", "회사채 시장 디스트레스(CMDI 전체)", ["cmdi", "market", "agg"]),
    (8, "CMDI_IG",  "회사채 시장 디스트레스(CMDI IG)",   ["ig", "investment"]),
    (9, "CMDI_HY",  "회사채 시장 디스트레스(CMDI HY)",   ["hy", "high"]),
]

def _td(d): return pd.to_datetime(d).strftime("%Y%m%d")

def fetch_acm(start):
    rows = []
    try:
        r = requests.get(ACM_CSV, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        dcol = next(c for c in df.columns if c.upper() in ("DATE", "OBS_DATE"))
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
        df = df[df[dcol] >= pd.to_datetime(start)]
        for mid, scd, snm, col in ACM_MAP:
            if col not in df.columns:
                log.warning("[ACM] 컬럼 없음 %s", col); continue
            for _, x in df[[dcol, col]].dropna().iterrows():
                rows.append({"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
                             "TD": _td(x[dcol]), "VALUE": float(x[col])})
        log.info("[ACM] %d행", len(rows))
    except Exception as e:
        log.error("[ACM] 실패: %s", e)
    return rows

def fetch_refrates(start, n=2500):
    rows = []
    for url in (REF_SECURED.format(n=n), REF_UNSECURED.format(n=n)):
        try:
            r = requests.get(url, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
            for rec in r.json().get("refRates", []):
                t = (rec.get("type") or "").upper()
                if t not in REF_MAP or rec.get("percentRate") is None:
                    continue
                if rec["effectiveDate"] < start:
                    continue
                mid, scd, snm = REF_MAP[t]
                rows.append({"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
                             "TD": _td(rec["effectiveDate"]), "VALUE": float(rec["percentRate"])})
        except Exception as e:
            log.error("[REF] 실패 %s: %s", url, e)
    log.info("[REF] %d행", len(rows))
    return rows

def fetch_cmdi(start):
    rows = []
    try:
        r = requests.get(CMDI_XLSX, headers=_UA, timeout=HTTP_TIMEOUT); r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))
        df.columns = [str(c).strip().lower() for c in df.columns]
        dcol = next(c for c in df.columns if "date" in c or c == "month")
        df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
        df = df[df[dcol] >= pd.to_datetime(start)]
        for mid, scd, snm, keys in CMDI_MAP:
            col = next((c for c in df.columns if any(k in c for k in keys) and c != dcol), None)
            if not col:
                log.warning("[CMDI] 컬럼 매칭 실패 %s", scd); continue
            for _, x in df[[dcol, col]].dropna().iterrows():
                rows.append({"MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": snm,
                             "TD": _td(x[dcol]), "VALUE": float(x[col])})
        log.info("[CMDI] %d행", len(rows))
    except Exception as e:
        log.error("[CMDI] 실패(파일명/포맷 확인 권장): %s", e)
    return rows

def run(start: str = START_DEFAULT, out_dir: str = "./MK_NYFED") -> dict:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")

    rows = fetch_acm(start) + fetch_refrates(start) + fetch_cmdi(start)
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
