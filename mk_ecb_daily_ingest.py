# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_ecb_daily_ingest.py
 Purpose     : MK_ECB(유로 금리) 10종을 ECB Data Portal API 로 수집하여
               MK_ECB_DATA_DB 포맷의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-09
 Source      : ECB Data Portal REST API (무료, 키 불필요)
               https://data-api.ecb.europa.eu/service/data/{flow}/{key}

 시리즈 키 (조사 확정)
 ----------------------------------------------------------------------------
   €STR 계열(dataflow EST):
     €STR-1M(복리평균)  : EST / B.EU000A2QQF24.CR
     €STR-3M(복리평균)  : EST / B.EU000A2QQF32.CR
     €STR(금리)         : EST / B.EU000A2X2A25.WT  (거래량가중 절사평균)
     €STR_INDEX(복리지수): EST / B.EU000A2QQF08.CI
   유로존 국채금리(dataflow YC, G_N_C = '전체 국채'):
     1·2·3·5·7·10년 spot : YC / B.U2.EUR.4F.G_N_C.SV_C_YM.SR_{n}Y

 출력 포맷 (MK_ECB_DATA_DB)
 ----------------------------------------------------------------------------
   MAST_ID, SERIES_CD(ECB 키), SERIES_NM, TD(YYYYMMDD), VALUE

 필요 패키지
 ----------
   pip install requests openpyxl pandas
==============================================================================
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("MK_ECB")

# -----------------------------------------------------------------------------
# 지표 정의 (MK_ECB_MAST 기준, MAST_ID 1~10)
#   flow : ECB dataflow (EST | YC)
#   key  : dataflow 내 시리즈 키
# -----------------------------------------------------------------------------
INDICATORS = [
    # MAST_ID, flow,  key,                                  nm
    (1,  "EST", "B.EU000A2QQF24.CR",                  "€STR 1개월 복리평균"),
    (2,  "EST", "B.EU000A2QQF32.CR",                  "€STR 3개월 복리평균"),
    (3,  "EST", "B.EU000A2X2A25.WT",                  "€STR 금리"),
    (4,  "EST", "B.EU000A2QQF08.CI",                  "€STR 복리지수"),
    (5,  "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_1Y",    "유로존 국채금리 1년"),
    (6,  "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_2Y",    "유로존 국채금리 2년"),
    (7,  "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_3Y",    "유로존 국채금리 3년"),
    (8,  "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_5Y",    "유로존 국채금리 5년"),
    (9,  "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_7Y",    "유로존 국채금리 7년"),
    (10, "YC",  "B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y",   "유로존 국채금리 10년"),
]

DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "TD", "VALUE"]

START_DEFAULT = "2026-05-01"
HTTP_TIMEOUT = 60
RETRY_MAX = 5
RETRY_BASE = 4.0
SLEEP_BETWEEN = 0.5
VALUE_DP = 6
_UA = {"User-Agent": "Mozilla/5.0 (compatible; bigset-ecb-ingest/1.0)",
       "Accept": "text/csv"}


def _http_get(url, params=None):
    last = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, headers=_UA, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as exc:
            last = exc
            log.warning("  요청 재시도 %d/%d (%s)", attempt, RETRY_MAX, str(exc)[:120])
            time.sleep(RETRY_BASE * attempt)
    raise last


def fetch_ecb(flow, key, start, end):
    """ECB Data Portal CSV (csvdata). TIME_PERIOD / OBS_VALUE 파싱."""
    url = f"https://data-api.ecb.europa.eu/service/data/{flow}/{key}"
    r = _http_get(url, params={"startPeriod": str(start), "endPeriod": str(end),
                               "format": "csvdata"})
    df = pd.read_csv(io.StringIO(r.text))
    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        raise RuntimeError(f"예상치 못한 응답 컬럼: {list(df.columns)[:8]}")
    s = pd.Series(pd.to_numeric(df["OBS_VALUE"], errors="coerce").values,
                  index=pd.to_datetime(df["TIME_PERIOD"]))
    return s.dropna().sort_index()


def collect_one(meta, start, end) -> list[dict]:
    mast_id, flow, key, nm = meta
    try:
        s = fetch_ecb(flow, key, start, end)
        if s.empty:
            log.warning("[%2d %-34s] 데이터 없음", mast_id, key)
            return []
        rows = [{"MAST_ID": mast_id, "SERIES_CD": f"{flow}.{key}", "SERIES_NM": nm,
                 "TD": pd.Timestamp(idx).strftime("%Y%m%d"),
                 "VALUE": round(float(v), VALUE_DP)} for idx, v in s.items()]
        log.info("[%2d %-34s] %3d행 수집", mast_id, key, len(rows))
        return rows
    except Exception as exc:
        log.error("[%2d %-34s] 수집 실패: %s", mast_id, key, str(exc)[:120])
        return []


def save_excel(df_new: pd.DataFrame, out_dir: str) -> dict:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    daily_path = out / f"MK_ECB_DATA_{today}.xlsx"
    df_new.to_excel(daily_path, sheet_name="MK_ECB_DATA_DB", index=False)

    master_path = out / "mk_ecb_data_master.xlsx"
    if master_path.exists():
        old = pd.read_excel(master_path, sheet_name="MK_ECB_DATA_DB", dtype={"TD": str})
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged["TD"] = merged["TD"].astype(str)
    merged = (merged.drop_duplicates(subset=["MAST_ID", "TD"], keep="last")
                    .sort_values(["MAST_ID", "TD"]).reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_ECB_DATA_DB", index=False)
    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(df_new), "master_rows": len(merged)}


def run(start: str = START_DEFAULT, out_dir: str = "./MK_ECB") -> dict:
    end = datetime.now().date()
    start_d = pd.to_datetime(start).date()
    log.info("MK_ECB 수집 시작 : %s ~ %s (%d종)", start_d, end, len(INDICATORS))
    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start_d, end))
        time.sleep(SLEEP_BETWEEN)
    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/시리즈 키를 확인하세요.")
        return {}
    df = pd.DataFrame(all_rows, columns=DATA_COLS).sort_values(["MAST_ID", "TD"]).reset_index(drop=True)
    result = save_excel(df, out_dir)
    ok = df["MAST_ID"].nunique()
    log.info("완료 : 시리즈 %d/%d종, 신규 %d행 → %s", ok, len(INDICATORS), result["new_rows"], result["master"])
    if ok < len(INDICATORS):
        miss = sorted(set(m[0] for m in INDICATORS) - set(df["MAST_ID"].unique()))
        log.warning("미수집 MAST_ID: %s (시리즈 키 점검 필요)", miss)
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_ECB_START", START_DEFAULT),
        out_dir=os.environ.get("MK_ECB_OUT", "./MK_ECB"))
