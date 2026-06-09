# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_yfinance_daily_ingest.py
 Purpose     : MK_YFINANCE(글로벌 ETF·지수) 31종의 종가를 yfinance 로 수집하여
               MK_YFINANCE_DATA_DB 포맷의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-09

 출력 포맷 (MK_YFINANCE_DATA_DB)
 ----------------------------------------------------------------------------
   MAST_ID, SERIES_CD, SERIES_NM, TD(YYYYMMDD), VALUE
   ※ 앞 컬럼은 정의서 시트와 일치. VALUE = 일별 종가(Close).

 수집 방식
 ----------
   매 실행마다 START(기본 2026-05-01)~오늘 구간을 조회해 누적 마스터에
   (MAST_ID, TD) 중복 없이 병합(keep="last" → 수정값 최신화).

 필요 패키지
 ----------
   pip install yfinance openpyxl pandas
==============================================================================
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("MK_YF")

# -----------------------------------------------------------------------------
# 지표 정의 (MK_YFINANCE_MAST 기준)
#   series_cd : 정의서 SERIES_CD (코스피 등 한글명 포함)
#   yf        : 실제 yfinance 티커 (한국 지수는 표준 심볼로 매핑)
# -----------------------------------------------------------------------------
INDICATORS = [
    # MAST_ID, series_cd,   yf,           short_nm
    (4,  "^GSPC",     "^GSPC",     "S&P 500 Index"),
    (5,  "VOO",       "VOO",       "Vanguard S&P 500 ETF"),
    (6,  "SPY",       "SPY",       "SPDR S&P 500 ETF Trust"),
    (8,  "VEA",       "VEA",       "Vanguard FTSE Developed Markets ETF"),
    (10, "VWO",       "VWO",       "Vanguard FTSE Emerging Markets ETF"),
    (12, "ACWI",      "ACWI",      "iShares MSCI ACWI ETF"),
    (15, "코스피",     "^KS11",     "KOSPI Composite Index"),
    (16, "코스닥",     "^KQ11",     "KOSDAQ Composite Index"),
    (17, "코스피200",  "^KS200",    "KOSPI 200 Index"),
    (19, "232080.KS", "232080.KS", "TIGER 코스닥150 ETF"),
    (21, "USMV",      "USMV",      "iShares MSCI USA Min Vol Factor ETF"),
    (22, "MVOL.L",    "MVOL.L",    "iShares Edge MSCI World Min Vol UCITS ETF"),
    (26, "BIL",       "BIL",       "SPDR Bloomberg 1-3 Month T-Bill ETF"),
    (28, "SHY",       "SHY",       "iShares 1-3 Year Treasury Bond ETF"),
    (30, "GOVT",      "GOVT",      "iShares U.S. Treasury Bond ETF"),
    (32, "BWX",       "BWX",       "SPDR Bloomberg Intl Treasury Bond ETF"),
    (33, "EGOV.L",    "EGOV.L",    "UBS JPM Global Government ESG Liquid Bond"),
    (35, "AGGG.L",    "AGGG.L",    "iShares Core Global Aggregate Bond UCITS"),
    (38, "QLTA",      "QLTA",      "iShares Aaa-A Rated Corporate Bond ETF"),
    (40, "IEAC.AS",   "IEAC.AS",   "iShares Core € Corp Bond UCITS ETF"),
    (42, "FLRN",      "FLRN",      "SPDR Bloomberg IG Floating Rate ETF"),
    (43, "EFRN.DE",   "EFRN.DE",   "iShares € Floating Rate Bond UCITS ETF"),
    (46, "MBB",       "MBB",       "iShares MBS ETF"),
    (47, "VMBS",      "VMBS",      "Vanguard Mortgage-Backed Securities ETF"),
    (49, "CMBS",      "CMBS",      "iShares CMBS ETF"),
    (52, "VNQ",       "VNQ",       "Vanguard Real Estate ETF"),
    (55, "IFRA",      "IFRA",      "iShares U.S. Infrastructure ETF"),
    (57, "IGF",       "IGF",       "iShares Global Infrastructure ETF"),
    (59, "PSQA",      "PSQA",      "Palmer Square CLO Senior Debt ETF"),
    (62, "SRLN",      "SRLN",      "SPDR Blackstone Senior Loan ETF"),
    (64, "BUYO",      "BUYO",      "KraneShares Man Buyout Beta Index ETF"),
]

DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "TD", "VALUE"]

START_DEFAULT = "2026-05-01"
SLEEP_BETWEEN = 0.3
VALUE_DP = 4


def fetch_close(yf_ticker, start, end):
    import yfinance as yf
    df = yf.download(yf_ticker, start=start, end=end + timedelta(days=1),
                     progress=False, auto_adjust=False, threads=False)
    if df is None or df.empty:
        return pd.Series(dtype=float)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return pd.to_numeric(df["Close"], errors="coerce").dropna()


def collect_one(meta, start, end) -> list[dict]:
    mast_id, series_cd, yf_ticker, nm = meta
    try:
        s = fetch_close(yf_ticker, start, end)
        if s.empty:
            log.warning("[%2d %-10s] 데이터 없음 (yf:%s)", mast_id, series_cd, yf_ticker)
            return []
        rows = [{"MAST_ID": mast_id, "SERIES_CD": series_cd, "SERIES_NM": nm,
                 "TD": pd.Timestamp(idx).strftime("%Y%m%d"),
                 "VALUE": round(float(v), VALUE_DP)} for idx, v in s.items()]
        log.info("[%2d %-10s] %3d행 수집 (yf:%s)", mast_id, series_cd, len(rows), yf_ticker)
        return rows
    except Exception as exc:
        log.error("[%2d %-10s] 수집 실패: %s", mast_id, series_cd, str(exc)[:120])
        return []


def save_excel(df_new: pd.DataFrame, out_dir: str) -> dict:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    daily_path = out / f"MK_YFINANCE_DATA_{today}.xlsx"
    df_new.to_excel(daily_path, sheet_name="MK_YFINANCE_DATA_DB", index=False)

    master_path = out / "mk_yfinance_data_master.xlsx"
    if master_path.exists():
        old = pd.read_excel(master_path, sheet_name="MK_YFINANCE_DATA_DB", dtype={"TD": str})
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged["TD"] = merged["TD"].astype(str)
    merged = (merged.drop_duplicates(subset=["MAST_ID", "TD"], keep="last")
                    .sort_values(["MAST_ID", "TD"]).reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_YFINANCE_DATA_DB", index=False)
    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(df_new), "master_rows": len(merged)}


def run(start: str = START_DEFAULT, out_dir: str = "./MK_YFINANCE") -> dict:
    end = datetime.now().date()
    start_d = pd.to_datetime(start).date()
    log.info("MK_YFINANCE 수집 시작 : %s ~ %s (%d종)", start_d, end, len(INDICATORS))
    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start_d, end))
        time.sleep(SLEEP_BETWEEN)
    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/패키지를 확인하세요.")
        return {}
    df = pd.DataFrame(all_rows, columns=DATA_COLS).sort_values(["MAST_ID", "TD"]).reset_index(drop=True)
    result = save_excel(df, out_dir)
    ok = df["MAST_ID"].nunique()
    log.info("완료 : 종목 %d/%d종, 신규 %d행 → %s", ok, len(INDICATORS), result["new_rows"], result["master"])
    if ok < len(INDICATORS):
        miss = sorted(set(m[0] for m in INDICATORS) - set(df["MAST_ID"].unique()))
        log.warning("미수집 MAST_ID: %s (티커 점검 필요)", miss)
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_YF_START", START_DEFAULT),
        out_dir=os.environ.get("MK_YF_OUT", "./MK_YFINANCE"))
