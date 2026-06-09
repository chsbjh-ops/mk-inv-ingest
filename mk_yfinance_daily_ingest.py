# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_yfinance_daily_ingest.py
 Purpose     : MK_YFINANCE(yfinance 통합 수집) — BM ETF + INV 시장지표를
               하나로 모아 OHLC+등락률 형식의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-09
 Note        : 기존 MK_INV 의 yfinance 수집분(환율·지수·원자재 13종)을 이 파이프라인으로
               통합. USE_GB(BM/INV) 컬럼으로 DB 단계에서 용도별 분리 가능.
               (국고채·VKOSPI·미국채는 별도/타 소스에서 처리)

 출력 포맷 (MK_YFINANCE_DATA_DB)
 ----------------------------------------------------------------------------
   MAST_ID, SERIES_CD, SERIES_NM, USE_GB, TD(YYYYMMDD),
   CLOSE_PRC, ADJ_CLOSE_PRC, OPEN_PRC, HIGH_PRC, LOW_PRC, VOLUME, CHG_RT
   - USE_GB        : 'BM'(벤치마크 ETF) | 'INV'(시장지표)
   - ADJ_CLOSE_PRC : 수정종가(배당·분할 반영, yfinance Adj Close)
   - VOLUME        : 거래량 보유 종목만 '664.51M' 형식 문자열
   - CHG_RT        : 전일 종가(Close) 대비 등락률(소수)

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
# 지표 정의
#   use_gb : BM(벤치마크 ETF) | INV(시장지표)
#   yf     : yfinance 티커
#   vol    : 거래량 보유여부(Y/N)
#   dp     : 가격 반올림 소수 자릿수
# -----------------------------------------------------------------------------
INDICATORS = [
    # ── INV 시장지표 (구 MK_INV yfinance 13종) ──────────────────────────────
    # MAST_ID, series_cd,       series_nm,                     use,   yf,         vol, dp
    (1,  "USD",          "원/달러 환율",                  "INV", "KRW=X",     "N", 2),
    (2,  "CNY",          "원/위안 환율",                  "INV", "CNYKRW=X",  "N", 2),
    (3,  "JPY",          "원/엔 환율",                    "INV", "JPYKRW=X",  "N", 4),
    (4,  "VIX",          "VIX 변동성지수",                "INV", "^VIX",      "N", 2),
    (6,  "WTI",          "WTI 원유",                      "INV", "CL=F",      "N", 2),
    (7,  "XAU",          "금(Gold) 현물",                 "INV", "GC=F",      "Y", 2),
    (13, "KS11",         "코스피(KOSPI) 지수",            "INV", "^KS11",     "Y", 2),
    (14, "KS200",        "코스피200 지수",                "INV", "^KS200",    "Y", 2),
    (15, "KQ11",         "코스닥(KOSDAQ) 지수",           "INV", "^KQ11",     "Y", 2),
    (16, "DJI",          "다우존스 산업평균지수",         "INV", "^DJI",      "Y", 2),
    (17, "SP500(SPX)",   "S&P500 지수",                   "INV", "^GSPC",     "N", 2),
    (18, "NASDAQ(IXIC)", "나스닥 종합지수",               "INV", "^IXIC",     "Y", 2),
    (19, "SOX",          "필라델피아 반도체지수",         "INV", "^SOX",      "N", 2),
    # ── BM 벤치마크 ETF (11종) ──────────────────────────────────────────────
    (5,  "VOO",          "Vanguard S&P 500 ETF",                    "BM", "VOO",     "Y", 2),
    (8,  "VEA",          "Vanguard FTSE Developed Markets ETF",     "BM", "VEA",     "Y", 2),
    (10, "VWO",          "Vanguard FTSE Emerging Markets ETF",      "BM", "VWO",     "Y", 2),
    (26, "BIL",          "SPDR Bloomberg 1-3 Month T-Bill ETF",     "BM", "BIL",     "Y", 2),
    (30, "GOVT",         "iShares U.S. Treasury Bond ETF",          "BM", "GOVT",    "Y", 2),
    (32, "BWX",          "SPDR Bloomberg Intl Treasury Bond ETF",   "BM", "BWX",     "Y", 2),
    (38, "QLTA",         "iShares Aaa-A Rated Corporate Bond ETF",  "BM", "QLTA",    "Y", 2),
    (40, "IEAC.AS",      "iShares Core € Corp Bond UCITS ETF",      "BM", "IEAC.AS", "Y", 2),
    (46, "MBB",          "iShares MBS ETF",                         "BM", "MBB",     "Y", 2),
    (47, "VMBS",         "Vanguard Mortgage-Backed Securities ETF", "BM", "VMBS",    "Y", 2),
    (57, "IGF",          "iShares Global Infrastructure ETF",       "BM", "IGF",     "Y", 2),
]

DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "USE_GB", "TD",
             "CLOSE_PRC", "ADJ_CLOSE_PRC", "OPEN_PRC", "HIGH_PRC", "LOW_PRC",
             "VOLUME", "CHG_RT"]

START_DEFAULT = "2026-05-01"
SLEEP_BETWEEN = 0.3


def fmt_volume(v):
    """거래량 → '664.51M' 형식 축약 문자열."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(v) or v == 0:
        return None
    for unit, div in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= div:
            return f"{v / div:.2f}{unit}"
    return f"{v:.0f}"


def fetch_ohlcv(yf_ticker, start, end):
    import yfinance as yf
    df = yf.download(yf_ticker, start=start, end=end + timedelta(days=1),
                     progress=False, auto_adjust=False, threads=False)
    if df is None or df.empty:
        return pd.DataFrame()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    if "Adj Close" not in df.columns:      # auto_adjust=False 면 보통 존재
        df["Adj Close"] = df["Close"]
    return df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]


def collect_one(meta, start, end) -> list[dict]:
    mast_id, series_cd, series_nm, use_gb, yf_ticker, vol_yn, dp = meta
    try:
        df = fetch_ohlcv(yf_ticker, start, end)
        if df.empty:
            log.warning("[%2d %-12s] 데이터 없음 (%s)", mast_id, series_cd, yf_ticker)
            return []
        df = df.sort_index()
        prev_close, rows = None, []
        for idx, r in df.iterrows():
            close = r.get("Close")
            if pd.isna(close):
                continue
            chg = round(close / prev_close - 1, 4) if prev_close else None
            rows.append({
                "MAST_ID": mast_id, "SERIES_CD": series_cd, "SERIES_NM": series_nm,
                "USE_GB": use_gb,
                "TD": pd.Timestamp(idx).strftime("%Y%m%d"),
                "CLOSE_PRC": round(float(close), dp),
                "ADJ_CLOSE_PRC": None if pd.isna(r.get("Adj Close")) else round(float(r.get("Adj Close")), dp),
                "OPEN_PRC": None if pd.isna(r.get("Open")) else round(float(r.get("Open")), dp),
                "HIGH_PRC": None if pd.isna(r.get("High")) else round(float(r.get("High")), dp),
                "LOW_PRC":  None if pd.isna(r.get("Low"))  else round(float(r.get("Low")),  dp),
                "VOLUME":   fmt_volume(r.get("Volume")) if vol_yn == "Y" else None,
                "CHG_RT":   chg,
            })
            prev_close = close
        log.info("[%2d %-12s] %3d행 수집 (%s:%s)", mast_id, series_cd, len(rows), use_gb, yf_ticker)
        return rows
    except Exception as exc:
        log.error("[%2d %-12s] 수집 실패: %s", mast_id, series_cd, str(exc)[:120])
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
                    .sort_values(["USE_GB", "MAST_ID", "TD"]).reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_YFINANCE_DATA_DB", index=False)
    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(df_new), "master_rows": len(merged)}


def run(start: str = START_DEFAULT, out_dir: str = "./MK_YFINANCE") -> dict:
    end = datetime.now().date()
    start_d = pd.to_datetime(start).date()
    n_bm = sum(1 for m in INDICATORS if m[3] == "BM")
    n_inv = sum(1 for m in INDICATORS if m[3] == "INV")
    log.info("MK_YFINANCE 수집 시작 : %s ~ %s (총 %d종 = BM %d + INV %d)",
             start_d, end, len(INDICATORS), n_bm, n_inv)
    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start_d, end))
        time.sleep(SLEEP_BETWEEN)
    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/패키지를 확인하세요.")
        return {}
    df = pd.DataFrame(all_rows, columns=DATA_COLS).sort_values(["USE_GB", "MAST_ID", "TD"]).reset_index(drop=True)
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
