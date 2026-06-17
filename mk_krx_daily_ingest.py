# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_krx_daily_ingest.py
 Purpose     : MK_KRX — 국내 투자자별 수급 + ETF NAV/괴리율을 pykrx 로 매일 수집
 Author      : PM (Bigset)
 Created     : 2026-06-17
 Source      : pykrx (KRX 정보데이터시스템 스크래핑, 무료 / 키 불필요)

 수집 범위
 ----------------------------------------------------------------------------
   A. 투자자별 순매수(거래대금)  : KOSPI·KOSDAQ × (외국인/기관합계/연기금/개인)
        → 뉴스레터 '수급 — 외국인의 귀환' 섹션
   B. ETF NAV·괴리율·거래대금·시총 : 전체 ETF 일별 스냅샷
        → 뉴스레터 'ETF 동향' 섹션 (괴리율 알람 / 순유입 랭킹 기초)

 미포함(후속) : 외국인 채권·국채선물 수급, 선물 수급, 외국인 잔액비중,
              ETF 순설정(상장좌수 기반 순유입) → 별도 소스/계산 필요

 출력 (2개 시트)
   MK_KRX_FLOW_DB : MAST_ID, SERIES_CD, SERIES_NM, MARKET, INVESTOR, TD, NET_BUY_EOK
   MK_KRX_ETF_DB  : TD, TICKER, NAME, CLOSE, NAV, DISPARITY_PCT, VALUE_TRD_EOK, MKTCAP_EOK

 필요 패키지 : pykrx, pandas, openpyxl
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
log = logging.getLogger("MK_KRX")

# -----------------------------------------------------------------------------
# 투자자별 수급 시리즈 정의
#   MARKET × INVESTOR 조합마다 MAST_ID 부여. INVESTOR_KEY 는 pykrx 인덱스 라벨
#   (한글)에 대한 부분일치 키 (라벨 변형 흡수).
# -----------------------------------------------------------------------------
FLOW_SERIES = [
    # mast_id, series_cd,      market,   investor_key, series_nm
    (1, "KOSPI_FORGN",  "KOSPI",  "외국인",   "외국인 코스피 순매수"),
    (2, "KOSPI_INST",   "KOSPI",  "기관합계", "기관계 코스피 순매수"),
    (3, "KOSPI_PENS",   "KOSPI",  "연기금",   "연기금 코스피 순매수"),
    (4, "KOSPI_INDIV",  "KOSPI",  "개인",     "개인 코스피 순매수"),
    (5, "KOSDAQ_FORGN", "KOSDAQ", "외국인",   "외국인 코스닥 순매수"),
    (6, "KOSDAQ_INST",  "KOSDAQ", "기관합계", "기관계 코스닥 순매수"),
    (7, "KOSDAQ_PENS",  "KOSDAQ", "연기금",   "연기금 코스닥 순매수"),
    (8, "KOSDAQ_INDIV", "KOSDAQ", "개인",     "개인 코스닥 순매수"),
]

FLOW_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "MARKET", "INVESTOR", "TD", "NET_BUY_EOK"]
ETF_COLS = ["TD", "TICKER", "NAME", "CLOSE", "NAV", "DISPARITY_PCT", "VALUE_TRD_EOK", "MKTCAP_EOK"]

START_DEFAULT = "20260501"
EOK = 1e8                       # 억원 환산
SLEEP = 0.4


def _bdays(start: str, end: str):
    """영업일 목록. pykrx 우선, 실패 시 평일 fallback."""
    from pykrx import stock
    try:
        days = stock.get_previous_business_days(fromdate=start, todate=end)
        return [d.strftime("%Y%m%d") for d in days]
    except Exception:
        rng = pd.bdate_range(pd.to_datetime(start), pd.to_datetime(end))
        return [d.strftime("%Y%m%d") for d in rng]


def _match_investor(df: pd.DataFrame, key: str):
    """순매수 행 추출: 인덱스(투자자 라벨) 공백제거 부분일치."""
    k = key.replace(" ", "")
    for idx in df.index:
        if k in str(idx).replace(" ", ""):
            col = "순매수" if "순매수" in df.columns else df.columns[-1]
            return df.loc[idx, col]
    return None


# -----------------------------------------------------------------------------
# A. 투자자별 순매수
# -----------------------------------------------------------------------------
def collect_flow(days) -> list[dict]:
    from pykrx import stock
    rows: list[dict] = []
    cache: dict[tuple, pd.DataFrame] = {}
    for td in days:
        for market in ("KOSPI", "KOSDAQ"):
            try:
                key = (td, market)
                if key not in cache:
                    cache[key] = stock.get_market_trading_value_by_investor(td, td, market)
                df = cache[key]
                if df is None or df.empty:
                    continue
                for mid, scd, mk, inv_key, nm in FLOW_SERIES:
                    if mk != market:
                        continue
                    val = _match_investor(df, inv_key)
                    if val is None or pd.isna(val):
                        continue
                    rows.append({
                        "MAST_ID": mid, "SERIES_CD": scd, "SERIES_NM": nm,
                        "MARKET": market, "INVESTOR": inv_key, "TD": td,
                        "NET_BUY_EOK": round(float(val) / EOK, 1),
                    })
            except Exception as exc:
                log.warning("  수급 %s/%s 실패: %s", td, market, str(exc)[:100])
            time.sleep(SLEEP)
    log.info("투자자 수급: %d행 수집 (%d영업일)", len(rows), len(days))
    return rows


# -----------------------------------------------------------------------------
# B. ETF NAV / 괴리율 (전체 ETF 일별 스냅샷)
# -----------------------------------------------------------------------------
def collect_etf(days) -> list[dict]:
    from pykrx import stock
    rows: list[dict] = []
    name_cache: dict[str, str] = {}
    for td in days:
        try:
            df = stock.get_etf_ohlcv_by_ticker(td)        # 전체 ETF 한 번에
        except Exception as exc:
            log.warning("  ETF %s 실패: %s", td, str(exc)[:100])
            time.sleep(SLEEP)
            continue
        if df is None or df.empty:
            continue
        for tkr, r in df.iterrows():
            close = r.get("종가")
            nav = r.get("NAV")
            if not close or not nav or pd.isna(close) or pd.isna(nav) or nav == 0:
                disp = None
            else:
                disp = round((float(close) / float(nav) - 1) * 100, 3)
            if tkr not in name_cache:
                try:
                    name_cache[tkr] = stock.get_etf_ticker_name(tkr)
                except Exception:
                    name_cache[tkr] = ""
            val_trd = r.get("거래대금")
            mktcap = r.get("시가총액")
            rows.append({
                "TD": td, "TICKER": tkr, "NAME": name_cache.get(tkr, ""),
                "CLOSE": None if pd.isna(close) else float(close),
                "NAV": None if pd.isna(nav) else float(nav),
                "DISPARITY_PCT": disp,
                "VALUE_TRD_EOK": None if (val_trd is None or pd.isna(val_trd)) else round(float(val_trd) / EOK, 1),
                "MKTCAP_EOK": None if (mktcap is None or pd.isna(mktcap)) else round(float(mktcap) / EOK, 1),
            })
        time.sleep(SLEEP)
    log.info("ETF 스냅샷: %d행 수집", len(rows))
    return rows


def _save(df_new, master_name, sheet, out_dir, key_cols):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    daily = out / f"{master_name.replace('master','').strip('_')}_{today}.xlsx"
    df_new.to_excel(daily, sheet_name=sheet, index=False)
    mpath = out / master_name
    if mpath.exists():
        old = pd.read_excel(mpath, sheet_name=sheet, dtype={"TD": str})
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged["TD"] = merged["TD"].astype(str)
    merged = merged.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
    merged.to_excel(mpath, sheet_name=sheet, index=False)
    return str(mpath), len(df_new), len(merged)


def run(start=START_DEFAULT, out_dir="./MK_KRX"):
    end = datetime.now().strftime("%Y%m%d")
    days = _bdays(start, end)
    log.info("MK_KRX 수집 시작 : %s ~ %s (%d영업일)", start, end, len(days))

    flow = collect_flow(days)
    etf = collect_etf(days)

    res = {}
    if flow:
        df = pd.DataFrame(flow, columns=FLOW_COLS).sort_values(["MAST_ID", "TD"]).reset_index(drop=True)
        p, n, m = _save(df, "mk_krx_flow_master.xlsx", "MK_KRX_FLOW_DB", out_dir, ["MAST_ID", "TD"])
        log.info("수급 저장: 신규 %d행 / 누적 %d행 → %s", n, m, p)
        res["flow"] = p
    if etf:
        df = pd.DataFrame(etf, columns=ETF_COLS).sort_values(["TD", "TICKER"]).reset_index(drop=True)
        p, n, m = _save(df, "mk_krx_etf_master.xlsx", "MK_KRX_ETF_DB", out_dir, ["TD", "TICKER"])
        log.info("ETF 저장: 신규 %d행 / 누적 %d행 → %s", n, m, p)
        res["etf"] = p
    if not res:
        log.error("수집 결과가 비었습니다. pykrx/네트워크를 확인하세요.")
    return res


if __name__ == "__main__":
    run(start=os.environ.get("MK_KRX_START", START_DEFAULT),
        out_dir=os.environ.get("MK_KRX_OUT", "./MK_KRX"))
