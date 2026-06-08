# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_inv_daily_ingest.py
 Purpose     : MK_INV(금융시장지표) 19종을 무료 공개 API로 수집하여
               MK_INV_DATA_DB 포맷의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-08
 Context     : 기존 Investing.com 수기 입력을 대체.
               Investing.com 은 공식 API 부재 + Cloudflare 차단(investpy 동작 불가)
               이므로, 동일 지표를 안정적인 무료 소스로 대체 수집한다.

 데이터 소스 매핑 (값은 Investing.com 과 동일, 출처만 변경)
 ----------------------------------------------------------------------------
   yfinance                : 환율 3, 변동성(VIX), 원자재 2, 주가지수 7  = 13종
   pandas_datareader(FRED) : 미국 국채 3/10/30년                        = 3종  (키 불필요)
   FinanceDataReader       : 국고채 3/10년                              = 2종  (키 불필요)
   pykrx (KRX)             : VKOSPI                                     = 1종
                                                                       --------
                                                                          19종

 필요 패키지
 ----------
   pip install yfinance pandas_datareader finance-datareader pykrx openpyxl pandas

 수집 방식
 ----------
   매 실행마다 START(기본 2026-05-01) ~ 오늘 구간을 조회해 누적 마스터에
   (MAST_ID, TD) 기준 중복 없이 병합한다. 즉 첫 실행은 5/1부터 1회 백필,
   이후 실행은 새로 생긴 날짜만 실질적으로 추가된다.

 사용법 (Colab)
 ----------
   !pip install yfinance pandas_datareader finance-datareader pykrx openpyxl -q
   from mk_inv_daily_ingest import run
   run(start="2026-05-01", out_dir="/content/drive/MyDrive/Bigset/MK_INV")
==============================================================================
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("MK_INV")

# -----------------------------------------------------------------------------
# 1. 지표 정의 (MK_INV_MAST 기준, MAST_ID 1~19)
#    src    : yf | fred | fdr | krx
#    ticker : 각 소스별 심볼
#    vol    : 거래량 보유여부(VOL_YN). True 면 VOLUME 채움
#    dp     : 가격 반올림 소수 자릿수
# -----------------------------------------------------------------------------
INDICATORS = [
    # MAST_ID, INV_CD,        INV_NM,                       src,    ticker,        vol,   dp
    (1,  "USD",          "원/달러 환율",                    "yf",   "KRW=X",        False, 2),
    (2,  "CNY",          "원/위안 환율",                    "yf",   "CNYKRW=X",     False, 2),
    (3,  "JPY",          "원/엔 환율",                      "yf",   "JPYKRW=X",     False, 4),
    (4,  "VIX",          "VIX 변동성지수",                  "yf",   "^VIX",         False, 2),
    (5,  "KSVKOSPI",     "코스피200 변동성지수(VKOSPI)",    "krx",  "VKOSPI",       False, 2),
    (6,  "WTI",          "WTI 원유",                        "yf",   "CL=F",         False, 2),
    (7,  "XAU",          "금(Gold) 현물",                   "yf",   "GC=F",         True,  2),
    (8,  "KR3YT",        "국고채 3년 금리",                 "fdr",  "KR3YT=RR",     False, 3),
    (9,  "KR10YT",       "국고채 10년 금리",                "fdr",  "KR10YT=RR",    False, 3),
    (10, "US3YT",        "미국 국채 3년 금리",              "fred", "DGS3",         False, 3),
    (11, "US10YT",       "미국 국채 10년 금리",             "fred", "DGS10",        False, 3),
    (12, "US30YT",       "미국 국채 30년 금리",             "fred", "DGS30",        False, 3),
    (13, "KS11",         "코스피(KOSPI) 지수",              "yf",   "^KS11",        True,  2),
    (14, "KS200",        "코스피200 지수",                  "yf",   "^KS200",       True,  2),
    (15, "KQ11",         "코스닥(KOSDAQ) 지수",             "yf",   "^KQ11",        True,  2),
    (16, "DJI",          "다우존스 산업평균지수",           "yf",   "^DJI",         True,  2),
    (17, "SP500(SPX)",   "S&P500 지수",                     "yf",   "^GSPC",        False, 2),
    (18, "NASDAQ(IXIC)", "나스닥 종합지수",                 "yf",   "^IXIC",        True,  2),
    (19, "SOX",          "필라델피아 반도체지수",           "yf",   "^SOX",         False, 2),
]

DATA_COLS = ["MAST_ID", "INV_CD", "INV_NM", "TD",
             "CLOSE_PRC", "OPEN_PRC", "HIGH_PRC", "LOW_PRC", "VOLUME", "CHG_RT"]

START_DEFAULT = "2026-05-01"   # 1회 백필 시작일. 매 실행마다 이 날짜~오늘을 재조회·병합.


# -----------------------------------------------------------------------------
# 2. 유틸
# -----------------------------------------------------------------------------
def fmt_volume(v) -> str | None:
    """거래량을 Investing.com 표기(예: 664.51M)와 동일한 축약 문자열로 변환."""
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


def ohlc_to_rows(df: pd.DataFrame, meta) -> list[dict]:
    """소스에서 받은 OHLC(V) DataFrame -> MK_INV_DATA_DB 행 리스트."""
    mast_id, inv_cd, inv_nm, _src, _tk, has_vol, dp = meta
    df = df.sort_index()                       # 날짜 오름차순
    prev_close = None
    rows = []
    for idx, r in df.iterrows():
        close = r.get("Close")
        if pd.isna(close):
            continue                           # 휴장/결측은 건너뜀
        chg = round(close / prev_close - 1, 4) if prev_close else None
        rows.append({
            "MAST_ID": mast_id,
            "INV_CD": inv_cd,
            "INV_NM": inv_nm,
            "TD": pd.Timestamp(idx).strftime("%Y%m%d"),
            "CLOSE_PRC": round(float(close), dp),
            "OPEN_PRC": None if pd.isna(r.get("Open")) else round(float(r.get("Open")), dp),
            "HIGH_PRC": None if pd.isna(r.get("High")) else round(float(r.get("High")), dp),
            "LOW_PRC":  None if pd.isna(r.get("Low"))  else round(float(r.get("Low")),  dp),
            "VOLUME":   fmt_volume(r.get("Volume")) if has_vol else None,
            "CHG_RT":   chg,
        })
        prev_close = close
    return rows


# -----------------------------------------------------------------------------
# 3. 소스별 수집기 (각 지표 독립 호출 → 일부 실패해도 전체는 진행)
# -----------------------------------------------------------------------------
def fetch_yf(ticker, start, end):
    import yfinance as yf
    df = yf.download(ticker, start=start, end=end + timedelta(days=1),
                     progress=False, auto_adjust=False, threads=False)
    if isinstance(df.columns, pd.MultiIndex):       # 단일 티커도 MultiIndex 로 올 때 평탄화
        df.columns = df.columns.get_level_values(0)
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_fred(ticker, start, end):
    from pandas_datareader import data as pdr      # 키 불필요
    s = pdr.DataReader(ticker, "fred", start, end)[ticker]
    # 금리는 종가만 존재 → OHLC 를 종가로 채움(데이터 정의서와 동일 컬럼 유지)
    return pd.DataFrame({"Open": s, "High": s, "Low": s, "Close": s, "Volume": float("nan")})


def fetch_fdr(ticker, start, end):
    import FinanceDataReader as fdr
    df = fdr.DataReader(ticker, start, end)
    for c in ["Open", "High", "Low"]:
        if c not in df.columns:
            df[c] = df["Close"]
    if "Volume" not in df.columns:
        df["Volume"] = float("nan")
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_krx_vkospi(start, end):
    """VKOSPI(코스피200 변동성지수) — KRX. pykrx 사용.

    ※ VKOSPI 는 yfinance/FRED/ECOS 에 없는 KRX 고유 지표입니다.
      pykrx 버전/지수코드에 따라 동작이 달라질 수 있어, Colab 최초 실행 시
      반드시 값 검증을 권장합니다. (대안: KRX 정보데이터시스템 수기/별도 수집)
    """
    from pykrx import stock
    s, e = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    # KRX 변동성지수(VKOSPI) 지수코드: '1232'
    df = stock.get_index_ohlcv_by_date(s, e, "1232")
    df = df.rename(columns={"시가": "Open", "고가": "High", "저가": "Low",
                            "종가": "Close", "거래량": "Volume"})
    keep = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    return df[keep]


def collect_one(meta, start, end) -> list[dict]:
    mast_id, inv_cd, inv_nm, src, ticker, *_ = meta
    try:
        if src == "yf":
            df = fetch_yf(ticker, start, end)
        elif src == "fred":
            df = fetch_fred(ticker, start, end)
        elif src == "fdr":
            df = fetch_fdr(ticker, start, end)
        elif src == "krx":
            df = fetch_krx_vkospi(start, end)
        else:
            raise ValueError(f"unknown src {src}")
        if df is None or df.empty:
            log.warning("[%2d %-12s] 데이터 없음 (%s:%s)", mast_id, inv_cd, src, ticker)
            return []
        rows = ohlc_to_rows(df, meta)
        log.info("[%2d %-12s] %3d행 수집 (%s:%s)", mast_id, inv_cd, len(rows), src, ticker)
        return rows
    except Exception as exc:                         # 개별 지표 실패는 전체를 막지 않음
        log.error("[%2d %-12s] 수집 실패: %s", mast_id, inv_cd, exc)
        return []


# -----------------------------------------------------------------------------
# 4. 엑셀 저장 (당일 파일 + 누적 마스터 병합)
# -----------------------------------------------------------------------------
def save_excel(df_new: pd.DataFrame, out_dir: str) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")

    daily_path = out / f"MK_INV_DATA_{today}.xlsx"
    df_new.to_excel(daily_path, sheet_name="MK_INV_DATA_DB", index=False)

    master_path = out / "mk_inv_data_master.xlsx"
    if master_path.exists():
        old = pd.read_excel(master_path, sheet_name="MK_INV_DATA_DB", dtype={"TD": str})
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged["TD"] = merged["TD"].astype(str)
    merged = (merged.drop_duplicates(subset=["MAST_ID", "TD"], keep="last")
                    .sort_values(["MAST_ID", "TD"])
                    .reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_INV_DATA_DB", index=False)

    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(df_new), "master_rows": len(merged)}


# -----------------------------------------------------------------------------
# 5. 엔트리포인트
# -----------------------------------------------------------------------------
def run(start: str = START_DEFAULT, out_dir: str = "./MK_INV") -> dict:
    end = datetime.now().date()
    start_d = pd.to_datetime(start).date()
    log.info("MK_INV 수집 시작 : %s ~ %s (%d종)", start_d, end, len(INDICATORS))

    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start_d, end))

    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/패키지 설치를 확인하세요.")
        return {}

    df = pd.DataFrame(all_rows, columns=DATA_COLS)
    df = df.sort_values(["MAST_ID", "TD"]).reset_index(drop=True)

    result = save_excel(df, out_dir)
    ok = df["MAST_ID"].nunique()
    log.info("완료 : 지표 %d/19종, 신규 %d행 → %s",
             ok, result["new_rows"], result["master"])
    if ok < len(INDICATORS):
        miss = sorted(set(m[0] for m in INDICATORS) - set(df["MAST_ID"].unique()))
        log.warning("미수집 MAST_ID: %s (소스/심볼 점검 필요)", miss)
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_INV_START", START_DEFAULT),
        out_dir=os.environ.get("MK_INV_OUT", "./MK_INV"))
