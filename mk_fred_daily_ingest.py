# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_fred_daily_ingest.py
 Purpose     : MK_FRED(미국 채권금리 등) 12종을 FRED 에서 수집하여
               MK_FRED_DATA_DB 포맷의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-08
 Context     : Colab 수집 시 런타임이 끊겨 적재가 중단되는 문제 대응.
               GitHub Actions(서버 실행)에서 끝까지 안정적으로 수집한다.
               FRED 는 pandas_datareader 경유로 API 키 없이 조회 가능.

 출력 포맷 (MK_FRED_DATA_DB)
 ----------------------------------------------------------------------------
   MAST_ID, SERIES_CD, SERIES_NM, TD(YYYYMMDD), VALUE
   ※ 앞 컬럼은 정의서 시트와 일치. SERIES_CD/NM 은 확인용으로 함께 출력.

 필요 패키지
 ----------
   pip install pandas_datareader openpyxl pandas
==============================================================================
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("MK_FRED")

# -----------------------------------------------------------------------------
# 1. 지표 정의 (MK_FRED_MAST 기준, MAST_ID 1~12)
#    series_cd : 정의서의 SERIES_CD (사내 코드)
#    fred_id   : 실제 FRED 시리즈 ID  (US1Y~US10Y 는 CMT 의 DGS 코드로 매핑)
#    nm        : 표시명
#    dp        : 반올림 소수 자릿수
# -----------------------------------------------------------------------------
INDICATORS = [
    # MAST_ID, series_cd,      fred_id,        nm,                                   dp
    (1,  "BAMLC0A3CA",   "BAMLC0A3CA",   "ICE BofA US Corp Index (BAMLC0A3CA)", 2),
    (2,  "BAMLC0A2CAA",  "BAMLC0A2CAA",  "ICE BofA US Corp Index (BAMLC0A2CAA)", 2),
    (3,  "US1Y",         "DGS1",         "미국 국채 1년물 (CMT)",                3),
    (4,  "US2Y",         "DGS2",         "미국 국채 2년물 (CMT)",                3),
    (5,  "US3Y",         "DGS3",         "미국 국채 3년물 (CMT)",                3),
    (6,  "US5Y",         "DGS5",         "미국 국채 5년물 (CMT)",                3),
    (7,  "US7Y",         "DGS7",         "미국 국채 7년물 (CMT)",                3),
    (8,  "US10Y",        "DGS10",        "미국 국채 10년물 (CMT)",               3),
    (9,  "DTB3",         "DTB3",         "3-Month Treasury Bill",                3),
    (10, "SOFR30DAYAVG", "SOFR30DAYAVG", "SOFR 30-Day Average",                  3),
    (11, "SOFR90DAYAVG", "SOFR90DAYAVG", "SOFR 90-Day Average",                  3),
    (12, "SOFRINDEX",    "SOFRINDEX",    "SOFR Index",                           8),
]

DATA_COLS = ["MAST_ID", "SERIES_CD", "SERIES_NM", "TD", "VALUE"]

START_DEFAULT = "2026-05-01"   # 1회 백필 시작일. 매 실행마다 이 날짜~오늘을 재조회.
# ※ FRED 는 지난주 값이 이번 주에 갱신/지연 게시되는 경우가 있어,
#   매 실행마다 START(=5/1)부터 다시 받아 최근 2주 이상을 항상 재비교한다.
#   병합은 (MAST_ID, TD) 기준 keep="last" 이므로 수정된 값이 최신값으로 덮어쓰기 된다.
RETRY_MAX = 3
RETRY_SLEEP = 3.0


# -----------------------------------------------------------------------------
# 2. 수집기 (시리즈별 독립 호출 + 재시도 → 끊김/일시오류에도 전체 진행)
# -----------------------------------------------------------------------------
def fetch_fred_series(fred_id: str, start: str, end):
    from pandas_datareader import data as pdr      # 키 불필요
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            s = pdr.DataReader(fred_id, "fred", start, end)[fred_id]
            return s
        except Exception as exc:
            last_err = exc
            log.warning("  %s 재시도 %d/%d (%s)", fred_id, attempt, RETRY_MAX, exc)
            time.sleep(RETRY_SLEEP)
    raise last_err


def collect_one(meta, start, end) -> list[dict]:
    mast_id, series_cd, fred_id, nm, dp = meta
    try:
        s = fetch_fred_series(fred_id, start, end)
        s = s.dropna()                              # FRED 결측(.) 제거
        rows = [{
            "MAST_ID": mast_id,
            "SERIES_CD": series_cd,
            "SERIES_NM": nm,
            "TD": pd.Timestamp(idx).strftime("%Y%m%d"),
            "VALUE": round(float(v), dp),
        } for idx, v in s.items()]
        log.info("[%2d %-12s] %4d행 수집 (FRED:%s)", mast_id, series_cd, len(rows), fred_id)
        return rows
    except Exception as exc:
        log.error("[%2d %-12s] 수집 실패: %s", mast_id, series_cd, exc)
        return []


# -----------------------------------------------------------------------------
# 3. 엑셀 저장 (당일 파일 + 누적 마스터 병합, (MAST_ID, TD) 중복 제거)
# -----------------------------------------------------------------------------
def save_excel(df_new: pd.DataFrame, out_dir: str) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")

    daily_path = out / f"MK_FRED_DATA_{today}.xlsx"
    df_new.to_excel(daily_path, sheet_name="MK_FRED_DATA_DB", index=False)

    master_path = out / "mk_fred_data_master.xlsx"
    if master_path.exists():
        old = pd.read_excel(master_path, sheet_name="MK_FRED_DATA_DB", dtype={"TD": str})
        merged = pd.concat([old, df_new], ignore_index=True)
    else:
        merged = df_new.copy()
    merged["TD"] = merged["TD"].astype(str)
    merged = (merged.drop_duplicates(subset=["MAST_ID", "TD"], keep="last")
                    .sort_values(["MAST_ID", "TD"])
                    .reset_index(drop=True))
    merged.to_excel(master_path, sheet_name="MK_FRED_DATA_DB", index=False)

    return {"daily": str(daily_path), "master": str(master_path),
            "new_rows": len(df_new), "master_rows": len(merged)}


# -----------------------------------------------------------------------------
# 4. 엔트리포인트
# -----------------------------------------------------------------------------
def run(start: str = START_DEFAULT, out_dir: str = "./MK_FRED") -> dict:
    end = datetime.now().date()
    log.info("MK_FRED 수집 시작 : %s ~ %s (%d종)", start, end, len(INDICATORS))

    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start, end))

    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/패키지 설치를 확인하세요.")
        return {}

    df = pd.DataFrame(all_rows, columns=DATA_COLS).sort_values(["MAST_ID", "TD"]).reset_index(drop=True)
    result = save_excel(df, out_dir)
    ok = df["MAST_ID"].nunique()
    log.info("완료 : 시리즈 %d/12종, 신규 %d행 → %s", ok, result["new_rows"], result["master"])
    if ok < len(INDICATORS):
        miss = sorted(set(m[0] for m in INDICATORS) - set(df["MAST_ID"].unique()))
        log.warning("미수집 MAST_ID: %s (시리즈 ID 점검 필요)", miss)
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_FRED_START", START_DEFAULT),
        out_dir=os.environ.get("MK_FRED_OUT", "./MK_FRED"))
