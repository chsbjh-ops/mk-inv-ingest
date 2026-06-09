# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_fred_daily_ingest.py
 Purpose     : MK_FRED(미국 국채·SOFR) 10종을 FRED 에서 수집하여
               MK_FRED_DATA_DB 포맷의 엑셀(.xlsx)로 매일 자동 저장
 Author      : PM (Bigset)
 Created     : 2026-06-08
 Context     : Colab 수집 시 런타임이 끊겨 적재가 중단되는 문제 대응.
               GitHub Actions(서버 실행)에서 끝까지 안정적으로 수집한다.
               FRED 호출은 requests 기반(재시도 강화). 키 없이도 동작.

 출력 포맷 (MK_FRED_DATA_DB)
 ----------------------------------------------------------------------------
   MAST_ID, SERIES_CD, SERIES_NM, TD(YYYYMMDD), VALUE
   ※ 앞 컬럼은 정의서 시트와 일치. SERIES_CD/NM 은 확인용으로 함께 출력.

 안정성
 ----------
   FRED 무료 CSV 경로(fredgraph.csv)는 간헐적 Read timeout 이 있다.
   - FRED_API_KEY 환경변수가 있으면 공식 API(api.stlouisfed.org)를 우선 사용(안정적).
   - 없으면 강화된 재시도/타임아웃으로 fredgraph.csv 를 호출(키 불필요).
   - 그래도 일부가 빠지면, 매 실행 5/1부터 재조회하므로 다음 실행에서 자동 보완된다.

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
    # (1) BAMLC0A3CA, (2) BAMLC0A2CAA — ICE BofA 지수: 라이선스(상업 이용 사전승인
    #   필요, FRED "Copyright: Pre-Approval Required")로 수집 제외.
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

FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()  # 있으면 공식 API 사용
HTTP_TIMEOUT = 60          # 초 (기존 30 → 60 으로 상향)
RETRY_MAX = 5              # 재시도 횟수 (기존 3 → 5)
RETRY_BASE = 4.0          # 선형 백오프 기준 (4,8,12,16,20초)
SLEEP_BETWEEN = 1.0       # 시리즈 간 간격 (rate limit 완화)
_UA = {"User-Agent": "Mozilla/5.0 (compatible; bigset-fred-ingest/1.0)"}


# -----------------------------------------------------------------------------
# 2. 수집기 (시리즈별 독립 호출 + 재시도 → 끊김/일시오류에도 전체 진행)
# -----------------------------------------------------------------------------
def _http_get(url, params=None):
    """재시도/타임아웃을 강화한 GET."""
    last = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(url, params=params, headers=_UA, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as exc:
            last = exc
            log.warning("  요청 재시도 %d/%d (%s)", attempt, RETRY_MAX, str(exc)[:120])
            time.sleep(RETRY_BASE * attempt)          # 선형 백오프
    raise last


def fetch_fred_series(fred_id: str, start, end):
    """공식 API(키 있으면) → 없으면 fredgraph.csv. 둘 다 pd.Series 반환."""
    if FRED_API_KEY:
        r = _http_get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": fred_id, "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "observation_start": str(start), "observation_end": str(end)},
        )
        obs = r.json().get("observations", [])
        idx = pd.to_datetime([o["date"] for o in obs])
        val = pd.to_numeric([o["value"] for o in obs], errors="coerce")  # '.' → NaN
        return pd.Series(val, index=idx)

    # 키 없을 때: 무료 CSV 경로 (강화된 재시도)
    r = _http_get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred_id}")
    df = pd.read_csv(io.StringIO(r.text))
    datecol, valcol = df.columns[0], df.columns[1]
    df[datecol] = pd.to_datetime(df[datecol], errors="coerce")
    df = df.dropna(subset=[datecol])
    mask = (df[datecol].dt.date >= start) & (df[datecol].dt.date <= end)
    df = df[mask]
    s = pd.to_numeric(df[valcol], errors="coerce")
    s.index = df[datecol]
    return s


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
    start_d = pd.to_datetime(start).date()
    log.info("MK_FRED 수집 시작 : %s ~ %s (%d종)", start_d, end, len(INDICATORS))

    src = "공식 API" if FRED_API_KEY else "무료 CSV"
    log.info("수집 경로: %s", src)
    all_rows: list[dict] = []
    for meta in INDICATORS:
        all_rows.extend(collect_one(meta, start_d, end))
        time.sleep(SLEEP_BETWEEN)

    if not all_rows:
        log.error("수집 결과가 비었습니다. 네트워크/패키지 설치를 확인하세요.")
        return {}

    df = pd.DataFrame(all_rows, columns=DATA_COLS).sort_values(["MAST_ID", "TD"]).reset_index(drop=True)
    result = save_excel(df, out_dir)
    ok = df["MAST_ID"].nunique()
    log.info("완료 : 시리즈 %d/%d종, 신규 %d행 → %s", ok, len(INDICATORS), result["new_rows"], result["master"])
    if ok < len(INDICATORS):
        miss = sorted(set(m[0] for m in INDICATORS) - set(df["MAST_ID"].unique()))
        log.warning("미수집 MAST_ID: %s (시리즈 ID 점검 필요)", miss)
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_FRED_START", START_DEFAULT),
        out_dir=os.environ.get("MK_FRED_OUT", "./MK_FRED"))
