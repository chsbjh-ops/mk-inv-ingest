# -*- coding: utf-8 -*-
"""
==============================================================================
 File        : mk_bond_daily_ingest.py
 Purpose     : MK_BOND — 금융위원회_채권시세정보(KRX 채권시장 시세·수익률)를
               공공데이터포털 OpenAPI 로 매일 수집하여 엑셀(.xlsx)로 저장
 Author      : PM (Bigset)
 Created     : 2026-06-17
 Source      : data.go.kr 금융위원회_채권시세정보 (무료, 이용허락범위 제한 없음)
               https://apis.data.go.kr/1160100/service/GetBondTradInfoService/getBondPriceInfo
 인증키      : 환경변수 DATA_GO_KR_KEY (GitHub Secret) — 코드/로그에 노출 금지

 설계
 ----------------------------------------------------------------------------
   · API가 반환하는 컬럼을 그대로 적재(필드명 변경에 강건). TD(기준일자)만 표준화.
   · 기간 범위(MK_BOND_START~오늘) 영업일 루프 + 페이지네이션 전량 수집.
   · 응답 비정상(인증/한도) 시 로그로 알리고 계속.

 필요 패키지 : requests, pandas, openpyxl
==============================================================================
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("MK_BOND")

URL = "https://apis.data.go.kr/1160100/service/GetBondTradInfoService/getBondPriceInfo"
KEY = os.environ.get("DATA_GO_KR_KEY", "").strip()

START_DEFAULT = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")  # 기본 최근 7일
NUM_ROWS = 1000
TIMEOUT = 60
RETRY_MAX = 4
SLEEP = 0.3


def _bdays(start: str, end: str):
    rng = pd.bdate_range(pd.to_datetime(start), pd.to_datetime(end))
    return [d.strftime("%Y%m%d") for d in rng]


def _get(params):
    last = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = requests.get(URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            log.warning("  재시도 %d/%d (%s)", attempt, RETRY_MAX, str(exc)[:100])
            time.sleep(2 * attempt)
    raise last


def fetch_day(basDt: str) -> list[dict]:
    rows, page = [], 1
    while True:
        js = _get({"serviceKey": KEY, "resultType": "json",
                   "numOfRows": NUM_ROWS, "pageNo": page, "basDt": basDt})
        body = js.get("response", {}).get("body", {})
        header = js.get("response", {}).get("header", {})
        code = header.get("resultCode")
        if code not in (None, "00", "0"):
            log.error("  %s API 오류 %s: %s", basDt, code, header.get("resultMsg"))
            break
        items = body.get("items", {})
        item = items.get("item", []) if isinstance(items, dict) else []
        if isinstance(item, dict):
            item = [item]
        if not item:
            break
        rows.extend(item)
        total = int(body.get("totalCount", 0) or 0)
        if page * NUM_ROWS >= total or not total:
            break
        page += 1
        time.sleep(SLEEP)
    if rows:
        log.info("[%s] %d행 수집", basDt, len(rows))
    return rows


def save_excel(df_new: pd.DataFrame, out_dir: str) -> dict:
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    daily = out / f"MK_BOND_DATA_{today}.xlsx"
    df_new.to_excel(daily, sheet_name="MK_BOND_DATA_DB", index=False)

    master = out / "mk_bond_data_master.xlsx"
    if master.exists():
        old = pd.read_excel(master, sheet_name="MK_BOND_DATA_DB", dtype=str)
        merged = pd.concat([old, df_new.astype(str)], ignore_index=True)
    else:
        merged = df_new.astype(str)
    # 중복 제거 키: 기준일자 + ISIN (필드명 가변 대응)
    td_col = next((c for c in merged.columns if c.lower() == "basdt"), None)
    isin_col = next((c for c in merged.columns if c.lower() == "isincd"), None)
    if td_col and isin_col:
        merged = merged.drop_duplicates(subset=[td_col, isin_col], keep="last")
    merged = merged.reset_index(drop=True)
    merged.to_excel(master, sheet_name="MK_BOND_DATA_DB", index=False)
    return {"daily": str(daily), "master": str(master),
            "new_rows": len(df_new), "master_rows": len(merged)}


def run(start: str = START_DEFAULT, out_dir: str = "./MK_BOND") -> dict:
    if not KEY:
        log.error("DATA_GO_KR_KEY 환경변수가 없습니다. (GitHub Secret 등록 필요)")
        return {}
    end = datetime.now().strftime("%Y%m%d")
    days = _bdays(start, end)
    log.info("MK_BOND 수집 시작 : %s ~ %s (%d영업일)", start, end, len(days))
    all_rows: list[dict] = []
    for d in days:
        all_rows.extend(fetch_day(d))
        time.sleep(SLEEP)
    if not all_rows:
        log.error("수집 결과가 비었습니다. 키/활용신청/날짜를 확인하세요.")
        return {}
    df = pd.DataFrame(all_rows)
    result = save_excel(df, out_dir)
    log.info("완료 : 신규 %d행 → %s (컬럼: %s)",
             result["new_rows"], result["master"], list(df.columns))
    return result


if __name__ == "__main__":
    run(start=os.environ.get("MK_BOND_START", START_DEFAULT),
        out_dir=os.environ.get("MK_BOND_OUT", "./MK_BOND"))
