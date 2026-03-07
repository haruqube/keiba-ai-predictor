"""netkeibaスクレイパー（キャッシュ付き）"""

import json
import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    CACHE_DIR, NETKEIBA_BASE_URL, NETKEIBA_DB_URL,
    REQUEST_HEADERS, SCRAPE_DELAY,
)

logger = logging.getLogger(__name__)

# Icon_GradeType の CSS クラス番号 → グレード名
_GRADE_CLASS_MAP = {
    "1": "G1",
    "2": "G2",
    "3": "G3",
    "5": "OP",
    "10": "未勝利",
    "11": "2勝",
    "12": "3勝",
    "13": "3勝",
    "15": "L",
    "16": "3勝",
    "17": "2勝",
    "18": "1勝",
}

# trainer 所属プレフィックス
_TRAINER_AFFILIATION_RE = re.compile(r"^(美浦|栗東)")


class KeibaScraper:
    """netkeibaからレース情報・結果をスクレイピング"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        # リトライ: 3回, バックオフ1秒, 500/502/503/504 で再試行
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get(self, url: str) -> str:
        cache_key = re.sub(r"[^a-zA-Z0-9_\-]", "_", url.replace("https://", ""))
        cache_file = CACHE_DIR / f"{cache_key}.html"

        if cache_file.exists():
            return cache_file.read_text(encoding="utf-8")

        time.sleep(SCRAPE_DELAY)
        resp = self.session.get(url, timeout=30)
        # netkeiba は EUC-JP だが、ページによって UTF-8 の場合もある
        # apparent_encoding (charset_normalizer) で自動検出
        resp.encoding = resp.apparent_encoding
        html = resp.text
        cache_file.write_text(html, encoding="utf-8")
        logger.debug("Fetched %s", url)
        return html

    def _get_json_cache(self, key: str) -> dict | list | None:
        cache_file = CACHE_DIR / f"{key}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))
        return None

    def _set_json_cache(self, key: str, data):
        cache_file = CACHE_DIR / f"{key}.json"
        cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def scrape_race_list(self, date: str) -> list[str]:
        """指定日のレースID一覧を取得 (date: YYYYMMDD)

        race_list_sub.html (AJAX用サブページ) からrace_idを抽出。
        """
        cache_key = f"race_list_{date}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{NETKEIBA_BASE_URL}/top/race_list_sub.html?kaisai_date={date}"
        html = self._get(url)

        race_ids = sorted(set(re.findall(r"race_id=(\d{12})", html)))

        self._set_json_cache(cache_key, race_ids)
        return race_ids

    def scrape_race_result(self, race_id: str) -> dict:
        """レース結果ページをパース"""
        cache_key = f"race_result_{race_id}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{NETKEIBA_BASE_URL}/race/result.html?race_id={race_id}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_info = self._parse_race_info(soup, race_id)
        results = self._parse_result_table(soup, race_id)
        race_info["results"] = results
        race_info["horse_count"] = len(results)

        self._set_json_cache(cache_key, race_info)
        return race_info

    def scrape_race_entry(self, race_id: str) -> dict:
        """出馬表ページをパース（レース前）"""
        cache_key = f"race_entry_{race_id}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{NETKEIBA_BASE_URL}/race/shutuba.html?race_id={race_id}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_info = self._parse_race_info(soup, race_id)
        entries = self._parse_entry_table(soup, race_id)
        race_info["entries"] = entries
        race_info["horse_count"] = len(entries)

        self._set_json_cache(cache_key, race_info)
        return race_info

    def _parse_race_info(self, soup: BeautifulSoup, race_id: str) -> dict:
        """レース情報をパース"""
        info = {"race_id": race_id}

        # レース名
        title = soup.select_one(".RaceName")
        info["race_name"] = title.get_text(strip=True) if title else ""

        # race_id構造: YYYYPPDDKKRR (PP=場所, DD=開催回, KK=日目, RR=レース番号)
        info["race_number"] = int(race_id[10:12])
        info["venue"] = race_id[4:6]

        # レース詳細 (距離, 馬場, 天候等)
        detail = soup.select_one(".RaceData01")
        if detail:
            text = detail.get_text(strip=True)
            d_match = re.search(r"(\d{4})m", text)
            info["distance"] = int(d_match.group(1)) if d_match else None
            info["surface"] = "芝" if "芝" in text else "ダ" if "ダ" in text else None
            info["direction"] = "右" if "右" in text else "左" if "左" in text else None
            info["weather"] = self._extract_between(text, "天候:", "/") or None
            info["track_condition"] = self._extract_between(text, "馬場:", "/") or self._extract_after(text, "馬場:")

        # グレード — Icon_GradeType span にはテキストがなく、CSSクラス名にグレード番号が入っている
        # 例: <span class="Icon_GradeType Icon_GradeType1"> → G1
        grade_el = soup.select_one("[class*='Icon_GradeType']")
        if grade_el:
            classes = grade_el.get("class", [])
            grade_num = None
            for cls in classes:
                m = re.search(r"Icon_GradeType(\d+)", cls)
                if m:
                    grade_num = m.group(1)
                    break
            info["grade"] = _GRADE_CLASS_MAP.get(grade_num) if grade_num else None
            if grade_num and not info["grade"]:
                logger.warning("Unknown grade class number: %s (race_id=%s)", grade_num, race_id)
        else:
            info["grade"] = None

        return info

    def _parse_result_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """レース結果テーブルをパース

        実際のセル構造 (15列):
        [0]着順 [1]枠番 [2]馬番 [3]馬名 [4]性齢 [5]斤量
        [6]騎手 [7]タイム [8]着差 [9]人気 [10]オッズ
        [11]上がり3F [12]通過順 [13]厩舎 [14]馬体重
        """
        results = []
        table = soup.select_one("table.RaceTable01")
        if not table:
            return results

        for row in table.select("tbody tr"):
            cells = row.select("td")
            if len(cells) < 14:
                continue
            try:
                horse_link = cells[3].select_one("a[href*='/horse/']")
                horse_id = ""
                if horse_link:
                    m = re.search(r"/horse/(\w+)", horse_link["href"])
                    horse_id = m.group(1) if m else ""

                result = {
                    "race_id": race_id,
                    "horse_id": horse_id,
                    "finish_position": self._safe_int(cells[0].get_text(strip=True)),
                    "bracket_number": self._safe_int(cells[1].get_text(strip=True)),
                    "horse_number": self._safe_int(cells[2].get_text(strip=True)),
                    "horse_name": cells[3].get_text(strip=True),
                    "sex_age": cells[4].get_text(strip=True),
                    "weight_carried": self._safe_float(cells[5].get_text(strip=True)),
                    "jockey": cells[6].get_text(strip=True),
                    "finish_time": cells[7].get_text(strip=True),
                    "margin": cells[8].get_text(strip=True),
                    "popularity": self._safe_int(cells[9].get_text(strip=True)),
                    "odds": self._safe_float(cells[10].get_text(strip=True)),
                    "last_3f": self._safe_float(cells[11].get_text(strip=True)),
                    "passing_order": cells[12].get_text(strip=True),
                    "trainer": _TRAINER_AFFILIATION_RE.sub("", cells[13].get_text(strip=True)),
                    "horse_weight": self._parse_horse_weight(cells[14].get_text(strip=True)) if len(cells) > 14 else None,
                    "horse_weight_change": self._parse_horse_weight_change(cells[14].get_text(strip=True)) if len(cells) > 14 else None,
                }
                results.append(result)
            except (IndexError, ValueError) as e:
                logger.warning("Result row parse error (race_id=%s): %s", race_id, e)
                continue

        return results

    def _parse_entry_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """出馬表テーブルをパース"""
        entries = []
        table = soup.select_one("table.Shutuba_Table")
        if not table:
            return entries

        for row in table.select("tr"):
            cells = row.select("td")
            if len(cells) < 10:
                continue
            try:
                horse_link = cells[3].select_one("a[href*='/horse/']")
                horse_id = ""
                if horse_link:
                    m = re.search(r"/horse/(\w+)", horse_link["href"])
                    horse_id = m.group(1) if m else ""

                entry = {
                    "race_id": race_id,
                    "horse_id": horse_id,
                    "bracket_number": self._safe_int(cells[0].get_text(strip=True)),
                    "horse_number": self._safe_int(cells[1].get_text(strip=True)),
                    "horse_name": cells[3].get_text(strip=True),
                    "sex_age": cells[4].get_text(strip=True),
                    "weight_carried": self._safe_float(cells[5].get_text(strip=True)),
                    "jockey": cells[6].get_text(strip=True),
                    "trainer": _TRAINER_AFFILIATION_RE.sub("", cells[7].get_text(strip=True)) if len(cells) > 7 else None,
                    "odds": self._safe_float(cells[9].get_text(strip=True)) if len(cells) > 9 else None,
                    "popularity": self._safe_int(cells[10].get_text(strip=True)) if len(cells) > 10 else None,
                }
                entries.append(entry)
            except (IndexError, ValueError) as e:
                logger.warning("Entry row parse error (race_id=%s): %s", race_id, e)
                continue

        return entries

    # ── ユーティリティ ──

    @staticmethod
    def _safe_int(text: str) -> int | None:
        try:
            return int(re.sub(r"[^\d]", "", text))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(text: str) -> float | None:
        try:
            return float(text.replace(",", ""))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _extract_between(text: str, start: str, end: str) -> str:
        idx = text.find(start)
        if idx == -1:
            return ""
        s = idx + len(start)
        e = text.find(end, s)
        return text[s:e].strip() if e != -1 else text[s:].strip()

    @staticmethod
    def _extract_after(text: str, marker: str) -> str | None:
        idx = text.find(marker)
        if idx == -1:
            return None
        return text[idx + len(marker):].strip()[:4]

    @staticmethod
    def _parse_horse_weight(text: str) -> int | None:
        m = re.match(r"(\d+)", text)
        return int(m.group(1)) if m else None

    @staticmethod
    def _parse_horse_weight_change(text: str) -> int | None:
        m = re.search(r"\(([+-]?\d+)\)", text)
        return int(m.group(1)) if m else None
