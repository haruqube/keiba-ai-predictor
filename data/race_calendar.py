"""今週のレースカレンダー取得"""

import re
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from config import NETKEIBA_BASE_URL, REQUEST_HEADERS, SCRAPE_DELAY
import time


def get_this_weekend_dates() -> list[str]:
    """今週の土日の日付をYYYYMMDD形式で返す"""
    today = datetime.now()
    # 土曜日までの日数
    days_until_sat = (5 - today.weekday()) % 7
    if days_until_sat == 0 and today.hour >= 18:
        days_until_sat = 7
    saturday = today + timedelta(days=days_until_sat)
    sunday = saturday + timedelta(days=1)
    return [saturday.strftime("%Y%m%d"), sunday.strftime("%Y%m%d")]


def get_kaisai_dates_from_calendar(year: int, month: int) -> list[str]:
    """netkeibaカレンダーから開催日一覧を取得"""
    url = f"{NETKEIBA_BASE_URL}/top/calendar.html?year={year}&month={month}"
    time.sleep(SCRAPE_DELAY)
    resp = requests.get(url, headers=REQUEST_HEADERS)
    resp.encoding = "EUC-JP"
    soup = BeautifulSoup(resp.text, "lxml")

    dates = []
    for link in soup.select("a[href*='kaisai_date=']"):
        href = link.get("href", "")
        m = re.search(r"kaisai_date=(\d{8})", href)
        if m and m.group(1) not in dates:
            dates.append(m.group(1))

    return sorted(dates)


def get_race_ids_for_date(date: str) -> list[str]:
    """指定日のレースID一覧を取得"""
    from data.scraper import KeibaScraper
    scraper = KeibaScraper()
    return scraper.scrape_race_list(date)


def get_weekend_race_ids() -> dict[str, list[str]]:
    """今週末のレースIDを日付ごとに取得"""
    dates = get_this_weekend_dates()
    result = {}
    for date in dates:
        race_ids = get_race_ids_for_date(date)
        if race_ids:
            result[date] = race_ids
    return result
