"""週次データ更新 — 先週の結果取得 + DB更新"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from datetime import datetime, timedelta
from rich.console import Console

from config import VENUE_CODES
from db.schema import get_connection, insert_race, insert_result, insert_horse
from data.scraper import KeibaScraper
from data.race_calendar import get_kaisai_dates_from_calendar

console = Console()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def _process_race(scraper, conn, race_id, date_str):
    """1レース分の結果を取得してDBに保存"""
    data = scraper.scrape_race_result(race_id)

    race_info = {
        "race_id": race_id,
        "date": f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
        "venue": VENUE_CODES.get(data.get("venue", ""), data.get("venue", "")),
        "race_number": data.get("race_number", 0),
        "race_name": data.get("race_name"),
        "grade": data.get("grade"),
        "distance": data.get("distance"),
        "surface": data.get("surface"),
        "direction": data.get("direction"),
        "weather": data.get("weather"),
        "track_condition": data.get("track_condition"),
        "horse_count": data.get("horse_count", 0),
    }
    insert_race(conn, race_info)

    for r in data.get("results", []):
        if r.get("horse_id"):
            insert_horse(conn, {
                "horse_id": r["horse_id"],
                "name": r.get("horse_name", ""),
            })
            insert_result(conn, r)


def update_recent_results(days_back: int = 7):
    """直近N日分の結果を取得してDBに保存"""
    scraper = KeibaScraper()
    conn = get_connection()

    today = datetime.now()
    total_races = 0

    for d in range(days_back, 0, -1):
        date = today - timedelta(days=d)
        date_str = date.strftime("%Y%m%d")
        race_ids = scraper.scrape_race_list(date_str)

        if not race_ids:
            continue

        console.print(f"[blue]{date_str}: {len(race_ids)}レース[/blue]")

        for race_id in race_ids:
            try:
                _process_race(scraper, conn, race_id, date_str)
                total_races += 1
            except Exception as e:
                console.print(f"  [red]Error {race_id}: {e}[/red]")

        conn.commit()

    conn.close()
    console.print(f"[green]更新完了: {total_races}レース[/green]")


def update_date_range(start_date: str, end_date: str):
    """指定期間の全結果を取得してDBに保存 (start/end: YYYY-MM-DD)"""
    scraper = KeibaScraper()
    conn = get_connection()

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_races = 0

    while current <= end:
        date_str = current.strftime("%Y%m%d")
        race_ids = scraper.scrape_race_list(date_str)

        if race_ids:
            console.print(f"[blue]{date_str}: {len(race_ids)}レース[/blue]")

            for race_id in race_ids:
                try:
                    _process_race(scraper, conn, race_id, date_str)
                    total_races += 1
                except Exception as e:
                    console.print(f"  [red]Error {race_id}: {e}[/red]")

            conn.commit()

        current += timedelta(days=1)

    conn.close()
    console.print(f"[green]更新完了: {total_races}レース[/green]")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="レースデータ更新")
    parser.add_argument("--days", type=int, default=7, help="何日前まで遡るか (デフォルト: 7)")
    parser.add_argument("--start", type=str, help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="終了日 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.start and args.end:
        update_date_range(args.start, args.end)
    else:
        update_recent_results(args.days)
