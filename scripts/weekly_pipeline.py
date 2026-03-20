"""週次パイプライン

--friday:  週末予測 → レポート出力 → X投稿
--monday:  結果取得 → 精度評価 → 信頼度分析 → 結果報告X投稿

Usage:
    python scripts/weekly_pipeline.py --friday
    python scripts/weekly_pipeline.py --monday
    python scripts/weekly_pipeline.py --friday --date 20260321,20260322
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from config import RESULTS_DIR, VENUE_CODES
from db.schema import get_connection, insert_result

console = Console()

LOG_DIR = RESULTS_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging(mode: str, date_str: str):
    """ファイル+コンソールの両方にログ出力"""
    log_file = LOG_DIR / f"weekly_{date_str}_{mode}.log"
    handlers = [
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
        force=True,
    )
    return log_file


def get_weekend_dates() -> list[str]:
    """直近の土日の日付を返す (YYYYMMDD形式)"""
    today = datetime.now()
    if today.weekday() == 5:  # 土曜
        dates = [today.strftime("%Y%m%d")]
    elif today.weekday() == 6:  # 日曜
        dates = [
            (today - timedelta(days=1)).strftime("%Y%m%d"),
            today.strftime("%Y%m%d"),
        ]
    else:
        # 次の土日
        days_until_sat = (5 - today.weekday()) % 7
        if days_until_sat == 0:
            days_until_sat = 7
        sat = today + timedelta(days=days_until_sat)
        sun = sat + timedelta(days=1)
        dates = [sat.strftime("%Y%m%d"), sun.strftime("%Y%m%d")]
    return dates


def get_last_weekend_dates() -> list[str]:
    """直近の過去の土日の日付を返す"""
    today = datetime.now()
    # 直近の日曜日
    days_since_sun = (today.weekday() + 1) % 7
    if days_since_sun == 0:
        days_since_sun = 7
    last_sun = today - timedelta(days=days_since_sun)
    last_sat = last_sun - timedelta(days=1)
    return [last_sat.strftime("%Y%m%d"), last_sun.strftime("%Y%m%d")]


def run_friday_pipeline(dates: list[str]):
    """金曜パイプライン: 予測 → レポート → X投稿"""
    logger = logging.getLogger(__name__)

    console.print(Panel("[bold]Friday Pipeline: 週末予測[/bold]", style="blue"))

    # Step 1: 予測
    console.print("\n[bold blue][Step 1] 週末予測生成...[/bold blue]")
    from scripts.predict_weekend import predict_weekend
    predictions = predict_weekend()

    if not predictions:
        console.print("[red]予測が生成されませんでした。[/red]")
        return

    logger.info("予測完了: %dレース", len(predictions))

    # Step 2: レポート
    console.print("\n[bold blue][Step 2] 予測レポート出力...[/bold blue]")
    from scripts.generate_article import generate_prediction_report
    report_path = generate_prediction_report()

    # Step 3: X投稿
    console.print("\n[bold blue][Step 3] X投稿...[/bold blue]")
    try:
        from publishing.x_poster import XPoster
        poster = XPoster()
        if poster.is_configured:
            # 信頼度HIGH のレース数を集計
            conn = get_connection()
            high_conf_count = 0
            for date_str in dates:
                formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                row = conn.execute("""
                    SELECT COUNT(DISTINCT race_id) as cnt
                    FROM predictions
                    WHERE race_id IN (
                        SELECT race_id FROM races WHERE date = ?
                    ) AND confidence >= 2.0 AND predicted_rank = 1
                """, (formatted,)).fetchone()
                high_conf_count += row["cnt"] if row else 0
            conn.close()

            date_display = "/".join(f"{d[4:6]}/{d[6:8]}" for d in dates)
            text = (
                f"{date_display} AI競馬予想\n\n"
                f"全{len(predictions)}R予測完了\n"
                f"HIGH信頼度: {high_conf_count}R\n\n"
                f"#競馬予想 #AI予想 #競馬"
            )
            poster.post(text)
            logger.info("X投稿完了")
        else:
            console.print("[dim]X API未設定 — 投稿スキップ[/dim]")
    except Exception as e:
        logger.warning("X投稿エラー: %s", e)

    console.print(f"\n[green]金曜パイプライン完了[/green]")
    if report_path:
        console.print(f'[dim]記事生成: 「{report_path} を読んでnote.com用の競馬予想記事を書いて」[/dim]')


def run_monday_pipeline(dates: list[str]):
    """月曜パイプライン: 結果取得 → 精度評価 → 信頼度分析 → X投稿"""
    logger = logging.getLogger(__name__)

    console.print(Panel("[bold]Monday Pipeline: 結果検証[/bold]", style="blue"))

    conn = get_connection()

    # Step 1: 結果取得
    console.print("\n[bold blue][Step 1] 結果取得...[/bold blue]")
    from data.scraper import KeibaScraper
    scraper = KeibaScraper()

    fetched = 0
    for date_str in dates:
        formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        pred_races = conn.execute("""
            SELECT DISTINCT p.race_id
            FROM predictions p
            JOIN races r ON p.race_id = r.race_id
            WHERE r.date = ?
        """, (formatted,)).fetchall()

        if not pred_races:
            console.print(f"  {formatted}: 予測データなし")
            continue

        for pr in pred_races:
            race_id = pr["race_id"]
            existing = conn.execute(
                "SELECT COUNT(*) as cnt FROM race_results WHERE race_id = ?",
                (race_id,)
            ).fetchone()["cnt"]
            if existing > 0:
                continue
            try:
                result_data = scraper.scrape_race_result(race_id)
                if result_data:
                    for rr in result_data.get("results", []):
                        rr["race_id"] = race_id
                        insert_result(conn, rr)
                    conn.commit()
                    fetched += 1
            except Exception as e:
                logger.warning("結果取得エラー %s: %s", race_id, e)

    logger.info("結果取得: 新規%dレース", fetched)

    # Step 2: 精度評価
    console.print("\n[bold blue][Step 2] 精度評価...[/bold blue]")
    from backtest.evaluator import evaluate_recent
    evaluate_recent(days_back=7)

    # Step 3: 信頼度別精度分析
    console.print("\n[bold blue][Step 3] 信頼度別精度分析...[/bold blue]")
    analyze_confidence_accuracy(conn, dates)

    conn.close()

    # Step 4: X投稿
    console.print("\n[bold blue][Step 4] 結果報告X投稿...[/bold blue]")
    try:
        from publishing.x_poster import XPoster
        poster = XPoster()
        if poster.is_configured:
            stats = get_weekend_stats(dates)
            if stats and stats["total"] > 0:
                date_display = "/".join(f"{d[4:6]}/{d[6:8]}" for d in dates)
                text = (
                    f"{date_display} AI競馬予想 結果報告\n\n"
                    f"対象: {stats['total']}R\n"
                    f"Top1的中率: {stats['top1_rate']:.1%}\n"
                    f"Top3重複率: {stats['top3_overlap']:.1%}\n\n"
                    f"#競馬予想 #AI予想 #競馬"
                )
                poster.post(text)
                logger.info("結果報告投稿完了")
        else:
            console.print("[dim]X API未設定 — 投稿スキップ[/dim]")
    except Exception as e:
        logger.warning("X投稿エラー: %s", e)

    console.print(f"\n[green]月曜パイプライン完了[/green]")


def analyze_confidence_accuracy(conn, dates: list[str]):
    """信頼度ティア別の的中率を分析"""
    formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in dates]
    placeholders = ",".join("?" * len(formatted_dates))

    rows = conn.execute(f"""
        SELECT p.race_id, p.horse_id, p.predicted_rank, p.confidence,
               rr.finish_position
        FROM predictions p
        JOIN races r ON p.race_id = r.race_id
        LEFT JOIN race_results rr ON p.race_id = rr.race_id AND p.horse_id = rr.horse_id
        WHERE r.date IN ({placeholders})
        ORDER BY p.race_id, p.predicted_rank
    """, formatted_dates).fetchall()

    if not rows:
        console.print("[dim]分析対象データなし[/dim]")
        return

    # レース単位で集計
    from collections import defaultdict
    races = defaultdict(list)
    for r in rows:
        races[r["race_id"]].append(r)

    tiers = {"HIGH": {"total": 0, "top1": 0, "top3": 0},
             "MID": {"total": 0, "top1": 0, "top3": 0},
             "LOW": {"total": 0, "top1": 0, "top3": 0}}

    for race_id, preds in races.items():
        if not preds or preds[0]["confidence"] is None:
            continue

        # 結果がないレースはスキップ
        has_results = any(p["finish_position"] is not None for p in preds)
        if not has_results:
            continue

        confidence = preds[0]["confidence"]
        if confidence >= 2.0:
            tier = "HIGH"
        elif confidence >= 0.8:
            tier = "MID"
        else:
            tier = "LOW"

        tiers[tier]["total"] += 1

        # 予測Top1 vs 実際Top1
        pred_top1 = min(preds, key=lambda p: p["predicted_rank"])
        actual_results = [p for p in preds if p["finish_position"] is not None]
        if actual_results:
            actual_top1 = min(actual_results, key=lambda p: p["finish_position"])
            if pred_top1["horse_id"] == actual_top1["horse_id"]:
                tiers[tier]["top1"] += 1

        # 予測Top3 vs 実際Top3
        pred_top3 = set(p["horse_id"] for p in sorted(preds, key=lambda p: p["predicted_rank"])[:3])
        actual_top3 = set(p["horse_id"] for p in sorted(actual_results, key=lambda p: p["finish_position"])[:3])
        tiers[tier]["top3"] += len(pred_top3 & actual_top3)

    table = Table(title="信頼度別精度")
    table.add_column("ティア", style="cyan")
    table.add_column("レース数", justify="right")
    table.add_column("Top1的中率", justify="right")
    table.add_column("Top3重複率", justify="right")

    for tier_name in ["HIGH", "MID", "LOW"]:
        t = tiers[tier_name]
        if t["total"] == 0:
            table.add_row(tier_name, "0", "-", "-")
        else:
            top1_rate = f"{t['top1']}/{t['total']} ({t['top1']/t['total']:.1%})"
            top3_rate = f"{t['top3']}/{t['total']*3} ({t['top3']/(t['total']*3):.1%})"
            table.add_row(tier_name, str(t["total"]), top1_rate, top3_rate)

    console.print(table)


def get_weekend_stats(dates: list[str]) -> dict | None:
    """週末の集計統計を取得"""
    conn = get_connection()
    formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:8]}" for d in dates]
    placeholders = ",".join("?" * len(formatted_dates))

    row = conn.execute(f"""
        SELECT COUNT(*) as total,
               SUM(top1_hit) as top1_hits,
               SUM(top3_hit) as top3_hits
        FROM prediction_results pr
        JOIN races r ON pr.race_id = r.race_id
        WHERE r.date IN ({placeholders})
    """, formatted_dates).fetchone()
    conn.close()

    if not row or row["total"] == 0:
        return None

    return {
        "total": row["total"],
        "top1_rate": row["top1_hits"] / row["total"],
        "top3_overlap": row["top3_hits"] / row["total"],
    }


def main():
    parser = argparse.ArgumentParser(description="競馬AI予想 週次パイプライン")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--friday", action="store_true", help="金曜パイプライン（予測+レポート+X投稿）")
    group.add_argument("--monday", action="store_true", help="月曜パイプライン（結果+評価+信頼度分析+X投稿）")
    parser.add_argument("--date", help="対象日 (YYYYMMDD, カンマ区切りで複数可)")
    args = parser.parse_args()

    if args.date:
        dates = [d.strip() for d in args.date.split(",")]
    elif args.monday:
        dates = get_last_weekend_dates()
    else:
        dates = get_weekend_dates()

    date_str = dates[0] if dates else datetime.now().strftime("%Y%m%d")

    if not args.friday and not args.monday:
        # 曜日自動判定
        weekday = datetime.now().weekday()
        if weekday == 4:  # 金曜
            args.friday = True
        elif weekday == 0:  # 月曜
            args.monday = True
        else:
            console.print(f"[yellow]--friday または --monday を指定してください[/yellow]")
            console.print("  金曜: --friday (予測+レポート)")
            console.print("  月曜: --monday (結果検証)")
            return

    mode = "friday" if args.friday else "monday"
    log_file = setup_logging(mode, date_str)
    logger = logging.getLogger(__name__)
    logger.info("ログ: %s", log_file)

    try:
        if args.friday:
            run_friday_pipeline(dates)
        else:
            run_monday_pipeline(dates)
    except Exception:
        logger.exception("パイプライン異常終了")
        sys.exit(1)


if __name__ == "__main__":
    main()
