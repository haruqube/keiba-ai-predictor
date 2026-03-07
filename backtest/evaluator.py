"""予測精度評価"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table

from db.schema import get_connection

console = Console()


def evaluate_recent(days_back: int = 7):
    """直近N日間の予測精度を評価"""
    conn = get_connection()

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    # 予測があり、結果も出ているレースを取得
    races = conn.execute("""
        SELECT DISTINCT r.race_id, r.date, r.race_name, r.venue
        FROM races r
        JOIN predictions p ON r.race_id = p.race_id
        JOIN race_results rr ON r.race_id = rr.race_id
        WHERE r.date >= ?
        ORDER BY r.date, r.race_number
    """, (cutoff,)).fetchall()

    if not races:
        console.print("[yellow]評価対象のレースがありません。[/yellow]")
        conn.close()
        return

    top1_hits = 0
    top3_hits = 0
    total = 0

    detail_rows = []

    for race in races:
        race_id = race["race_id"]

        # 予測上位
        preds = conn.execute("""
            SELECT p.horse_id, p.predicted_rank, p.mark,
                   e.horse_name
            FROM predictions p
            LEFT JOIN entries e ON p.race_id = e.race_id AND p.horse_id = e.horse_id
            WHERE p.race_id = ?
            ORDER BY p.predicted_rank
        """, (race_id,)).fetchall()

        # 実際の結果
        results = conn.execute("""
            SELECT horse_id, finish_position, horse_name
            FROM race_results
            WHERE race_id = ? AND finish_position IS NOT NULL
            ORDER BY finish_position
        """, (race_id,)).fetchall()

        if not preds or not results:
            continue

        total += 1

        pred_top1 = preds[0]["horse_id"]
        pred_top3 = set(p["horse_id"] for p in preds[:3])
        actual_top1 = results[0]["horse_id"]
        actual_top3 = set(r["horse_id"] for r in results[:3])

        is_top1 = pred_top1 == actual_top1
        is_top3 = len(pred_top3 & actual_top3) > 0

        if is_top1:
            top1_hits += 1
        if is_top3:
            top3_hits += 1

        # prediction_resultsに保存
        conn.execute("""
            INSERT OR REPLACE INTO prediction_results
            (race_id, predicted_top1, predicted_top3, actual_top1, actual_top3,
             top1_hit, top3_hit)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            race_id,
            preds[0]["horse_name"] or pred_top1,
            ", ".join(p["horse_name"] or p["horse_id"] for p in preds[:3]),
            results[0]["horse_name"] or actual_top1,
            ", ".join(r["horse_name"] or r["horse_id"] for r in results[:3]),
            1 if is_top1 else 0,
            1 if is_top3 else 0,
        ))

        detail_rows.append({
            "date": race["date"],
            "race": race["race_name"] or race_id,
            "pred": preds[0]["horse_name"] or "?",
            "actual": results[0]["horse_name"] or "?",
            "top1": "O" if is_top1 else "X",
            "top3": "O" if is_top3 else "X",
        })

    conn.commit()
    conn.close()

    # 結果表示
    if total == 0:
        console.print("[yellow]評価対象レースなし[/yellow]")
        return

    summary = Table(title=f"予測精度 (直近{days_back}日)")
    summary.add_column("指標", style="cyan")
    summary.add_column("値", style="green")
    summary.add_row("対象レース数", str(total))
    summary.add_row("Top-1的中", f"{top1_hits}/{total} ({top1_hits/total:.1%})")
    summary.add_row("Top-3的中", f"{top3_hits}/{total} ({top3_hits/total:.1%})")
    console.print(summary)

    # 詳細
    if detail_rows:
        detail = Table(title="レース別結果")
        detail.add_column("日付")
        detail.add_column("レース")
        detail.add_column("予測1着")
        detail.add_column("実際1着")
        detail.add_column("Top1")
        detail.add_column("Top3")

        for row in detail_rows[-20:]:  # 直近20件
            style = "green" if row["top1"] == "O" else ""
            detail.add_row(
                row["date"], row["race"], row["pred"],
                row["actual"], row["top1"], row["top3"],
                style=style,
            )
        console.print(detail)

    return {"total": total, "top1_rate": top1_hits / total, "top3_rate": top3_hits / total}


def evaluate_all_time():
    """全期間の予測精度を集計"""
    conn = get_connection()
    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(top1_hit) as top1_hits,
            SUM(top3_hit) as top3_hits
        FROM prediction_results
    """).fetchone()
    conn.close()

    if not row or row["total"] == 0:
        console.print("[yellow]まだ結果データがありません。[/yellow]")
        return

    total = row["total"]
    console.print(f"\n[bold]累計成績[/bold]")
    console.print(f"  対象: {total}レース")
    console.print(f"  Top-1的中率: {row['top1_hits']}/{total} ({row['top1_hits']/total:.1%})")
    console.print(f"  Top-3的中率: {row['top3_hits']}/{total} ({row['top3_hits']/total:.1%})")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--all", action="store_true", help="全期間")
    args = parser.parse_args()

    if args.all:
        evaluate_all_time()
    else:
        evaluate_recent(args.days)
