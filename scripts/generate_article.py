"""予測データをMarkdown形式で出力 — Claude Codeで記事化する素材"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from rich.console import Console

from config import RESULTS_DIR
from db.schema import get_connection

console = Console()


def generate_prediction_report(target_date: str | None = None) -> str:
    """予測データを整形してMarkdownレポートとして出力

    このファイルをClaude Codeに読ませて記事を生成してもらう。
    """
    conn = get_connection()

    # 予測済みレースを取得
    if target_date:
        races = conn.execute("""
            SELECT DISTINCT r.* FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            WHERE r.date = ?
            ORDER BY r.venue, r.race_number
        """, (target_date,)).fetchall()
    else:
        races = conn.execute("""
            SELECT DISTINCT r.* FROM races r
            JOIN predictions p ON r.race_id = p.race_id
            ORDER BY r.date DESC, r.venue, r.race_number
            LIMIT 36
        """).fetchall()

    if not races:
        console.print("[red]予測データがありません。predict_weekend.pyを先に実行してください。[/red]")
        conn.close()
        return ""

    dates = sorted(set(r["date"] for r in races))
    venues = sorted(set(r["venue"] for r in races if r["venue"]))

    lines = []
    lines.append(f"# 競馬AI予測データ {', '.join(dates)}")
    lines.append(f"開催: {' / '.join(venues)}")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    for race in races:
        race_id = race["race_id"]

        lines.append(f"---")
        lines.append(f"## {race['venue']} {race['race_number']}R {race['race_name'] or ''}")
        lines.append(f"- 距離: {race['distance']}m {race['surface'] or ''} {race['direction'] or ''}")
        lines.append(f"- グレード: {race['grade'] or '一般'}")
        lines.append(f"- 馬場: {race['track_condition'] or '不明'} / 天候: {race['weather'] or '不明'}")
        lines.append(f"- 出走: {race['horse_count'] or '?'}頭")
        lines.append("")

        # 予測データ
        predictions = conn.execute("""
            SELECT p.*, e.horse_name, e.horse_number, e.jockey, e.odds, e.popularity,
                   e.weight_carried, e.horse_weight, e.horse_weight_change
            FROM predictions p
            LEFT JOIN entries e ON p.race_id = e.race_id AND p.horse_id = e.horse_id
            WHERE p.race_id = ?
            ORDER BY p.predicted_rank
        """, (race_id,)).fetchall()

        if not predictions:
            lines.append("(予測データなし)")
            lines.append("")
            continue

        marks = ["◎", "○", "▲", "△", "△"]

        lines.append("| 印 | 馬番 | 馬名 | 騎手 | 斤量 | オッズ | 人気 | スコア | 直近成績 |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        for i, p in enumerate(predictions):
            mark = marks[i] if i < len(marks) else ""

            # 直近5戦の成績
            recent = conn.execute("""
                SELECT rr.finish_position, r.date
                FROM race_results rr
                JOIN races r ON rr.race_id = r.race_id
                WHERE rr.horse_id = ? AND rr.race_id != ?
                ORDER BY r.date DESC LIMIT 5
            """, (p["horse_id"], race_id)).fetchall()

            recent_str = " ".join(
                str(r["finish_position"]) for r in recent if r["finish_position"]
            ) or "-"

            # 馬体重
            weight_str = ""
            if p["horse_weight"]:
                change = p["horse_weight_change"]
                change_str = f"({'+' if change and change > 0 else ''}{change})" if change else ""
                weight_str = f"{p['horse_weight']}{change_str}"

            lines.append(
                f"| {mark} "
                f"| {p['horse_number'] or '?'} "
                f"| {p['horse_name'] or p['horse_id']} "
                f"| {p['jockey'] or '?'} "
                f"| {p['weight_carried'] or '?'} "
                f"| {p['odds'] or '?'} "
                f"| {p['popularity'] or '?'} "
                f"| {p['predicted_score']:.3f} "
                f"| {recent_str} |"
            )

        lines.append("")

        # 補足: 上位馬の距離・馬場別成績
        lines.append("**上位馬の詳細:**")
        for i, p in enumerate(predictions[:5]):
            horse_id = p["horse_id"]
            name = p["horse_name"] or horse_id

            # 同距離・同馬場の成績
            dist_results = conn.execute("""
                SELECT rr.finish_position
                FROM race_results rr
                JOIN races r ON rr.race_id = r.race_id
                WHERE rr.horse_id = ? AND r.distance = ? AND r.surface = ?
                ORDER BY r.date DESC LIMIT 10
            """, (horse_id, race["distance"], race["surface"])).fetchall()

            dist_str = " ".join(str(r["finish_position"]) for r in dist_results if r["finish_position"]) or "経験なし"

            # 同会場の成績
            venue_results = conn.execute("""
                SELECT rr.finish_position
                FROM race_results rr
                JOIN races r ON rr.race_id = r.race_id
                WHERE rr.horse_id = ? AND r.venue = ?
                ORDER BY r.date DESC LIMIT 10
            """, (horse_id, race["venue"])).fetchall()

            venue_str = " ".join(str(r["finish_position"]) for r in venue_results if r["finish_position"]) or "経験なし"

            lines.append(f"- {marks[i] if i < len(marks) else ''} {name}: 同距離[{dist_str}] / 同会場[{venue_str}]")

        lines.append("")

    conn.close()

    report = "\n".join(lines)

    # ファイルに保存
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    date_file = dates[0].replace("-", "") if dates else datetime.now().strftime("%Y%m%d")
    report_path = RESULTS_DIR / f"prediction_report_{date_file}.md"
    report_path.write_text(report, encoding="utf-8")

    console.print(f"[green]予測レポート保存: {report_path}[/green]")
    console.print(f"[dim]このファイルをClaude Codeに読ませて記事を生成してください。[/dim]")
    console.print(f'[dim]例: 「{report_path} を読んでnote.com用の競馬予想記事を書いて」[/dim]')

    return str(report_path)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="対象日 (YYYY-MM-DD)")
    args = parser.parse_args()
    generate_prediction_report(args.date)
