"""全体オーケストレーション — 週次パイプライン"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from rich.console import Console
from rich.panel import Panel

console = Console()


def run_monday_pipeline():
    """月曜パイプライン: 結果更新 → 精度検証"""
    console.print(Panel("Monday Pipeline", style="bold blue"))

    # 1. 先週の結果を取得
    console.print("\n[bold]Step 1: 先週結果の取得[/bold]")
    from scripts.update_data import update_recent_results
    update_recent_results(days_back=3)

    # 2. 予測精度を検証
    console.print("\n[bold]Step 2: 予測精度検証[/bold]")
    from backtest.evaluator import evaluate_recent
    evaluate_recent(days_back=7)

    console.print("\n[green]月曜パイプライン完了[/green]")


def run_friday_pipeline():
    """金曜パイプライン: データ取得 → 予測 → レポート出力"""
    console.print(Panel("Friday Pipeline", style="bold blue"))

    # 1. 週末出走馬取得 + 予測
    console.print("\n[bold]Step 1: 週末予測生成[/bold]")
    from scripts.predict_weekend import predict_weekend
    predictions = predict_weekend()

    if not predictions:
        console.print("[red]予測が生成されませんでした。[/red]")
        return

    # 2. 予測レポート出力
    console.print("\n[bold]Step 2: 予測レポート出力[/bold]")
    from scripts.generate_article import generate_prediction_report
    report_path = generate_prediction_report()

    console.print("\n[green]金曜パイプライン完了[/green]")
    console.print(f"\n[bold]次のステップ:[/bold]")
    console.print(f'  Claude Codeで: 「{report_path} を読んでnote.com用の競馬予想記事を書いて」')


def run_full_pipeline():
    """曜日に応じたパイプラインを実行"""
    weekday = datetime.now().weekday()
    if weekday == 0:  # 月曜
        run_monday_pipeline()
    elif weekday == 4:  # 金曜
        run_friday_pipeline()
    else:
        console.print(f"[yellow]Today is {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][weekday]}.[/yellow]")
        console.print("  --monday / --friday for manual execution.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Weekly pipeline")
    parser.add_argument("--monday", action="store_true", help="Run Monday pipeline")
    parser.add_argument("--friday", action="store_true", help="Run Friday pipeline")
    args = parser.parse_args()

    if args.monday:
        run_monday_pipeline()
    elif args.friday:
        run_friday_pipeline()
    else:
        run_full_pipeline()
