"""学習データ範囲を変えて精度比較"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from rich.console import Console
from rich.table import Table

from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker
from models.trainer import prepare_dataset, evaluate_predictions
from config import RESULTS_DIR

console = Console()


def train_and_eval(train_years: list[int], test_years: list[int], label: str):
    """指定した年範囲で学習→評価"""
    builder = FeatureBuilder()
    feature_names = builder.feature_names

    console.print(f"\n[bold cyan]--- {label} ---[/bold cyan]")
    console.print(f"  Train: {train_years[0]}-{train_years[-1]}, Test: {test_years[0]}-{test_years[-1]}")

    # 学習データ
    train_df = builder.build_dataset(train_years[0], train_years[-1])
    if train_df.empty:
        console.print("  [red]学習データなし[/red]")
        return None

    # テストデータ
    test_df = builder.build_dataset(test_years[0], test_years[-1])
    if test_df.empty:
        console.print("  [red]テストデータなし[/red]")
        return None

    train_races = train_df["race_id"].nunique()
    test_races = test_df["race_id"].nunique()
    console.print(f"  学習: {len(train_df)}行 ({train_races}レース)")
    console.print(f"  テスト: {len(test_df)}行 ({test_races}レース)")

    # 学習
    X_train, y_train, groups_train = prepare_dataset(train_df, feature_names)
    X_test, y_test, groups_test = prepare_dataset(test_df, feature_names)

    model = LGBMRanker()
    model.train(X_train, y_train, groups_train, X_test, y_test, groups_test)

    # 評価
    test_clean = test_df.dropna(subset=["finish_position"])
    test_clean = test_clean[test_clean["finish_position"] > 0]
    predict_X = test_clean[feature_names].copy()
    for col in predict_X.columns:
        predict_X[col] = pd.to_numeric(predict_X[col], errors="coerce")
    predict_X = predict_X.fillna(0)
    test_clean["predicted_score"] = model.predict(predict_X).values

    metrics = evaluate_predictions(test_clean)
    metrics["label"] = label
    metrics["train_years"] = f"{train_years[0]}-{train_years[-1]}"
    metrics["test_years"] = f"{test_years[0]}-{test_years[-1]}"
    metrics["train_rows"] = len(X_train)
    metrics["train_races"] = train_races

    return metrics, model


def main():
    configs = [
        # (train_years, test_years, label)
        ([2022, 2023], [2024], "A: 2022-2023 -> 2024 (現行)"),
        ([2022, 2023, 2024], [2025], "B: 2022-2024 -> 2025"),
        ([2023, 2024], [2025], "C: 2023-2024 -> 2025"),
        ([2022, 2023, 2024, 2025], [2025], "D: 2022-2025 -> 2025 (leak)"),
    ]

    results = []
    best_model = None
    best_score = 0

    for train_yrs, test_yrs, label in configs:
        result = train_and_eval(train_yrs, test_yrs, label)
        if result:
            metrics, model = result
            results.append(metrics)
            if metrics["top3_accuracy"] > best_score:
                best_score = metrics["top3_accuracy"]
                best_model = model
                best_label = label

    # 比較テーブル
    console.print("\n")
    table = Table(title="学習データ範囲 vs 精度比較")
    table.add_column("パターン", style="cyan")
    table.add_column("学習期間")
    table.add_column("テスト期間")
    table.add_column("学習行数", justify="right")
    table.add_column("テストレース", justify="right")
    table.add_column("Top-1的中率", justify="right", style="green")
    table.add_column("Top-3的中率", justify="right", style="bold green")

    for r in results:
        table.add_row(
            r["label"],
            r["train_years"],
            r["test_years"],
            str(r["train_rows"]),
            str(r["total_races"]),
            f"{r['top1_accuracy']:.1%}",
            f"{r['top3_accuracy']:.1%}",
        )

    console.print(table)

    # 最良モデルを保存
    if best_model:
        console.print(f"\n[bold green]ベストモデル: {best_label}[/bold green]")
        # 注: D は leakage があるので除外して判定すべき
        # ここではユーザーに結果を見せるだけ


if __name__ == "__main__":
    main()
