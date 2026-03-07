"""モデル学習・評価"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

from config import TRAIN_YEARS, TEST_YEARS, RESULTS_DIR
from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker

console = Console()


def prepare_dataset(df: pd.DataFrame, feature_names: list[str]):
    """DataFrameからX, y, groupを作成"""
    # 着順がないレコードを除外
    df = df.dropna(subset=["finish_position"])
    df = df[df["finish_position"] > 0]

    # グループ（レースごとの馬の数）
    groups = df.groupby("race_id").size().tolist()
    race_order = df.groupby("race_id").ngroup()

    X = df[feature_names].copy()
    y = df["finish_position"].copy()

    # object型 → numeric に変換（None混在で object になるカラム対策）
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")

    # 欠損値を中央値で埋める
    for col in X.columns:
        median = X[col].median()
        fill_val = median if pd.notna(median) else 0
        X[col] = X[col].fillna(fill_val)

    return X, y, groups


def evaluate_predictions(df: pd.DataFrame, score_col: str = "predicted_score") -> dict:
    """予測精度を評価"""
    metrics = {"top1_hits": 0, "top3_hits": 0, "total_races": 0}

    for race_id, group in df.groupby("race_id"):
        if group["finish_position"].isna().all():
            continue

        metrics["total_races"] += 1

        # 予測順位
        group = group.sort_values(score_col, ascending=False)
        predicted_top1 = group.iloc[0]["horse_id"]
        predicted_top3 = set(group.iloc[:3]["horse_id"])

        # 実際の順位
        actual = group.sort_values("finish_position")
        actual_top1 = actual.iloc[0]["horse_id"]
        actual_top3 = set(actual.iloc[:3]["horse_id"])

        if predicted_top1 == actual_top1:
            metrics["top1_hits"] += 1
        if len(predicted_top3 & actual_top3) > 0:
            metrics["top3_hits"] += 1

    total = metrics["total_races"]
    if total > 0:
        metrics["top1_accuracy"] = metrics["top1_hits"] / total
        metrics["top3_accuracy"] = metrics["top3_hits"] / total
    else:
        metrics["top1_accuracy"] = 0
        metrics["top3_accuracy"] = 0

    return metrics


def train_and_evaluate():
    """学習 → 評価のフルフロー"""
    builder = FeatureBuilder()
    feature_names = builder.feature_names

    # 学習データ作成
    console.print("[bold blue]学習データ構築中...[/bold blue]")
    train_df = builder.build_dataset(TRAIN_YEARS[0], TRAIN_YEARS[-1])
    if train_df.empty:
        console.print("[red]学習データが空です。init_db.pyを先に実行してください。[/red]")
        return

    console.print(f"  学習データ: {len(train_df)}行, {train_df['race_id'].nunique()}レース")

    # テストデータ作成
    console.print("[bold blue]テストデータ構築中...[/bold blue]")
    test_df = builder.build_dataset(TEST_YEARS[0], TEST_YEARS[-1])
    console.print(f"  テストデータ: {len(test_df)}行, {test_df['race_id'].nunique()}レース")

    # 学習
    X_train, y_train, groups_train = prepare_dataset(train_df, feature_names)
    X_test, y_test, groups_test = prepare_dataset(test_df, feature_names)

    console.print("[bold blue]モデル学習中...[/bold blue]")
    model = LGBMRanker()
    model.train(X_train, y_train, groups_train, X_test, y_test, groups_test)

    # 評価
    test_df_clean = test_df.dropna(subset=["finish_position"])
    test_df_clean = test_df_clean[test_df_clean["finish_position"] > 0]
    predict_X = test_df_clean[feature_names].copy()
    for col in predict_X.columns:
        predict_X[col] = pd.to_numeric(predict_X[col], errors="coerce")
    predict_X = predict_X.fillna(0)
    test_df_clean["predicted_score"] = model.predict(predict_X).values

    metrics = evaluate_predictions(test_df_clean)

    table = Table(title="モデル評価結果")
    table.add_column("指標", style="cyan")
    table.add_column("値", style="green")
    table.add_row("総レース数", str(metrics["total_races"]))
    table.add_row("Top-1的中率", f"{metrics['top1_accuracy']:.1%}")
    table.add_row("Top-3的中率", f"{metrics['top3_accuracy']:.1%}")
    console.print(table)

    # 特徴量重要度
    fi = model.feature_importance()
    fi_table = Table(title="特徴量重要度 Top10")
    fi_table.add_column("特徴量", style="cyan")
    fi_table.add_column("重要度", style="green")
    for _, row in fi.head(10).iterrows():
        fi_table.add_row(row["feature"], f"{row['importance']:.0f}")
    console.print(fi_table)

    # モデル保存
    model_path = str(RESULTS_DIR / "model_lgbm.pkl")
    model.save(model_path)
    console.print(f"[green]モデル保存: {model_path}[/green]")

    return model, metrics


if __name__ == "__main__":
    train_and_evaluate()
