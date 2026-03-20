"""モデル学習・評価

- 時系列クロスバリデーション対応
- 評価指標: Top1/Top3精度, NDCG@3, MRR
- 特徴量重要度の詳細ログ (gain + split)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

from config import TRAIN_YEARS, TEST_YEARS, RESULTS_DIR
from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker

console = Console()
logger = logging.getLogger(__name__)


def prepare_dataset(df: pd.DataFrame, feature_names: list[str]):
    """DataFrameからX, y, groupを作成"""
    df = df.dropna(subset=["finish_position"])
    df = df[df["finish_position"] > 0]

    groups = df.groupby("race_id").size().tolist()

    X = df[feature_names].copy()
    y = df["finish_position"].copy()

    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")

    for col in X.columns:
        median = X[col].median()
        fill_val = median if pd.notna(median) else 0
        X[col] = X[col].fillna(fill_val)

    return X, y, groups


def evaluate_test_set(test_df: pd.DataFrame, model: LGBMRanker,
                      feature_cols: list[str]) -> dict:
    """テストセットの詳細評価（Top1/Top3/NDCG@3/MRR）"""
    test_df = test_df.copy()
    X_test = test_df[feature_cols].copy()
    for col in X_test.columns:
        X_test[col] = pd.to_numeric(X_test[col], errors="coerce")
    X_test = X_test.fillna(0)
    test_df["pred_score"] = model.predict(X_test).values

    top1_hits = 0
    top3_hits = 0
    total_races = 0
    ndcg_scores = []
    mrr_scores = []

    for race_id, group in test_df.groupby("race_id"):
        if len(group) < 3:
            continue
        total_races += 1

        pred_ranking = group.sort_values("pred_score", ascending=False)
        actual_ranking = group.sort_values("finish_position")

        pred_top1 = pred_ranking.iloc[0]["horse_id"]
        pred_top3 = set(pred_ranking.head(3)["horse_id"])
        actual_top1 = actual_ranking.iloc[0]["horse_id"]
        actual_top3 = set(actual_ranking.head(3)["horse_id"])

        # Top1的中
        if pred_top1 == actual_top1:
            top1_hits += 1
        # Top3重複数（予測Top3と実際Top3の一致数）
        top3_hits += len(pred_top3 & actual_top3)

        # NDCG@3
        n = len(group)
        pred_order = pred_ranking["horse_id"].tolist()
        actual_positions = dict(zip(group["horse_id"], group["finish_position"]))
        dcg = 0.0
        for i, hid in enumerate(pred_order[:3]):
            pos = actual_positions.get(hid, n)
            relevance = max(0, n - pos + 1)
            dcg += relevance / np.log2(i + 2)
        ideal_relevances = sorted(
            [max(0, n - p + 1) for p in actual_positions.values()], reverse=True
        )
        idcg = sum(r / np.log2(i + 2) for i, r in enumerate(ideal_relevances[:3]))
        ndcg_scores.append(dcg / idcg if idcg > 0 else 0.0)

        # MRR — 実際の1着が予測の何位にいるか
        for i, hid in enumerate(pred_order):
            if hid == actual_top1:
                mrr_scores.append(1.0 / (i + 1))
                break
        else:
            mrr_scores.append(0.0)

    results = {
        "total_races": total_races,
        "top1_hits": top1_hits,
        "top1_accuracy": top1_hits / total_races if total_races > 0 else 0.0,
        "top3_hits": top3_hits,
        "top3_overlap": top3_hits / (total_races * 3) if total_races > 0 else 0.0,
        "ndcg_at_3": np.mean(ndcg_scores) if ndcg_scores else 0.0,
        "mrr": np.mean(mrr_scores) if mrr_scores else 0.0,
    }

    return results


def time_series_cv(df: pd.DataFrame, feature_cols: list[str],
                   n_splits: int = 3) -> list[dict]:
    """時系列クロスバリデーション

    データを時系列順に分割し、常に過去→未来の方向で学習→評価する
    """
    dates = sorted(df["race_date"].unique())
    split_size = len(dates) // (n_splits + 1)

    results = []
    for fold in range(n_splits):
        train_end_idx = split_size * (fold + 1)
        val_start_idx = train_end_idx
        val_end_idx = min(train_end_idx + split_size, len(dates))

        if val_end_idx <= val_start_idx:
            break

        train_dates = set(dates[:train_end_idx])
        val_dates = set(dates[val_start_idx:val_end_idx])

        train_df = df[df["race_date"].isin(train_dates)]
        val_df = df[df["race_date"].isin(val_dates)]

        if train_df.empty or val_df.empty:
            continue

        X_train, y_train, group_train = prepare_dataset(train_df, feature_cols)
        X_val, y_val, group_val = prepare_dataset(val_df, feature_cols)

        model = LGBMRanker()
        model.train(X_train, y_train, group_train, X_val, y_val, group_val)

        fold_results = evaluate_test_set(val_df, model, feature_cols)
        fold_results["fold"] = fold
        fold_results["train_size"] = len(train_df)
        fold_results["val_size"] = len(val_df)
        results.append(fold_results)

        console.print(f"  Fold {fold}: train={len(train_df)} val={len(val_df)} "
                       f"Top1={fold_results['top1_accuracy']:.1%} "
                       f"NDCG@3={fold_results['ndcg_at_3']:.4f}")

    if results:
        avg_top1 = np.mean([r["top1_accuracy"] for r in results])
        avg_ndcg = np.mean([r["ndcg_at_3"] for r in results])
        avg_mrr = np.mean([r["mrr"] for r in results])
        console.print(f"  [bold]CV Average: Top1={avg_top1:.1%}  "
                       f"NDCG@3={avg_ndcg:.4f}  MRR={avg_mrr:.4f}[/bold]")

    return results


def train_and_evaluate(use_cv: bool = False):
    """学習 → 評価のフルフロー"""
    logging.basicConfig(level=logging.INFO)

    builder = FeatureBuilder()
    feature_names = builder.feature_names

    # 全データをまとめて取得
    all_years = sorted(set(TRAIN_YEARS + TEST_YEARS))
    console.print(f"[bold blue]データ構築中 ({all_years[0]}-{all_years[-1]})...[/bold blue]")
    all_df = builder.build_dataset(all_years[0], all_years[-1])

    if all_df.empty:
        console.print("[red]データが空です。init_db.pyを先に実行してください。[/red]")
        return

    all_df = all_df.dropna(subset=["finish_position"])
    all_df = all_df[all_df["finish_position"] > 0]
    console.print(f"  総データ: {len(all_df)}行, {all_df['race_id'].nunique()}レース")

    # 時系列クロスバリデーション
    if use_cv:
        console.print("[bold blue]時系列クロスバリデーション...[/bold blue]")
        time_series_cv(all_df, feature_names)

    # 時系列で train/val 分割（85%/15%）
    all_dates = sorted(all_df["race_date"].unique())
    val_cutoff = all_dates[int(len(all_dates) * 0.85)]
    train_df = all_df[all_df["race_date"] < val_cutoff]
    val_df = all_df[all_df["race_date"] >= val_cutoff]

    if train_df.empty:
        console.print("[red]学習データが空です。[/red]")
        return

    X_train, y_train, groups_train = prepare_dataset(train_df, feature_names)
    X_val, y_val, groups_val = prepare_dataset(val_df, feature_names) if not val_df.empty else (None, None, None)

    console.print(f"[bold blue]モデル学習中...[/bold blue]")
    console.print(f"  学習: {len(train_df)}行, {len(groups_train)}レース (< {val_cutoff})")
    if X_val is not None:
        console.print(f"  検証: {len(val_df)}行, {len(groups_val)}レース (>= {val_cutoff})")

    model = LGBMRanker()
    model.train(X_train, y_train, groups_train, X_val, y_val, groups_val)

    # モデル保存
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = str(RESULTS_DIR / "model_lgbm.pkl")
    model.save(model_path)
    console.print(f"[green]モデル保存: {model_path}[/green]")

    # 特徴量重要度（gain + split）
    fi_gain = model.feature_importance("gain")
    fi_split = model.feature_importance("split")
    fi = fi_gain.merge(fi_split, on="feature", suffixes=("_gain", "_split"))
    fi = fi.sort_values("importance_gain", ascending=False)
    fi.to_csv(str(RESULTS_DIR / "feature_importance.csv"), index=False)

    fi_table = Table(title="特徴量重要度 Top10")
    fi_table.add_column("特徴量", style="cyan")
    fi_table.add_column("Gain", style="green", justify="right")
    fi_table.add_column("Split", style="yellow", justify="right")
    for _, row in fi.head(10).iterrows():
        fi_table.add_row(row["feature"], f"{row['importance_gain']:.0f}", f"{row['importance_split']:.0f}")
    console.print(fi_table)

    # Validation セット評価
    if not val_df.empty:
        metrics = evaluate_test_set(val_df, model, feature_names)

        result_table = Table(title="モデル評価結果")
        result_table.add_column("指標", style="cyan")
        result_table.add_column("値", style="green")
        result_table.add_row("総レース数", str(metrics["total_races"]))
        result_table.add_row("Top-1的中率", f"{metrics['top1_accuracy']:.1%}")
        result_table.add_row("Top-3重複率", f"{metrics['top3_overlap']:.1%}")
        result_table.add_row("NDCG@3", f"{metrics['ndcg_at_3']:.4f}")
        result_table.add_row("MRR", f"{metrics['mrr']:.4f}")
        console.print(result_table)

        return model, metrics

    return model, {}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--cv", action="store_true", help="時系列CVも実行")
    args = parser.parse_args()
    train_and_evaluate(use_cv=args.cv)
