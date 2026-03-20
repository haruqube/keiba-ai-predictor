"""週末レース予測生成"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from rich.console import Console
from rich.table import Table

from config import RESULTS_DIR, VENUE_CODES
from db.schema import get_connection, insert_entry, insert_prediction
from data.scraper import KeibaScraper
from data.race_calendar import get_weekend_race_ids
from features.builder import FeatureBuilder
from models.lgbm_ranker import LGBMRanker

console = Console()


def predict_weekend():
    """今週末の全レースの予測を生成"""
    # モデル読み込み
    model_path = str(RESULTS_DIR / "model_lgbm.pkl")
    model = LGBMRanker()
    try:
        model.load(model_path)
    except FileNotFoundError:
        console.print("[red]モデルが見つかりません。先に train_model.py を実行してください。[/red]")
        return []

    scraper = KeibaScraper()
    builder = FeatureBuilder()
    conn = get_connection()

    weekend_races = get_weekend_race_ids()
    all_predictions = []

    for date_str, race_ids in weekend_races.items():
        date_display = f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:8]}"
        console.print(f"\n[bold blue]== {date_display} ({len(race_ids)}レース) ==[/bold blue]")

        for race_id in race_ids:
            try:
                # 出馬表取得
                entry_data = scraper.scrape_race_entry(race_id)
                race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

                # エントリーをDBに保存
                for e in entry_data.get("entries", []):
                    if e.get("horse_id"):
                        e["race_id"] = race_id
                        insert_entry(conn, e)

                # レース情報もDBに保存
                from db.schema import insert_race
                race_info = {
                    "race_id": race_id,
                    "date": race_date,
                    "venue": VENUE_CODES.get(entry_data.get("venue", ""), entry_data.get("venue", "")),
                    "race_number": entry_data.get("race_number", 0),
                    "race_name": entry_data.get("race_name"),
                    "grade": entry_data.get("grade"),
                    "distance": entry_data.get("distance"),
                    "surface": entry_data.get("surface"),
                    "direction": entry_data.get("direction"),
                    "weather": entry_data.get("weather"),
                    "track_condition": entry_data.get("track_condition"),
                    "horse_count": len(entry_data.get("entries", [])),
                }
                insert_race(conn, race_info)
                conn.commit()

                # 特徴量生成
                df = builder.build_race_features(race_id, race_date)
                if df.empty:
                    continue

                # 予測
                feature_names = builder.feature_names
                X = df[feature_names].copy()
                for col in X.columns:
                    X[col] = pd.to_numeric(X[col], errors="coerce")
                X = X.fillna(0)
                df["predicted_score"] = model.predict(X).values
                df = df.sort_values("predicted_score", ascending=False)
                df["predicted_rank"] = range(1, len(df) + 1)

                # 印をつける
                marks = ["◎", "○", "▲", "△", "△"]
                df["mark"] = [marks[i] if i < len(marks) else "" for i in range(len(df))]

                # 信頼度スコア: 1位と3位のスコア差（conf_gap13）
                # 新馬・未勝利戦は予測不能なので信頼度0
                race_grade = entry_data.get("grade", "")
                is_unpredictable = race_grade in ("新馬", "未勝利", "")

                if len(df) >= 3 and not is_unpredictable:
                    scores_sorted = df["predicted_score"].values  # already sorted desc
                    # 上位3頭が同スコア（NaN埋めで全同値）なら信頼度0
                    if scores_sorted[0] == scores_sorted[2]:
                        confidence = 0.0
                    else:
                        confidence = float(scores_sorted[0] - scores_sorted[2])
                else:
                    confidence = 0.0

                # 予測をDBに保存
                for _, row in df.iterrows():
                    insert_prediction(conn, {
                        "race_id": race_id,
                        "horse_id": row["horse_id"],
                        "predicted_score": row["predicted_score"],
                        "predicted_rank": row["predicted_rank"],
                        "mark": row["mark"],
                        "confidence": confidence,
                    })

                conn.commit()

                # 表示
                race_name = entry_data.get("race_name", race_id)
                # 信頼度ティア
                if confidence >= 0.5:
                    conf_label = "[bold green]HIGH[/bold green]"
                elif confidence >= 0.2:
                    conf_label = "[yellow]MID[/yellow]"
                else:
                    conf_label = "[dim]LOW[/dim]"
                table = Table(title=f"{race_name} (confidence: {confidence:.2f} {conf_label})")
                table.add_column("印")
                table.add_column("馬番")
                table.add_column("馬名")
                table.add_column("騎手")
                table.add_column("オッズ")
                table.add_column("スコア")

                for _, row in df.head(5).iterrows():
                    # entriesから情報取得
                    entry_row = conn.execute(
                        "SELECT * FROM entries WHERE race_id = ? AND horse_id = ?",
                        (race_id, row["horse_id"])
                    ).fetchone()

                    table.add_row(
                        row["mark"],
                        str(entry_row["horse_number"] if entry_row else "?"),
                        entry_row["horse_name"] if entry_row else row["horse_id"],
                        entry_row["jockey"] if entry_row else "?",
                        f"{entry_row['odds']}倍" if entry_row and entry_row["odds"] else "?",
                        f"{row['predicted_score']:.3f}",
                    )
                console.print(table)

                all_predictions.append({
                    "race_id": race_id,
                    "race_info": race_info,
                    "predictions": df.to_dict("records"),
                })

            except Exception as e:
                console.print(f"  [red]Error {race_id}: {e}[/red]")

    conn.close()
    console.print(f"\n[bold green]予測完了: {len(all_predictions)}レース[/bold green]")
    return all_predictions


if __name__ == "__main__":
    predict_weekend()
