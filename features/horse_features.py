"""馬の成績ベース特徴量"""

import sqlite3
from features.base import BaseFeatureBuilder
from db.schema import get_connection


class HorseFeatureBuilder(BaseFeatureBuilder):
    """馬の過去成績から特徴量を生成"""

    @property
    def feature_names(self) -> list[str]:
        return [
            "avg_finish_3", "avg_finish_5", "avg_finish_all",
            "win_rate", "top3_rate",
            "avg_last_3f_3", "avg_last_3f_5",
            "best_finish", "worst_finish",
            "race_count", "win_count",
            "avg_odds_3", "avg_popularity_3",
            "days_since_last_race",
            "distance_win_rate", "surface_win_rate",
            "avg_horse_weight", "weight_trend",
        ]

    def build(self, race_id: str, horse_id: str, race_date: str) -> dict:
        conn = get_connection()
        try:
            return self._build(conn, race_id, horse_id, race_date)
        finally:
            conn.close()

    def _build(self, conn: sqlite3.Connection, race_id: str, horse_id: str, race_date: str) -> dict:
        # 当該レースより前の戦績を取得
        rows = conn.execute("""
            SELECT rr.*, r.distance, r.surface, r.date as race_date
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.horse_id = ? AND r.date < ?
            ORDER BY r.date DESC
        """, (horse_id, race_date)).fetchall()

        feats = {name: None for name in self.feature_names}
        if not rows:
            feats["race_count"] = 0
            return feats

        positions = [r["finish_position"] for r in rows if r["finish_position"]]
        last_3f_vals = [r["last_3f"] for r in rows if r["last_3f"]]
        odds_vals = [r["odds"] for r in rows if r["odds"]]
        pop_vals = [r["popularity"] for r in rows if r["popularity"]]
        weights = [r["horse_weight"] for r in rows if r["horse_weight"]]

        feats["race_count"] = len(rows)
        feats["win_count"] = sum(1 for p in positions if p == 1)

        if positions:
            feats["avg_finish_all"] = sum(positions) / len(positions)
            feats["avg_finish_3"] = sum(positions[:3]) / min(3, len(positions))
            feats["avg_finish_5"] = sum(positions[:5]) / min(5, len(positions))
            feats["best_finish"] = min(positions)
            feats["worst_finish"] = max(positions)
            feats["win_rate"] = feats["win_count"] / len(positions)
            feats["top3_rate"] = sum(1 for p in positions if p <= 3) / len(positions)

        if last_3f_vals:
            feats["avg_last_3f_3"] = sum(last_3f_vals[:3]) / min(3, len(last_3f_vals))
            feats["avg_last_3f_5"] = sum(last_3f_vals[:5]) / min(5, len(last_3f_vals))

        if odds_vals:
            feats["avg_odds_3"] = sum(odds_vals[:3]) / min(3, len(odds_vals))

        if pop_vals:
            feats["avg_popularity_3"] = sum(pop_vals[:3]) / min(3, len(pop_vals))

        # 前走からの経過日数
        if rows[0]["race_date"]:
            from datetime import datetime
            try:
                last_dt = datetime.strptime(rows[0]["race_date"], "%Y-%m-%d")
                curr_dt = datetime.strptime(race_date, "%Y-%m-%d")
                feats["days_since_last_race"] = (curr_dt - last_dt).days
            except ValueError:
                pass

        # 距離適性（当該レースの距離で絞る）
        race_row = conn.execute(
            "SELECT distance, surface FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        if race_row and race_row["distance"]:
            dist = race_row["distance"]
            dist_range = 200  # ±200m
            dist_rows = [r for r in rows if r["distance"] and abs(r["distance"] - dist) <= dist_range]
            if dist_rows:
                dist_positions = [r["finish_position"] for r in dist_rows if r["finish_position"]]
                if dist_positions:
                    feats["distance_win_rate"] = sum(1 for p in dist_positions if p == 1) / len(dist_positions)

            # 馬場適性
            if race_row["surface"]:
                surf_rows = [r for r in rows if r["surface"] == race_row["surface"]]
                if surf_rows:
                    surf_positions = [r["finish_position"] for r in surf_rows if r["finish_position"]]
                    if surf_positions:
                        feats["surface_win_rate"] = sum(1 for p in surf_positions if p == 1) / len(surf_positions)

        # 馬体重トレンド
        if len(weights) >= 2:
            feats["avg_horse_weight"] = sum(weights[:3]) / min(3, len(weights))
            feats["weight_trend"] = weights[0] - weights[1]

        return feats
