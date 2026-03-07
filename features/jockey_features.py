"""騎手特徴量"""

import sqlite3
from features.base import BaseFeatureBuilder
from db.schema import get_connection


class JockeyFeatureBuilder(BaseFeatureBuilder):
    """騎手の成績から特徴量を生成"""

    @property
    def feature_names(self) -> list[str]:
        return [
            "jockey_win_rate", "jockey_top3_rate",
            "jockey_race_count",
            "jockey_venue_win_rate",
            "jockey_distance_win_rate",
            "jockey_surface_win_rate",
            "jockey_recent_win_rate",
        ]

    def build(self, race_id: str, horse_id: str, race_date: str) -> dict:
        conn = get_connection()
        try:
            return self._build(conn, race_id, horse_id, race_date)
        finally:
            conn.close()

    def _build(self, conn: sqlite3.Connection, race_id: str, horse_id: str, race_date: str) -> dict:
        feats = {name: None for name in self.feature_names}

        # 当該レースの出走情報から騎手名を取得
        jockey = self._get_jockey(conn, race_id, horse_id, race_date)
        if not jockey:
            return feats

        # 騎手の過去成績
        rows = conn.execute("""
            SELECT rr.finish_position, r.venue, r.distance, r.surface, r.date as race_date
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.jockey = ? AND r.date < ?
            ORDER BY r.date DESC
        """, (jockey, race_date)).fetchall()

        if not rows:
            feats["jockey_race_count"] = 0
            return feats

        positions = [r["finish_position"] for r in rows if r["finish_position"]]
        feats["jockey_race_count"] = len(positions)

        if positions:
            feats["jockey_win_rate"] = sum(1 for p in positions if p == 1) / len(positions)
            feats["jockey_top3_rate"] = sum(1 for p in positions if p <= 3) / len(positions)

        # 直近30戦の勝率
        recent = positions[:30]
        if recent:
            feats["jockey_recent_win_rate"] = sum(1 for p in recent if p == 1) / len(recent)

        # 場所別勝率
        race_row = conn.execute(
            "SELECT venue, distance, surface FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()

        if race_row:
            # 場別
            venue_rows = [r for r in rows if r["venue"] == race_row["venue"] and r["finish_position"]]
            if venue_rows:
                v_pos = [r["finish_position"] for r in venue_rows]
                feats["jockey_venue_win_rate"] = sum(1 for p in v_pos if p == 1) / len(v_pos)

            # 距離別
            if race_row["distance"]:
                dist = race_row["distance"]
                dist_rows = [r for r in rows if r["distance"] and abs(r["distance"] - dist) <= 200 and r["finish_position"]]
                if dist_rows:
                    d_pos = [r["finish_position"] for r in dist_rows]
                    feats["jockey_distance_win_rate"] = sum(1 for p in d_pos if p == 1) / len(d_pos)

            # 馬場別
            if race_row["surface"]:
                surf_rows = [r for r in rows if r["surface"] == race_row["surface"] and r["finish_position"]]
                if surf_rows:
                    s_pos = [r["finish_position"] for r in surf_rows]
                    feats["jockey_surface_win_rate"] = sum(1 for p in s_pos if p == 1) / len(s_pos)

        return feats

    def _get_jockey(self, conn, race_id, horse_id, race_date):
        # まずentriesから、なければrace_resultsから
        row = conn.execute(
            "SELECT jockey FROM entries WHERE race_id = ? AND horse_id = ?",
            (race_id, horse_id)
        ).fetchone()
        if row:
            return row["jockey"]

        row = conn.execute(
            "SELECT jockey FROM race_results WHERE race_id = ? AND horse_id = ?",
            (race_id, horse_id)
        ).fetchone()
        return row["jockey"] if row else None
