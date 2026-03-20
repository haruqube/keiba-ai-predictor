"""レース条件特徴量"""

from features.base import BaseFeatureBuilder
from db.schema import get_connection
from config import GRADE_MAP


class RaceFeatureBuilder(BaseFeatureBuilder):
    """レース条件や出走馬情報から特徴量を生成"""

    @property
    def feature_names(self) -> list[str]:
        return [
            "distance", "is_turf", "is_right",
            "grade_code",
            "horse_count",
            "horse_number", "bracket_number",
            "weight_carried",
            "is_good_track",
            "month",
        ]

    def build(self, race_id: str, horse_id: str, race_date: str) -> dict:
        conn = get_connection()
        try:
            return self._build(conn, race_id, horse_id, race_date)
        finally:
            conn.close()

    def _build(self, conn, race_id, horse_id, race_date):
        feats = {name: None for name in self.feature_names}

        # レース情報
        race = conn.execute(
            "SELECT * FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()

        if race:
            feats["distance"] = race["distance"]
            feats["is_turf"] = 1 if race["surface"] == "芝" else 0
            feats["is_right"] = 1 if race["direction"] == "右" else 0
            feats["grade_code"] = GRADE_MAP.get(race["grade"], 10) if race["grade"] else 10
            feats["horse_count"] = race["horse_count"]
            feats["is_good_track"] = 1 if race["track_condition"] in ("良", None) else 0

            if race["date"]:
                try:
                    feats["month"] = int(race["date"].split("-")[1])
                except (IndexError, ValueError):
                    pass

        # 出走馬情報（entries or race_results）
        entry = conn.execute(
            "SELECT * FROM entries WHERE race_id = ? AND horse_id = ?",
            (race_id, horse_id)
        ).fetchone()

        if not entry:
            entry = conn.execute(
                "SELECT * FROM race_results WHERE race_id = ? AND horse_id = ?",
                (race_id, horse_id)
            ).fetchone()

        if entry:
            feats["horse_number"] = entry["horse_number"]
            feats["bracket_number"] = entry["bracket_number"]
            feats["weight_carried"] = entry["weight_carried"]

        return feats
