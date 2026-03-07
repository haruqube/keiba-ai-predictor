"""全特徴量を集約するビルダー"""

import pandas as pd
from db.schema import get_connection
from features.horse_features import HorseFeatureBuilder
from features.jockey_features import JockeyFeatureBuilder
from features.race_features import RaceFeatureBuilder


class FeatureBuilder:
    """全特徴量ビルダーを統合し、レース単位のDataFrameを生成"""

    def __init__(self):
        self.builders = [
            HorseFeatureBuilder(),
            JockeyFeatureBuilder(),
            RaceFeatureBuilder(),
        ]

    @property
    def feature_names(self) -> list[str]:
        names = []
        for b in self.builders:
            names.extend(b.feature_names)
        return names

    def build_race_features(self, race_id: str, race_date: str) -> pd.DataFrame:
        """1レース分の全出走馬の特徴量DataFrameを作成"""
        conn = get_connection()
        try:
            # 出走馬一覧を取得（entriesまたはrace_results）
            horses = conn.execute(
                "SELECT DISTINCT horse_id FROM entries WHERE race_id = ?",
                (race_id,)
            ).fetchall()

            if not horses:
                horses = conn.execute(
                    "SELECT DISTINCT horse_id FROM race_results WHERE race_id = ?",
                    (race_id,)
                ).fetchall()

            if not horses:
                return pd.DataFrame()

            rows = []
            for h in horses:
                horse_id = h["horse_id"]
                if not horse_id:
                    continue
                feat_row = {"race_id": race_id, "horse_id": horse_id}
                for builder in self.builders:
                    feats = builder.build(race_id, horse_id, race_date)
                    feat_row.update(feats)
                rows.append(feat_row)

            return pd.DataFrame(rows)
        finally:
            conn.close()

    def build_dataset(self, year_start: int, year_end: int) -> pd.DataFrame:
        """指定年範囲のレースすべてから学習データセットを作成"""
        conn = get_connection()
        races = conn.execute("""
            SELECT race_id, date FROM races
            WHERE date >= ? AND date <= ?
            ORDER BY date
        """, (f"{year_start}-01-01", f"{year_end}-12-31")).fetchall()
        conn.close()

        all_dfs = []
        for race in races:
            race_id = race["race_id"]
            race_date = race["date"]
            df = self.build_race_features(race_id, race_date)
            if not df.empty:
                # 着順をターゲットとして追加
                conn2 = get_connection()
                results = conn2.execute(
                    "SELECT horse_id, finish_position FROM race_results WHERE race_id = ?",
                    (race_id,)
                ).fetchall()
                conn2.close()

                finish_map = {r["horse_id"]: r["finish_position"] for r in results}
                df["finish_position"] = df["horse_id"].map(finish_map)
                df["race_date"] = race_date
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
