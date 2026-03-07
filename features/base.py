"""特徴量ビルダー抽象基底クラス"""

from abc import ABC, abstractmethod
import pandas as pd


class BaseFeatureBuilder(ABC):
    """特徴量ビルダーの基底クラス"""

    @abstractmethod
    def build(self, race_id: str, horse_id: str, race_date: str) -> dict:
        """指定した馬・レースの特徴量を辞書で返す"""
        pass

    @property
    @abstractmethod
    def feature_names(self) -> list[str]:
        """このビルダーが生成する特徴量名のリスト"""
        pass
