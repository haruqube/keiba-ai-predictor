"""モデル学習スクリプト"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from models.trainer import train_and_evaluate

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="競馬予想モデル学習")
    parser.add_argument("--cv", action="store_true", help="時系列クロスバリデーションも実行")
    args = parser.parse_args()
    train_and_evaluate(use_cv=args.cv)
