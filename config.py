"""競馬予想AI設定"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── パス ──
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = BASE_DIR / "db" / "keiba.db"
RESULTS_DIR = BASE_DIR / "results"
TEMPLATES_DIR = BASE_DIR / "publishing" / "templates"

# ── X (Twitter) ──
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "")

# ── スクレイピング ──
SCRAPE_DELAY = 1.5  # 秒
NETKEIBA_BASE_URL = "https://race.netkeiba.com"
NETKEIBA_DB_URL = "https://db.netkeiba.com"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ── モデル ──
TRAIN_YEARS = [2022, 2023]
TEST_YEARS = [2024]
LGBM_PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "ndcg_eval_at": [1, 3, 5],
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_data_in_leaf": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
}
LGBM_NUM_BOOST_ROUND = 500
LGBM_EARLY_STOPPING_ROUNDS = 50

# ── 記事 ──
NOTE_PRICE_NORMAL = 200
NOTE_PRICE_G1 = 500

# ── 場コード ──
VENUE_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}

# ── レースグレード ──
GRADE_MAP = {
    "G1": 1, "G2": 2, "G3": 3, "OP": 4, "L": 5,
    "3勝": 6, "2勝": 7, "1勝": 8, "新馬": 9, "未勝利": 10,
}
