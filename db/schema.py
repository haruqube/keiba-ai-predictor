"""SQLiteスキーマ定義・DB初期化"""

import sqlite3
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS races (
        race_id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        venue TEXT NOT NULL,
        race_number INTEGER NOT NULL,
        race_name TEXT,
        grade TEXT,
        distance INTEGER,
        surface TEXT,
        direction TEXT,
        weather TEXT,
        track_condition TEXT,
        horse_count INTEGER,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS horses (
        horse_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        sex TEXT,
        birth_year INTEGER,
        father TEXT,
        mother TEXT,
        trainer TEXT,
        owner TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS race_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        horse_id TEXT NOT NULL,
        finish_position INTEGER,
        bracket_number INTEGER,
        horse_number INTEGER,
        horse_name TEXT,
        sex_age TEXT,
        weight_carried REAL,
        jockey TEXT,
        finish_time TEXT,
        margin TEXT,
        speed_figure REAL,
        passing_order TEXT,
        last_3f REAL,
        horse_weight INTEGER,
        horse_weight_change INTEGER,
        odds REAL,
        popularity INTEGER,
        trainer TEXT,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        FOREIGN KEY (horse_id) REFERENCES horses(horse_id),
        UNIQUE(race_id, horse_id)
    );

    CREATE TABLE IF NOT EXISTS entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        horse_id TEXT NOT NULL,
        bracket_number INTEGER,
        horse_number INTEGER,
        horse_name TEXT,
        sex_age TEXT,
        weight_carried REAL,
        jockey TEXT,
        trainer TEXT,
        odds REAL,
        popularity INTEGER,
        horse_weight INTEGER,
        horse_weight_change INTEGER,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id, horse_id)
    );

    CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        horse_id TEXT NOT NULL,
        predicted_score REAL,
        predicted_rank INTEGER,
        mark TEXT,
        confidence REAL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id, horse_id)
    );

    CREATE TABLE IF NOT EXISTS prediction_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        predicted_top1 TEXT,
        predicted_top3 TEXT,
        actual_top1 TEXT,
        actual_top3 TEXT,
        top1_hit INTEGER DEFAULT 0,
        top3_hit INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id)
    );

    CREATE INDEX IF NOT EXISTS idx_results_race ON race_results(race_id);
    CREATE INDEX IF NOT EXISTS idx_results_horse ON race_results(horse_id);
    CREATE INDEX IF NOT EXISTS idx_results_jockey ON race_results(jockey);
    CREATE INDEX IF NOT EXISTS idx_races_date ON races(date);
    CREATE INDEX IF NOT EXISTS idx_entries_race ON entries(race_id);
    CREATE INDEX IF NOT EXISTS idx_predictions_race ON predictions(race_id);
    """)

    conn.commit()
    conn.close()


def insert_race(conn: sqlite3.Connection, race: dict):
    conn.execute("""
        INSERT OR REPLACE INTO races
        (race_id, date, venue, race_number, race_name, grade,
         distance, surface, direction, weather, track_condition, horse_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        race["race_id"], race["date"], race["venue"], race["race_number"],
        race.get("race_name"), race.get("grade"), race.get("distance"),
        race.get("surface"), race.get("direction"), race.get("weather"),
        race.get("track_condition"), race.get("horse_count"),
    ))


def insert_horse(conn: sqlite3.Connection, horse: dict):
    conn.execute("""
        INSERT OR IGNORE INTO horses
        (horse_id, name, sex, birth_year, father, mother, trainer, owner)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        horse["horse_id"], horse["name"], horse.get("sex"),
        horse.get("birth_year"), horse.get("father"), horse.get("mother"),
        horse.get("trainer"), horse.get("owner"),
    ))


def insert_result(conn: sqlite3.Connection, result: dict):
    conn.execute("""
        INSERT OR REPLACE INTO race_results
        (race_id, horse_id, finish_position, bracket_number, horse_number,
         horse_name, sex_age, weight_carried, jockey, finish_time, margin,
         speed_figure, passing_order, last_3f, horse_weight, horse_weight_change,
         odds, popularity, trainer)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["race_id"], result["horse_id"], result.get("finish_position"),
        result.get("bracket_number"), result.get("horse_number"),
        result.get("horse_name"), result.get("sex_age"),
        result.get("weight_carried"), result.get("jockey"),
        result.get("finish_time"), result.get("margin"),
        result.get("speed_figure"), result.get("passing_order"),
        result.get("last_3f"), result.get("horse_weight"),
        result.get("horse_weight_change"), result.get("odds"),
        result.get("popularity"), result.get("trainer"),
    ))


def insert_entry(conn: sqlite3.Connection, entry: dict):
    conn.execute("""
        INSERT OR REPLACE INTO entries
        (race_id, horse_id, bracket_number, horse_number, horse_name,
         sex_age, weight_carried, jockey, trainer, odds, popularity,
         horse_weight, horse_weight_change)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["race_id"], entry["horse_id"], entry.get("bracket_number"),
        entry.get("horse_number"), entry.get("horse_name"),
        entry.get("sex_age"), entry.get("weight_carried"),
        entry.get("jockey"), entry.get("trainer"),
        entry.get("odds"), entry.get("popularity"),
        entry.get("horse_weight"), entry.get("horse_weight_change"),
    ))


def insert_prediction(conn: sqlite3.Connection, pred: dict):
    conn.execute("""
        INSERT OR REPLACE INTO predictions
        (race_id, horse_id, predicted_score, predicted_rank, mark, confidence)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        pred["race_id"], pred["horse_id"], pred.get("predicted_score"),
        pred.get("predicted_rank"), pred.get("mark"), pred.get("confidence"),
    ))


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
