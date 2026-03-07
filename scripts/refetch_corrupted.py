"""壊れたキャッシュ (U+FFFD) を削除して再取得"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from rich.console import Console

from config import CACHE_DIR

console = Console()
REPLACEMENT_BYTES = b"\xef\xbf\xbd"  # U+FFFD in UTF-8


def delete_corrupted_caches():
    """U+FFFD を含む HTML/JSON キャッシュを削除"""
    deleted_html = 0
    deleted_json = 0

    for f in sorted(os.listdir(CACHE_DIR)):
        path = CACHE_DIR / f
        if not path.is_file():
            continue
        if not (f.endswith(".html") or f.endswith(".json")):
            continue

        with open(path, "rb") as fh:
            if REPLACEMENT_BYTES in fh.read():
                path.unlink()
                if f.endswith(".html"):
                    deleted_html += 1
                else:
                    deleted_json += 1

    console.print(f"[green]削除完了: HTML {deleted_html}件, JSON {deleted_json}件[/green]")
    return deleted_html + deleted_json


def main():
    console.print("[bold blue]== 壊れたキャッシュの削除 ==[/bold blue]")
    total = delete_corrupted_caches()

    if total == 0:
        console.print("[green]壊れたキャッシュはありません[/green]")
        return

    console.print(f"\n[bold blue]== 再取得 ==[/bold blue]")
    console.print("init_db.py で 2023-2026 年のデータを再取得します...")

    from scripts.init_db import collect_past_data
    collect_past_data(start_year=2023, end_year=2026)

    console.print("\n[bold green]完了！[/bold green]")


if __name__ == "__main__":
    main()
