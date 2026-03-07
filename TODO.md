## TODO
- [ ] X APIキー設定・投稿テスト
- [ ] note.com記事の運用フロー確立（手動投稿→自動化）
- [ ] バックテスト結果の確認・精度評価
- [ ] モデルチューニング（特徴量追加、ハイパラ調整）
- [ ] weekly_pipeline.py のCron設定（金曜/月曜自動実行）

## 完了
- [x] DBスキーマ設計・初期化
- [x] netkeibaスクレイパー実装
- [x] 過去データ取得（2022-2026、13,299レース）
- [x] 特徴量設計（馬・騎手・レース条件、38特徴量）
- [x] LightGBM LambdaRankモデル学習
- [x] 週末予測スクリプト
- [x] 記事生成テンプレート
- [x] X投稿機能
- [x] Git初期化・GitHub移行

## メモ
- 学習済みモデル: results/model_lgbm.pkl
- DB: db/keiba.db (約50MB、gitignore済み)
- 予測実績: 189件（entries テーブル）
