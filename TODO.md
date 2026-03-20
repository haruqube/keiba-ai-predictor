## TODO
- [ ] Supabaseプロジェクト作成・テーブル構築・.envにキー設定
- [ ] GitHub Pagesダッシュボードの動作確認（Supabase接続後）
- [ ] X APIキー設定・投稿テスト
- [ ] 信頼度閾値の最適化（実データでバックテスト → HIGH/MID/LOW 閾値調整）
- [ ] バックテスト結果の確認・精度評価（会場別・グレード別）
- [ ] モデルチューニング（ハイパーパラメータグリッドサーチ）
- [ ] weekly_pipeline.py のCron設定（金曜/月曜自動実行）
- [ ] N+1クエリ最適化（features/builder.py のバッチ化）
- [ ] スクレイパー並列化（ThreadPoolExecutor導入）

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
- [x] models/trainer.py 修正（時系列CV、NDCG@3/MRR、gain+split重要度）
- [x] 信頼度スコア実装（conf_gap13: 1位vs3位スコア差、HIGH/MID/LOW ティア）
- [x] weekly_pipeline.py 大幅アップグレード（ログ出力、信頼度別精度分析、X投稿統合）
- [x] Supabase同期スクリプト作成（sync_to_supabase.py）
- [x] GitHub Pagesダッシュボード作成（4階層SPA: 日付→会場→レース→詳細）
- [x] config.py にSupabase設定追加
- [x] requirements.txt にsupabase追加

## メモ
- 学習済みモデル: results/model_lgbm.pkl
- DB: db/keiba.db (約50MB、gitignore済み)
- 予測実績: 189件（entries テーブル）
- 信頼度指標: conf_gap13（1位vs3位スコア差）、閾値 HIGH>=2.0, MID>=0.8（暫定。バックテスト後に要調整）
- ダッシュボード: docs/index.html（Supabase接続設定が必要）
