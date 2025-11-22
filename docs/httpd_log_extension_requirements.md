# DuckDB httpd_log エクステンション 要件定義書

## 概要

S3互換オブジェクトストレージに蓄積されたApache httpdログをDuckDBから直接クエリ可能にするエクステンション。ログ解析はSQLで自由に行えるため、柔軟な分析が可能。

## 目的

- S3互換ストレージ上のhttpdログファイルをテーブルとして読み込む
- Apache Common Log Formatをパースしてカラム化
- 複数ファイルのglob指定による一括読み込み

## 技術スタック

| 項目 | 選定 |
|------|------|
| 開発言語 | C++ |
| ベース | duckdb/extension-template |
| 対象OS | Linux, macOS |
| DuckDBバージョン | 開発時点の最新安定版（v1.1.x系） |
| 配布方法 | 自前ビルド（将来的にコミュニティ公開を検討） |

## 対象ストレージ

S3互換APIを使用する以下のストレージ：

- Amazon S3
- MinIO
- OpenStack Swift（S3互換API経由）

接続設定はDuckDB既存の`httpfs`エクステンション設定を利用：
- `s3_endpoint`
- `s3_access_key_id`
- `s3_secret_access_key`
- その他httpfs設定

## 対象ログフォーマット

Apache Common Log Format:

```
%h %l %u %t "%r" %>s %b
```

例：
```
192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326
```

## 使用方法

```sql
-- 基本的な使用方法
SELECT * FROM read_httpd_log('s3://bucket/logs/*.log.gz');

-- 日付パスパターン
SELECT * FROM read_httpd_log('s3://bucket/logs/2025/01/*.log.gz');

-- フィルタと組み合わせ
SELECT 
    client_ip, 
    path, 
    status, 
    timestamp
FROM read_httpd_log('s3://bucket/logs/*.log.gz')
WHERE status >= 400
ORDER BY timestamp DESC;
```

## 出力スキーマ

| カラム名 | 型 | 説明 |
|----------|------|------|
| client_ip | VARCHAR | クライアントIPアドレス |
| ident | VARCHAR | identd識別子（通常は"-"） |
| auth_user | VARCHAR | 認証ユーザー名 |
| timestamp | TIMESTAMP | リクエスト日時（UTC正規化） |
| timestamp_raw | VARCHAR | リクエスト日時（元の文字列、オフセット付き） |
| method | VARCHAR | HTTPメソッド（GET, POSTなど） |
| path | VARCHAR | リクエストパス |
| protocol | VARCHAR | プロトコル（HTTP/1.0, HTTP/1.1など） |
| status | INTEGER | HTTPステータスコード |
| bytes | BIGINT | レスポンスサイズ（バイト） |
| filename | VARCHAR | ソースファイル名 |
| parse_error | BOOLEAN | パース失敗フラグ |
| raw_line | VARCHAR | パース失敗時の生データ（成功時はNULL） |

## パース失敗時の挙動

- パース失敗行はスキップせず出力する
- `parse_error = true` を設定
- `raw_line` に元の行データを保持
- 他のカラムはNULL
- 処理は中断せず継続

これにより以下が可能：
- 正常データのみ取得: `WHERE parse_error = false`
- 問題行の確認: `WHERE parse_error = true`
- フォーマット変更の検知

## 想定データ規模

- 1ファイルあたり: 約2GB
- 3サーバーからの冗長ログをマージして解析
- gzip圧縮済み（DuckDB httpfsが透過的に展開）

## DuckDB既存機能の利用

以下はDuckDB/httpfsの既存機能を利用し、本エクステンションでは実装しない：

| 機能 | 担当 |
|------|------|
| S3接続 | httpfsエクステンション |
| gzip展開 | DuckDB本体（透過的） |
| glob展開 | httpfsエクステンション |

## スコープ外（将来対応候補）

以下は初期バージョンでは対応しない：

| 機能 | 理由・備考 |
|------|-----------|
| tar.gz対応 | 現状はgzipのみで運用 |
| Apache Combined Log Format | 初期はCommonのみ |
| SwiftネイティブAPI | S3互換APIで統一 |
| Windows対応 | 需要に応じて後日 |
| server_nameカラム | ファイル命名規則の整理が必要 |

## エクステンション名・関数名

| 項目 | 名前 |
|------|------|
| エクステンション名 | httpd_log |
| テーブル関数名 | read_httpd_log |

## 開発方針

1. duckdb/extension-templateをフォーク
2. Linux/macOS向けCMake設定
3. Apache Common Log Formatパーサー実装
4. httpfs経由でS3ファイル取得
5. テーブル関数としてDuckDBに登録

## 成功基準

- `read_httpd_log('s3://...')` でログがテーブルとして読み込める
- glob指定で複数ファイルを一括処理できる
- パース失敗行が検出・確認できる
- 2GB規模のファイルが処理できる

---

*作成日: 2025-01-22*
*バージョン: 1.0*
