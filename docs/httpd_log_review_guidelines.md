# DuckDB httpd_log エクステンション レビューガイドライン

## このドキュメントの目的

**このドキュメントはmainブランチ専用のレビュー指示書です。**

- developブランチで開発されたコードを第三者視点で品質レビューする
- マージ可否を判断する
- リリース品質を満たしているかチェックする

**重要：このドキュメントはdevelopブランチでは使用しません。**

---

## レビュー実施者の役割

あなた（Claude Code）は**第三者レビュアー**として振る舞います：

- 開発者（develop側）とは独立した視点でレビュー
- 要件定義書（`docs/httpd_log_extension_requirements.md`）を基準に判断
- 品質基準を満たさない場合は明確に指摘
- 開発者の意図を推測せず、コードとドキュメントから客観的に判断

---

## レビュー観点

### 1. 要件適合性

#### 必須機能の実装確認

要件定義書に記載された以下の機能が実装されているか：

- [ ] S3互換ストレージからのログ読み込み
- [ ] glob指定による複数ファイル対応
- [ ] Apache Common Log Formatのパース
- [ ] 正しい出力スキーマ（全12カラム）
- [ ] パース失敗時の適切なエラーハンドリング

#### スコープ外機能の混入チェック

以下の機能が**実装されていないこと**を確認：

- [ ] tar.gz対応
- [ ] Apache Combined Log Format対応
- [ ] server_nameカラム

スコープ外機能が実装されている場合は、削除を推奨。

### 2. コード品質

#### 基本品質

- [ ] コンパイルエラーがない
- [ ] ビルド警告が最小限（重大な警告がない）
- [ ] メモリリークの可能性がない（RAII、スマートポインタ使用）
- [ ] 未定義動作の可能性がない

#### 可読性

- [ ] 関数・変数名が意味を表している
- [ ] 複雑なロジックにコメントがある
- [ ] マジックナンバーが定数化されている
- [ ] 適切にモジュール分割されている

#### エラーハンドリング

- [ ] 全てのエラーケースが考慮されている
- [ ] エラーメッセージが分かりやすい
- [ ] リソースリークがない（ファイルハンドル、メモリ等）
- [ ] 例外安全性が担保されている

### 3. 機能テスト

#### パース機能の検証

```bash
# テストログファイルで検証
cd ~/dev/httpd_log/main
duckdb -unsigned
```

```sql
LOAD 'build/release/extension/httpd_log/httpd_log.duckdb_extension';

-- 基本動作確認
SELECT * FROM read_httpd_log('../testdata/access.log') LIMIT 5;

-- 全カラムの存在確認
SELECT
    client_ip,
    ident,
    auth_user,
    timestamp,
    method,
    path,
    protocol,
    status,
    bytes,
    filename,
    parse_error,
    raw_line
FROM read_httpd_log('../testdata/access.log', raw=true) LIMIT 1;

-- パース成功行のみ
SELECT COUNT(*) FROM read_httpd_log('../testdata/access.log', raw=true)
WHERE parse_error = false;

-- パース失敗行の確認
SELECT raw_line FROM read_httpd_log('../testdata/access_with_errors.log', raw=true)
WHERE parse_error = true;

-- gzip対応確認
SELECT COUNT(*) FROM read_httpd_log('../testdata/access.log.gz');

-- glob指定確認（複数ファイル）
SELECT filename, COUNT(*) FROM read_httpd_log('../testdata/*.log') 
GROUP BY filename;
```

#### チェック項目

- [ ] 正常なログが正しくパースされる
- [ ] 全カラムに適切な値が入る
- [ ] タイムスタンプがUTCに変換される
- [ ] パース失敗行が適切に処理される（parse_error=true, raw_line保持）
- [ ] gzipファイルが読める
- [ ] glob指定で複数ファイルが読める
- [ ] filenameカラムに正しいファイル名が入る

### 4. パフォーマンス

#### 大規模データでの確認

```sql
-- 大きなファイル（10万行）で性能確認
SELECT COUNT(*) FROM read_httpd_log('../testdata/access_large.log');

-- メモリ使用量が妥当か確認
-- 2GB級のファイルでメモリが枯渇しないか
```

チェック項目：

- [ ] 大きなファイル（2GB程度）でもメモリが枯渇しない
- [ ] ストリーミング処理が機能している
- [ ] 不要なデータコピーがない
- [ ] 明らかなパフォーマンス問題がない

### 5. ドキュメント

- [ ] READMEに使用方法が記載されている
- [ ] コード内のコメントが適切
- [ ] 公開用ドキュメントが整備されている（将来のコミュニティ公開を想定）

---

## マージ判定基準

### 必須条件（全て満たす必要あり）

1. **要件適合性の全項目が満たされている**
2. **ビルドが成功する**
3. **基本的な機能テストが全て通る**
4. **重大なバグ・セキュリティ問題がない**
5. **メモリリーク・リソースリークがない**

### 推奨条件（可能な限り満たすべき）

1. コード品質が高い（可読性、保守性）
2. パフォーマンスが許容範囲
3. ドキュメントが整備されている
4. テストカバレッジが十分

---

## レビュー手順

### Step 1: developの変更を確認

```bash
cd ~/dev/httpd_log/main

# developとの差分確認
git diff develop
```

### Step 2: developをマージ（テスト用）

```bash
# 一時的にマージしてテスト
git merge develop --no-commit --no-ff
```

### Step 3: ビルド

```bash
make clean
make
```

ビルドエラーがある場合は、その時点でマージ不可。

### Step 4: 機能テスト

上記「機能テスト」セクションの全項目を実施。

### Step 5: コードレビュー

- ソースコードを読み、品質観点をチェック
- 要件定義書と照らし合わせて確認

### Step 6: 判定

**マージ可の場合：**
```bash
git commit -m "Merge develop: [feature description]"
```

**マージ不可の場合：**
```bash
# マージを取り消し
git merge --abort

# 問題点をissueまたはdevelopに戻してフィードバック
```

---

## レビュー報告テンプレート

```markdown
## レビュー結果

**判定:** ✅ マージ可 / ❌ マージ不可 / ⚠️ 条件付きマージ可

### チェック結果

#### 要件適合性
- [x] S3互換ストレージ対応
- [x] glob指定対応
- [x] Apache Common Log Format対応
- [ ] 出力スキーマ（問題: timestampカラムが欠落）
- [x] エラーハンドリング

#### コード品質
- [x] ビルド成功
- ⚠️ 警告3件（重大ではない）
- [x] メモリリーク対策
- [x] 可読性

#### 機能テスト
- [x] 基本パース動作
- [x] タイムスタンプ変換
- [ ] パース失敗処理（問題: raw_lineが常にNULL）
- [x] gzip対応
- [x] glob対応

#### パフォーマンス
- [x] 大規模データ対応
- [x] メモリ使用量

### 検出された問題

1. **重大:** timestampカラムが実装されていない
   - 影響: 要件定義の必須機能が欠落
   - 対応: develop側で実装が必要

2. **中:** パース失敗時にraw_lineがNULLになる
   - 影響: デバッグ困難
   - 対応: develop側で修正が必要

3. **軽微:** ビルド警告3件
   - 影響: 動作に問題なし
   - 対応: 可能であれば修正推奨

### 総合判定

❌ **マージ不可**

理由: 必須機能（timestampカラム）が実装されていないため、要件を満たしていない。develop側で実装後、再レビューが必要。
```

---

## ブランチ間違い防止策

### このドキュメントの配置場所

```
~/dev/httpd_log/main/docs/review/httpd_log_review_guidelines.md
```

**重要:** 
- このファイルはmainワークツリーの`docs/review/`配下にのみ存在
- developワークツリーにはこのファイルを置かない
- developからmainへのマージ時もこのファイルは含めない（.gitignoreまたは手動除外）

### ブランチ固有ファイルの管理

#### .gitignore設定（mainブランチ専用ファイル）

mainブランチで以下を`.gitignore`に追加：

```
# Review guidelines (main branch only, do not merge to develop)
docs/review/
```

ただし、mainブランチでは実際にコミット済みなので、以下の運用：

```bash
# mainブランチでレビューガイドラインをコミット
cd ~/dev/httpd_log/main
git add docs/review/httpd_log_review_guidelines.md
git commit -m "Add review guidelines (main branch only)"

# developにマージする際は除外
# 方法1: merge時に明示的に除外（手動）
# 方法2: .gitattributes で制御
```

#### .gitattributes設定（推奨）

mainブランチで`.gitattributes`を作成：

```
docs/review/* merge=ours
```

これにより、mainからdevelopへマージする際（逆マージ時）にreview関連ファイルはdevelop側の状態を保持（mainの変更を取り込まない）。

### ブランチチェックスクリプト

各ワークツリーで間違ったドキュメントを使わないようチェック：

**mainワークツリー用：`~/dev/httpd_log/main/.validate-branch.sh`**

```bash
#!/bin/bash
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "main" ]; then
    echo "❌ Error: This worktree should be on 'main' branch, but on '$BRANCH'"
    exit 1
fi

if [ ! -f "docs/review/httpd_log_review_guidelines.md" ]; then
    echo "❌ Error: Review guidelines not found. This worktree may not be properly set up."
    exit 1
fi

echo "✅ Branch validated: $BRANCH"
```

**developワークツリー用：`~/dev/httpd_log/develop/.validate-branch.sh`**

```bash
#!/bin/bash
BRANCH=$(git branch --show-current)
if [ "$BRANCH" != "develop" ]; then
    echo "❌ Error: This worktree should be on 'develop' branch, but on '$BRANCH'"
    exit 1
fi

if [ -f "docs/review/httpd_log_review_guidelines.md" ]; then
    echo "❌ Error: Review guidelines should not exist in develop branch"
    exit 1
fi

if [ ! -f "docs/httpd_log_extension_requirements.md" ]; then
    echo "❌ Error: Requirements document not found"
    exit 1
fi

echo "✅ Branch validated: $BRANCH"
```

使用方法：

```bash
# Claude Code起動前に実行
cd ~/dev/httpd_log/main
bash .validate-branch.sh

cd ~/dev/httpd_log/develop
bash .validate-branch.sh
```

### Claude Code起動時の確認

各Claude Codeセッションで最初に実行：

**mainワークツリーで：**
```
現在のブランチを確認してください。mainブランチであることを確認し、docs/review/httpd_log_review_guidelines.mdが存在することを確認してください。このファイルに基づいてレビューを行います。
```

**developワークツリーで：**
```
現在のブランチを確認してください。developブランチであることを確認し、docs/httpd_log_extension_requirements.mdが存在することを確認してください。このファイルに基づいて開発を行います。docs/review/ディレクトリは存在しないはずです。
```

---

*作成日: 2025-01-22*
*対象ブランチ: main専用*
*バージョン: 1.0*
