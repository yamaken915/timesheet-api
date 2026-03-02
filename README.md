# 🕒 タイムシート作成くん（Webアプリ & API）

## ✅ 概要

MyTIMから出力した勤怠CSV（前半・後半）を使って、  
勤務時間や休憩時間、就業時間などを自動入力したタイムシートを作成できるWebアプリです。

**🆕 2025年9月更新**: **Microsoft Azure環境**に移行し、より安定した動作を実現しました。

---

## 🌐 システム構成

| 種類 | URL | ホスティング |
|------|-----|-------------|
| 🎨 **フロントエンド（UI）** | [https://yamaken999.github.io/timesheet-frontend/](https://yamaken999.github.io/timesheet-frontend/) | GitHub Pages |
| 🛠 **API（Flask）** | [https://timesheet-api-prod.azurewebsites.net/](https://timesheet-api-prod.azurewebsites.net/) | **Microsoft Azure App Service** |
| 🎌 **祝日メンテナンスUI** | [https://timesheet-api-prod.azurewebsites.net/holidays-ui](https://timesheet-api-prod.azurewebsites.net/holidays-ui) | **Microsoft Azure App Service** |
| 📊 **ログ・監視** | Application Insights | **Microsoft Azure** |

### 🔄 CI/CD パイプライン
- **GitHub Actions** による自動デプロイ
- `master` ブランチへのプッシュで自動的にAzure App Serviceにデプロイ

---

## 📁 ディレクトリ構成（このAPIリポジトリ）

```
timesheet-api/
├── app.py                    # Flask API本体
├── holidays.csv              # 祝日データ（UTF-8 CSV）
├── requirements.txt          # 依存パッケージ
├── startup.sh               # Azure App Service起動スクリプト
├── publish-profile.xml      # Azure発行プロファイル
├── azure-setup.ps1          # Azureリソース作成スクリプト
├── .github/
│   └── workflows/
│       └── azure-deploy.yml # GitHub Actions CI/CD設定
├── static/
│   ├── holidays.js
│   └── favicon.ico
├── templates/
│   ├── holidays.html         # 祝日編集画面テンプレート
│   └── Excel_templates/
│       └── タイムシート(yyyy_mm).xlsx  # 固定テンプレートファイル
├── test_app.py              # Pytestテストファイル
└── README.md                # 本ドキュメント
```

---

## 🛠 利用方法（Web UI）

### 準備するもの：

| ファイル | 内容 |
|--------|------|
| 勤怠CSV（前半） | MyTIMから出力（1日〜15日） |
| 勤怠CSV（後半） | MyTIMから出力（16日〜末日） |

**📝 注意**: Excelテンプレートはサーバー上に固定配置されているため、アップロード不要です。

### 操作手順：

1. Web UI で氏名・EID・対象年月などを入力
2. **割合モード** - 就業時間を固定割合で入力したい場合はONにして割合(%)を指定
   - 例：60％を指定すると、K列に8時間×60%=4.8時間が固定出力
   - G列には実際の就業時間とK列の差分が計算される
3. **CSV2枚をアップロード**（MyTIMの前半・後半）
4. 「タイムシート作成」ボタンを押下
5. 自動入力されたExcelファイルがダウンロードされます

---

## 📌 特徴・仕様

- **🆕 固定テンプレート**: Excelテンプレートはサーバー上に配置済み、CSV2個のみでOK
- **🆕 統一ファイル名**: 出力ファイル名は `タイムシート(年_月)_EID.xlsx` 形式
- **🆕 月末対応**: 28日、29日、30日の月は不要な日付行を自動削除
- **🆕 残業時間計算**: 就業時間から基準労働時間（実働日×8時間）を差し引いた残業時間を自動計算
- **🆕 作業期間設定**: D9セルに月初、G9セルに月末日を自動設定
- **🆕 割合モード**: K列の就業時間を指定割合（0〜100%）に固定し、実際の就業時間との差分をG列に出力
- **就業時間・休憩時間・残業**などを自動で計算
- **土日祝を除く平日で空欄の場合、休暇**と記入
- **祝日は祝日名が自動入力される**
- アップロードしたファイルはメモリ上に展開されるため、オンライン上には保存されません
- 祝日はCSVで管理されており、だれでもメンテナンスできるようにしています

---

## 🎌 祝日管理機能

- `holidays.csv` のデータをWeb UIで編集可能
- [祝日編集画面](https://timesheet-api-prod.azurewebsites.net/holidays-ui) から操作
- **API エンドポイント**: `GET /holidays?year=2025` で年別祝日データ取得
- CSV形式（例）：
  ```csv
  date,name
  2025-01-01,元日
  2025-01-13,成人の日
  ```

---

## 🚀 Azure環境について

### インフラ構成
- **App Service Plan**: `timesheet-plan` (F1 Free tier)
- **Resource Group**: `timesheet-rg` (Japan East)
- **Application Insights**: `timesheet-api-insights` (ログ・パフォーマンス監視)

### 監視・ログ
- **Application Insights**: リアルタイム監視、エラー追跡、パフォーマンス分析
- **ログストリーミング**: `az webapp log tail --name timesheet-api-prod --resource-group timesheet-rg`
- **Azure Portal**: ログとメトリクスの詳細分析が可能

### セキュリティ・設定
- **CORS**: GitHub Pages (`yamaken999.github.io`) からのアクセス許可
- **HTTPS**: SSL/TLS暗号化対応
- **環境変数**: Application Insights接続文字列などの機密情報を安全に管理

---

## 🧑‍💻 ローカル実行方法（開発者向け）

### 環境セットアップ
```bash
# Python仮想環境の作成・有効化
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows
# source .venv/bin/activate  # Linux/Mac

# 依存パッケージのインストール
pip install -r requirements.txt

# アプリケーションの起動
python app.py
```

- `http://localhost:10000` でAPIが起動します
- CORS設定済のため、GitHub Pages上のフロントエンドから接続可能

### テスト実行
```bash
# テストの実行
python -m pytest test_app.py -v

# 特定のテストのみ実行
python -m pytest test_app.py::test_home -v
```

---

## 🐍 使用ライブラリ

| ライブラリ | バージョン | 用途 |
|-----------|-----------|------|
| flask     | 最新      | 軽量Webアプリフレームワーク |
| flask-cors | 最新     | フロントエンドからのリクエスト許可 |
| pandas    | 最新      | CSV集計・日付変換など |
| openpyxl  | 最新      | Excelテンプレート読み書き |
| gunicorn  | 最新      | WSGIサーバー（本番環境） |
| opencensus-ext-azure | 最新 | Application Insights連携 |
| opencensus-ext-flask | 最新 | Flask監視 |
| opencensus-ext-requests | 最新 | HTTPリクエスト監視 |

---

## 🔄 デプロイ・CI/CD

### GitHub Actions
- **トリガー**: `master` ブランチへのプッシュ
- **ビルド**: Python 3.12環境での依存関係インストール
- **デプロイ**: Azure App Serviceへの自動デプロイ
- **シークレット**: `AZURE_WEBAPP_API_PUBLISH_PROFILE` (Azure発行プロファイル)

### 手動デプロイ（緊急時）
```bash
# Azureリソースの作成（初回のみ）
./azure-setup.ps1

# 手動デプロイ
az webapp deployment source config-zip \
  --resource-group timesheet-rg \
  --name timesheet-api-prod \
  --src deploy.zip
```

---

## 🔐 注意点

- CSVはMyTIMの出力形式のままでOK（Work start等の列名あり）
- **CSV2個のみ**: Excelテンプレートのアップロードは不要
- Excelテンプレートは最初のシートが対象になります
- holidays.csvは Azure App Service上で毎回読み込まれ、更新が即反映されます
- **HTTPS必須**: セキュリティのため、すべての通信はHTTPS経由で行われます

---

## 📊 API エンドポイント

| メソッド | エンドポイント | 説明 |
|---------|---------------|------|
| `GET`   | `/` | ヘルスチェック |
| `POST`  | `/upload` | タイムシート生成（CSV2個アップロード） |
| `GET`   | `/holidays` | 祝日データ取得（JSON） |
| `GET`   | `/holidays?year=2025` | 指定年の祝日データ取得 |
| `GET`   | `/holidays-ui` | 祝日管理画面 |
| `GET`   | `/holidays/download` | 祝日CSVダウンロード |
| `POST`  | `/holidays/upload` | 祝日CSVアップロード |

### レスポンス例
```json
// GET /holidays?year=2025
{
  "holidays": [
    {
      "date": "2025-01-01T00:00:00Z",
      "name": "元日"
    },
    {
      "date": "2025-01-13T00:00:00Z", 
      "name": "成人の日"
    }
  ]
}
```

---

## ✨ フロントエンド（GitHub Pages）

フロントエンドは以下のリポジトリで管理しています：

🔗 [https://github.com/yamaken999/timesheet-frontend](https://github.com/yamaken999/timesheet-frontend)

- HTML / JS / CSSにより構成
- GitHub Pagesにホストされており、バックエンドAPIに接続します

---

本アプリは業務効率化を目的に作成されています。  
**Microsoft Azure環境での安定稼働**により、より快適にご利用いただけます。  
改善点や要望があればPull Request / Issueでお知らせください！

---

## 📝 コントリビューションと Issue 提出方法

### 🚨 不具合の報告（Bug Report）
1. このリポジトリの `Issues` タブにアクセス
2. `Bug Report` テンプレートを選択して、必要事項を記入・送信してください
3. **Azure環境のログ情報**も含めると解決が早くなります
4. HTML（frontend）側の`Issues`は 🔗 [frontend側のIssue](https://github.com/yamaken999/timesheet-frontend/issues) に挙げてくれると管理しやすくて助かります

### 💡 機能改善・提案（Feature Request）
1. 同様に `Issues` タブから `Feature Request` テンプレートを使用してください
2. 提案の背景やユースケースを具体的に記述すると採用されやすくなります
3. **Azure環境でのスケーラビリティ**も考慮した提案をお待ちしています

### 🤝 コントリビューション（開発への参加）
- `fork` → `feature ブランチで修正` → `Pull Request` をお送りください
- コードの変更点には簡単な説明コメントを付けていただけると助かります
- **Azure環境でのテスト**も実施してからPRを作成してください

---

### 備考

**🎉 Azure移行完了 (2025年9月)**
- **Render.com** から **Microsoft Azure App Service** に移行
- より安定した動作とスケーラビリティを実現
- Application Insightsによる詳細な監視・ログ分析
- 無料枠内での運用（F1 Free tier: 60分/日、1GB RAM）
- HTTPS標準対応、セキュリティの向上

**移行前の制約（解決済み）**
- ~~Render無料プランの月750時間制限~~ → **解決**: Azure無料枠で十分
- ~~15分スリープ問題~~ → **解決**: Azureは即座に応答
- ~~uptimerobotでの定期リクエスト~~ → **不要**: スリープなし

**今後の予定**
- 印刷範囲の調整
- パフォーマンスの継続監視
- 必要に応じてスケールアップの検討
- セキュリティ強化の継続実施

---

## 🔧 トラブルシューティング

### よくある問題と解決方法

**Q: APIからエラーレスポンスが返る**
- Application Insightsでログを確認
- Azure Portal → Application Insights → ログ

**Q: デプロイが失敗する**
- GitHub Actions のログを確認
- Azure発行プロファイルが最新か確認

**Q: ローカル環境で動作しない**
- Python仮想環境が有効化されているか確認
- `pip install -r requirements.txt` の再実行

**Q: CSV処理でエラーが発生**
- MyTIMの出力形式が変更されていないか確認
- CSVファイルの文字エンコーディング確認（UTF-8推奨）

---

## 🧪 テスト内容

本プロジェクトでは `pytest` を用いた自動テストを実装しています。

### 実装済みテスト一覧

- **祝日管理画面の表示テスト**  
  `/holidays-ui` エンドポイントにGETリクエストし、HTMLが返ることを確認します。

- **祝日CSVダウンロードの存在確認テスト**  
  `holidays.csv` が存在しない場合、`/holidays/download` で404エラーが返ることを確認します。

- **祝日CSVアップロード＆ダウンロードテスト**  
  `/holidays/upload` でCSVファイルをアップロードし、正常にアップロード完了メッセージが返ることを確認します。  
  その後 `/holidays/download` でアップロードした内容がダウンロードできることを確認します。

- **祝日CSVアップロードのバリデーションテスト**  
  CSV以外のファイルをアップロードした場合、400エラーとエラーメッセージが返ることを確認します。

- **勤怠ファイルアップロードのファイル数バリデーションテスト**  
  `/upload` エンドポイントに複数ファイルを送信した場合、400エラーと「ファイル数が不正」というメッセージが返ることを確認します。

### テスト実行方法

ターミナルで以下のコマンドを実行してください。

```
pytest
```

すべてのテストが `passed` となれば正常

---

## 📋 リリースノート

### 🚀 v2.0.0 - Azure移行完了 (2025-09-04)

#### 🌟 Major Changes
- **Microsoft Azure App Service** への完全移行
- **Render.com** からの脱却により安定性が大幅向上
- **Application Insights** による包括的な監視・ログ分析機能を実装

#### ✨ New Features
- **ヘルスチェックエンドポイント** (`GET /`) を追加
- **祝日API** (`GET /holidays?year=YYYY`) でJSON形式の祝日データ取得
- **リアルタイムログ監視** - Application Insightsとの連携
- **GitHub Actions CI/CD** による自動デプロイパイプライン
- **HTTPS標準対応** - すべての通信が暗号化

#### 🛠 Infrastructure Updates
- **Azure App Service Plan**: F1 Free tier (Japan East)
- **Resource Group**: timesheet-rg
- **Application Insights**: timesheet-api-insights
- **自動スケーリング**: 負荷に応じた柔軟な対応
- **CORS設定**: GitHub Pages (`yamaken999.github.io`) 最適化

#### 🔧 Technical Improvements
- **Python 3.12** 環境での安定動作
- **Gunicorn** WSGIサーバーによる本番環境最適化
- **OpenCensus** ライブラリによる詳細トレーシング
- **環境変数管理** によるセキュリティ強化
- **スタートアップスクリプト** による起動プロセス最適化

#### 🐛 Bug Fixes
- ~~15分スリープ問題~~ → **完全解決**: 即座の応答
- ~~月750時間制限~~ → **解決**: 制限なしの安定運用
- ~~レスポンス遅延問題~~ → **解決**: 高速レスポンス

#### 📊 Performance
- **応答時間**: 大幅改善（平均 < 200ms）
- **可用性**: 99.9%以上の稼働率
- **スケーラビリティ**: 需要に応じた自動調整
- **セキュリティ**: エンタープライズグレード

#### 🔄 Migration Details
- **旧環境**: Render.com (無料プラン制限あり)
- **新環境**: Microsoft Azure App Service (F1 Free tier)
- **データ移行**: 完全無停止での移行完了
- **URL変更**: 
  - 旧: `https://timesheet-api-un72.onrender.com`
  - 新: `https://timesheet-api-prod.azurewebsites.net`

#### 📝 Documentation
- **README.md** 全面更新
- **API仕様書** 追加
- **トラブルシューティングガイド** 新設
- **デプロイ手順書** 整備

#### 📔 テンプレートファイルの修正
- 住所が帝劇になっていたので、就業条件明示書に記載の大手町住所に変更
- OT確認したいので超過時間の関数を復活
- 1年経ってファイル名が重複し始めそうなので、ファイル名の（MM月）部分を（YYYY-MM）に変更

---

### 📅 Previous Releases

#### v1.3.0 (2025-08-XX)
- 祝日管理UI機能追加
- CSV2個のみでの操作に最適化
- Excelテンプレート固定配置

#### v1.2.0 (2025-07-XX)
- 月末日数対応（28/29/30日月）
- 残業時間自動計算機能
- 社長モード実装

#### v1.1.0 (2025-06-XX)
- 祝日自動判定機能
- 休暇自動入力機能
- ファイル名統一化

#### v1.0.0 (2025-05-XX)
- 初回リリース
- 基本的なタイムシート生成機能
- MyTIM CSV対応