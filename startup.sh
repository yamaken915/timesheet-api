#!/bin/bash
# Azure App Service用の起動スクリプト

# 依存関係のインストール
pip install -r requirements.txt

# アプリケーションの起動（Azure App ServiceはPORT環境変数を使用）
gunicorn --bind 0.0.0.0:${PORT:-8000} --workers 1 app:app
