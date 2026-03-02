# Azure Functions用のメインファイル
import azure.functions as func
from openpyxl import load_workbook
from datetime import datetime, date
import pandas as pd
import tempfile
import os
import io
import logging

# Azure Functions アプリケーションの初期化
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# CORSヘッダーを追加するヘルパー関数
def add_cors_headers(response: func.HttpResponse) -> func.HttpResponse:
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route(route="", methods=["GET"])
def home(req: func.HttpRequest) -> func.HttpResponse:
    """
    ヘルスチェック用のルートエンドポイント
    """
    logging.info("Health check endpoint accessed")
    response = func.HttpResponse(
        body='{"status": "OK", "message": "Timesheet API is running"}',
        mimetype="application/json",
        status_code=200
    )
    return add_cors_headers(response)

@app.route(route="upload", methods=["POST", "OPTIONS"])
def generate_timesheet(req: func.HttpRequest) -> func.HttpResponse:
    """
    メインのタイムシート生成機能
    CSV2個とExcel1個のファイルをアップロードして、勤怠データを処理し
    Excelテンプレートに反映して新しいファイルを生成する
    """
    
    # OPTIONSリクエスト（CORSプリフライト）への対応
    if req.method == "OPTIONS":
        response = func.HttpResponse(status_code=200)
        return add_cors_headers(response)
    
    try:
        # フォームデータの取得
        files = req.files.getlist("files")
        name = req.form.get("name")
        eid = req.form.get("eid")
        organization = req.form.get("organization")
        year = int(req.form.get("year"))
        month = int(req.form.get("month"))
        task = req.form.get("task")
        ratio_mode = req.form.get("ratio_mode") == "on"
        ratio_percent_raw = req.form.get("ratio_percent")
        ratio_day_fraction = None

        if ratio_mode:
            try:
                ratio_percent = float(ratio_percent_raw)
            except (TypeError, ValueError):
                response = func.HttpResponse("就業時間割合は0〜100の数値で指定してください", status_code=400)
                return add_cors_headers(response)

            if ratio_percent < 0 or ratio_percent > 100:
                response = func.HttpResponse("就業時間割合は0〜100の範囲で指定してください", status_code=400)
                return add_cors_headers(response)

            ratio_hours = 8.0 * ratio_percent / 100.0
            ratio_day_fraction = ratio_hours / 24.0

        # アップロードファイルの分類と検証
        csv_files = [f for f in files if f.filename.lower().endswith(".csv")]
        if len(csv_files) != 2:
            response = func.HttpResponse("CSV2個をアップロードしてください", status_code=400)
            return add_cors_headers(response)

        # テンプレートファイルのパスを設定
        template_path = os.path.join("templates", "Excel_templates", "タイムシート(yyyy_mm).xlsx")
        if not os.path.exists(template_path):
            response = func.HttpResponse("テンプレートファイルが見つかりません", status_code=500)
            return add_cors_headers(response)

        # CSV読み込み＆データ結合処理
        df_all = pd.concat([pd.read_csv(f.stream) for f in csv_files], ignore_index=True)
        
        # 日時データの型変換（エラーの場合はNaTに変換）
        df_all["Work start"] = pd.to_datetime(df_all["Work start"], errors="coerce")
        df_all["Work end"] = pd.to_datetime(df_all["Work end"], errors="coerce")
        df_all["Break start"] = pd.to_datetime(df_all["Break start"], errors="coerce")
        df_all["Break end"] = pd.to_datetime(df_all["Break end"], errors="coerce")
        
        # 日付列を作成し、開始時刻順にソート
        df_all["Date"] = df_all["Work start"].dt.date
        df_all.sort_values("Work start", inplace=True)

        # Excelテンプレートファイルの読み込み
        wb = load_workbook(filename=template_path)
        ws = wb.worksheets[0]
        ws.title = f"{month}月"

        # 月の日数を設定
        days_in_month = pd.Timestamp(year=year, month=month, day=1).days_in_month

        # 基本情報の設定
        d9_date = date(year, month, 1)
        g9_date = date(year, month, days_in_month)
        ws["D6"] = organization
        ws["D8"] = name
        ws["D9"] = d9_date
        ws["G9"] = g9_date

        # 祝日データの読み込み
        holiday_dict = {}
        try:
            with open("holidays.csv", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    parts = line.split(",")
                    if len(parts) >= 2:
                        holiday_dict[parts[0]] = parts[1]
        except:
            pass

        # 各日のデータ処理ループ
        for day in range(1, days_in_month + 1):
            current_date = date(year, month, day)
            row = 12 + day
            date_str = current_date.strftime("%Y-%m-%d")
            day_data = df_all[df_all["Date"] == current_date]
            holiday_name = holiday_dict.get(date_str)

            # C列（業務内容・祝日名など）の記入
            if holiday_name:
                ws[f"C{row}"] = holiday_name
            elif current_date.weekday() < 5:
                ws[f"C{row}"] = task
            else:
                ws[f"C{row}"] = ""

            # 勤務時間の処理（勤務データが存在する場合）
            if not day_data.empty:
                start_time = day_data["Work start"].min()
                end_time = day_data["Work end"].max()
                ws[f"H{row}"] = start_time.time()
                ws[f"I{row}"] = end_time.time()

                valid_breaks = day_data.dropna(subset=["Break start", "Break end"])
                total_break_duration = (valid_breaks["Break end"] - valid_breaks["Break start"]).sum()
                total_work_duration = (end_time - start_time) - total_break_duration

                if ratio_mode:
                    ws[f"K{row}"] = ratio_day_fraction
                    actual_work_hours_decimal = total_work_duration.total_seconds() / 86400
                    ws[f"G{row}"] = actual_work_hours_decimal - ratio_day_fraction
                else:
                    ws[f"K{row}"] = total_work_duration.total_seconds() / 86400
                    ws[f"G{row}"] = None

                break_minutes = int(total_break_duration.total_seconds() // 60)
                ws[f"J{row}"] = break_minutes / 1440

                for col in ["H", "I", "J", "K", "G"]:
                    if ws[f"{col}{row}"].value is not None:
                        ws[f"{col}{row}"].number_format = "h:mm"

            elif current_date.weekday() < 5 and not holiday_name:
                ws[f"C{row}"] = "休暇"

        # セル検索用の関数を定義
        def find_cell_by_value(ws, value, column=None):
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value == value and (column is None or cell.column == column):
                        return cell
            return None

        # 月の日数に応じて不要な日付行を削除
        if days_in_month < 31:
            start_delete_row = 12 + days_in_month + 1
            end_delete_row = 12 + 31
            
            for row_to_delete in range(end_delete_row, start_delete_row - 1, -1):
                ws.delete_rows(row_to_delete)
            
            try:
                actual_day_cell = find_cell_by_value(ws, "実働日")
                if actual_day_cell:
                    merge_range = f"A{actual_day_cell.row}:B{actual_day_cell.row}"
                    ws.merge_cells(merge_range)
            except:
                pass

        # サマリ部分の計算式設定
        end_row = 12 + days_in_month

        work_time_cell = find_cell_by_value(ws, "就業時間", column=5)
        if work_time_cell:
            target = ws.cell(row=work_time_cell.row, column=6)
            target.value = f"=SUM(K13:K{end_row})/TIME(1,,)"
            target.number_format = "0.0"
            
            overtime_target = ws.cell(row=work_time_cell.row, column=9)
            overtime_target.value = f"=F{work_time_cell.row}-C{work_time_cell.row}*8"
            overtime_target.number_format = "0.00"

        actual_day_cell = find_cell_by_value(ws, "実働日")
        if actual_day_cell:
            target = ws.cell(row=actual_day_cell.row, column=3)
            target.value = f"=COUNTA(H13:H{end_row})"

        # ファイル出力処理
        safe_eid = eid.replace(" ", "_").replace("　", "_")
        output_filename = f"タイムシート({year:04d}_{month:02d})_{safe_eid}.xlsx"
        output_stream = io.BytesIO()
        wb.save(output_stream)
        output_stream.seek(0)
        
        response = func.HttpResponse(
            body=output_stream.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            status_code=200
        )
        response.headers['Content-Disposition'] = f'attachment; filename="{output_filename}"'
        return add_cors_headers(response)
        
    except Exception as e:
        logging.error(f"Error in generate_timesheet: {str(e)}")
        response = func.HttpResponse(f"エラーが発生しました: {str(e)}", status_code=500)
        return add_cors_headers(response)

@app.route(route="holidays", methods=["GET"])
def get_holidays(req: func.HttpRequest) -> func.HttpResponse:
    """
    祝日データをJSON形式で返すAPI
    """
    year = req.params.get('year')
    logging.info(f"Holidays API accessed with year parameter: {year}")
    
    try:
        holidays_df = pd.read_csv('holidays.csv')
        
        if year:
            year = int(year)
            holidays_df['date'] = pd.to_datetime(holidays_df['date'])
            holidays_df = holidays_df[holidays_df['date'].dt.year == year]
        
        holidays_list = []
        for _, row in holidays_df.iterrows():
            holidays_list.append({
                'date': row['date'],
                'name': row['name']
            })
        
        logging.info(f"Returning {len(holidays_list)} holidays for year {year}")
        response = func.HttpResponse(
            body='{"holidays":' + str(holidays_list).replace("'", '"') + '}',
            mimetype="application/json",
            status_code=200
        )
        return add_cors_headers(response)
        
    except Exception as e:
        logging.error(f"Error in holidays API: {str(e)}")
        response = func.HttpResponse(
            body='{"error":"' + str(e) + '"}',
            mimetype="application/json",
            status_code=500
        )
        return add_cors_headers(response)

@app.route(route="holidays-ui", methods=["GET"])
def holidays_ui(req: func.HttpRequest) -> func.HttpResponse:
    """
    祝日管理画面を表示するエンドポイント
    """
    try:
        with open("templates/holidays.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        response = func.HttpResponse(
            body=html_content,
            mimetype="text/html",
            status_code=200
        )
        return add_cors_headers(response)
    except Exception as e:
        response = func.HttpResponse(f"Error loading holidays UI: {str(e)}", status_code=500)
        return add_cors_headers(response)

@app.route(route="holidays/download", methods=["GET"])
def download_holidays(req: func.HttpRequest) -> func.HttpResponse:
    """
    祝日データ（holidays.csv）をダウンロードするエンドポイント
    """
    try:
        with open("holidays.csv", "rb") as f:
            csv_content = f.read()
        response = func.HttpResponse(
            body=csv_content,
            mimetype="text/csv",
            status_code=200
        )
        response.headers['Content-Disposition'] = 'attachment; filename="holidays.csv"'
        return add_cors_headers(response)
    except:
        response = func.HttpResponse("holidays.csv が見つかりません", status_code=404)
        return add_cors_headers(response)

@app.route(route="holidays/upload", methods=["POST", "OPTIONS"])
def upload_holidays(req: func.HttpRequest) -> func.HttpResponse:
    """
    祝日データ（holidays.csv）をアップロードするエンドポイント
    """
    if req.method == "OPTIONS":
        response = func.HttpResponse(status_code=200)
        return add_cors_headers(response)
    
    try:
        file = req.files.get("file")
        if file and file.filename.endswith(".csv"):
            with open("holidays.csv", "wb") as f:
                f.write(file.stream.read())
            response = func.HttpResponse("アップロード完了", status_code=200)
        else:
            response = func.HttpResponse("CSVファイルのみアップロード可能です", status_code=400)
        return add_cors_headers(response)
    except Exception as e:
        response = func.HttpResponse(f"アップロードエラー: {str(e)}", status_code=500)
        return add_cors_headers(response)
