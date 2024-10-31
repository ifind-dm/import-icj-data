import pandas as pd
from datetime import datetime

def parse_and_format_date(date_str, output_format='date'):
    """
    日付文字列を解析し、指定の形式に変換する関数（タイムゾーン変換なし）
    :param date_str: 解析する日付文字列
    :param output_format: 'date' または 'datetime'
    :return: 変換された日付文字列
    """
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",     # マイクロ秒付き
        "%Y-%m-%d %H:%M:%S",        # 基本形式
        "%Y-%m-%d",                 # 日付のみ
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if output_format == 'date':
                return dt.strftime('%Y-%m-%d')
            else:
                return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    
    try:
        # pandas の to_datetime を使用して柔軟に解析
        dt = pd.to_datetime(date_str)
        if output_format == 'date':
            return dt.strftime('%Y-%m-%d')
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        # 全ての解析が失敗した場合
        return None