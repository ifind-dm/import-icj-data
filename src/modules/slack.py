import requests
import json
from typing import Optional

class Slack:
    def __init__(self, webhook_url: Optional[str] = None, 
                 bot_name: Optional[str] = "通知Bot",
                 icon_emoji: Optional[str] = ":robot_face:"):
        self.web_hook_url = webhook_url or 'https://hooks.slack.com/services/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'
        self.bot_name = bot_name
        self.icon_emoji = icon_emoji

    
    def slack_notify_success(self, text: str, additional_info: Optional[dict] = None, custom_bot_name: Optional[str] = None):
        """
        成功時の通知を送信します。
        
        Args:
            text (str): 送信するメッセージ本文
            additional_info (dict, optional): 追加で送信したい情報
            custom_bot_name (str, optional): この通知でのみ使用する特別なBot名
        """
        payload = {
            "text": f"✅ {text}",
            "username": custom_bot_name or self.bot_name,
            "icon_emoji": self.icon_emoji,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *成功通知*\n{text}"
                    }
                }
            ]
        }
        
        if additional_info:
            info_text = "\n".join([f"• {k}: {v}" for k, v in additional_info.items()])
            payload["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*詳細情報:*\n{info_text}"
                }
            })
        
        self._send_notification(payload)
    
    def slack_notify_failure(self, text: str, error_details: Optional[str] = None, custom_bot_name: Optional[str] = None):
        """
        失敗時の通知を送信します。チャンネル全体へのメンションを含みます。
        
        Args:
            text (str): 送信するメッセージ本文
            error_details (str, optional): エラーの詳細情報
            custom_bot_name (str, optional): この通知でのみ使用する特別なBot名
        """
        payload = {
            "text": f"<!channel> ❌ {text}",  # フォールバック用のテキスト
            "username": custom_bot_name or self.bot_name,
            "icon_emoji": self.icon_emoji,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<!channel> ❌ *エラー通知*\n{text}"
                    }
                }
            ]
        }
        
        if error_details:
            payload["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*エラー詳細:*\n```{error_details}```"
                }
            })
        
        self._send_notification(payload)
    
    def _send_notification(self, payload: dict):
        """
        実際の通知送信を行う内部メソッド
        
        Args:
            payload (dict): 送信するペイロード
        """
        try:
            response = requests.post(
                self.web_hook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Slack通知の送信に失敗しました: {str(e)}")