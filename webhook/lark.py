import requests
import json



def post(token:str, title: str, content):
    url = f"https://open.feishu.cn/open-apis/bot/v2/hook/{token}"
    headers = {"Content-Type": "application/json"}
    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [
                        [{
                            "tag": "text",
                            "text": content
                        }
                        ]
                    ]
                }
            }
        }
    }
    try:
        proxies = {"http": None, "https": None}
        requests.post(url, headers=headers, data=json.dumps(data),proxies=proxies)
    except Exception as e:
        print("lark post error: ", e)
