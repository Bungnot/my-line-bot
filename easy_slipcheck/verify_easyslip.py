# easy_slipcheck/verify_easyslip.py
import requests

EASYSLIP_API_URL = "https://developer.easyslip.com/api/v1/verify"
EASYSLIP_TOKEN = "175fb82a-7a52-42ef-b57c-2bc44aaeced2"  # เช่น Bearer 214a7c6a... (ห้ามลืมใส่จริง)

def verify_slip(file_path: str):
    """ส่งรูปสลิปไปให้ EasySlip ตรวจสอบ"""
    with open(file_path, "rb") as f:
        files = {"file": f}
        headers = {"Authorization": f"Bearer {EASYSLIP_TOKEN}"}
        res = requests.post(EASYSLIP_API_URL, headers=headers, files=files)
        return res.json()
