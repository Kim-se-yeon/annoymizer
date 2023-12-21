import requests
import json

headers = {'Content-Type': 'application/json'}
def send_request(url, method="POST", data=None):
    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers, data=json.dumps(data))
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    response.raise_for_status()
    return response.json()