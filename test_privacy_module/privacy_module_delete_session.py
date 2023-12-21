import requests


def livy_session_delete(session_num):
    host = 'http://10.70.171.107:8998'
    headers = {'Content-Type': 'application/json'}

    current_session = f'/sessions/{session_num}'

    ## Close session Phase ##
    session_url = host + current_session
    requests.delete(session_url, headers=headers)


livy_session_delete()