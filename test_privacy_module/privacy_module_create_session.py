import json, requests, textwrap, time


def livy_session_create():
    host = 'http://10.70.171.107:8998'
    data = {'driverMemory': '1G',
            'driverCores': 1,
            'executorMemory': '1G',
            'executorCores': 1,
            'numExecutors': 1,
            'kind': 'pyspark',
            'conf': {'spark.submit.pyFiles': 'local:/usr/local/spark/anonymizer/modules',
                     'spark.executor.extraJavaOptions': '-Dfile.encoding=UTF-8',
                     'spark.driver.extraJavaOptions': '-Dfile.encoding=UTF-8',
                     "spark.sql.execution.arrow.pyspark.enabled": "true",
                     "spark.yarn.appMasterEnv.LANG": "ko_KR.UTF-8",
                     "spark.yarn.appMasterEnv.PYTHONIOENCODING": "utf8"}}
    headers = {'Content-Type': 'application/json'}

    ## 1. Start Session Phase ##
    r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
    print('r :', r)
    current_session = r.headers['location']
    print('current_session :', current_session)
    session_url = host + r.headers['location']

    session_status_idle = False
    session_count = 0

    while not session_status_idle:
        r = requests.get(session_url, headers=headers)
        if r.json().get('state', 'starting') == 'idle':
            session_status_idle = True
        elif r.json().get('state', 'starting') == 'dead':
            session_url = host + current_session
            requests.delete(session_url, headers=headers)
            raise ValueError('session dead')
        time.sleep(1)
        # session 무한루프에서 나오기 위하여 프로그램 종료
        if session_count >= 60:
            raise ValueError('session create error발생')
        else:
            session_count += 1

    statements_url = session_url + '/statements'
    print('statements_url :', statements_url)

    data = {'code': textwrap.dedent(f'''
    # input_df = spark.read.csv("file:///usr/local/spark/anonymizer/data/people_pain_info.csv", header=True, inferSchema=True)
    # input_df = spark.read.csv("file:///usr/local/spark/anonymizer/data/privacy_sample.csv", header=True, inferSchema=True)
    input_df = spark.read.csv("file:///usr/local/spark/anonymizer/data/npie_test.csv", header=True, inferSchema=True)

    ''')}

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    print(r.json())

    data = {'code': textwrap.dedent(f'''
            from Suppression import Suppression
            from StatisticalTool import Aggregation, MicroAggregation
            from Generalization import Rounding, TopBottomCoding, CharCategorization, RangeMethod
            from Encryption import OneWayEncryption, TwoWayEncryption, FormatPreservingEncryptor
            from Randomization import Randomization
            from PrivacyProtectionModel import KAnonymity, LDiversity, TClosenessChecker, TClosenessChecker2
            from dataframe_to_json import df2_to_json
    ''')}
    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    print(r.json())


livy_session_create()