import json, requests, textwrap, time


def processing():
    host = 'http://10.70.171.107:8998'
    headers = {'Content-Type': 'application/json'}

    session_num = 613
    statements_url = f'http://10.70.171.107:8998/sessions/{session_num}/statements'

    # data = {'code': textwrap.dedent(f'''
    # ### 삭제 ###
    # # 삭제
    # print("result :", Suppression(input_df).general(column_name='나이', preview='T'))
    # # 부분삭제
    # print("result :", Suppression(input_df).partial(column_name='이름', index_list=[1, 2], index_reverse='F', preview='T'))
    # # 행항목 삭제
    # print("result :", Suppression(input_df).record(column_name='나이', std_dev_multiplier=2, preview='T'))
    # # 로컬 삭제
    # print("result :", Suppression(input_df).local(column_name='나이', std_dev_multiplier=2, preview='T'))
    # # 마스킹
    # print("result :", Suppression(input_df).masking(column_name='나이', index_list=[1], index_reverse='F', preview='T'))

    # ### 총계처리 ###
    # print("result :", Aggregation(input_df).sum(column_name='나이', preview='T'))
    # print("result :", Aggregation(input_df).avg(column_name='나이', preview='T'))
    # print("result :", Aggregation(input_df).max(column_name='나이', preview='T'))
    # print("result :", Aggregation(input_df).min(column_name='나이', preview='T'))
    # # 최빈값
    # print("result :", Aggregation(input_df).mode(column_name='나이', preview='T'))
    # # 중앙값
    # print("result :", Aggregation(input_df).median(column_name='나이', preview='T'))
    #
    # print("result :", MicroAggregation(input_df).sum(column_name='나이', start_value=20, end_value=70, preview='T'))
    # print("result :", MicroAggregation(input_df).avg(column_name='나이', start_value=20, end_value=70, preview='T'))
    # print("result :", MicroAggregation(input_df).max(column_name='나이', start_value=20, end_value=70, preview='T'))
    # print("result :", MicroAggregation(input_df).min(column_name='나이', start_value=20, end_value=70, preview='T'))
    # print("result :", MicroAggregation(input_df).mode(column_name='나이', start_value=20, end_value=70, preview='T'))
    # print("result :", MicroAggregation(input_df).median(column_name='나이', start_value=20, end_value=70, preview='T'))

    # ### 일반화 ###
    # # 일반 라운딩
    # print("result :", Rounding(input_df).round(column_name='나이', digit=-1, round_type="nearest", preview='T'))
    # # 랜덤 라운딩
    # print("result :", Rounding(input_df).random_round(column_name='나이', digit=-1, preview='T'))
    # # 상하단 코딩
    # print("result :", TopBottomCoding(input_df).topbottom_coding(column_name='나이', ratio=0.3, preview='T'))
    # # 범위 방법(일정 간격)
    # print("result :", RangeMethod(input_df).range_method_standard_interval(column_name='나이', min_value=20, max_value=70, standard_interval=10, preview='T'))
    # # 범위 방법(임의의 수 기준)
    # setting_interval = [(0, 20, "Young"), (20, 30, "Adult"), (30, 50, "Middle-aged"),(50, float("inf"), "Senior")]
    # print("result :", RangeMethod(input_df).range_method_setting_interval(column_name='A_age', setting_interval=setting_interval, except_value='tessstt', preview='T'))
    # # 문자데이터 범주화
    # print("result :", CharCategorization(input_df).replace_chars_column(column_name='주소', char_tuples=[("경기 ", ["경기도 오산시 누읍동", "경기도 부천시 여월동"])], preview='T'))

    ### 암호화 ###
    # # 일방향 임호화
    # print("result :", OneWayEncryption(input_df).apply_hash(column_name='전화번호', algorithm='SHA256', encoding='base64', noise='1234', prefix=False, suffix=False, preview='T'))
    # # 양방향 암호화
    # print("result :", TwoWayEncryption(input_df).apply_encrypt(column_name='전화번호', key='douzone', encoding='base64', preview='T'))
    # # 형태보존 암호화
    # print("result :", FormatPreservingEncryptor(input_df).apply_encrypt(column_name='나이', data_type='letters', max_value_size=10, key_size=16, preview='T'))

    # ### 무작위화 ###
    # # 잡음 추가
    # print("result :", Randomization(input_df).noise_addition(column_name='나이', calc="+", val=100, preview='T'))
    # # 순열
    # print("result :", Randomization(input_df).permutation(column_name='나이', preview='T'))
    # # 그룹별 순열
    # print("result :", Randomization(input_df).permutation_group(column_name='나이', group_column='생일', preview='T'))
    # # 난수 생성기
    # print("result :", Randomization(input_df).random_number_generator(column_name='나이', preview='T'))

    # ### 프라이버시 보호 모델 ###
    # sensitive_column_list = ["A_adress", "A_phone", "B_height", "B_weight"]
    # klt_select_list = ["K", "L"]
    # k_column_name_list = ["A_age"]
    # k_value = 1
    # l_column_name_list = ["A_adress"]
    # l_value = 1
    # t_column_name_list = ["A_adress"]
    # t_value = 0.2
    #
    # if klt_select_list == ["K"]:
    #     df_list = KAnonymity(input_df).check_k_anonymity(column_name_list=k_column_name_list, k=k_value)
    #     rs_enough_df = df_list[0]
    #     rs_not_enough_df = df_list[1]
    #     result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
    #     print("result :", result)
    # elif klt_select_list == ["K", "L"]:
    #     df_list = KAnonymity(input_df).check_k_anonymity(column_name_list=k_column_name_list, k=k_value)
    #     enough_df = df_list[0]
    #     not_enough_df = df_list[1]
    #     df_list2 = LDiversity(enough_df).compute(column_name_list=l_column_name_list, sensitive_column_list=sensitive_column_list, l=l_value)
    #     rs_enough_df = df_list2[0]
    #     not_enough_df2 = df_list2[1]
    #     rs_not_enough_df = not_enough_df.union(not_enough_df2)
    #     result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
    #     print("result :", result)
    # elif klt_select_list == ["K", "T"]:
    #     df_list = KAnonymity(input_df).check_k_anonymity(column_name_list=k_column_name_list, k=k_value)
    #     enough_df = df_list[0]
    #     not_enough_df = df_list[1]
    #     df_list2 = TClosenessChecker2(enough_df).t_closeness_module(column_name_list=t_column_name_list, sensitive_column_list=sensitive_column_list, t=t_value)
    #     rs_enough_df = df_list2[0]
    #     not_enough_df2 = df_list2[1]
    #     rs_not_enough_df = not_enough_df.union(not_enough_df2)
    #     result = df2_to_json(rs_enough_df.limit(20), rs_not_enough_df.limit(20))
    #     print("result :", result)
    # ''')}
    #
    # r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    # print(r.json())

    # data = {'code': textwrap.dedent(f'''
    # from pyspark.sql.functions import udf, col
    # from pyspark.sql.types import StringType, IntegerType
    # import pyspark.sql.functions as F
    # from functools import reduce
    # import random
    # import re
    # from dataframe_to_json import df_to_json
    #
    # def range_method_setting_interval(input_df, column_name, setting_interval, preview='T', replacement_value=None):
    #     def value_to_custom_name(value, setting_interval):
    #         for min_value, max_value, custom_name in setting_interval:
    #             if min_value <= value < max_value:
    #                 return custom_name
    #         return replacement_value if replacement_value is not None else str(value)
    #
    #     value_to_custom_name_udf = udf(lambda value: value_to_custom_name(value, setting_interval), StringType())
    #     output_df = input_df.withColumn(column_name + '_new', value_to_custom_name_udf(input_df[column_name]))
    #
    #     if replacement_value is not None:
    #         output_df = output_df.withColumn(column_name, F.when(output_df[column_name + '_new'].isNull(), replacement_value)
    #                                         .otherwise(output_df[column_name]))
    #
    #     if preview == 'T':
    #         output_df = output_df.select(column_name, column_name + '_new')
    #         output_df = df_to_json(output_df.limit(20))
    #     elif preview == 'F':
    #         output_df = output_df.withColumn(column_name, col(column_name + '_new')).drop(column_name + '_new')
    #
    #     return output_df
    #     ''')}
    #
    # r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    # print(r.json())

    data = {'code': textwrap.dedent(f'''
    # # setting_interval = [(0, 20, "Young"), (20, 30, "Adult"), (30, 50, "Middle-aged"),(50, float("inf"), "Senior")]
    # setting_interval = [(0, 20, "Young"), (20, 30, "Adult"), (30, 50, "Middle-aged"),(50, 60, "Senior")]
    # print(RangeMethod(input_df).range_method_setting_interval(column_name='A_age', setting_interval=setting_interval, except_value='tessstt', preview='T'))
    print(CharCategorization(input_df).replace_chars_column(column_name='주소', char_tuples=[("경기 ", ["경기도 오산시 누읍동", "경기도 부천시 여월동"])], preview='T'))

    ''')}

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    print(r.json())

processing()