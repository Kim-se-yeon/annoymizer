import json, requests, textwrap, time


def preview_processing():
    host = 'http://10.70.171.107:8998'
    headers = {'Content-Type': 'application/json'}

    session_num = 723
    current_session = f'/sessions/{session_num}'
    statements_url = f'http://10.70.171.107:8998/sessions/{session_num}/statements'

    # data = {'code': textwrap.dedent(f'''
    # from StatisticalTool import Aggregation, MicroAggregation
    # from Generalization import Rounding, TopBottomCoding, CharCategorization, RangeMethod
    # from Suppression import Suppression
    # from Randomization import Randomization
    #
    # output_df = Suppression(input_df).partial(column_name='ptno', index_list=[0,2], index_reverse='T', preview='F')
    # output_df = Aggregation(output_df).avg(column_name='ptnt_clnc_dx_sno', preview='F')
    # # output_df = MicroAggregation(output_df).avg(column_name='ptnt_scin_sno', start_value=0, end_value=60, preview='F')
    # output_df = TopBottomCoding(output_df).topbottom_coding(column_name='ptnt_scin_sno', ratio=0.1, preview='F')
    # output_df = RangeMethod(output_df).range_method_standard_interval(column_name='mcdp_cd', min_value=100000, max_value=400000, standard_interval=200000, preview='F')
    # output_df = Rounding(output_df).round(column_name='chdr_id', digit=-2, round_type='nearest', preview='F')
    # output_df = CharCategorization(output_df).replace_chars_column(column_name='scin_korn_nm2',char_tuples=[('피부병',['여드름','화상','습진'])],preview='F')
    # output_df = Randomization(output_df).random_number_generator(column_name='scin_cd2', preview='F')
    # output_df = Randomization(output_df).noise_addition(column_name='adms_mcdp_cd', calc='*',val=2, preview='F')
    # output_df = Randomization(output_df).permutation(column_name='scin_nm', preview='F')
    #
    # print(' 그냥 / 삭제 / 총계 / 부분총계X / 상하단코딩 / 범위방법 / 라운딩 / 문자 범주화 / 난수화 / 잡음추가 / 순열')
    # input_df.select('mdrp_no', 'ptno', 'ptnt_clnc_dx_sno', 'ptnt_scin_sno', 'codv_cd', 'mcdp_cd', 'chdr_id', 'scin_korn_nm2', 'scin_cd2', 'adms_mcdp_cd', 'scin_nm').show()
    # output_df.select('mdrp_no', 'ptno', 'ptnt_clnc_dx_sno', 'ptnt_scin_sno', 'codv_cd', 'mcdp_cd', 'chdr_id', 'scin_korn_nm2', 'scin_cd2', 'adms_mcdp_cd', 'scin_nm').show()
    # ''')}

    data = {'code': textwrap.dedent(f'''
print(Suppression(input_df).partial(column_name='A_phone', index_list=[0, 1], separator='-', index_reverse=False, preview='T'))
''')}

    r = requests.post(statements_url, data=json.dumps(data), headers=headers)
    print(r.json())

preview_processing()