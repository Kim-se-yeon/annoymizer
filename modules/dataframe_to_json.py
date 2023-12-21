import pyspark.sql.functions as F
import json


def df_to_json(df):
    column_list = df.columns
    before_column = column_list[0]
    after_column = column_list[1]

    result = df.select(
        F.collect_list(before_column).alias(before_column),
        F.collect_list(after_column).alias(after_column)
    ).toJSON().collect()

    result_json = {
        "before": {before_column: []},
        "after": {after_column: []}
    }

    for row in result:
        json_row = json.loads(row, encoding='utf-8')
        result_json["before"][before_column].extend(json_row[before_column])
        result_json["after"][after_column].extend(json_row[after_column])
    result_data = json.dumps(result_json, ensure_ascii=False)

    return result_data


# 순열 그룹
def df_to_json_group(df):
    column_list = df.columns
    group_column = column_list[0]
    before_column = column_list[1]
    after_column = column_list[2]

    result = df.select(
        F.collect_list(group_column).alias(group_column),
        F.collect_list(before_column).alias(before_column),
        F.collect_list(after_column).alias(after_column)
    ).toJSON().collect()

    result_json = {
        "group_column": {group_column: []},
        "before": {before_column: []},
        "after": {after_column: []}
    }

    for row in result:
        json_row = json.loads(row, encoding='utf-8')
        result_json["group_column"][group_column].extend(json_row[group_column])
        result_json["before"][before_column].extend(json_row[before_column])
        result_json["after"][after_column].extend(json_row[after_column])
    result_data = json.dumps(result_json, indent=4, ensure_ascii=False)

    return result_data


# 프라이버시 보호 모델
def df2_to_json(enough_df, not_enough_df):
    result_json = {
        "enough_df": {},
        "not_enough_df": {}
    }

    for column in enough_df.columns:
        result_json["enough_df"][column] = enough_df.select(column).rdd.flatMap(lambda x: x).collect()

    for column in not_enough_df.columns:
        result_json["not_enough_df"][column] = not_enough_df.select(column).rdd.flatMap(lambda x: x).collect()

    result_data = json.dumps(result_json, indent=4, ensure_ascii=False)

    return result_data
