from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F
from functools import reduce
import random
import re
from dataframe_to_json import df_to_json

limit_value = 20

class Rounding:
    """
    | 라운딩 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        round_type (str) : 라운드 설정 [올림, 내림, 반올림]
        digit (int) : 자릿수 (0 = 1의자리까지, -1 = 10의자리까지, -2 = 100의자리까지, 1 = 소수점1의자리까지
        preview (boolean) : 프리뷰 여부
    Sample:
        Rounding(output_df).round(column_name='mcdp_cd', digit=-2, round_type=nearest, preview='T')
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def round(self, column_name, digit, round_type, preview='T'):
        if round_type == "nearest":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"round(`{column_name}`, {digit})"))
        elif round_type == "up":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"ceil(`{column_name}`, {digit})"))
        elif round_type == "down":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"floor(`{column_name}`, {digit})"))
        else:
            raise ValueError("Invalid rounding direction. Possible values are 'up', 'down', or 'nearest'.")
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    def random_round(self, column_name, digit, preview='T'):
        round_type_list = ["nearest", "up", "down"]
        random_value = random.choice(round_type_list)
        if random_value == "nearest":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"round(`{column_name}`, {digit})"))
        elif random_value == "up":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"ceil(`{column_name}` {digit})"))
        elif random_value == "down":
            output_df = self.input_df.withColumn(column_name+'_new', F.expr(f"floor(`{column_name}`, {digit})"))
        else:
            raise ValueError("Invalid rounding direction. Possible values are 'up', 'down', or 'nearest'.")
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df


class TopBottomCoding:
    """
    | 상하단 코딩 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        ratio(int) : 비율 (표준편차에서 양끝의 퍼센트)
            ex) 1,2,3 을 보통 넣음
        preview (boolean) : 프리뷰 여부
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def topbottom_coding(self, column_name, ratio, preview='T'):
        input_df = self.input_df.withColumn(column_name, F.col(column_name).cast(IntegerType()))
        quantile_low = input_df.approxQuantile(column_name, [ratio], 0)[0]
        quantile_high = input_df.approxQuantile(column_name, [1 - ratio], 0)[0]

        lower_mean = int(input_df.filter(F.col(column_name) <= quantile_low).agg(F.avg(column_name)).collect()[0][0])
        upper_mean = int(input_df.filter(F.col(column_name) >= quantile_high).agg(F.avg(column_name)).collect()[0][0])

        output_df = input_df.withColumn(
            column_name+'_new',
            F.when(F.col(column_name) <= quantile_low, lower_mean)
            .when(F.col(column_name) >= quantile_high, upper_mean)
            .otherwise(F.col(column_name))
        )
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df


class RangeMethod:
    """
    | 범위방법 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        min_value (int) : 범위 시작값
        max_value (int) : 범위 끝값
        standard_interval (int) : 적용 간격
        setting_interval (list) : 적용 간격 (사용자 정의 리스트)
            ex) [(start1, end1, result1), (start2, end2, result2), ...]
        scope_list (int) : 사용자 정의 리스트
        preview (boolean) : 프리뷰 여부
    Sample:
        RangeMethod(output_df).range_method_standard_interval(column_name='mdrp_no', min_value=100000, max_value=400000, standard_interval=limit_value0000, preview='F')
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def range_method_standard_interval(self, column_name, min_value, max_value, standard_interval, preview='T'):
        def find_label(value, min_value, max_value, standard_interval):
            if value < min_value:
                return "<{}".format(min_value)
            elif value >= max_value:
                return ">={}".format(max_value)
            else:
                bins = list(range(min_value, max_value + standard_interval, standard_interval))
                for i, bin_start in enumerate(bins[:-1]):
                    if bin_start <= value < bin_start + standard_interval:
                        return "[{}~{})".format(bin_start, bin_start + standard_interval)
            return None

        find_label_udf = udf(lambda value: find_label(value, min_value, max_value, standard_interval), StringType())
        output_df = self.input_df.withColumn(column_name+'_new', find_label_udf(self.input_df[column_name]))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    def range_method_setting_interval(self, column_name, setting_interval, except_value=None, preview='T'):
        def value_to_custom_name(value, setting_interval):
            for min_value, max_value, custom_name in setting_interval:
                if min_value <= value < max_value:
                    return custom_name
            return except_value if except_value is not None else str(value)

        value_to_custom_name_udf = udf(lambda value: value_to_custom_name(value, setting_interval), StringType())
        output_df = self.input_df.withColumn(column_name + '_new', value_to_custom_name_udf(self.input_df[column_name]))

        if except_value is not None:
            output_df = output_df.withColumn(column_name,
                                             F.when(output_df[column_name + '_new'].isNull(), except_value)
                                             .otherwise(output_df[column_name]))

        if preview == 'T':
            output_df = output_df.select(column_name, column_name + '_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, col(column_name + '_new')).drop(column_name + '_new')

        return output_df


class CharCategorization:
    """
    | 문자 범주화 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str): 컬럼
        char_tuples (tuple or list) : 변경하고자 하는 내용
            ex) [(after, before)]
                [(after, [before_1, before_2, ..])]
                [(after_1, before_1), (after_2, [before_2_1, before_2_2, ...]), ...]
        preview (boolean) : 프리뷰 여부
    Sample:
        CharCategorization(input_df).replace_chars_column(column_name='customer_id',char_tuples=char_tuples,preview='F').show()
    """

    def __init__(self, input_df):
        self.input_df = input_df

    def replace_chars_udf(self, char_tuples):
        char_dict = {}
        for new_char, old_chars in char_tuples:
            if isinstance(old_chars, str):
                old_chars = [old_chars]
            for old_char in old_chars:
                char_dict[old_char] = new_char

        def replace_chars(s):
            return reduce(lambda a, kv: re.sub(r'\b{}\b'.format(re.escape(kv[0])), kv[1], a), char_dict.items(), s)

        return udf(replace_chars, StringType())

    def replace_chars_column(self, column_name, char_tuples, preview='T'):
        replace_chars_udf = self.replace_chars_udf(char_tuples)
        df = self.input_df.withColumn(column_name+'_categorical', replace_chars_udf(column_name))
        if preview == 'T':
            output_df = df.select(column_name, column_name+'_categorical')
            output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            output_df = df.drop(column_name).withColumnRenamed(column_name+"_categorical", column_name)
            output_df = output_df.select(*self.input_df.columns)
            return output_df
