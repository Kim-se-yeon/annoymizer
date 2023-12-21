import pyspark.sql.functions as F
import pyspark.sql.types as T
from dataframe_to_json import df_to_json

limit_value = 20

class Suppression:
    """
    | 삭제 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        index_list : 부분 삭제할 문자열 인덱스
        separator : 구분자 값 (Optional)
        index_reverse : 인덱스 반전 여부
        std_dev_multiplier : Z-score (2 또는 3이 일반적)
        preview (boolean) : 프리뷰 여부
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def general(self, column_name, preview='T'):
        if preview == 'T':
            output_df = self.input_df.withColumn(column_name+'_new', F.lit("*"))
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = self.input_df.drop(column_name)
        return output_df

    def partial(self, column_name, index_list, separator=None, index_reverse=False, preview='T'):
        def remove_separator_string(string, index_array, index_reverse, separator):
            if string is None:
                return None
            split_separator = string.split(separator)
            if index_reverse:
                return separator.join([char for idx, char in enumerate(split_separator) if idx in index_array])
            else:
                return separator.join([char for idx, char in enumerate(split_separator) if idx not in index_array])

        def remove_string(string, index_array, index_reverse):
            if string is None:
                result = None
            if index_reverse:
                result = "".join([char for idx, char in enumerate(string) if idx in index_array])
            elif not index_reverse:
                result = "".join([char for idx, char in enumerate(string) if idx not in index_array])
            return result

        input_df = self.input_df.withColumn(column_name, F.col(column_name).cast(T.StringType()))
        index_array = F.array([F.lit(i) for i in index_list])

        if separator is not None:
            remove_separator_udf = F.udf(remove_separator_string, T.StringType())
            output_df = input_df.withColumn(column_name + '_new', remove_separator_udf(F.col(column_name),
                                                                                       F.lit(separator),
                                                                                       index_array,
                                                                                       F.lit(index_reverse)))
        else:
            remove_index_udf = F.udf(remove_string, T.StringType())
            output_df = input_df.withColumn(column_name+'_new', remove_index_udf(F.col(column_name),
                                                                                 index_array,
                                                                                 F.lit(index_reverse)))

        if preview == 'T':
            output_df = output_df.select(column_name, column_name + '_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name + '_new'))
            output_df = output_df.drop(column_name + '_new')
        return output_df

    def record(self, column_name, std_dev_multiplier, preview='T'):
        input_df = self.input_df.withColumn(column_name, F.regexp_replace(column_name, '[^0-9]', ''))
        input_df = input_df.withColumn(column_name, F.col(column_name).cast(T.LongType()))
        stats = \
        input_df.select(F.mean(F.col(column_name)).alias('mean'), F.stddev_samp(F.col(column_name)).alias('stddev')).collect()[0]
        avg = stats['mean']
        stddev = stats['stddev']
        if preview == 'T':
            output_df = input_df.withColumn(column_name+'_new', F.when(F.abs(F.col(column_name) - avg) > (std_dev_multiplier * stddev), '').otherwise(F.col(column_name)))
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = input_df.where(F.abs(F.col(column_name) - avg) <= (std_dev_multiplier * stddev))
        return output_df

    def local(self, column_name, std_dev_multiplier, preview='T'):
        input_df = self.input_df.withColumn(column_name, F.regexp_replace(column_name, '[^0-9]', ''))
        input_df = input_df.withColumn(column_name, F.col(column_name).cast(T.LongType()))
        stats = \
        input_df.select(F.mean(F.col(column_name)).alias('mean'), F.stddev_samp(F.col(column_name)).alias('stddev')).collect()[0]
        avg = stats['mean']
        stddev = stats['stddev']
        output_df = input_df.withColumn(column_name+'_new',
                                       F.when(F.abs(F.col(column_name) - avg) > (std_dev_multiplier * stddev), '')
                                       .otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    def masking(self, column_name, index_list, index_reverse=False, preview='T'):
        def mask_string(string, index_list, index_reverse):
            if string is None:
                result = string
            if index_reverse:
                result = ''.join('*' if idx not in index_list else char for idx, char in enumerate(string))
            elif not index_reverse:
                result = ''.join('*' if idx in index_list else char for idx, char in enumerate(string))
            return result

        output_df = self.input_df.withColumn(column_name, F.col(column_name).cast(T.StringType()))
        index_array = F.array([F.lit(i) for i in index_list])
        mask_index_udf = F.udf(mask_string, F.StringType())
        output_df = output_df.withColumn(column_name+'_new', mask_index_udf(F.col(column_name), index_array, F.lit(index_reverse)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df


