from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from dataframe_to_json import df_to_json


limit_value = 20

class Aggregation:
    """
    | 총계처리 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        preview (boolean) : 프리뷰 여부
    Sample:
        Aggregation(output_df).avg(column_name='ptnt_clnc_dx_sno', preview='T')
    """
    def __init__(self, input_df):
        self.input_df = input_df

    # 총합값
    def sum(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        total = column_data.agg(F.sum(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(total))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 평균값
    def avg(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        average = column_data.agg(F.avg(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(average))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최대값
    def max(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        maximum = column_data.agg(F.max(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(maximum))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최소값
    def min(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        minimum = column_data.agg(F.min(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(minimum))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최빈값
    def mode(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        try:
            mode = column_data.stat.approxQuantile(column_name, [0.5], 0.01)[0]
        except:
            mode = 0
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(mode))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 중앙값
    def median(self, column_name, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        median = column_data.agg(F.expr(f"percentile(`{column_name}`, 0.5)")).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.lit(median))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df


class MicroAggregation:
    """
    | 부분총계처리 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        start_value (int) : 시작값
        end_value (int) : 종료값
        preview (boolean) : 프리뷰 여부
    Sample:
        MicroAggregation(output_df).avg(column_name='ptnt_scin_sno', start_value=0, end_value=60, preview='T')
    """
    def __init__(self, input_df):
        self.input_df = input_df

    # 총합값
    def sum(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        total = column_data.agg(F.sum(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(total)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 평균값
    def avg(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        average = column_data.agg(F.avg(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(average)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최대값
    def max(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        maximum = column_data.agg(F.max(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(maximum)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최소값
    def min(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        minimum = column_data.agg(F.min(column_name)).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(minimum)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 최빈값
    def mode(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        try:
            mode = column_data.stat.approxQuantile(column_name, [0.5], 0.01)[0]
        except:
            mode = 0
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(mode)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

    # 중앙값
    def median(self, column_name, start_value, end_value, preview='T'):
        column_data = self.input_df.select(column_name)
        column_data = column_data.withColumn(column_name, F.col(column_name).cast(DoubleType()))
        column_data = column_data.filter((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value))
        median = column_data.agg(F.expr(f"percentile(`{column_name}`, 0.5)")).collect()[0][0]
        output_df = self.input_df.withColumn(column_name+'_new', F.when((F.col(column_name) >= start_value) & (F.col(column_name) <= end_value), F.lit(median)).otherwise(F.col(column_name)))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, F.col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df

