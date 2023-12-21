from pyspark.sql.functions import col, rand, udf, collect_list, row_number, monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import Window
import random
import string
from dataframe_to_json import df_to_json, df_to_json_group

limit_value = 20

class Randomization:
    """
    | 무작위화 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        calc (str) : 계산기호
        val (int) : 어떤값을 계산해서 잡음처리 할지
        preview (boolean) : 프리뷰 여부
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def noise_addition(self, column_name, calc, val, preview='T'):
        if calc == '+':
            output_df = self.input_df.withColumn(column_name+'_new', self.input_df[column_name] + val)
        elif calc == '-':
            output_df = self.input_df.withColumn(column_name+'_new', self.input_df[column_name] - val)
        elif calc == '*':
            output_df = self.input_df.withColumn(column_name+'_new', self.input_df[column_name] * val)
        elif calc == '/':
            output_df = self.input_df.withColumn(column_name+'_new', self.input_df[column_name] / val)
        elif calc == '%':
            output_df = self.input_df.withColumn(column_name+'_new', self.input_df[column_name] % val)

        if preview == 'T':
            output_df = output_df.select(column_name, column_name + '_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.drop(column_name).withColumnRenamed(column_name+'_new', column_name)


        return output_df

    def permutation(self, column_name, preview='T'):
        output_df = self.input_df.withColumn("tmp_index", rand()) \
            .sort("tmp_index") \
            .withColumn("row_id", monotonically_increasing_id()) \
            .sort("row_id")

        shuffled_df = self.input_df.withColumn("tmp_index", rand()) \
            .sort("tmp_index") \
            .withColumn("row_id", monotonically_increasing_id()) \
            .sort("row_id")

        output_df = output_df.join(
            shuffled_df.select("row_id", column_name).withColumnRenamed(column_name, column_name + "_new"), on="row_id",how="inner") \
            .drop("tmp_index", "row_id")

        if preview == 'T':
            output_df = output_df.select(column_name, column_name + '_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, col(column_name + '_new'))
            output_df = output_df.drop(column_name + '_new')

        return output_df

    def permutation_group(self, column_name, group_column, preview='T'):
        shuffle_udf = udf(lambda x: random.sample(x, len(x)), ArrayType(StringType()))
        output_df = self.input_df.groupBy(group_column).agg(collect_list(column_name).alias("collected_values"))
        output_df = output_df.select(group_column, shuffle_udf("collected_values").alias("shuffled_values"))

        window = Window.partitionBy(group_column).orderBy('id')
        input_df = self.input_df.withColumn("id", monotonically_increasing_id())
        input_df = input_df.withColumn("row_number", row_number().over(window) - 1)
        output_df = input_df.join(output_df, on=group_column)
        output_df = output_df.withColumn(column_name + "_new", col("shuffled_values")[col("row_number")]).drop(
            "shuffled_values", "row_number")

        output_df = output_df.orderBy("id").drop("id")

        if preview == 'T':
            output_df = output_df.select(group_column, column_name, column_name + '_new')
            output_df = df_to_json_group(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, col(column_name + '_new'))
            output_df = output_df.drop(column_name + '_new')

        return output_df

    def random_number_generator(self, column_name, preview='T'):
        def randomize_value(column_name):
            length = len(str(column_name))
            return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

        randomize_udf = udf(randomize_value, StringType())
        output_df = self.input_df.withColumn(column_name+'_new', randomize_udf(column_name))
        if preview == 'T':
            output_df = output_df.select(column_name, column_name+'_new')
            output_df = df_to_json(output_df.limit(limit_value))
        elif preview == 'F':
            output_df = output_df.withColumn(column_name, col(column_name+'_new'))
            output_df = output_df.drop(column_name+'_new')
        return output_df



