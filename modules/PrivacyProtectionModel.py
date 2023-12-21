from pyspark.sql.functions import udf, count, col, lit, countDistinct
from pyspark.sql import Window
from pyspark.ml.feature import Tokenizer, Bucketizer
from pyspark.sql.types import FloatType, StringType, DoubleType
from pyspark.sql.functions import col, count, lit, when
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, when
from dataframe_to_json import df2_to_json


class KAnonymity:
    """
    | K-익명성을 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name_list (list): 컬럼
        k (int) : k 값 (2 이상)
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def check_k_anonymity(self, column_name_list, k):
        output_df = self.input_df
        output_df = output_df.withColumn("k_anonymity", lit(1))
        for column_name in column_name_list:
            # 검증할 컬럼들로 그룹화하여 그룹 내 데이터 포인트 수 계산
            grouped_data = self.input_df.groupby(column_name).agg(count('*').alias('count'))

            # k 이상인 데이터 포인트가 모든 그룹에 대해 존재하면 k 익명성 충족
            # k 익명성 충족 한 경우
            # k_anonymized_enough = grouped_data.filter(grouped_data["count"] >= k)
            # k_anonymized_enough_values = k_anonymized_enough.select(column_name).distinct()

            # k 익명성 충족 안한 경우
            k_anonymized_not_enough = grouped_data.filter(grouped_data["count"] < k)
            k_anonymized_not_enough_values = k_anonymized_not_enough.select(column_name).distinct()
            output_df = output_df.withColumn("k_anonymity", when(col(column_name).isin(k_anonymized_not_enough_values.rdd.flatMap(lambda x: x).collect()), 0).otherwise(output_df["k_anonymity"]))

        k_anonymized_enough_df = output_df.filter(output_df["k_anonymity"] == 1)
        k_anonymized_not_enough_df = output_df.filter(output_df["k_anonymity"] == 0)
        # result = df2_to_json(k_anonymized_enough_df.limit(20), k_anonymized_not_enough_df.limit(20))

        return k_anonymized_enough_df, k_anonymized_not_enough_df


class LDiversity:
    """
    | L-다양성을 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name_list (list): 동질집합 구성 컬럼 리스트
        sensitive_column_list (list): l-다양성을 적용할 민감성 정보 컬럼 리스트
        l-num (int): 2 이상의 정수로 k값보다 작음

        return 조건
        l 다양성을 충족한 df, 미 충족한 df
    """
    def __init__(self, input_df):
        self.input_df = input_df

    def compute(self, column_name_list, sensitive_column_list, l):
        window = Window.orderBy(column_name_list)
        df = self.input_df.withColumn('index', F.row_number().over(window))
        violating_rows_df = None
        for sensitive_column in sensitive_column_list:
            grouped_df = df.groupBy(column_name_list).agg(countDistinct(sensitive_column).alias('l_col'))
            joined_df = df.join(grouped_df, on=column_name_list, how='inner')
            temp_violating_rows_df = joined_df.filter(col('l_col') < l)
            if violating_rows_df is None:
                violating_rows_df = temp_violating_rows_df
            else:
                violating_rows_df = violating_rows_df.union(temp_violating_rows_df)
        violating_rows_df = violating_rows_df.dropDuplicates(subset=['index'])
        merged_df = df.join(violating_rows_df.select('index').withColumnRenamed('index', 'violating_index'),
                            df['index'] == col('violating_index'), how='left_outer')
        non_violating_rows_df = merged_df.filter(col('violating_index').isNull())
        non_violating_rows_df = non_violating_rows_df.drop('violating_index').drop("index")
        violating_rows_df = violating_rows_df.drop("index").drop("l_col")

        # result = df2_to_json(non_violating_rows_df.limit(20), violating_rows_df.limit(20))

        return non_violating_rows_df, violating_rows_df


class TClosenessChecker:
    """
    | T-근접성을 구현한 클래스
    Args:
        T (int) : T 값
        column_name (str): 컬럼
        data :
        bucket_count :

        return 조건
        T 근접성을 충족한 df, 미 충족한 df
    """
    def __init__(self, t, columns, data, bucket_count):
        self.t = t
        self.columns = columns
        self.bucket_count = bucket_count
        self.data = self.cast_to_double(data)
        self.bucketizers = [Bucketizer(splits=[float("-inf")] + self._get_splits(col) + [float("inf")], inputCol=col,
                                       outputCol=col + "_bucket") for col in self.columns]

    def cast_to_double(self, data):
        col_type = data.schema[self.columns].dataType
        if isinstance(col_type, StringType):
            raise TypeError("The type of column must be continuous variable.")
        if not isinstance(col_type, DoubleType):
            return data.withColumn(col, data[self.columns].cast(DoubleType()))
        else:
            return data

    def check_t_closeness(self):
        # 입력 데이터 프레임을 구간으로 분할
        bucketized_df = self.data
        for bucketizer, col in zip(self.bucketizers, self.columns):
            input_col = col
            output_col = col + "_bucket"
            bucketized_df = bucketizer.transform(bucketized_df).withColumn(output_col,
                                                                           bucketizer.transform(bucketized_df)[
                                                                               input_col])

            # Create a Vector column from the bucketized output column
            bucketized_df = bucketized_df.withColumn(output_col + "_vec", bucketized_df[output_col].cast(DoubleType()))

            # 구간별 레이블 확률 계산
        bucket_cols = [f"{col}_bucket" for col in self.columns]
        bucketized_df_agg = bucketized_df.groupBy(*bucket_cols).agg(F.count(F.lit(1)).alias("count"),
                                                                    F.avg(F.col(*bucket_cols)).alias("mean_label"))
        total_rows = bucketized_df_agg.agg(count(lit(1))).collect()[0][0]
        bucketized_df_agg = bucketized_df_agg.withColumn("partition_freq", F.col("count") / total_rows)
        for bucket_col in bucket_cols:
            bucketized_df_agg = bucketized_df_agg.withColumn(output_col + "_hashed", F.sha2(
                F.concat(F.col(bucket_col), F.lit("_"), F.col("partition_freq")), 256))
        total_count = self.data.count()
        bucketized_df_agg = bucketized_df_agg.withColumn("probability", F.col("count") / total_count)

        # 각 구간의 레이블 분포와 전체 레이블 분포의 차이가 t 이하인지 검증
        label_prob = bucketized_df_agg.groupBy(*bucket_cols).agg(count(lit(1)).alias("count"))
        total_label_count = label_prob.select(sum(F.col("count"))).collect()[0][0]
        label_prob = label_prob.withColumn("probability", F.col("count") / total_label_count)
        merged_df = bucketized_df_agg.alias("a").join(
            label_prob.withColumnRenamed("probability", "probability_right").alias("b"),
            on=output_col,
            how="inner").select(output_col,
                                F.col("a.count").alias("count_left"),
                                F.col("b.count").alias("count_right"),
                                "mean_label",
                                "partition_freq", output_col + "_hashed",
                                F.col("probability").alias("probability_left"),
                                F.col("probability_right"))
        merged_df = merged_df.withColumn("prob_diff", F.abs(F.col("probability_right") - F.col("mean_label")))
        t_closeness = merged_df.filter(F.col("prob_diff") > self.t).count() == 0
        result_df = self.data.join(
            merged_df.select(output_col, output_col + "_hashed", "prob_diff"),
            F.col(input_col) == F.col(output_col)
        )

        if t_closeness:
            print("T-closeness is satisfied.")
            return result_df
        else:
            print("T-closeness is not satisfied.")
            non_t_close = merged_df.filter(F.col("prob_diff") > self.t)
            return non_t_close

    def _get_splits(self, col):
        values = sorted(self.data.select(col).distinct().rdd.flatMap(lambda x: x).collect())
        num_buckets = self.bucket_count - 1
        bucket_size = len(values) // num_buckets
        splits = [values[i * bucket_size] for i in range(1, num_buckets)]
        return list(map(float, splits))


class TClosenessChecker2:
    """
        | T-근접성을 구현한 클래스
        Args:
            T (int) : T 값
            column_name_list (list): 동질집합 구성 컬럼 리스트
            sensitive_column_list (list): T-근접성을 적용할 민감성 정보 컬럼 리스트
            return 조건
            T 근접성을 충족한 df, 미 충족한 df
        """
    def __init__(self, input_df):
        self.input_df = input_df

    def t_closeness_module(self, column_name_list, sensitive_column_list, t):
        df = self.input_df
        # 동질집합 구성
        groups = df.groupBy(column_name_list).count()

        # 전체 분포 계산
        total_distributions = {}
        for sensitive_column in sensitive_column_list:
            total_distributions[sensitive_column] = df.groupBy(sensitive_column).count().withColumn('total_freq',
                                                                                                    col('count') / df.count())

        # 동질집합 내 분포 계산 및 t-근접성 검정
        non_violating_rows_df = df.limit(0)
        violating_rows_df = df.limit(0)

        for group in groups.collect():
            group_df = df
            for column_name, value in zip(column_name_list, group):
                group_df = group_df.where(col(column_name) == value)

            is_non_violating_group = True
            for sensitive_column in sensitive_column_list:
                group_distribution = group_df.groupBy(sensitive_column).count().withColumn('group_freq',
                                                                                           col('count') / group_df.count())
                joined_distribution = group_distribution.join(total_distributions[sensitive_column],
                                                              on=sensitive_column)
                joined_distribution = joined_distribution.withColumn('dist_diff',
                                                                     F.abs(col('group_freq') - col('total_freq')))

                if joined_distribution.agg({'dist_diff': 'max'}).collect()[0]['max(dist_diff)'] > t:
                    is_non_violating_group = False
                    break

            if is_non_violating_group:
                if non_violating_rows_df is None:
                    non_violating_rows_df = group_df
                else:
                    non_violating_rows_df = non_violating_rows_df.union(group_df)
            else:
                if violating_rows_df is None:
                    violating_rows_df = group_df
                else:
                    violating_rows_df = violating_rows_df.union(group_df)

        # result = df2_to_json(non_violating_rows_df.limit(20), violating_rows_df.limit(20))

        return non_violating_rows_df, violating_rows_df


