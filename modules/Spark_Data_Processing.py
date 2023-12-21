from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import to_date, date_format, to_timestamp
from pyspark.sql.functions import col
from pyspark.sql.functions import asc, desc
from pyspark.sql.functions import *

class Spark_Data_Processing :
    """
    |
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str) : 컬럼
        preview (boolean) : 프리뷰 여부
    """

    def __init__(self, input_df):
        self.input_df = input_df

# column_name_list : list / ['name','weight']
    def select_column(self, column_name_list):
        output_df = self.input_df.select(column_name_list)
        return output_df

# column_name_list : list / ['name','weight']
    def deselect_column(self, column_name_list):
        output_df = self.input_df.select("*")
        for column in column_name_list:
            output_df = output_df.drop(column)
        return output_df
    
    
# column_name : str
# new_column_name : str / 같을 시 대체
# cast_type : str // string, int, float
    def type_cast(self, column_name, new_column_name, cast_type):
        output_df = self.input_df.withColumn(new_column_name, col(column_name).cast(cast_type))
        return output_df
    
    
# new_column_name : str / 기존컬럼명 적용시 대체
# group : list / 파라미터 없을 시 전체 인덱스 생성    
    def add_row_number(self, new_column_name, group=None):
        if group is None :
            output_df = self.input_df.withColumn(new_column_name, monotonically_increasing_id())
        else :
            output_df = self.input_df.withColumn(new_column_name, row_number().over(Window.partitionBy(group).orderBy(group)))
        return output_df

    
    
# column_name : str
# new_column_name : str / 같은 이름시 대체
# in_format : str
# out_format : str
# 'yyyy'
# 'yyyyMM'
# 'yyyyMMdd'
# 'yyyyMMddHHmmss'
# 'yyyy-MM-dd'
# 'yyyy-MM-dd HH:mm:ss'
    def datatime_formatter(self, column_name, new_column_name, in_format, out_format):
        # output_df = None
        if in_format == 'yyyy':
            if out_format == 'yyyyMM':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMM"))
            elif out_format == 'yyyyMMdd':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMdd"))
            elif out_format == 'yyyyMMddHHmmss':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMddHHmmss"))
            elif out_format == 'yyyy-MM-dd':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyy-MM-dd"))
            elif out_format == 'yyyy-MM-dd HH:mm:ss':
                output_df = self.input_df.withColumn(new_column_name, to_timestamp(self.input_df[column_name]))

        elif in_format == 'yyyyMM':
            if out_format == 'yyyy':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 4))
            elif out_format == 'yyyyMMdd':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMM"))
                output_df = output_df.withColumn(new_column_name, date_format(output_df[new_column_name], "yyyyMMdd"))
            elif out_format == 'yyyyMMddHHmmss':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMM"))
                output_df = output_df.withColumn(new_column_name, date_format(output_df[new_column_name], "yyyyMMddHHmmss"))
            elif out_format == 'yyyy-MM-dd':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMM"))
            elif out_format == 'yyyy-MM-dd HH:mm:ss':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMM"))
                output_df = output_df.withColumn(new_column_name, to_timestamp(output_df[new_column_name]))

        elif in_format == 'yyyyMMdd':
            if out_format == 'yyyy':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 4))
            elif out_format == 'yyyyMM':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 6))
            elif out_format == 'yyyyMMddHHmmss':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMMdd"))
                output_df = output_df.withColumn(new_column_name, date_format(output_df[new_column_name], "yyyyMMddHHmmss"))
            elif out_format == 'yyyy-MM-dd':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMMdd"))
            elif out_format == 'yyyy-MM-dd HH:mm:ss':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMMdd"))
                output_df = output_df.withColumn(new_column_name, to_timestamp(output_df[new_column_name]))

        elif in_format == 'yyyyMMddHHmmss':
            if out_format == 'yyyy':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 4))
            elif out_format == 'yyyyMM':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 6))
            elif out_format == 'yyyyMMdd':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 8))
            elif out_format == 'yyyy-MM-dd':
                output_df = self.input_df.withColumn(new_column_name, substring(self.input_df[column_name], 1, 8))
                output_df = output_df.withColumn(new_column_name, to_date(output_df[new_column_name], "yyyyMMdd"))
            elif out_format == 'yyyy-MM-dd HH:mm:ss':
                output_df = self.input_df.withColumn(new_column_name, to_timestamp(self.input_df[column_name], "yyyyMMddHHmmss"))

        elif in_format == 'yyyy-MM-dd':
            if out_format == 'yyyy':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyy"))
            elif out_format == 'yyyyMM':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMM"))
            elif out_format == 'yyyyMMdd':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMdd"))
            elif out_format == 'yyyyMMddHHmmss':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMddHHmmss"))
            elif out_format == 'yyyy-MM-dd HH:mm:ss':
                output_df = self.input_df.withColumn(new_column_name, to_date(self.input_df[column_name], "yyyyMMdd"))
                output_df = output_df.withColumn(new_column_name, to_timestamp(output_df[new_column_name]))

        elif in_format == 'yyyy-MM-dd HH:mm:ss':
            if out_format == 'yyyy':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyy"))
            elif out_format == 'yyyyMM':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMM"))
            elif out_format == 'yyyyMMdd':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMdd"))
            elif out_format == 'yyyyMMddHHmmss':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyyMMddHHmmss"))
            elif out_format == 'yyyy-MM-dd':
                output_df = self.input_df.withColumn(new_column_name, date_format(self.input_df[column_name], "yyyy-MM-dd"))

        return output_df

    

# capitalize_mode : str / upper or lower# in_format : str
    def capitalize_column_name(self, capitalize_mode):
        if capitalize_mode == 'upper' :
            output_df = self.input_df.select([col(c).alias(c.upper()) for c in self.input_df.columns])
        elif capitalize_mode == 'lower' :
            output_df = self.input_df.select([col(c).alias(c.lower()) for c in self.input_df.columns])
        return output_df


# rename_dic : dictionary

    def change_column_name(self, rename_dic):
        output_df = self.input_df
        for old_name, new_name in rename_dic.items():
            output_df = output_df.withColumnRenamed(old_name, new_name)
        return output_df

# filtering : str
    def filtering(self, filtering):
        output_df = self.input_df.where(filtering)
        return output_df

# column_name : str
# string: str
    def string_filter(self, column_name, string):
        output_df = self.input_df.filter(col(column_name).like(f"%{string}%"))
        return output_df
    
# column_name : str
# new_column_name : str
# target_string : str or int / 넓값 변환 시('Null', 'NULL', 'null', 'None', 'NONE', 'none')
# replace_string : str or int / 널값으로 변환 시('Null', 'NULL', 'null', 'None', 'NONE', 'none')
    def replace_variable(self, column_name, new_column_name, target_string, replace_string)  :
        null_list = ['Null', 'NULL', 'null', 'None', 'NONE', 'none']
        if target_string in null_list :
            if replace_string in null_list:
                output_df = self.input_df.withColumn(new_column_name, when(col(column_name).isNull(), lit(None)).otherwise(col(column_name)))  
            else :
                output_df = self.input_df.withColumn(new_column_name, when(col(column_name).isNull(), replace_string).otherwise(col(column_name)))   
        else :
            if replace_string in null_list:
                output_df = self.input_df.withColumn(new_column_name, when(col(column_name) == target_string, lit(None)).otherwise(col(column_name)))  
            else :
                output_df = self.input_df.withColumn(new_column_name, when(col(column_name) == target_string, replace_string).otherwise(col(column_name)))
        return output_df

    
# Sort_Rule : dictionary
    def sorting(self, sort_rule):
        sort_columns = [col(column).asc() if sort_rule[column] == "asc" else col(column).desc() for column in sort_rule]
        output_df = self.input_df.orderBy(*sort_columns)
        return output_df
    
    
# column_name_list : list / 빈값일 시 전체 컬럼 distinct
    def distincting(self, column_name_list=None) :
        if column_name_list is None :
            output_df = self.input_df.dropDuplicates()
        else :
            output_df = self.input_df.dropDuplicates(column_name_list)
        return output_df
    
# column_name_list : list
    def delete_missing_data(self, column_name_list) :
        output_df = self.input_df
        for column_name in column_name_list :
            if output_df.select(column_name).dtypes[0][1] == 'string' :
                output_df = output_df.dropna(subset=column_name)
            elif output_df.select(column_name).dtypes[0][1] == 'int' :
                output_df = output_df.filter(output_df[column_name].isNotNull())
        return output_df
    
    
# new_column_name : str / 기존 컬럼명 적용 시 대체
# conditions : list /
# otherwise : str
    def add_column_with_conditions(self, new_column_name, conditions, otherwise):
        expression = when(conditions[0][0], conditions[0][1])
        for condition, value in conditions[1:]:
            expression = expression.when(condition, value)
        output_df = df.withColumn(new_column_name, expression.otherwise(otherwise))
        return output_df
        
        
# column_name_lst = ['name','weight']
# df3 = Spark_Data_Processing(df).select_column(column_name_lst)
# df3.show()

# column_name = 'weight'
# new_column_name = 'weight2'
# cast_type = 'string'
# df3 = Spark_Data_Processing(df).type_cast(column_name, new_column_name, cast_type)
# df3.show()

# new_column_name = 'key'
# group = ['sex', 'height']
# df3 = Spark_Data_Processing(df).add_row_number(new_column_name, group)
# df3.show()

# column_name = 'birth'
# new_column_name = 'key'
# in_format = 'yyyyMMdd'
# out_format = 'yyyy-MM-dd'
# df3 = Spark_Data_Processing(df).datatime_formatter(column_name, new_column_name, in_format, out_format)
# df3.show()

# capitalize_mode = 'upper'
# df3 = Spark_Data_Processing(df).capitalize_column_name(capitalize_mode)
# df3.show()

# rename_dic = {'juso': 'xxx', 'weight': 'aaa'}
# df3 = Spark_Data_Processing(df).change_column_name(rename_dic)
# df3.show()

# filtering = "height > 175 or name in ('kim','anh')"
# df3 = Spark_Data_Processing(df).filtering(filtering)
# df3.show()

# column_name = 'name'
# string = 'k'
# df3 = Spark_Data_Processing(df).string_filter(column_name, string)
# df3.show()

# column_name = 'juso'
# new_column_name = 'new'
# target_string = 'none'
# replace_string = 'xxxxx'
# df3 = Spark_Data_Processing(df).replace_variable(column_name, new_column_name, target_string, replace_string)
# df3.show()

# sort_rule = {"name" : "desc", "height" : "asc"}
# df3 = Spark_Data_Processing(df).sorting(sort_rule)
# df3.show()

# column_list = ['name']
# df3 = Spark_Data_Processing(df).distincting(column_list)
# df3.show()

# column_name_list = ['birth', 'sex']
# df3 = Spark_Data_Processing(df).delete_missing_data(column_name_list)
# df3.show()

