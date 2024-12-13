from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化 Spark 会话
spark = SparkSession.builder.appName("FundsFlowAnalysis").getOrCreate()

# 读取 CSV 文件
data = spark.read.option("header", "true").csv("input/user_balance_table.csv", inferSchema=True).rdd

# 定义必要的列
required_columns = ['report_date', 'user_id', 'total_purchase_amt', 'total_redeem_amt']
header = data.first()

if all(col in header for col in required_columns):
    # 去除头部行
    data = data.filter(lambda row: row != header)

    # 任务1：按日期计算资金流入和流出
    print("按日期汇总资金流入和流出情况：")
    daily_in_out = data.map(lambda row: (row['report_date'], (row['total_purchase_amt'], row['total_redeem_amt']))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # 转换为 DataFrame 并显示
    df_daily = daily_in_out.map(lambda x: (x[0], x[1][0], x[1][1])).toDF(["report_date", "total_purchase_amt", "total_redeem_amt"])
    df_daily.show()
    output_path = "output/task1.in_out_daily"
    df_daily.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    # 任务2：统计2014年8月的活跃用户
    print("统计2014年8月活跃用户：")
    august_data = data.filter(lambda row: str(row['report_date']).startswith('201408'))

    # 计算用户在2014年8月的活跃天数
    user_active_days = august_data.map(lambda row: (row['user_id'], row['report_date'])) \
        .distinct() \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(lambda a, b: a + b)

    # 筛选出活跃天数>=5的用户
    active_users = user_active_days.filter(lambda x: x[1] >= 5).count()
    print(f"2014年8月的活跃用户总数: {active_users}")

else:
    print(f"CSV文件中缺少必要的列: {set(required_columns) - set(header)}")
