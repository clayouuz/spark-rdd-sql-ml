from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, row_number
from pyspark.sql.window import Window

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("Spark SQL Tasks") \
    .getOrCreate()

# 读取 user_balance_table.csv
user_balance_path = "input/user_balance_table.csv"
user_balance_df = spark.read.option("header", "true").csv(user_balance_path)

# 读取 user_profile_table.csv
user_profile_path = "input/user_profile_table.csv"
user_profile_df = spark.read.option("header", "true").csv(user_profile_path)

# 给user_balance_df注册临时表，表名设为'user_balance_table'
user_balance_df.createOrReplaceTempView("user_balance_table")

# 给user_profile_df注册临时表，表名设为'user_profile_table'
user_profile_df.createOrReplaceTempView("user_profile_table")

# # 将 user_profile 表的信息合并到 user_balance 表中
user_balance_with_city_df = user_balance_df.join(user_profile_df, on="user_id", how="left")

# # 注册表
user_balance_with_city_df.createOrReplaceTempView("user_balance")

# 任务 1: 按城市统计 2014年3月1日的平均余额
query1 = """
SELECT
    city,
    AVG(CAST(tBalance AS DOUBLE)) AS avg_balance
FROM
    user_balance
WHERE
    report_date = '20140301'
GROUP BY
    city
ORDER BY
    avg_balance DESC
"""
result1 = spark.sql(query1)
result1.show()

# 任务 2: 统计每个城市总流量前三高的用户
query2 = """
-- 2. 统计每个城市总流量前3高的用户
SELECT city_id, user_id, total_flow
FROM (
    SELECT 
        up.City AS city_id, 
        ub.user_id, 
        SUM(ub.total_purchase_amt + ub.total_redeem_amt) AS total_flow,
        RANK() OVER (PARTITION BY up.City ORDER BY SUM(ub.total_purchase_amt + ub.total_redeem_amt) DESC) AS ranking
    FROM 
        user_profile_table up
    JOIN 
        user_balance_table ub 
    ON 
        up.user_id = ub.user_id
    WHERE 
        SUBSTR(ub.report_date, 1, 6) = '201408'
    GROUP BY 
        up.City, ub.user_id
) AS ranked_users
WHERE ranking <= 3;
"""
result2 = spark.sql(query2)
result2.show()

# 保存结果
result1.write.csv("output/task2.1_avg_balance.csv", header=True, mode="overwrite")
result2.write.csv("output/task2.2_top3_users.csv", header=True, mode="overwrite")
