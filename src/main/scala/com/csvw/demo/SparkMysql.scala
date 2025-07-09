package com.csvw.demo

import org.apache.spark.sql.SparkSession

object SparkMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Mysql demo").getOrCreate()

    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "mcp")
      .option("password", "3GlgK1IU6x")
      .option("url", "jdbc:mysql://10.122.48.84:4884/mcp")
      .option("dbtable", "mq_repair_type")
      .load()
      .createOrReplaceTempView("mq_repair_type")

    spark.sql("select *, now() from mq_repair_type limit 1").show()

  }
}
