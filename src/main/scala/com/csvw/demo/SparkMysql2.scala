package com.csvw.demo

import org.apache.spark.sql.SparkSession

object SparkMysql2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Mysql demo").getOrCreate()

    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "mapstguser")
      .option("password", "8EwgBblGlwH9")
      .option("url", "jdbc:mysql://d9cdchn0my02.mysql.database.chinacloudapi.cn:3306/xiaoshu?useUnicode=yes&useSSL=true&rewriteBatchedStatements=true&requireSSL=true&characterEncoding=UTF-8&useLegacyDatetimeCode=no&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&interactiveClient=true&autoReconnect=true&enabledTLSProtocols=TLSv1.2")
      .option("dbtable", "etl_task_run")
      .option("batchsize", 500)
      .load().createTempView("v_etl_task_run")
    spark.sql("select * from v_etl_task_run").show()

  }
}
