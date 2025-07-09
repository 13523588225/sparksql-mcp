package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkKuduApp2 {

  def main(args: Array[String]): Unit = {

    val sparkKudu: SparkSession = SparkSession.builder()
      .getOrCreate()
    val kuduMaster = "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051" // 替换为实际 Kudu Master 地址
    val meb_fh01t04 = "ods.fab_fis_90103_rpt_meb_fh01t04_nt_streaming" // 替换为实际表名
    val meb_fh01t01 = "ods.fab_fis_90102_rpt_meb_fh01t01_nt_streaming" // 替换为实际表名

    // 读取 Kudu 表数据
    //    val kuduDF = UserGroupInformation.getLoginUser.doAs(new PrivilegedExceptionAction[DataFrame]() {
    //      override def run: DataFrame = {
    //        val kuduDF = sparkKudu.read
    //          .format("kudu")
    //          .option("kudu.master", kuduMaster)
    //          .option("kudu.table", tableName)
    //          .load()
    //        kuduDF
    //      }
    //    })
    val kuduDF: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t04)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF.createOrReplaceTempView("meb_fh01t04")
    val kuduDF2: DataFrame = sparkKudu.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", meb_fh01t01)
      .option("kudu.master", kuduMaster)
      .load()
    kuduDF2.createOrReplaceTempView("meb_fh01t01")

    // 打印 Schema
    println("Table Schema:")
    kuduDF.printSchema()

    val sparkHive = SparkSession.builder().appName("example").config("spark.sql.warehouse.dir", "/user/hive/warehouse").enableHiveSupport().getOrCreate()
    sparkHive.sql(
      """
        |insert overwrite table default.meb_test
        |select t1.* from meb_fh01t04 t1
        |join meb_fh01t01 t2
        |on (t1.werk=t2.werk and t1.spj=t2.spj and t1.kanr=t2.knr)
        |"""
        .stripMargin)


    sparkKudu.stop()
    sparkHive.stop()
  }
}
