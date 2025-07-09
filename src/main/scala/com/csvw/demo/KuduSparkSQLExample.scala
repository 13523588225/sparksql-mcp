package com.csvw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object KuduSparkSQLExample {
  def main(args: Array[String]): Unit = {
    // Verify arguments
    if (args.length < 2) {
      println("Usage: KuduSparkSQLExample <sourceTable> <targetTable> [kuduMaster]")
      System.exit(1)
    }

    val sourceTable = args(0)
    val targetTable = args(1)
    // Use provided kuduMaster or default if not specified
    val kuduMaster = if (args.length >= 3) args(2) else "bigdata-09.csvw.com:7051,bigdata-08.csvw.com:7051,bigdata-10.csvw.com:7051"

    // Initialize SparkSession with Kudu extensions
    val spark: SparkSession = SparkSession.builder()
      .appName(s"Kudu Upsert: $sourceTable to $targetTable")
      //      .config("spark.sql.extensions", "org.apache.kudu.spark.kudu.KuduSparkSessionExtension")
      .getOrCreate()

    try {
      // Read source table
      val kuduDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
        .option("kudu.table", sourceTable)
        .option("kudu.master", kuduMaster)
        .load()
      kuduDF.createOrReplaceTempView("source_view")

      // Get source data with optional filter
      val sourceDF = spark.sql("""SELECT * FROM source_view""")

      // Upsert operation
      sourceDF.write
        .format("org.apache.kudu.spark.kudu")
        .option("kudu.master", kuduMaster)
        .option("kudu.table", targetTable)
        .option("operation", "upsert")
        .mode("append")
        .save()

      println(s"Successfully upserted data from $sourceTable to $targetTable")
    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        throw e
    } finally {
      spark.stop()
    }
  }
}
