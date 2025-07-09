package com.csvw.demo

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object mcpdate {
  def main(args: Array[String]): Unit = {
    val timestamp = System.currentTimeMillis() - 24 * 60 * 60 * 1000
    val ds = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp)

    println(s"当前时间（默认格式）: $ds")

    val t1 = System.currentTimeMillis()
    val ds1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t1)
    Thread.sleep(251)
    val t2 = System.currentTimeMillis()
    val ds2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(t2)
    val dif= (t2 - t1) / 1000

    println(s"ds1时间: $ds1")
    println(s"ds2时间: $ds2")
    println(s"读取时间差: $dif")

    // 获取当月第一天
    val today = LocalDate.now()
    val firstDayOfMonth = today.withDayOfMonth(1)

    // 获取上个月第一天
    val firstDayOfLastMonth = today.minusMonths(1).withDayOfMonth(1)

    // 格式化输出
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    println(s"当前时间（默认格式）: $ds")
    println(s"当月首日: ${firstDayOfMonth.format(formatter)}") // 输出: 2025-04-01
    println(s"上个月首日: ${firstDayOfLastMonth.format(formatter)}") // 输出: 2025-03-01

  }

  def today(): Unit = {
    // 获取当月第一天
    val today = LocalDate.now()
    val firstDayOfMonth = today.withDayOfMonth(1)
  }
}
