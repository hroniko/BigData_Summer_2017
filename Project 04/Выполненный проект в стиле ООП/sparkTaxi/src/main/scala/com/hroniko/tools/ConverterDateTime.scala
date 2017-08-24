package com.hroniko.tools

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by hroniko on 25.07.17.
  */
object ConverterDateTime {

  // Функции работы с датой:

  // 1 Конвертация Jarus UnixDateTime в JodaDateTime (вида 1483304399)
  def convertUnixToJodaDateTime(dateTime: String): DateTime = {
    new DateTime(dateTime.toLong * 1000)
  }

  // 2 Конвертация CDR DateTime в JodaDateTime по шаблону yyyyMMddHHmmss (вида 20170101235959)
  def convertCdrToJodaDataTime(dateTime: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    formatter.parseDateTime(dateTime)
  }

  // 3 Конвертация JodaDateTime в строку по шаблону yyyyMMdd
  def convertJodaDateTimeToString(date: DateTime): String = {
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    date.toString(formatter)
  }

  // 4 Конвертация JodaDateTime в строку по шаблону yyyy-MM-dd HH:mm:ss (для первого стейджа, что-то вида 2017-01-05 00:40:00)
  def convertJodaDateTimeToStringForStageOne(date: DateTime): String = {
    val formatter = DateTimeFormat.forPattern("yyyyMMddHHmmss") // val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss") // В прошлой версии так делал, а в примерах без тире
    date.toString(formatter)
  }

  // 5 Конвертация DateTime в JodaDateTime по шаблону yyyyMMdd (вида 20170101)
  def convertToJodaDataTimeYyyyMMdd(dateTime: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    formatter.parseDateTime(dateTime)
  }

}
