package com.hroniko.tools

import com.hroniko.entities.TransactionClass
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by hroniko on 26.07.17.
  */

// Класс-фильтр транзаций по принадлежности заданному интервалу дней (от такого-то числа по такое-то число)
object TransactionDayFilter {

  def getForPeriod (fullTransactionRdd : RDD[TransactionClass], dateStart : DateTime, dateEnd : DateTime) : RDD[TransactionClass] = {
    // Отбираем только транзакции за какой-то определенный период, напрмер за  период с 2017-01-05 до 2017-01-06

    val start = ConverterDateTime.convertJodaDateTimeToString (dateStart).toLong
    val end = ConverterDateTime.convertJodaDateTimeToString (dateEnd).toLong

    val filterTransactionRdd = fullTransactionRdd.filter(obj =>
      ((ConverterDateTime.convertJodaDateTimeToString (obj.getDateTime())).toLong >= start)
        & (((ConverterDateTime.convertJodaDateTimeToString (obj.getDateTime())).toLong) < end) )
    filterTransactionRdd
  }




}
