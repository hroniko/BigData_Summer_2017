package com.hroniko.processStaging

import com.hroniko.entities.TransactionClass
import com.hroniko.tools.ConverterDateTime
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by hroniko on 25.07.17.
  */
object StageTwo {

  def executeIt(sc: SparkContext, filterRdd: RDD[TransactionClass]): RDD[String] = {

    val forOneDaySpecialKeyRdd = filterRdd.map({ case obj =>
      (ConverterDateTime.convertJodaDateTimeToString(obj.getDateTime()) + "\t" + obj.getPhone() + "\t" + obj.getName() + "\t" + obj.getCity() + "\t" + obj.getRegion() + "\t" + obj.getOnOffLine(), obj)
    })

    val groupRdd = forOneDaySpecialKeyRdd.groupByKey()

    val driverRdd = groupRdd.map({ case (key, obj) => {
      if (obj.head.getOnOffLine().equals("1")) { // Если элемент единичка, то перед нами ярус, запускаем для него процедуру определения, водитель это или нет
        (key, isItDriver(obj.toList), obj)
      }
      else {
        (key, 0, obj) // Иначе не водитель
      }
    }
    })

    val tripRdd = driverRdd.map({ case (key, driver, obj) => {
      if (obj.head.getOnOffLine().equals("1") & driver == 0) { // Если последний элемент единичка, то перед нами ярус, и если это не водитель, то ставим ему в качестве количества поездок количество всех транзакций value.size
        (key, driver, obj.size, obj)
      }
      else { // Иначе перед нами CDR или сессия с навигатора водителя (ее можно проверить на длительность), для него запускаем процедуру расчета количества поездок
        (key, driver, tripCount(obj.toList), obj) // и запускаем процедуру расчета количества поездок
      }
    }
    })

    val tripWithoutSessionRdd = tripRdd.map({ case (key, driver, trip, value) => (key + "\t" + driver, trip) })

    val tripAggregateRdd = tripWithoutSessionRdd.reduceByKey((x, y) => x + y)

    val stageTwoRdd = tripAggregateRdd.map({ case (key, value) => (key + "\t" + value) })
    stageTwoRdd

  }


  // Дополнительные вспомогательные функции

  // 5 Получение списка дат транзакций одного пользователя и определение, является ли он водителем (1 или 0)

  def isItDriver(dataList: List[TransactionClass]): Int = {

    if (dataList.size < 2) {
      return 0
    } else {
      var driverFlag = 1
      for (i <- 1 to dataList.size - 1) {
        val dateOne = dataList(i - 1).getDateTime()
        val dateTwo = dataList(i).getDateTime()
        val duration = (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60)
        if (duration >= 60) {
          driverFlag = 0
        }
      }
      val dateOne = dataList(0).getDateTime()
      val dateTwo = dataList(dataList.size - 1).getDateTime()
      val duration = (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60)
      if (duration <= 120) {
        driverFlag = 0
      }
      return driverFlag
    }
  }


  // 6 Метод рассчета количества поездок для CDR сессии транзакций
  def tripCount(dataList: List[TransactionClass]): Int = {
    if (dataList.size < 2) {
      return 1
    } else {
      var count = 1
      var duration = 0L
      for (i <- 1 to dataList.size - 1) {
        val dateOne = dataList(i - 1).getDateTime()
        val dateTwo = dataList(i).getDateTime()
        duration = duration + (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60)
        if (duration >= 60) {
          count = count + 1
          duration = 0L
        }
      }
      return count
    }
  }

}
