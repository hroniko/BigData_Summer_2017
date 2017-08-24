package com.hroniko

import com.hroniko.processStaging.{StageOne, StageThree, StageTwo}
import org.apache.spark.{SparkConf, SparkContext}
import com.hroniko.settings.properties._
import com.hroniko.tools.{ConverterDateTime, TransactionDayFilter} // Импортируем настройки

/**
  * Created by hroniko on 18.07.17.
  */
object main {

  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем Конфиг и Контекст
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkTaxi") // Локальный
    val sc = new SparkContext(sparkConf)

    // ------------------------------------ Первый стейдж ------------------------------------------

    // 2 Вызываем первый стейдж:
    val stageOneRdd = StageOne.executeIt(sc)
    // 3 Конвертируем время к формату 2017-01-05 00:40:00 и все к строке
    val stageOneStringRdd = stageOneRdd.map(obj => obj.toGoodString())
    // 4 И сохраняем результат в файл
    stageOneStringRdd.saveAsTextFile(pathToOutputFileStageOne)

    // ------------------------------------- Фильтрация --------------------------------------------

    // 5 Отбираем только транзакции за какой-то определенный период, например за  период с 2017-01-05 до 2017-01-06
    val filterRdd = TransactionDayFilter.getForPeriod(stageOneRdd,
      ConverterDateTime.convertToJodaDataTimeYyyyMMdd(startDay),
      ConverterDateTime.convertToJodaDataTimeYyyyMMdd(endDay))

    // ------------------------------------ Второй стейдж ------------------------------------------

    // 6 Вызываем второй стейдж:
    val stageTwoRdd = StageTwo.executeIt(sc, filterRdd)
    // 7 И сохраняем результат в файл
    stageTwoRdd.saveAsTextFile(pathToOutputFileStageTwo)

    // ------------------------------------ Третий стейдж ------------------------------------------

    // 8 Вызываем третий стейдж:
    val finalStageRdd = StageThree.executeIt(sc, stageTwoRdd)
    // 9 И сохраняем результат в файл
    finalStageRdd.saveAsTextFile(pathToOutputFileStageThree)

    // ---------------------------------------------------------------------------------------------


    // 11 Закрываем Спарк-контекст
    sc.stop()
  }





}