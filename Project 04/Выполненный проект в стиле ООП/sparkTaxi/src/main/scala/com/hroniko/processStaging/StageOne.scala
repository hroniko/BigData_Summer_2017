package com.hroniko.processStaging


import com.hroniko.entities.TransactionClass
import com.hroniko.settings.properties._
import com.hroniko.tools.{ConverterToTransaction, LoaderFileToRDD}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by hroniko on 25.07.17.
  */
object StageOne {

  def executeIt(sc: SparkContext): RDD[TransactionClass] = {

    // 1 Подгружаем данные из input-папки:
    val jarusHttpTrueColumnsRdd = LoaderFileToRDD.loadJarusHttpRdd(sc, pathToFileJarusHttp, separatorJarusHttp) // Данные яруса http
    val jarusSslTrueColumnsRdd = LoaderFileToRDD.loadJarusSslRdd(sc, pathToFileJarusSsl, separatorJarusSsl) // Данные яруса ssl
    val cdrTrueColumnsRdd = LoaderFileToRDD.loadCdrRdd(sc, pathToFileCdrRdd, separatorCdr) // Данные cdr

    // 2 Подгружаем словари:
    val ipsDimTrueColumnsRdd = LoaderFileToRDD.loadDictionaryIps(sc, pathToFileIpsDim, separatorDimIps) // Словарь ips
    val phonesDimTrueColumnsRdd = LoaderFileToRDD.loadDictionaryPhones(sc, pathToFilePhonesDim, separatorDimPhones) // Словарь phones
    val hostJarusDimTrueColumnsRdd = LoaderFileToRDD.loadDictionaryHostJarus(sc, pathToFileHostJarusDim, separatorDimHostJarus) // Словарь hostJarus
    val regionBsDimTrueColumnsRdd = LoaderFileToRDD.loadDictionaryRegionBs(sc, pathToFileRegionBsDim, separatorDimRegionBs) // Словарь regionBs


    // 3 Используя словари, конвертируем каждый из rdd к сущности transactionClass
    val jarusSslTransaction = ConverterToTransaction.convertJarusSsl(jarusSslTrueColumnsRdd, ipsDimTrueColumnsRdd, regionBsDimTrueColumnsRdd)
    val jarusHttpTransaction = ConverterToTransaction.convertJarusHttp(jarusHttpTrueColumnsRdd, hostJarusDimTrueColumnsRdd, regionBsDimTrueColumnsRdd)
    val cdrTransaction = ConverterToTransaction.convertCdr(cdrTrueColumnsRdd, phonesDimTrueColumnsRdd, regionBsDimTrueColumnsRdd)

    // 4 Соединяем все транзакции в один RDD:
    val fullTransaction = jarusSslTransaction.union(jarusHttpTransaction).union(cdrTransaction)

    fullTransaction


  }

}
