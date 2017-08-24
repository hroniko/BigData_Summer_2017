package com.hroniko.tools

import com.hroniko.dictionary.{DimHostJarus, Ips, Phones, RegionBs}
import com.hroniko.entities._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by hroniko on 25.07.17.
  */

object LoaderFileToRDD { //  передаем спаркКонтекст, путь к файлу с данными и разделитель

  // 1 Функция загрузки данных из файла яруса http в RDD объектов JarusHttpClass
  def loadJarusHttpRdd(sc: SparkContext, pathToFile: String, sepType: String): RDD[JarusHttpClass] = {

    val jarusHttpSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val jarusHttpTrueColumnsRdd = jarusHttpSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c2 = arr(2)
      val c6 = arr(6)
      val c7 = arr(7)
      val c8 = arr(9)
      val res = c0.split(" ")
      res.map(c0 => new JarusHttpClass(ConverterDateTime.convertUnixToJodaDateTime(c0), c2, c6 + " " + c7, c8))
    })

    jarusHttpTrueColumnsRdd
  }


  // 2 Функция загрузки данных из файла яруса ssl в RDD объектов JarusSslClass
  def loadJarusSslRdd(sc: SparkContext, pathToFile: String, sepType: String): RDD[JarusSslClass] = {

    val jarusSslSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val jarusSslTrueColumnsRdd = jarusSslSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c2 = arr(2)
      val c6 = arr(6)
      val c7 = arr(7)
      val c8 = arr(9)
      val res = c0.split(" ")
      res.map(c0 => new JarusSslClass(ConverterDateTime.convertUnixToJodaDateTime(c0), c2, c6 + " " + c7, c8))
    })

    jarusSslTrueColumnsRdd
  }


  // 3 Функция загрузки данных из файла cdr
  def loadCdrRdd(sc: SparkContext, pathToFile: String, sepType: String): RDD[CdrClass] = {

    val cdrSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val cdrTrueColumnsRdd = cdrSplitRdd.flatMap(arr => {
      val c2 = arr(2)
      val c6 = arr(6)
      val c8 = arr(8)
      val c9 = arr(9)
      val c10 = arr(10)
      val c32 = arr(32)
      val c26 = arr(26)
      val c23 = arr(23)
      val res = c2.split(" ")
      res.map(c2 => (c2, c6, c8, c9, c10, c32, c26 + " " + c23))
    })

    val cdrFiltredRdd = cdrTrueColumnsRdd.filter(x => x._2 == "2")
      .filter(x => x._3.toInt > 5)
      .filter(x => x._6 == "V")

    val cdrZipRdd = cdrFiltredRdd.map({ case (datetime, call_action_code, call_duration_seconds, call_to_tn_sgsn, calling_no_ggsn, basic_service_type, lacCell) =>
      new CdrClass(ConverterDateTime.convertCdrToJodaDataTime(datetime), call_duration_seconds, call_to_tn_sgsn, calling_no_ggsn, lacCell)
    })

    cdrZipRdd

  }


  // 4 Функция загрузки данных из словаря ips.tsv
  def loadDictionaryIps(sc: SparkContext, pathToFile: String, sepType: String): RDD[Ips] = {

    val ipsDimSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val ipsDimTrueColumnsRdd = ipsDimSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c1 = arr(1)
      val res = c0.split("    ")
      res.map(c0 => new Ips(c0, c1))
    })

    ipsDimTrueColumnsRdd
  }

  // 5 Функция загрузки данных из словаря phones.tsv
  def loadDictionaryPhones(sc: SparkContext, pathToFile: String, sepType: String): RDD[Phones] = {

    val phonesDimSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val phonesDimTrueColumnsRdd = phonesDimSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c1 = arr(1)
      val c2 = arr(2)
      val res = c0.split("    ")
      res.map(c0 => new Phones(c2, c0, c1))
    })

    phonesDimTrueColumnsRdd
  }


  // 6 Функция загрузки данных из словаря dim_host_jarus.csv
  def loadDictionaryHostJarus(sc: SparkContext, pathToFile: String, sepType: String): RDD[DimHostJarus] = {

    val hostJarusDimSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val hostJarusDimTrueColumnsRdd = hostJarusDimSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c1 = arr(1)
      val res = c0.split("    ")
      res.map(c0 => new DimHostJarus(c0, c1))
    })

    hostJarusDimTrueColumnsRdd

  }


  // 7 Функция загрузки данных из словаря dim_host_jarus.csv
  def loadDictionaryRegionBs(sc: SparkContext, pathToFile: String, sepType: String): RDD[RegionBs] = {

    val regionBsDimSplitRdd = loadFileToRDDArray(sc, pathToFile, sepType)

    val regionBsDimTrueColumnsRdd = regionBsDimSplitRdd.flatMap(arr => {
      val c0 = arr(0)
      val c1 = arr(1)
      val c2 = arr(2)
      val res = c0.split("    ")
      res.map(c0 => new RegionBs(c0 + " " + c1, c2))
    })

    regionBsDimTrueColumnsRdd
  }


  // Вспомогательная функция загрузки и сплиттинга
  def loadFileToRDDArray(sc: SparkContext, patchToFile: String, sepType: String): RDD[Array[String]] = {

    val loadFile = sc.textFile(patchToFile)
    val loadFileSplitRdd = loadFile.map(line => line.split(sepType))

    loadFileSplitRdd
  }


}