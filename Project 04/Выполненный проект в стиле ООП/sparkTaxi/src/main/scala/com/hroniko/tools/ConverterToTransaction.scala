package com.hroniko.tools

import com.hroniko.dictionary.{DimHostJarus, Ips, Phones, RegionBs}
import com.hroniko.entities.{CdrClass, JarusHttpClass, JarusSslClass, TransactionClass}
import org.apache.spark.rdd.RDD

/**
  * Created by hroniko on 25.07.17.
  */
object ConverterToTransaction {

  // 1 Функция конвертирования JarusSsl to Transaction
  def convertJarusSsl(jarusSslTrueColumnsRdd: RDD[JarusSslClass], ipsDimTrueColumnsRdd: RDD[Ips], regionBsDimTrueColumnsRdd: RDD[RegionBs]): RDD[TransactionClass] = {

    val jarusSslFirstKeyRdd = jarusSslTrueColumnsRdd.map(obj => (obj.getIP(), obj))

    val ipsDimFirstKeyRdd = ipsDimTrueColumnsRdd.map(obj => (obj.getIP(), obj))

    val jarusSslJoinIpsDimRdd = jarusSslFirstKeyRdd.join(ipsDimFirstKeyRdd)

    val jarusSslString = jarusSslJoinIpsDimRdd.map({ case (ip, obj) => {
      obj._1.setName(obj._2.getName()) // Переписываем имя сервиса в объект
      obj._1
    }
    })

    val jarusSslInsertRegion = insertRegionJarusSsl(jarusSslString, regionBsDimTrueColumnsRdd)

    SslToTransactionClass(jarusSslInsertRegion)

  }


  // 2 Функция конвертирования JarusHttp to Transaction
  def convertJarusHttp(jarusHttpTrueColumnsRdd: RDD[JarusHttpClass], hostJarusDimTrueColumnsRdd: RDD[DimHostJarus], regionBsDimTrueColumnsRdd: RDD[RegionBs]): RDD[TransactionClass] = {

    val jarusHttpFirstKeyRdd = jarusHttpTrueColumnsRdd.map(obj => (obj.getHost(), obj))

    val hostJarusDimFirstKeyRdd = hostJarusDimTrueColumnsRdd.map(obj => (obj.getHost(), obj))

    val jarusHttpJoinIpsDimRdd = jarusHttpFirstKeyRdd.join(hostJarusDimFirstKeyRdd)

    val jarusHttpString = jarusHttpJoinIpsDimRdd.map({ case (host, obj) => {
      obj._1.setName(obj._2.getName()) // Переписываем имя сервиса в объект
      obj._1
    }
    })

    val jarusHttpInsertRegion = insertRegionJarusHttp(jarusHttpString, regionBsDimTrueColumnsRdd)

    HttpToTransactionClass(jarusHttpInsertRegion)

  }


  // 3 Функция конвертирования СВR to Transaction
  def convertCdr(cdrTrueColumnsRdd: RDD[CdrClass], phonesDimTrueColumnsRdd: RDD[Phones], regionBsDimTrueColumnsRdd: RDD[RegionBs]): RDD[TransactionClass] = {

    val cdrZipRdd = cdrTrueColumnsRdd.map(obj => (obj.getCallPhone(), obj))

    val phonesDimRdd = phonesDimTrueColumnsRdd.map(obj => (obj.getPhone(), obj))

    val cdrJoinPhonesDimRdd = cdrZipRdd.join(phonesDimRdd)

    val cdrString = cdrJoinPhonesDimRdd.map({ case (callPhone, obj) => {
      obj._1.setName(obj._2.getName()) // Переписываем имя сервиса в объект
      obj._1.setCity(obj._2.getCity()) // Переписываем название города в объект
      obj._1
    }
    })

    val cdrKeyValueRdd = cdrString.map(obj => (obj.getLacCell(), obj))

    val regionBsDimFirstKeyRdd = regionBsDimTrueColumnsRdd.map(obj => (obj.getLacCell(), obj))
    val cdrKeyValueJoinPhonesDimRdd = cdrKeyValueRdd.join(regionBsDimFirstKeyRdd)

    val cdrWithRegionRdd = cdrKeyValueJoinPhonesDimRdd.map({ case (lacCell, obj) => {
      obj._1.setRegion(obj._2.getRegion()) // Переписываем имя региона в объект
      obj._1
    }
    })

    CdrToTransactionClass(cdrWithRegionRdd)

  }


  // ДОПОЛНИТЕЛЬНО:


  // Добавление региона

  def insertRegionJarusSsl(jarusSslString: RDD[JarusSslClass], regionBsDimTrueColumnsRdd: RDD[RegionBs]): RDD[JarusSslClass] = {

    val jarusFullFirstKeyRdd = jarusSslString.map(obj => (obj.getLacCell(), obj)) //

    val regionBsDimFirstKeyRdd = regionBsDimTrueColumnsRdd.map(obj => (obj.getLacCell(), obj)).persist() // И кешируем, так как он нам еще понадобиться

    val jarusFullJoinIpsDimRdd = jarusFullFirstKeyRdd.join(regionBsDimFirstKeyRdd)

    val jarusFullString = jarusFullJoinIpsDimRdd.map({ case (lacCell, obj) => {
      obj._1.setRegion(obj._2.getRegion())
      obj._1
    }
    }).persist()

    jarusFullString
  }

  def insertRegionJarusHttp(jarusHttpString: RDD[JarusHttpClass], regionBsDimTrueColumnsRdd: RDD[RegionBs]): RDD[JarusHttpClass] = {

    val jarusFullFirstKeyRdd = jarusHttpString.map(obj => (obj.getLacCell(), obj)) //

    val regionBsDimFirstKeyRdd = regionBsDimTrueColumnsRdd.map(obj => (obj.getLacCell(), obj)).persist()

    val jarusFullJoinIpsDimRdd = jarusFullFirstKeyRdd.join(regionBsDimFirstKeyRdd)

    val jarusFullString = jarusFullJoinIpsDimRdd.map({ case (lacCell, obj) => {
      obj._1.setRegion(obj._2.getRegion())
      obj._1
    }
    }).persist()

    jarusFullString
  }

  // Окончательное конвертирование к сущности транзакции
  def SslToTransactionClass(jarusSslInsertRegion: RDD[JarusSslClass]): RDD[TransactionClass] = {
    val res = jarusSslInsertRegion.map(obj => new TransactionClass(obj.getDateTime(), obj.getPhone(), obj.getName(), obj.getRegion(), 1))
    res
  }

  // Окончательное конвертирование к сущности транзакции
  def HttpToTransactionClass(jarusHttpInsertRegion: RDD[JarusHttpClass]): RDD[TransactionClass] = {
    val res = jarusHttpInsertRegion.map(obj => new TransactionClass(obj.getDateTime(), obj.getPhone(), obj.getName(), obj.getRegion(), 1))
    res
  }

  // Окончательное конвертирование к сущности транзакции
  def CdrToTransactionClass(cdrInsertRegion: RDD[CdrClass]): RDD[TransactionClass] = {
    val res = cdrInsertRegion.map(obj => {
      val x = new TransactionClass(obj.getDateTime(), obj.getPhone(), obj.getName(), obj.getRegion(), 0)
      x.setCity(obj.getCity())
      x
    })
    res
  }


}
