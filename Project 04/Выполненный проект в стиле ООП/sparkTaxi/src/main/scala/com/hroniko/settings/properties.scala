package com.hroniko.settings

import java.io.File
/**
  * Created by hroniko on 25.07.17.
  */
// Объект настроек приложения
object properties {

  // Пути к папкам и файлам:

  val sep = File.separator // разделитель // java.io.File.separator

  // 1 Общий путь до корневой папки /media/hroniko/DATA/BigData/04_fourth/resources/
  val rootPath = sep + "media" + sep + "hroniko" + sep + "DATA" + sep + "BigData" + sep + "04_fourth" + sep + "resources" + sep



  // 2 Путь к папке с файлами входных данных:     /media/hroniko/DATA/BigData/04_fourth/resources/input
  val pathInput = rootPath + "input" + sep
  // Пути к входным файлам:
  val pathToFileJarusHttp = pathInput + "jarus_http.csv"
  val pathToFileJarusSsl= pathInput + "jarus_ssl.csv"
  val pathToFileCdrRdd = pathInput + "cdr.csv"




  // 3 Путь к папке с файлами словрей:     /media/hroniko/DATA/BigData/04_fourth/resources/dim
  val pathDim = rootPath + "dim" + sep
  // Пути к файлам словарей:
  val pathToFileIpsDim = pathDim + "ips.tsv"
  val pathToFileHostJarusDim = pathDim + "dim_host_jarus.csv"
  val pathToFileRegionBsDim = pathDim + "region_bs.csv"
  val pathToFilePhonesDim = pathDim + "phones.tsv"
  val pathToFileTelCdrDim = pathDim + "dim_tel_cdr.csv"



  // 4 Путь к папке с файлами выходных данных:     /media/hroniko/DATA/BigData/04_fourth/resources/myOutput
  val pathOutput = rootPath + "myOutput5" + sep
  val pathToOutputFileStageOne = pathOutput + "stage1.txt"
  val pathToOutputFileStageTwo = pathOutput + "stage2.txt"
  val pathToOutputFileStageThree = pathOutput + "stage3.txt"


  // Разделители

  val separatorJarusHttp = "\t"
  val separatorJarusSsl = "\t"
  val separatorCdr = "\\|"

  val separatorDimIps = "\t"
  val separatorDimPhones = "\t"
  val separatorDimHostJarus = ";"
  val separatorDimRegionBs = ","


  // Период (дни), для которого считаем статистику:
  val startDay = "20170105"
  val endDay = "20170106"

}
