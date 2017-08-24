package com.hroniko

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by hroniko on 10.07.17.
  */
object main {

  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем Конфиг и Контекст
    val sparkConf = new SparkConf().setMaster("local").setAppName("myapp") // Локальный
    val sc = new SparkContext(sparkConf)

    // Выполняем распределение коллекции, получаем объект из параллелайза, к нему применяем все действия
    // val psk = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))  -- от старого задания

    // 2 Читаем csv-файла // Получаем RDD[ String ]
    val textFile = sc.textFile("/media/hroniko/DATA/1/1.csv")  // val textFile = sc.textFile("file:///usr/local/spark/README.md")
    textFile.foreach(println) // на всякий случай проверяем, что прочитали

    // 3 превращаем RDD[ String ] сначала в RDD[ Array[ String ]
    val splitRdd = textFile.map( line => line.split(";") )

    // 4 а затем в RDD[ ( String, String ) ], точнее сразу в RDD[ ( String, Double ) ] // Нагуглил на стековерфлоу этот момент и допилил
    val normRdd = splitRdd.flatMap( arr => {
      val stringKey = arr( 0 )
      val stringValue = arr( 1 )
      val stringCounts = stringValue.split( " " )
      // И превращаем первый столбец в правильную дату-строку yyyyMM, а второй столбец в округленный до двух знаков после запятой Double
      stringCounts.map( word => ( convertToDateString(stringKey), roundDouble(word.toDouble, 2) ) )
    } )

    // 5 Проверяем, что в итоге получилось после преобразований столбцов:
    normRdd.foreach( { case ( word, title ) => println( s"{ $word, $title }" ) } )

    // 6 Группируем по ключу:
    val groupRdd = normRdd.groupByKey()
    groupRdd.foreach( { case ( word, title ) => println( s"{ $word, $title }" ) } )

    // 7 Превращаем в RDD[ ( String, List [ Double ] ) ]
    val keyAndListRdd = groupRdd.map({case (x, iter) => (x, iter.toList)})

    // 8 Находим максимум и минимум для листа каждого ключа
    val minMaxRdd = groupRdd.map({case (x, y) => (x, y.min, y.max)})
    minMaxRdd.foreach( { case ( x, y, z ) => println( s"{ $x; $y; $z }" ) } )


    // 9 Преобразуем трехстолбцовый массив к одностолбцовому массиву строк (как еще обозвать его?):
    val resStringRdd = minMaxRdd.map({case (x, y, z) => x.toString + "; " + y.toString + "; " + z.toString})


    // 10 Сохраняем в файл
    resStringRdd.saveAsTextFile("/media/hroniko/DATA/1/2.csv")

    // 11 Закрываем Спарк-контекст
    sc.stop()
  }

  // Функции работы с датой:

  // 1 Первичная конвертация строки в дату по шаблону yyyy-MM-dd HH:mm:ss
  def convertToDate(dateTime: String): Date = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val utilDate = formatter.parse(dateTime)
    new java.sql.Date(utilDate.getTime)
  }

  // 2 Окончательная конвертация даты в строку по шаблону yyyyMM
  def convertToString(date: Date): String = {
    val formatter = new SimpleDateFormat("yyyyMM")
    formatter.format(date)
  }

  // 3 Все вместе:
  def convertToDateString(date: String): String = {
    convertToString(convertToDate(date))
  }

  // Функция округления Double до нужного количества знаков после запятой: (с использованием scala.math.BigDecimal)
  def roundDouble(double: Double, size: Int): Double = {
    BigDecimal(double).setScale(size, BigDecimal.RoundingMode.HALF_UP).toDouble
  }



}