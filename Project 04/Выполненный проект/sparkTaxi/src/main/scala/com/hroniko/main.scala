package com.hroniko

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by hroniko on 18.07.17.
  */
object main {

  def main(args: Array[String]): Unit = {

    // 1 Подготавливаем Конфиг и Контекст
    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkTaxi") // Локальный
    val sc = new SparkContext(sparkConf)

    // ------------------------------------------------------------------------------------------
    // 2 Подгружаем данные из input-папки (используем разделитель separator из джавы:
    val sep = File.separator // разделитель // java.io.File.separator
    // Общий путь до корневой папки /media/hroniko/DATA/BigData/04_fourth/resources/
    val rootPuth = sep + "media" + sep + "hroniko" + sep + "DATA" + sep + "BigData" + sep + "04_fourth" + sep + "resources" + sep
    // Путь к папке с файлами входных данных:     /media/hroniko/DATA/BigData/04_fourth/resources/input
    val puthInput = rootPuth + "input" + sep

    // Путь к папке с файлами выходных данных:     /media/hroniko/DATA/BigData/04_fourth/resources/myOutput
    val puthOutput = rootPuth + "myOutput" + sep

    // 2.1 Подгружаем jarus_http.csv - файл:
    val jarusHttp = sc.textFile(puthInput + "jarus_http.csv")
    /// jarusHttp.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак табуляции "\t"

    // 2.1.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val jarusHttpSplitRdd = jarusHttp.map( line => line.split("\t") )

    /* 2.1.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (время в Unix формате),
    2ий столбец - телефонный номер абонента,
    6ой и 7ой - соотвественно LAC и CELL вышки,
    9ый - хост, по которому запрашивали страницу
     */
    val jarusHttpTrueColumnsRdd = jarusHttpSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c2 = arr( 2 )
      val c6 = arr( 6 )
      val c7 = arr( 7 )
      val c8 = arr( 9 )
      val stringCounts = c0.split( " " )
      stringCounts.map( c0 => ( c0, c2, c6 + " " + c7, c8 ) )
    } )
    // 2.1.3 Проверяем, что в итоге получилось после отсечения лишних столбцов:
    /// jarusHttpTrueColumnsRdd.foreach( { case ( x1, x2, x3, x4) => println( s"{ $x1, $x2, $x3, $x4 }" ) } )




    // 2.2 Аналогичным образом подгружаем jarus_ssl.csv - файл:
    val jarusSsl= sc.textFile(puthInput + "jarus_ssl.csv")
    /// jarusSsl.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак табуляции "\t"

    // 2.2.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val jarusSslSplitRdd = jarusSsl.map( line => line.split("\t") )

    /* 2.2.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (время в Unix формате),
    2ий столбец - телефонный номер абонента,
    6ой и 7ой - соотвественно LAC и CELL вышки,
    9ый - запрошенный IP-адрес
     */
    val jarusSslTrueColumnsRdd = jarusSslSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c2 = arr( 2 )
      val c6 = arr( 6 )
      val c7 = arr( 7 )
      val c8 = arr( 9 )
      val res = c0.split( " " )
      res.map( c0 => ( c0, c2, c6 + " " + c7, c8 ) )
    } )
    // 2.2.3 Проверяем, что в итоге получилось после отсечения лишних столбцов:
    /// jarusSslTrueColumnsRdd.foreach( { case ( x1, x2, x3, x4) => println( s"{ $x1, $x2, $x3, $x4 }" ) } )


    // А дальше нам надо избавиться от айпи, заменить их названиями сервисов такси в jarusSslTrueColumnsRdd
    // Для этого подгружаем словарь айпишек


    // ------------------------------------------------------------------------------------------
    // 3 Подгружаем словарь соотвествия ip и названий сервисов такси из dim-папки (файл ips):
    // Путь к папке с файлами входных данных:     /media/hroniko/DATA/BigData/04_fourth/resources/dim
    val puthDim = rootPuth + "dim" + sep


    // 3.1 Подгружаем ips.tsv - файл:
    val ipsDim = sc.textFile(puthDim + "ips.tsv")
    /// ipsDim.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак табуляции "\t"
    // 3.1.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val ipsDimSplitRdd = ipsDim.map( line => line.split("\t") )

    /* 3.1.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (Название сервиса),
    1ий столбец - IP адрес, по которому доступен сервис
     */
    val ipsDimTrueColumnsRdd = ipsDimSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c1 = arr( 1 )
      val res = c0.split( "    " )
      res.map( c0 => ( c0, c1) ) // На первое место
    } )
    // 3.1.3 Проверяем, что в итоге получилось после отсечения лишних столбцов: // Получили строки что-то вроде { Таксолёт, 37.140.192.81 }
    /// ipsDimTrueColumnsRdd.foreach( { case ( x1, x2) => println( s"{ $x1, $x2 }" ) } )

    // 3.1.4 Чтобы все прошло безболезненно, наш внутренний джойн, который присоединит еще столбец названий сервисов справа, и уберет то, чего нет в словаре,
    // превращаем наши два RDD из вида RDD[ ( String, String, ..., String ) ] в вид RDD[ ( String, [ String ] ) ], в качестве ключа выставив  айпи:
    val jarusSslFirstKeyRdd = jarusSslTrueColumnsRdd.map({case (datetime, phone, lacCell, ip ) => (ip, (datetime, phone, lacCell))}) //

    // 3.1.5 Проверяем, что в итоге получилось после перегруппировки: // Что-то вроде { 178.248.237.53, (1483441200, 79105558888, 100000 100000) }
    /// jarusSslFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.1.6 Аналогично поступаем со словарем:
    val ipsDimFirstKeyRdd = ipsDimTrueColumnsRdd.map({case (name, ip ) => (ip,(name))})
    // 3.1.7 Проверяем, что в итоге получилось после перегруппировки: // Что-то типа { 91.221.86.1, (НонСтоп) }
    /// ipsDimFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.1.8 Соединяем внутренним джойном наши RDD : jarusSslFirstKeyRdd и ipsDimFirstKeyRdd
    val jarusSslJoinIpsDimRdd = jarusSslFirstKeyRdd.join(ipsDimFirstKeyRdd)
    // Поверяем, что вышло, вышло чтто-то вида { 104.36.192.132, ((1483441200, 79105556666, 22101 34967),Uber) }
    /// jarusSslJoinIpsDimRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.1.9 Тперь можем избавиться от ненужного более айпи в ключе, заменив его на номер абонента,
    // и еще избавиться от двух вложенных таплов, то есть перейти к виду RDD[ ( String, String, ..., String ) ]
    val jarusSslString = jarusSslJoinIpsDimRdd.map(x => (x._2._1._2, x._2._1._1, x._2._1._3, x._2._2)) // Захардкодил, сорри, тут должно получится что-то типа (телефон, дата, лакЦелл, имя сервиса)
    /// jarusSslString .foreach( { case ( phone, datetime, lacCell, name) => println( s"{ $phone, $datetime, $lacCell, $name }" ) } ) // Что-то вида { 79105550000, 1483304399, 33602 1153, Gett }



    // Дальше надо преобразовать данные jarus_http, там у нас есть хосты, но нет названия сервисов.
    // Для этого соединим данные с данными из словаря dim_host_jarus.csv и потом заменим хост на соотвествующее ему имя сервиса

    // 3.2 Подгружаем dim_host_jarus.csv - файл:
    val hostJarusDim = sc.textFile(puthDim + "dim_host_jarus.csv")
    /// hostJarusDim.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак ";"
    // 3.2.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val hostJarusDimSplitRdd = hostJarusDim.map( line => line.split(";") )

    /* 3.2.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (Название сервиса),
    1ий столбец - хост-адрес, по которому доступен сервис
     */
    val hostJarusDimTrueColumnsRdd = hostJarusDimSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c1 = arr( 1 )
      val res = c0.split( "    " )
      res.map( c0 => ( c0, c1) )
    } )

    // 3.2.3 Проверяем, что в итоге получилось после отсечения лишних столбцов: // Получили строки что-то вроде { Gett, ru.gett.com }
    /// hostJarusDimTrueColumnsRdd.foreach( { case ( x1, x2) => println( s"{ $x1, $x2 }" ) } )


    // 3.2.4 превращаем наши два RDD  (ярус и словарь) из вида RDD[ ( String, String, ..., String ) ] в вид RDD[ ( String, [ String ] ) ], в качестве ключа выставив хост:
    val jarusHttpFirstKeyRdd = jarusHttpTrueColumnsRdd.map({case (datetime, phone, lacCell, host ) => (host, (datetime, phone, lacCell))}) //

    // 3.2.5 Проверяем, что в итоге получилось после перегруппировки: // Что-то вроде { rutaxi.ru, (1483304400,79005552222,62302 4327) }
    /// jarusHttpFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.2.6 Аналогично поступаем со словарем:
    val hostJarusDimFirstKeyRdd = hostJarusDimTrueColumnsRdd.map({case (name, host ) => (host,(name))})
    // 3.2.7 Проверяем, что в итоге получилось после перегруппировки: // Что-то типа { 91.221.86.1, (НонСтоп) }
    /// hostJarusDimFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.2.8 Соединяем внутренним джойном наши RDD : jarusHttpFirstKeyRdd и hostJarusDimFirstKeyRdd
    val jarusHttpJoinIpsDimRdd = jarusHttpFirstKeyRdd.join(hostJarusDimFirstKeyRdd)
    // Поверяем, что вышло, вышло что-то вида { ru.uber.com, ((1483441200,79005556666,22101 34967),Uber) }
    /// jarusHttpJoinIpsDimRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 3.2.9 Теперь можем избавиться от ненужного более хоста в ключе, заменив его на номер абонента,
    // и еще избавиться от двух вложенных таплов, то есть перейти к виду RDD[ ( String, String, ..., String ) ]
    val jarusHttpString = jarusHttpJoinIpsDimRdd.map(x => (x._2._1._2, x._2._1._1, x._2._1._3, x._2._2)) // тут должно получится что-то типа (телефон, дата, лакЦелл, имя сервиса)
    /// jarusHttpString .foreach( { case ( phone, datetime, lacCell, name) => println( s"{ $phone, $datetime, $lacCell, $name }" ) } ) // Что-то вида { 79005552222, 1483304400, 62302 4327, Рутакси }

    // И вот теперь уже данные из двух ярусов приведены к общему виду (телефон, дата, лакЦелл, имя сервиса), можно их соединить перед тем, как обрабатывать дальше

    // 3.3 Соединяем данные двух ярусов:
    val jarusFullRdd = jarusHttpString.union(jarusSslString)
    // и сразу проверяем
    /// jarusFullRdd.foreach( { case ( phone, datetime, lacCell, name) => println( s"{ $phone, $datetime, $lacCell, $name }" ) } )

    // ------------------------------------------------------------------------------------------


    // 4 Теперь можем определиться с регионом для обобщенного набора яруса на основе данных о лакЦелл и словаря region_bs.csv


    // 4.1 Подгружаем region_bs.csv - файл:
    val regionBsDim = sc.textFile(puthDim + "region_bs.csv")
    /// regionBsDim.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак ","
    // 4.1.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val regionBsDimSplitRdd = regionBsDim.map( line => line.split(",") )

    /* 4.1.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (лак),
    1ий столбец (целл), будем его конкатенировать через пробел с нулевым столбцом,
    2ий стоббец - Регион
     */
    val regionBsDimTrueColumnsRdd = regionBsDimSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c1 = arr( 1 )
      val c2 = arr( 2 )
      val res = c0.split( "    " )
      res.map( c0 => ( c0 + " " + c1, c2) )
    } )

    // 4.1.3 Проверяем, что в итоге получилось после отсечения лишних столбцов: // Получили строки что-то вроде { 63204 37493, Краснодарский край }
    /// regionBsDimTrueColumnsRdd.foreach( { case ( x1, x2) => println( s"{ $x1, $x2 }" ) } )



    // 4.1.4 превращаем наши два RDD  (объединенный ярус и словарь регионов) из вида RDD[ ( String, String, ..., String ) ] в вид RDD[ ( String, [ String ] ) ], в качестве ключа выставив лакЦелл:
    val jarusFullFirstKeyRdd = jarusFullRdd.map({case (phone, datetime, lacCell, name ) => (lacCell, (phone, datetime, name))}) //

    // 4.1.5 Проверяем, что в итоге получилось после перегруппировки: // Что-то вроде { 33602 1153, (79105551111,1483570860,Gett) }
    /// jarusFullFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )


    // 4.1.6 Аналогично поступаем со словарем:
    val regionBsDimFirstKeyRdd = regionBsDimTrueColumnsRdd.map({case ( lacCell, region ) => ( lacCell, (region) )}).persist() // И кешируем, так как он нам еще понадобиться
    // 4.1.7 Проверяем, что в итоге получилось после перегруппировки: // Что-то вида { 58190 20926, (НонСтоп) }
    /// regionBsDimFirstKeyRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 4.1.8 Соединяем внутренним джойном наши RDD : jarusFullFirstKeyRdd и regionBsDimFirstKeyRdd
    val jarusFullJoinIpsDimRdd = jarusFullFirstKeyRdd.join(regionBsDimFirstKeyRdd)
    // Поверяем, что вышло, вышло что-то вида { 22101 34967, ((79005556666,1483434000,Uber),Московская область) }
    /// jarusFullJoinIpsDimRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 4.1.9 И теперь можем избавиться от ненужного более лакЦелла в ключе, заменив его на номер абонента,
    // и еще избавиться от двух вложенных таплов, то есть перейти к виду RDD[ ( String, String, ..., String ) ]
    val jarusFullString = jarusFullJoinIpsDimRdd.map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)).persist() // И кешируем, так как он нам еще понадобиться // тут должно получится что-то типа (телефон, дата, имя сервиса, регион)
    /// jarusFullString.foreach( { case ( phone, datetime, name, region) => println( s"{ $phone, $datetime, $name, $region }" ) } ) // Что-то вида { 79005554444, 1483909200, Максим, Сочи }

    // ------------------------------------------------------------------------------------------


    // 5 Далее нам надо поработать с телефонными звонками
    // 5.1 Подгружаем cdr.csv - файл:
    val cdrRdd = sc.textFile(puthInput + "cdr.csv")
    /// cdrRdd.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит знак прямой слэш "\\|"

    // 5.1.1 превращаем RDD[ String ] в RDD[ Array[ String ]
    val cdrSplitRdd = cdrRdd.map( line => line.split("\\|") )

    /* 5.1.2 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    2 - дата-время в формате yyyyMMddHHmmss
    6 - call_action_code = 2 (нужно проверить, что он равен двойке - то есть это исходящий вызов от человека на сервис такси)
    8 - call_duration_seconds - продолжительность вызова, она должна быть больше пяти (тут используем фильтр) Перевести в инт и потом сравнивать
    9 - call_to_tn_sgsn - Это куда звонят, то есть это номер сервиса ПРОВЕРЯТЬ это поле на вхождение в словарь!!!
    10 - calling_no_ggsn - Это кто звонит
    32 - basic_service_type - должен быть равен "V" ПРОВЕРЯТЬ, что оно равно V
    26 - Lac
    23 - Cell

    9, 32 поля позволяют точнее идентифицировать транзакцию
     */
    val cdrTrueColumnsRdd = cdrSplitRdd.flatMap( arr => {
      val c2 = arr( 2 )
      val c6 = arr( 6 )
      val c8 = arr( 8 )
      val c9 = arr( 9 )
      val c10 = arr( 10 )
      val c32 = arr( 32 )
      val c26 = arr( 26 )
      val c23 = arr( 23 )
      val res = c2.split( " " )
      res.map( c2 => ( c2, c6, c8, c9, c10, c32, c26 + " " + c23 ) )
    } )
    // 5.1.3 Проверяем, что в итоге получилось после отсечения лишних столбцов:
    /// cdrTrueColumnsRdd.foreach( { case ( x1, x2, x3, x4, x5, x6, x7) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6, $x7 }" ) } )

    // 5.2.4 Применяем фильтр по органичениям на вход для CDR:
    val cdrFiltredRdd = cdrTrueColumnsRdd.filter(x => x._2 == "2") //call_action_code = 2 (нужно проверить, что он равен двойке - то есть это исходящий вызов от человека на сервис такси)
        .filter(x => x._3.toInt > 5) // продолжительность вызова, она должна быть больше пяти (тут используем фильтр) Перевести в инт и потом сравнивать
        .filter(x => x._6 == "V") // - basic_service_type - должен быть равен "V" ПРОВЕРЯТЬ, что оно равно V
    /// cdrFiltredRdd.foreach( { case ( x1, x2, x3, x4, x5, x6, x7) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6, $x7 }" ) } )


    // 5.2.5 Убираем ненужные больше столбцы: call_action_code и basic_service_type и делаем ключом телефон, по которому звонят - он однозначно определяет сервис, можно посмотреть с словаре телефно назначенный сервис
    val cdrZipRdd = cdrFiltredRdd.map({case (datetime, call_action_code, call_duration_seconds, call_to_tn_sgsn, calling_no_ggsn, basic_service_type, lacCell ) => (call_to_tn_sgsn, (datetime, call_duration_seconds, call_to_tn_sgsn, calling_no_ggsn, lacCell))})
    /// cdrZipRdd.foreach( { case ( x1, x2) => println( s"{ $x1, $x2 }" ) } )

    // 5.2.6 И дальше будем соединять через Inner Join со словарем телефонов и сервисов по номеру телефона как ключу
    // Для этого подгружаем словарь телефонов:
    // 5.2.7 Подгружаем phones.tsv" - файл:
    val phonesDim = sc.textFile(puthDim + "phones.tsv")
    /// phonesDim.foreach(println) // на всякий случай проверяем, что прочитали
    // Разделителем в нем служит таб "\t"
    // 5.2.8 превращаем RDD[ String ] в RDD[ Array[ String ]
    val phonesDimSplitRdd = phonesDim.map( line => line.split("\t") )

    /* 5.2.9 а затем в RDD[ ( String, String, ..., String ) ], оставляя только нужные столбцы:
    0ый столбец (город),
    1ий столбец (регион),
    2ий столбец - телефон
     */
    val phonesDimTrueColumnsRdd = phonesDimSplitRdd.flatMap( arr => {
      val c0 = arr( 0 )
      val c1 = arr( 1 )
      val c2 = arr( 2 )
      val res = c0.split( "    " )
      res.map( c0 => ( c2, (c0, c1)) )
    } )

    // 5.2.10 Проверяем, что в итоге получилось после отсечения лишних столбцов: // Получили строки что-то вроде { 74212999999, (Хабаровск,Максим) }
    /// phonesDimTrueColumnsRdd.foreach( { case ( x1, x2) => println( s"{ $x1, $x2 }" ) } )

    // 5.2.11 Соединяем внутренним джойном наши RDD : cdrZipRdd и phonesDimTrueColumnsRdd
    val cdrJoinPhonesDimRdd = cdrZipRdd.join(phonesDimTrueColumnsRdd)
    // Поверяем, что вышло, вышло что-то вида { 74955042222, ((20170109000000,6,74955042222,79605555555,22101 34967),(Москва,Максим)) }
    /// cdrJoinPhonesDimRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 5.2.12 Теперь можем избавиться от ненужного более телефона, на который звонили, в ключе, заменив его на номер абонента,
    // и еще избавиться от двух вложенных таплов, то есть перейти к виду RDD[ ( String, String, ..., String ) ]
    val cdrString = cdrJoinPhonesDimRdd.map(x => (x._2._1._4, x._2._1._1, x._2._1._2, x._2._1._5, x._2._2._1, x._2._2._2)) // тут должно получится что-то типа (телефон, дата, длительность звонка в сек, лакЦелл, город, имя сервиса)
    /// cdrString.foreach( { case ( phone, datetime, duration, lacCell, city, name ) => println( s"{ $phone, $datetime, $duration, $lacCell, $city, $name }" ) } ) // Что-то вида { 79605557777, 20170105000001, 6, 58190 20925, Волгоград, Сатурн }

    // 5.3 Далее соединим со словарем region_bs.csv, он у нас уже подгружен и закэширован в переменную regionBsDimFirstKeyRdd
    // 5.3.1 А вот cdrString надо будет опять перегруппировать к виду ключ-значение, тут должно получится что-то типа (лакЦелл, (телефон, дата, длительность звонка в сек, город, имя сервиса))
    val cdrKeyValueRdd = cdrString.map( { case ( phone, datetime, duration, lacCell, city, name ) => (lacCell, (phone, datetime, duration, city, name)) })
    // Поверяем, что вышло, вышло что-то вида { 58190 20925, (79605557777,20170105000001,6,Волгоград,Сатурн) }
    /// cdrKeyValueRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 5.3.2 Соединяем внутренним джойном наши RDD : cdrKeyValueRdd и regionBsDimFirstKeyRdd
    val cdrKeyValueJoinPhonesDimRdd = cdrKeyValueRdd.join(regionBsDimFirstKeyRdd)
    // Поверяем, что вышло, вышло что-то вида { 58190 20926, ((79605557777,20170105005959,6,Волгоград,Максим),Волгоградская область) }
    /// cdrKeyValueJoinPhonesDimRdd.foreach( { case ( key, value) => println( s"{ $key, $value }" ) } )

    // 5.3.4 Теперь можем избавиться от ненужного более ЛакЦелла в ключе, заменив его на номер абонента,
    // и еще избавиться от двух вложенных таплов, то есть перейти к виду RDD[ ( String, String, ..., String ) ]
    val cdrWithRegionRdd = cdrKeyValueJoinPhonesDimRdd.map( x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._5, x._2._1._4, x._2._2) )
    // Поверяем, что вышло, вышло что-то вида { 79605557777, 20170105000001, 6, Сатурн, Волгоград, Волгоградская область}
    /// cdrWithRegionRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } )

    // 2017-07-20
    // 5.3.5 от столбца продолжительность вызова тоже уже можно избавиться, а вместо него добавть идентификатор,
    // откуда приехала транзакция (онлайн или оффлайн, в данном случае оффлайн, и он = 0)
    val cdrWithOfflineIdRdd = cdrWithRegionRdd.map({ case ( phone, datetime, duration, name, city, region ) => (phone, datetime, name, city, region, 0) })
    // Поверяем, что вышло, вышло что-то вида { 79605556666, 20170105040001, Зебра, Пермь, Пермский край, 0}
    /// cdrWithOfflineIdRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } )

    // ----------------------------------------------------------------------------------------------------------------


    // 6 Неплохо бы к такому же виду привести и данные яруса

    // 6.1 Добавляем к данным яруса столбец с инфрмацией о городе (заглушка "-99") и столбец с инфой, откуда приехала транзация (онлайн, =1)

    val jarusWithOnlineIdRdd = jarusFullString.map({ case ( phone, datetime, name, region ) => (phone, datetime, name, "-99", region, 1) }) // тут должно получится что-то типа (телефон, дата, имя сервиса, регион)
    /// jarusWithOnlineIdRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } ) // Что-то вида { 79005555555, 1483653660, Максим, -99, Сочи, 1}


    // ----------------------------------------------------------------------------------------------------------------

    // 7 Работаем с датами

    // 7.1 Приводим к нормальному виду даты в ярусе (используем конвертер из unix-формата в формат JodaTime:
    val jarusJodaDateTimeRdd = jarusWithOnlineIdRdd.map({ case ( phone, datetime, name, city, region, onOffLine ) => (phone, convertUnixToJodaDateTime(datetime), name, city, region, onOffLine) })
    /// jarusJodaDateTimeRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } ) // Что-то вида { 79005556666, 2017-01-03T12:00:00.000+03:00, Uber, -99, Московская область, 1}

    // 7.2 Приводим к нормальному виду даты в cdr (обычный конвертер в JodaTime):
    val cdrJodaDateTimeRdd = cdrWithOfflineIdRdd.map({ case ( phone, datetime, name, city, region, onOffLine ) => (phone, convertCdrToJodaDataTime(datetime), name, city, region, onOffLine) })
    /// cdrJodaDateTimeRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } ) // Что-то вида { 79605556666, 2017-01-05T04:00:01.000+03:00, Зебра, Пермь, Пермский край, 0}

    // ----------------------------------------------------------------------------------------------------------------

    // 8 А теперь соединяем данные cdr и яруса и сохраняем в файл stage1.txt
    // 8.1 Соединяем файлы:
    val fullRdd = cdrJodaDateTimeRdd.union(jarusJodaDateTimeRdd).persist()


    // 8.2 Сохраняем в textFile, но предварительно конвертнем еще время к формату 2017-01-05 00:40:00 convertJodaDateTimeToStringForStageOne
    val stageOneRdd = fullRdd.map({ case ( phone, datetime, name, city, region, onOffLine ) => (phone + "\t" + convertJodaDateTimeToStringForStageOne(datetime) + "\t" + name + "\t" + city + "\t" + region + "\t" + onOffLine) })

    stageOneRdd.saveAsTextFile(puthOutput + "stage1.txt")



    // ----------------------------------------------------------------------------------------------------------------

    // 9 Отбираем только транзакции за какой-то определенный день, напрмер за 2017-01-05 (тут пока захардкодил)

    val forOneDayRdd = fullRdd.filter(x => convertJodaDateTimeToString (x._2) == "20170105")
    /// forOneDayRdd.foreach( { case ( x1, x2, x3, x4, x5, x6) => println( s"{ $x1, $x2, $x3, $x4, $x5, $x6}" ) } ) // Что-то вида { 79605556666, 2017-01-05T04:00:01.000+03:00, Зебра, Пермь, Пермский край, offline}

    // ----------------------------------------------------------------------------------------------------------------

    // 10 АГРЕГИРОВАНИЕ (часть первая - до пользователя, то есть по номеру телефона)
    /*
      Тут мы уже строим не до сервиса и города, а идентифицируем человека
      и здесь как раз подсчитываем следующее:
      1 - тип сервиса (оффлайн 0 / онлайн 1)
      2 - является ли пользователь водителем такси или нет (1 - водитель, 0 - нет)
      3 - количество его поездок по данному региону (например, три поездки по данному региону)
	 */

    // 10.1 Делаем ключем конструкцию вида 20170105	79605557777	Максим	Волгоград	Волгоградская область	0 , 2017-01-05 00:00:01  // Ключ длинный, а значение - это правильная дата без урезаний, чтобы при соединении по ключу потом даты собрались все в массив
    // Чтобы можно было потом произвести агрегирование по этому ключу и не потерять никакие данные
    val forOneDaySpecialKeyRdd = forOneDayRdd.map({ case ( phone, datetime, name, city, region, onOffLine ) => (convertJodaDateTimeToString(datetime) + "\t" + phone + "\t" + name + "\t" + city + "\t" + region + "\t" + onOffLine, datetime) })
    /// forOneDaySpecialKeyRdd.foreach( { case (key, value) => println( s"$key, { $value }" ) } ) // Что-то вида  20170105	79105551111	Gett	-99	Воронежская область	1, { 2017-01-05T02:01:00.000+03:00 }


    // 10.2 Группируем по ключу полученный RDD
    val groupRdd = forOneDaySpecialKeyRdd.groupByKey()
    /// groupRdd.foreach( { case (key, value) => println( s"$key, { ${value} }" ) } ) // Что-то вида 20170105	79605556666	Зебра	Пермь	Пермский край	0, { CompactBuffer(2017-01-05T00:00:00.000+03:00, 2017-01-05T01:00:01.000+03:00, 2017-01-05T04:00:01.000+03:00) }

    // Теперь в каждой паре ключ-значение в качестве значения выступает отсортированный массив дат транзакций
    // данного пользователя в формате JodaTime
    // Это позволяет надеяться, что мы сможен нормально обсчитать, сколько поездок было у этого пользователя
    // и не водитель ли он, напишем внизу соответсвующие функции, в которые будем передавать этот CompactBuffer

    // 10.3 Определяем и проставляем столбец с информацией о том, водитель это или нет, но только для ярусов, то есть там, где последний столбец содержит 1
    val driverRdd = groupRdd.map({ case ( key, value ) => {
      // Чтобы определить, что у нас ярус (мы блин потеряли информацию об этом в отдельном столбце, приходится сплитить заново
      val res = key.split( "\t" )
      val flagJarus = res.toList
      val tail = flagJarus.apply(5)
      if (tail.equals("1")){ // Если последний элемент единичка, то перед нами ярус, запускаем для него процедуру определения, водитель это или нет
        ( key, isItDriver(value.toList), value)
      }
      else{
        ( key, 0, value ) // Иначе не водитель
      }
    }
    })
    /// driverRdd.foreach( { case (key, driver, value) => println( s"{ $key, $driver, $value }" ) } ) // Что-то вида { 20170105	79105551111	Gett	-99	Воронежская область	1, 1, CompactBuffer(2017-01-05T00:00:00.000+03:00, 2017-01-05T00:40:00.000+03:00, 2017-01-05T01:30:00.000+03:00, 2017-01-05T02:01:00.000+03:00) }

    // 10.4 И остается только подсчитать количество поездок и присоединить еще один столбец
    /*
    По поводу подсчета количества поездок. Будем пользоваться упрощенной схемой:
    - по интернет-транзакциям каждое посещение сервиса такси считаем поездкой.
    - в СВR считаем, что если в течение часа происходит несколько звонков, то считать это одной поездкой
    */
    val tripRdd = driverRdd.map({ case ( key, driver, value ) => {
      // Чтобы определить, что у нас ярус (мы блин потеряли информацию об этом в отдельном столбце, приходится сплитить заново
      val res = key.split( "\t" )
      val flagJarus = res.toList
      val tail = flagJarus.apply(5)
      if (tail.equals("1") & driver==0){ // Если последний элемент единичка, то перед нами ярус, и если это не водитель, то ставим ему в качестве количества поездок количество всех транзакций value.size
        ( key, driver, value.size, value)
      }
      else{ // Иначе перед нами CDR или сессия с навигатора водителя (ее можно проверить на длительность), для него запускаем процедуру расчета количества поездок
        ( key, driver, tripCount(value.toList), value ) // и запускаем процедуру расчета количества поездок
      }
    }
    })
    /// tripRdd.foreach( { case (key, driver, trip, value) => println( s"{ $key, $driver, $trip, $value }" ) } ) // Что-то вида { 20170105	79605557777	Максим	Волгоград	Волгоградская область	0, 0, 1, CompactBuffer(2017-01-05T00:00:00.000+03:00, 2017-01-05T00:59:59.000+03:00) }



    // 10.5 Убираем ненужный более столбец value с сессиями
    val tripWithoutSessionRdd = tripRdd.map({ case ( key, driver, trip, value ) => (key + "\t" + driver, trip)})
    /// tripWithoutSessionRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	79105551111	Gett	-99	Воронежская область	1	1, 2 }

    // 10.6 И наконец делаем агрегирование по ключу, суммируя значения value
    val tripAggregateRdd = tripWithoutSessionRdd.reduceByKey( (x, y) => x + y )
    /// tripAggregateRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	79105551111	Gett	-99	Воронежская область	1	1, 2 }

    // 10.7 Собираем все в одну строку и сохраняем в текст-файл, на этом второй стедж будет завершен
    val stageTwoRdd = tripAggregateRdd.map({ case ( key, value ) => (key + "\t" + value)}).persist()
    stageTwoRdd.saveAsTextFile(puthOutput + "stage2.txt")



    // ----------------------------------------------------------------------------------------------------------------


    // 11 Финальная агрегация до сервиса и региона (для каждого сервиса и региона мы подсчитываем суммарное количество поездок)

    // 11.1 Сначала перегруппировываем столбцы, собираем все необходимое в ключ, а в значение - только количество поездок:
    val finalSplitRdd = stageTwoRdd.map({ case ( line ) => {
      val res = line.split( "\t" )
      val resList = res.toList
      ( resList.apply(0) + "\t" + resList.apply(2) + "\t" + resList.apply(5) + "\t" + resList.apply(3) + "\t" + resList.apply(4), resList.apply(7).toInt )
    }
    })
    /// finalSplitRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	Зебра	0	Пермь	Пермский край, 3 }


    // 11.2 Делаем агрегирование по ключу, суммируя значения value
    val finalAggregateRdd = finalSplitRdd.reduceByKey( (x, y) => x + y )
    /// finalAggregateRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	Gett	1	-99	Воронежская область, 4 }

    // 11.3 Собираем все в одну строку и сохраняем в текст-файл, на этом финальный стедж будет завершен
    val finalStageRdd = finalAggregateRdd.map({ case ( key, value ) => (key + "\t" + value)})
    finalStageRdd.saveAsTextFile(puthOutput + "finalStage.txt")


    // 11 Закрываем Спарк-контекст
    sc.stop()
  }

  // Функции работы с датой:

  // 1 Конвертация Jarus UnixDateTime в JodaDateTime (вида 1483304399)
  def convertUnixToJodaDateTime (dateTime : String): DateTime = {
    new DateTime(dateTime.toLong * 1000)
  }

  // 2 Конвертация CDR DateTime в JodaDateTime по шаблону yyyyMMddHHmmss (вида 20170101235959)
  def convertCdrToJodaDataTime ( dateTime : String ): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    formatter.parseDateTime(dateTime)
  }

  // 3 Конвертация JodaDateTime в строку по шаблону yyyyMMdd
  def convertJodaDateTimeToString (date : DateTime): String = {
    val formatter = DateTimeFormat.forPattern("yyyyMMdd")
    date.toString(formatter)
  }

  // 4 Конвертация JodaDateTime в строку по шаблону yyyy-MM-dd HH:mm:ss (для первого стейджа, что-то вида 2017-01-05 00:40:00)
  def convertJodaDateTimeToStringForStageOne (date : DateTime): String = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    date.toString(formatter)
  }



  // Дополнительные вспомогательные функции

  // 5 Получение списка дат транзакций одного пользователя и определение, является ли он водителем (1 или 0)
  /* Чтобы понять, что пользователь водитель, у нас общая продолжительность сессии (то есть вот первая транзакция число,
   приходит вторая транзакция со временем), так вот у нас должна быть разница между двумя соседними транзакциями менее
   60 минут и общая продолжительность дневной сесии должна быть больше 2х часов. То есть это говорит о том, что он
   не просто там катался на такси, а он целый рабочий день профигачил, то есть несколько часов он пользовался
   сервисами с несколькими сессиями
   */
  def isItDriver (dataList : List[DateTime]): Int = {

    if(dataList.size < 2){ // Проверяем длину листа, если в ней меньше 2 элементов, нет смысла обходить коллецию, считаем, что это не водитель
      return 0
    } else{   // Иначе обходим коллекцию, считая разницу в минутах между соседними датами
      var driverFlag = 1 // Ищначально подразумеваем, что перед нами водитель, а потом в ходе проверки можем передумать и снять флаг
      // 1 Проверяем, что разница между всеми соседними транзациями менее 60 минут, иначе сбрасываем флаг водителя
      for ( i <- 1 to dataList.size - 1 )  {
        // Вытягиваем две соседние даты
        val dateOne = dataList.apply(i-1)
        // println(dateOne)
        val dateTwo = dataList.apply(i)
        // println(dateTwo)
        // Вычисляем интервал между транзакциями в минутах:
        val duration = (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60 )
        // println("Интервал в минутах: " + duration)
        if (duration >= 60){
          driverFlag = 0
        }
      }
      // 2 И второе условие, проверяем длительность между первой и последней транзакйией сесии, оно должно быть больше 2 часов (120 минут), тогда это водитель, иначе сбрасываем флаг
      val dateOne = dataList.apply(0)
      val dateTwo = dataList.apply(dataList.size - 1)
      val duration = (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60 )
      if (duration <= 120){
        driverFlag = 0
      }

      return driverFlag
    }
  }


  // 6 Метод рассчета количества поездок для CDR сессии транзакций
  def tripCount (dataList : List[DateTime]): Int = {

    if(dataList.size < 2){ // Проверяем длину листа, если в ней меньше 2 элементов, нет смысла обходить коллецию, считаем, что всего одна поездка
      return 1
    } else{   // Иначе обходим коллекцию, считая разницу в минутах между соседними датами
      var count = 1 // Изначально подразумеваем, что у нас всего одна поездка, а потом в ходе проверки можем увеличивать счетчик поездок
      // 1 Идем по транзакциям и считаем время, как только станет больше 1 часа, увеличиваем счетчик
      var duration = 0L
      for ( i <- 1 to dataList.size - 1 )  {
        // Вытягиваем две соседние даты
        val dateOne = dataList.apply(i-1)
        // println(dateOne)
        val dateTwo = dataList.apply(i)
        // println(dateTwo)
        // Вычисляем интервал между транзакциями в минутах:
        duration = duration + (dateTwo.getMillis - dateOne.getMillis) / (1000 * 60 )
        // println("Интервал в минутах: " + duration)
        if (duration >= 60){
          count = count + 1 // Увеличиваем счетчик,
          duration = 0L // Обнуляем длительность интервала
        }
      }

      return count
    }
  }


}