package com.hroniko.processStaging

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by hroniko on 25.07.17.
  */
object StageThree {

  def executeIt (sc : SparkContext, stageTwoRdd : RDD[String]) : RDD[String] = {


    // Финальная агрегация до сервиса и региона (для каждого сервиса и региона мы подсчитываем суммарное количество поездок)

    // 1 Сначала перегруппировываем столбцы, собираем все необходимое в ключ, а в значение - только количество поездок:
    val finalSplitRdd = stageTwoRdd.map({ case ( line ) => {
      val res = line.split( "\t" )
      val resList = res.toList
      ( resList(0) + "\t" + resList(2) + "\t" + resList(5) + "\t" + resList(3) + "\t" + resList(4), resList(7).toInt )
    }
    })
    /// finalSplitRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	Зебра	0	Пермь	Пермский край, 3 }

    // 2 Делаем агрегирование по ключу, суммируя значения value
    val finalAggregateRdd = finalSplitRdd.reduceByKey( (x, y) => x + y )
    /// finalAggregateRdd.foreach( { case (key, value) => println( s"{ $key, $value }" ) } ) // Что-то вида { 20170105	Gett	1	-99	Воронежская область, 4 }

    // 3 Собираем все в одну строку и отдаем из стейджа, на этом финальный стедж будет завершен
    val finalStageRdd = finalAggregateRdd.map({ case ( key, value ) => (key + "\t" + value)})
    finalStageRdd

  }

}
