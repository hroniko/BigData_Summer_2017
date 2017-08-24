package com.hroniko.entities

import com.hroniko.tools.ConverterDateTime._
import org.joda.time.DateTime

/**
  * Created by hroniko on 25.07.17.
  */
class TransactionClass (dateTime : DateTime, phone : String, name : String, region : String, onOffLine : Int) extends java.io.Serializable{  // phone, datetime, name, region

  var city = "-99"


  def getDateTime() : DateTime = {
    dateTime
  }


  def getName() : String = {
    name
  }


  def getRegion() : String = {
    region
  }


  def getPhone() : String = {
    phone
  }

  def setCity(newCity : String) = {
    city = newCity
  }

  def getCity() : String = {
    city
  }


  def getOnOffLine() : Int = {
    onOffLine
  }

  def toGoodString() : String = {

    val res = phone + "\t" + convertJodaDateTimeToStringForStageOne (dateTime) + "\t" + name + "\t" + city + "\t" + region + "\t" + onOffLine
    res

}


}
