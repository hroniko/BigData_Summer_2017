package com.hroniko.entities

import org.joda.time.DateTime

/**
  * Created by hroniko on 25.07.17.
  */

class CdrClass (dateTime : DateTime, call_duration_seconds : String, call_to_tn_sgsn : String, calling_no_ggsn : String, lacCell : String) extends java.io.Serializable{

  var name = ""
  var region = ""
  var city = ""

  def getDateTime() : DateTime = {
    dateTime
  }


  def getName() : String = {
    name
  }

  def setName(newname : String) = {
    name = newname
  }

  def getLacCell() : String = {
    lacCell
  }

  def getRegion() : String = {
    region
  }

  def setRegion(newRegion : String) = {
    region = newRegion
  }

  def getPhone() : String = {
    calling_no_ggsn
  }

  def getCallPhone() : String = {
    call_to_tn_sgsn
  }

  def setCity(newCity : String) = {
    city = newCity
  }

  def getCity() : String = {
    city
  }

}

