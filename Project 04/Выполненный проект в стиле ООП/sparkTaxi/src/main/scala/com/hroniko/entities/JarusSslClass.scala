package com.hroniko.entities

import org.joda.time.DateTime

/**
  * Created by hroniko on 25.07.17.
  */
class JarusSslClass (dateTime : DateTime, phone : String, lacCell : String, ip : String) extends java.io.Serializable{

  var name = ""
  var region = ""

  def getDateTime() : DateTime = {
    dateTime
  }

  def getIP() : String = {
    ip
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
    phone
  }

}
