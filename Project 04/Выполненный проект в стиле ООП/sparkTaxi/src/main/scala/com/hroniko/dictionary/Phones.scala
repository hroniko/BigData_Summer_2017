package com.hroniko.dictionary

/**
  * Created by hroniko on 25.07.17.
  */
class Phones  (city : String, name : String, phone : String) extends java.io.Serializable{

  def getCity() : String = {
    city
  }

  def getName() : String = {
    name
  }

  def getPhone() : String = {
    phone
  }

}
