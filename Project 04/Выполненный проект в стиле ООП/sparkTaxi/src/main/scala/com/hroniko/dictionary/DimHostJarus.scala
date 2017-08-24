package com.hroniko.dictionary

/**
  * Created by hroniko on 25.07.17.
  */
class DimHostJarus (name : String, host : String) extends java.io.Serializable{

  def getHost() : String = {
    host
  }

  def getName() : String = {
    name
  }

}
