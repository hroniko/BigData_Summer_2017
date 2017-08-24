package com.hroniko.dictionary

/**
  * Created by hroniko on 25.07.17.
  */
class Ips (name : String, ip : String) extends java.io.Serializable{

  def getIP() : String = {
    ip
  }

  def getName() : String = {
    name
  }

}
