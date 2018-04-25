package caseClass

import scala.collection.mutable.ArrayBuffer

/**
  * Created by moca on 2018/4/18.
  */
case class Native (
    var request:String,
    var ver:String,
    var api:ArrayBuffer[Int],
    var battr:ArrayBuffer[Int]
                  ) extends Serializable
