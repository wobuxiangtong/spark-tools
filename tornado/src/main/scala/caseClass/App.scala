package caseClass

import java.util


/**
  * Created by moca on 2018/4/18.
  */
case class App (
    var id:String,
    var name:String,
    var bundle:String,
    var domain:String,
    var storeurl:String,
    var cat:List[String],
    var sectionset:List[String],
    var pagecat:List[String],
    var ver:String,
    var privacypolicy:Int,
    var paid:Int,
    var publisher:Publisher,
    var content:Content,
    var keywords:String
               ) extends Serializable
