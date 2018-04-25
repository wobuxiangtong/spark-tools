package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Site (
    var id:String,
    var name:String,
    var domain:String,
    var cat:List[String],
    var privacypolicy:Int,
    var page:String,
    var ref:String,
    var publisher:Publisher,
    var content:Content
                ) extends Serializable
