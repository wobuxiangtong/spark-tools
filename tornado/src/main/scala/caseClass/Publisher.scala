package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Publisher(
  var id:String,
  var name:String,
  var cat:List[String],
  var domain:String
                    ) extends Serializable
