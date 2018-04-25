package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Deal (
    var id:String,
    var bidfloor:Float=0,
    var bidfloorcur:String="USD",
    var at:Int,
    var wseat:List[String],
    var wadomain:List[String]
                ) extends Serializable
