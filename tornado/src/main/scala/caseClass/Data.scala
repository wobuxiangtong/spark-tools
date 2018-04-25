package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Data (
    var id:String,
    var name:String,
    var segment:List[Segment]
                ) extends Serializable
