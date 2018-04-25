package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class User (
    var id:String,
    var buyerid:String,
    var yob:Int,
    var gender:String,
    var keywords:String,
    var customerdata:String,
    var geo:Geo,
    var data:List[Data]
                ) extends Serializable
