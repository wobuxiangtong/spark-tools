package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case  class Banner(
                    var w:Int,
                    var h:Int,
                    var wmax:Int,
                    var hmax:Int,
                    var wmin:Int,
                    var hmin:Int,
                    var id:String="",
                    var btype:List[Int],
                    var battr:List[Int],
                    var pos:Int,
                    var mimes:List[String],
                    var topframe:Int,
                    var expdir:List[Int],
                    var api:List[Int]
                    ) extends Serializable
