package caseClass

case class ImpressionData(
                          val timezone:Float = 0.0f,
                          val request_unix:Int = 0,
                          val publisher_id:Int,
                          val inventory_id:Int,
                          val plcmt_id:Int,
                          val campaign_id:Int,
                          val delivery_id:Int,
                          val ad_id:Int,
                          val urid:String = "",
                          val expires:Long,
                          val pay_for:String = "",
                          val bid:Double,
                          val option:MatchOption
                         ) extends Serializable
