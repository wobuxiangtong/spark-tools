package caseClass


/**
  * Created by moca on 2017/6/1.
  */
case class EffectiveCharging(
                              val publisher_id:Int,
                              val inventory_id:Int,
                              val plcmt_id:Int,
                              val campaign_id:Int,
                              val delivery_id:Int,
                              val ad_id:Int,
                              val timezone:Float,
                              val urid:String = "",
                              val expires:Long,
                              val pay_for:String = "",
                              val bid:Double,
                              val request_unix:Long,
                              val option:MatchOption
                            ) extends Serializable
