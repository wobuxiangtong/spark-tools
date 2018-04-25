package caseClass

import scala.collection.mutable

case class ClickFraudLog (
                          val clk_timestamp:Int,
                          val aff_id:Int,
                          val aff_pub:String = "",
                          val ip:String = "",
                          val gaid:String = "",
                          val idfa:String = ""
                        )

case class ConvFraudLog (
                            val clk_timestamp:Int,
                            val campaign_id:Int,
                            val aff_id:Int,
                            val aff_pub:String = "",
                            val ip:String = "",
                            val gaid:String = "",
                            val idfa:String = "",
                            val invalid_type: String = ""
                         )
case class EventFraudLog (
                           val clk_timestamp:Int,
                           val event_value:Int,
                           val aff_id:Int,
                           val aff_pub:String = "",
                           val ip:String = "",
                           val gaid:String = "",
                           val idfa:String = ""
                         )
case class ClickFraudIPMsg(
                              val ip : String = "0.0.0.0",
                              val fraudType: String = "",
                              val intervalRadius : Int = 30,
                              val fraudTimestamp : Int = 0,
                              val clkCnt : Long = 0,
                              val clkTimestampSet : mutable.Set[Int] = mutable.Set[Int](),
                              val adidSet : mutable.Set[String] = mutable.Set[String](),
                              val affSet :mutable.Set[String] = mutable.Set[String]()
                          )
