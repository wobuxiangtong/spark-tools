package caseClass

import java.util

/**
  * Created by moca on 2018/4/18.
  */
case class BidRequest(

  /** the bid request id */
  var id:String="",
  // The impressions objects;
  var imp:List[Impression],
  /** the bid request site id */
  var site:Site,
  var app:App,
  var device:Device,
  var user:User,
  var test:Int = 0,
  var at:Int = 2,
  var tmax:Int,
  var wseat:List[String],
  var allmaps:Int = 0,
  var cur:List[String],
  var bcat:List[String],
  var badv:List[String],
  var regs:Regs
) extends Serializable
