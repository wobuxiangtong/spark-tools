package caseClass

/**
  * Created by aaron on 2017/9/8.
  */
case class EventStreming (
                           val timezone:Float,
                           val campaign_id:Int,
                           val group_id:Int,
                           val aff_id:Int,
                           val aff_pub:String="",
                           val event_value:Int,
                           val install_timestamp:Int,
                           val event_timestamp:Int,
                           val order_amount:Float,
                           val order_currency:String = "",
                           val algorithm:Int,
                           val kpi:Int,
                           val ip:String ="",
                           val ua:String ="",
                           val gaid:String ="",
                           val idfa:String ="",
                           val adid:String =""
                         )extends Serializable

case class EventStremingNewer (
                           val timezone:Float,
                           val campaign_id:Int,
                           val group_id:Int,
                           val aff_id:Int,
                           val aff_pub:String="",
                           val event_value:Int,
                           val install_timestamp:Int,
                           val event_timestamp:Int,
                           val order_amount:Float,
                           val order_currency:String = "",
                           val algorithm:Int,
                           val kpi:Int,
                           val click_id:String =""
                         )extends Serializable

