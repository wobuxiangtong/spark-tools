package caseClass

/**
  * Created by aaron on 2017/9/29.
  */
 case class ClickDataExt (
                           val timezone:Float,
                           val clk_timestamp:Int,
                           val campaign_id:Int,
                           val group_id:Int,
                           val aff_id:Int,
                           val aff_pub:String = "",
                           val country_code:String = "",
                           val province_geoname_id:Int,
                           val city_geoname_id:Int,
                           val os:String = "",
                           val device_type:String = "",
                           val device_make:String = "",
                           val device_model:String = "",
                           val algorithm:Int,
                           val ip:String = "",
                           val ua:String = "",
                           val gaid:String = "",
                           val idfa:String = "",
                           val adid:String = ""
                         ) extends Serializable
