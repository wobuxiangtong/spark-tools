package caseClass

/**
  * Created by moca on 2017/6/1.
  */
case class MatchOption(
                       val urid:String = "",
                       val publisher_id:Int,
                       val inventory_id:Int,
                       val country_code:String = "",
                       val region_ids:List[String],
                       val plcmts:String = "",
                       val stdplcmts:Map[Int,Int],
                       val ua:String = "",
                       val devicetype:String = "",
                       val make:String = "",
                       val model:String = "",
                       val os:String = "",
                       val osv:String = "",
                       val hwv:String = "",
                       val language:String = "",
                       val carrier:String = "",
                       val connectiontype:String = "",
                       val ip:String = "",
                       val lat:String = "",
                       val lon:String = "",
                       val yob:String = "",
                       val agegroup:String = "",
                       val gender:String = "",
                       val keywords:String = "",
                       val interests:String = "",
                       val behaviors:String = "",
                       val gaid:String = "",
                       val idfa:String = "",
                       val dpid:String = "",
                       val did:String = "",
                       val mac:String = "",
                       val request_unix:Long,
                       val request_time:String = ""
                      ) extends Serializable

