package caseClass

import com.alibaba.fastjson.annotation.JSONField

/**
  * Created by moca on 2018/4/18.
  */
case class Geo (
    var lat:Float,
    var lon:Float,
    @JSONField(name="type")
    var location_type:Int,
    var country:String,
    var region:String,
    var regionfips104:String,
    var metro:String,
    var city:String,
    var zip:String,
    var utcoffset:Int
) extends Serializable
