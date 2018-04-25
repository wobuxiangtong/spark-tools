package caseClass

/**
  * Created by moca on 2018/4/18.
  */
case class Device (
    var ua:String,
    var geo:Geo,
    var dnt:Int,
    var lmt:Int,
    var ip:String,
    var ipv6:String,
    var devicetype:Int,
    var make:String,
    var model:String,
    var os:String,
    var osv:String,
    var hwv:String,
    var h:Int,
    var w:Int,
    var ppi:Int,
    var pxratio:Float,
    var js:Int,
    var flashver:String,
    var language:String,
    var carrier:String,
  /**
    *  0 unknown,
    *  1 Ethernet,
    *  2 WIFI,
    *  3 Cellular Network - unknown Generation,
    *  4 Cellular Network - 2G
    *  5 Cellular Network - 3G
    *  6 Cellular Network - 4G
    */
    var connectiontype:Int,
    var ifa:String,
    var didsha1:String,
    var didmd5:String,
    var dpidsha1:String,
    var dpidmd5:String,
    var macsha1:String,
    var macmd5:String,
    var macidsha1:String
                  ) extends Serializable
