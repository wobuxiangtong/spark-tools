package caseClass

import java.util

/**
  * Created by moca on 2018/4/18.
  */
case class Video (
    var mimes:util.ArrayList[String],
    var minduration:util.ArrayList[Int],
    var maxduration:util.ArrayList[Int],
    var protocol:Int,
    var protocols:util.ArrayList[Int],
    var w:Int,
    var h:Int,
    var startdelay:Int,
    var linearity:Int,
    var sequence:Int,
    var battr:util.ArrayList[Int],
    var maxextended:Int,
    var minbitrate:Int,
    var maxbitrate:Int,
    var boxingallowed:Int,
    var playbackmethod:util.ArrayList[Int],
    var delivery:util.ArrayList[Int],
    var pos:Int,
    var companionad:util.ArrayList[Banner],
    var api:util.ArrayList[Int],
    var companiontype:util.ArrayList[Int]
                 ) extends Serializable
