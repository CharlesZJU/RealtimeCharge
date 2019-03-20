package org.qf.utils

import java.text.SimpleDateFormat


object MyScalaUtils {
  def string2Time(timestr: String,patten:String) = {
    new SimpleDateFormat(patten).parse(timestr).getTime
  }

}
