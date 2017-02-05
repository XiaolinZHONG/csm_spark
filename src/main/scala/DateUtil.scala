

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by zyong on 2016/11/2.
  */
object DateUtil {

  /**
    *
    * @param dayGap
    * @param format
    * @return
    */
  def getDate(dayGap: Int = 0, format: String = "yyyy-MM-dd"): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, dayGap)
    sdf.format(cal.getTime)
  }

}
