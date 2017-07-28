/*
 * Copyright 2017 Pathogen.io
 *
 * Pathogen is licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pathogen.spark

import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.util.Random

object DateUtils {

  def getTicks(fq: Frequency.Value, event: Event): List[Long] = {
    val recoveryTime = getStopDate(fq, event)
    val startTime = getStartDate(fq, event)
    val ticks = _getTicks(fq, startTime, recoveryTime, List.empty[Long])
    if(ticks.isEmpty) List(startTime.getMillis) else ticks
  }

  def getStartDate(fq: Frequency.Value, event: Event): DateTime = {
    val dt = new DateTime(event.eventStart)
    fq match {
      case Frequency.MILLISECOND => new DateTime(dt.millisOfDay().roundFloorCopy())
      case Frequency.SECOND => new DateTime(dt.secondOfDay().roundFloorCopy())
      case Frequency.MINUTE => new DateTime(dt.minuteOfDay().roundFloorCopy())
      case Frequency.HOUR => new DateTime(dt.hourOfDay().roundFloorCopy())
      case Frequency.DAY => new DateTime(dt.dayOfMonth().roundFloorCopy())
      case Frequency.WEEK => new DateTime(dt.weekOfWeekyear().roundFloorCopy())
      case Frequency.MONTH => new DateTime(dt.monthOfYear().roundFloorCopy())
      case Frequency.YEAR => new DateTime(dt.year().roundFloorCopy())
    }
  }

  def getStopDate(fq: Frequency.Value, event: Event):DateTime = {
    val dt = new DateTime(event.eventStop)
    fq match {
      case Frequency.MILLISECOND => new DateTime(dt.millisOfDay().roundCeilingCopy())
      case Frequency.SECOND => new DateTime(dt.secondOfDay().roundCeilingCopy())
      case Frequency.MINUTE => new DateTime(dt.minuteOfDay().roundCeilingCopy())
      case Frequency.HOUR => new DateTime(dt.hourOfDay().roundCeilingCopy())
      case Frequency.DAY => new DateTime(dt.dayOfMonth().roundCeilingCopy())
      case Frequency.WEEK => new DateTime(dt.weekOfWeekyear().roundCeilingCopy())
      case Frequency.MONTH => new DateTime(dt.monthOfYear().roundCeilingCopy())
      case Frequency.YEAR => new DateTime(dt.year().roundCeilingCopy())
    }
  }

  def randomStartDate(min: Long, max: Long, seed: Long = System.currentTimeMillis()): Long = {
    (new Random(seed).nextDouble() * ((max - min) + 1) + min).toLong
  }

  @tailrec
  protected def _getTicks(fq: Frequency.Value, curTime: DateTime, maxTime: DateTime, ticks: List[Long]): List[Long] = {
    val nexTime = {
      fq match {
        case Frequency.MILLISECOND => curTime.plusMillis(1)
        case Frequency.SECOND => curTime.plusSeconds(1)
        case Frequency.MINUTE => curTime.plusMinutes(1)
        case Frequency.HOUR => curTime.plusHours(1)
        case Frequency.DAY => curTime.plusDays(1)
        case Frequency.WEEK => curTime.plusWeeks(1)
        case Frequency.MONTH => curTime.plusMonths(1)
        case Frequency.YEAR => curTime.plusYears(1)
      }
    }
    if (nexTime.isAfter(maxTime)) return ticks
    _getTicks(fq, nexTime, maxTime, ticks :+ curTime.getMillis)
  }

  object Frequency extends Enumeration with Serializable {
    val MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR = Value
  }

}
