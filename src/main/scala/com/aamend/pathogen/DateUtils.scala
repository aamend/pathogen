/*
 * Copyright 2017 Antoine Amend
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

package com.aamend.pathogen

import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.util.Random

object DateUtils {

  def getTicks(fq: Frequency.Value, from: Long, to: Long, inceptionWindow: Int = 0): List[Long] = {
    val recoveryTime = new DateTime(getEndDate(fq, to, inceptionWindow))
    val startTime = new DateTime(getStartDate(fq, from, inceptionWindow))
    _getTicks(fq, startTime, recoveryTime, List.empty[Long])
  }

  def getEndDate(fq: Frequency.Value, to: Long, inceptionWindow: Int = 0): DateTime = {
    new DateTime(to)
  }

  def getStartDate(fq: Frequency.Value, from: Long, inceptionWindow: Int = 0): DateTime = {
    val dt = new DateTime(from)
    fq match {
      case Frequency.MILLISECOND => roundDate(fq, dt.millisOfDay(), inceptionWindow)
      case Frequency.SECOND => roundDate(fq, dt.secondOfDay(), inceptionWindow)
      case Frequency.MINUTE => roundDate(fq, dt.minuteOfDay(), inceptionWindow)
      case Frequency.HOUR => roundDate(fq, dt.hourOfDay(), inceptionWindow)
      case Frequency.DAY => roundDate(fq, dt.dayOfMonth(), inceptionWindow)
      case Frequency.WEEK => roundDate(fq, dt.weekOfWeekyear(), inceptionWindow)
      case Frequency.MONTH => roundDate(fq, dt.monthOfYear(), inceptionWindow)
      case Frequency.YEAR => roundDate(fq, dt.year(), inceptionWindow)
    }
  }

  def roundDate(fq: Frequency.Value, dt: DateTime.Property, inceptionWindow: Int = 0): DateTime = {
    val rounded = new DateTime(dt.roundFloorCopy())
    val incubationDate = {
      fq match {
        case Frequency.MILLISECOND => rounded.minusMillis(inceptionWindow)
        case Frequency.SECOND => rounded.minusSeconds(inceptionWindow)
        case Frequency.MINUTE => rounded.minusMinutes(inceptionWindow)
        case Frequency.HOUR => rounded.minusHours(inceptionWindow)
        case Frequency.DAY => rounded.minusDays(inceptionWindow)
        case Frequency.WEEK => rounded.minusWeeks(inceptionWindow)
        case Frequency.MONTH => rounded.minusMonths(inceptionWindow)
        case Frequency.YEAR => rounded.minusYears(inceptionWindow)
      }
    }
    incubationDate
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
