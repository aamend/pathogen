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

import scala.util.Random

object DateUtils {

  def getTicks(fq: Frequency.Value, from: Long, to: Long, inceptionWindow: Int = 0): List[Long] = {
    val recoveryTime = new DateTime(getEndDate(fq, to, inceptionWindow))
    var observation = new DateTime(getStartDate(fq, from, inceptionWindow))
    val observations = collection.mutable.MutableList[Long]()
    while (observation.isBefore(recoveryTime)) {
      val timestamp = observation.getMillis
      observations += timestamp
      observation = {
        fq match {
          case Frequency.SECOND => observation.plusSeconds(1)
          case Frequency.MINUTE => observation.plusMinutes(1)
          case Frequency.HOUR => observation.plusHours(1)
          case Frequency.DAY => observation.plusDays(1)
          case Frequency.WEEK => observation.plusWeeks(1)
          case Frequency.MONTH => observation.plusMonths(1)
          case Frequency.YEAR => observation.plusYears(1)
        }
      }
    }
    observations.toList
  }

  def getEndDate(fq: Frequency.Value, to: Long, inceptionWindow: Int = 0): DateTime = {
    new DateTime(to)
  }

  def getStartDate(fq: Frequency.Value, from: Long, inceptionWindow: Int = 0): DateTime = {
    val dt = new DateTime(from)
    fq match {
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

  def randomStartDate(min: Long, max: Long): Long = {
    (Random.nextDouble() * ((max - min) + 1) + min).toLong
  }

  object Frequency extends Enumeration with Serializable {
    val SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR = Value
  }

}
