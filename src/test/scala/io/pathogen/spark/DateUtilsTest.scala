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

import java.text.SimpleDateFormat

import io.pathogen.spark.DateUtils.Frequency
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class DateUtilsTest extends FlatSpec with Matchers {

  val rightNowString = "2017-07-06T22:37:23.999"
  val SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val rightNow: DateTime = new DateTime(SDF.parse(rightNowString))
  val from: DateTime = new DateTime(SDF.parse("2017-07-07T00:00:00.000"))
  val to: DateTime = from.plusDays(1)

  val rightNowEvent = Event(
    1L,
    rightNow.getMillis,
    to.getMillis
  )

  "ticks" should "be generated" in {

    val event = Event(
      1L,
      from.getMillis,
      to.getMillis
    )

    val ticks = DateUtils.getTicks(
      Frequency.HOUR,
      event
    )

    ticks.foreach(p => println(new DateTime(p)))
    ticks.head shouldBe from.getMillis
    ticks.last shouldBe to.minusHours(1).getMillis
    ticks.size shouldBe 24
  }

  rightNowString should "find start date of SECOND" in {
    SDF.format(DateUtils.getStartDate(Frequency.SECOND, rightNowEvent).toDate) shouldBe "2017-07-06T22:37:23.000"
  }

  rightNowString should "find start date of MINUTE" in {
    SDF.format(DateUtils.getStartDate(Frequency.MINUTE, rightNowEvent).toDate) shouldBe "2017-07-06T22:37:00.000"
  }

  rightNowString should "find start date of HOUR" in {
    SDF.format(DateUtils.getStartDate(Frequency.HOUR, rightNowEvent).toDate) shouldBe "2017-07-06T22:00:00.000"
  }

  rightNowString should "find start date of DAY" in {
    SDF.format(DateUtils.getStartDate(Frequency.DAY, rightNowEvent).toDate) shouldBe "2017-07-06T00:00:00.000"
  }

  rightNowString should "find start date of WEEK" in {
    SDF.format(DateUtils.getStartDate(Frequency.WEEK, rightNowEvent).toDate) shouldBe "2017-07-03T00:00:00.000"
  }

  rightNowString should "find start date of MONTH" in {
    SDF.format(DateUtils.getStartDate(Frequency.MONTH, rightNowEvent).toDate) shouldBe "2017-07-01T00:00:00.000"
  }

  rightNowString should "find start date of YEAR" in {
    SDF.format(DateUtils.getStartDate(Frequency.YEAR, rightNowEvent).toDate) shouldBe "2017-01-01T00:00:00.000"
  }

}
