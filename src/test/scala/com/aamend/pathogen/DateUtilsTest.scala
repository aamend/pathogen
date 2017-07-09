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

import java.text.SimpleDateFormat

import com.aamend.pathogen.DateUtils.Frequency
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}

class DateUtilsTest extends FlatSpec with Matchers {

  val rightNowString = "2017-07-06T22:37:23.999"
  val SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
  val rightNow: DateTime = new DateTime(SDF.parse(rightNowString))

  rightNowString should "be rounded down to SECOND" in {
    SDF.format(DateUtils.roundDate(Frequency.SECOND, rightNow.secondOfDay()).toDate) shouldBe "2017-07-06T22:37:23.000"
  }

  rightNowString should "be rounded down to MINUTE" in {
    SDF.format(DateUtils.roundDate(Frequency.MINUTE, rightNow.minuteOfDay()).toDate) shouldBe "2017-07-06T22:37:00.000"
  }

  rightNowString should "be rounded down to HOUR" in {
    SDF.format(DateUtils.roundDate(Frequency.HOUR, rightNow.hourOfDay()).toDate) shouldBe "2017-07-06T22:00:00.000"
  }

  rightNowString should "be rounded down to DAY" in {
    SDF.format(DateUtils.roundDate(Frequency.DAY, rightNow.dayOfYear()).toDate) shouldBe "2017-07-06T00:00:00.000"
  }

  rightNowString should "be rounded down to WEEK" in {
    SDF.format(DateUtils.roundDate(Frequency.WEEK, rightNow.weekOfWeekyear()).toDate) shouldBe "2017-07-03T00:00:00.000"
  }

  rightNowString should "be rounded down to MONTH" in {
    SDF.format(DateUtils.roundDate(Frequency.MONTH, rightNow.monthOfYear()).toDate) shouldBe "2017-07-01T00:00:00.000"
  }

  rightNowString should "be rounded down to YEAR" in {
    SDF.format(DateUtils.roundDate(Frequency.YEAR, rightNow.year()).toDate) shouldBe "2017-01-01T00:00:00.000"
  }

  rightNowString should "find start date of SECOND with 10 seconds inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.SECOND, rightNow.getMillis, 10).toDate) shouldBe "2017-07-06T22:37:13.000"
  }

  rightNowString should "find start date of MINUTE with 10 minutes inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.MINUTE, rightNow.getMillis, 10).toDate) shouldBe "2017-07-06T22:27:00.000"
  }

  rightNowString should "find start date of HOUR with 10 hours inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.HOUR, rightNow.getMillis, 10).toDate) shouldBe "2017-07-06T12:00:00.000"
  }

  rightNowString should "find start date of DAY with 1 day inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.DAY, rightNow.getMillis, 1).toDate) shouldBe "2017-07-05T00:00:00.000"
  }

  rightNowString should "find start date of WEEK with 1 week inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.WEEK, rightNow.getMillis, 1).toDate) shouldBe "2017-06-26T00:00:00.000"
  }

  rightNowString should "find start date of MONTH with 1 month inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.MONTH, rightNow.getMillis, 1).toDate) shouldBe "2017-06-01T00:00:00.000"
  }

  rightNowString should "find start date of YEAR with 1 year inception time" in {
    SDF.format(DateUtils.getStartDate(Frequency.YEAR, rightNow.getMillis, 1).toDate) shouldBe "2016-01-01T00:00:00.000"
  }

}
