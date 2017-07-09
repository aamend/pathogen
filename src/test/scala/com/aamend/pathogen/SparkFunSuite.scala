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

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, Matchers}

class SparkFunSuite extends FunSuite with Matchers {

  def localTest(name : String)(f : SparkContext => Unit) : Unit = {

    this.test(name) {

      val sparkConf = new SparkConf()
        .setAppName(name)
        .setMaster("local")
        .set("spark.default.parallelism", "1")

      val sc = new SparkContext(sparkConf)

      try {
        f(sc)
      } finally {
        sc.stop()
      }
    }
  }
}