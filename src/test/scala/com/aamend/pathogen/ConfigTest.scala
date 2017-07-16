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

import com.aamend.pathogen.DateUtils.Frequency
import org.scalatest.{FlatSpec, Matchers}

class ConfigTest extends FlatSpec with Matchers {

  val inceptionWindow = 10
  val simulations = 10
  val maxIterations = 10
  val tolerance = 0.002f
  val forgetfulness = 0.05f

  "Configuration object" should "be built" in {

    val config = ConfigBuilder.create
      .withSamplingRate(Frequency.YEAR)
      .withInceptionWindow(inceptionWindow)
      .withSimulations(simulations)
      .withMaxIterations(maxIterations)
      .withTolerance(tolerance)
      .withForgetfulness(forgetfulness)
      .build

    config shouldBe Config(
      Frequency.YEAR,
      inceptionWindow,
      simulations,
      maxIterations,
      tolerance,
      forgetfulness
    )
  }

}
