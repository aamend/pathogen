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

import io.pathogen.spark.DateUtils.Frequency

/**
  * @param samplingRate    Frequency used to detect time correlation (Hour, Day, Month, etc.)
  * @param simulations     The number of simulations to run (the more the merrier)
  * @param maxIterations   The maximum number of iterations in Pregel
  * @param tolerance       The absolute zero when convergence is reached
  * @param forgetfulness   How an amplitude is decayed after being conveyed
  * @param erratic         If non null, the default causality for un-related events
  */
case class Config(
                   samplingRate: Frequency.Value = Frequency.DAY,
                   simulations: Int = 100,
                   maxIterations: Int = 100,
                   tolerance: Float = 0.001f,
                   forgetfulness: Float = 0.0f,
                   erratic: Float = 0.0f
                 )

trait ConfigGrammar {

  def withSamplingRate(samplingRate: Frequency.Value): ConfigGrammar

  def withSimulations(simulations: Int): ConfigGrammar

  def withMaxIterations(maxIterations: Int): ConfigGrammar

  def withTolerance(tolerance: Float): ConfigGrammar

  def withForgetfulness(forgetfulness: Float): ConfigGrammar

  def withErratic(erratic: Float): ConfigGrammar

  def build: Config

}

object ConfigBuilder {
  def create: ConfigGrammar = new ConfigBuilder()
}

case class ConfigBuilder(
                          samplingRate: Frequency.Value = Frequency.DAY,
                          simulations: Int = 100,
                          maxIterations: Int = 100,
                          tolerance: Float = 0.001f,
                          forgetfulness: Float = 0.0f,
                          erratic: Float = 0.0f
                        ) extends ConfigGrammar {

  val config: Config = Config(
    samplingRate,
    simulations,
    maxIterations,
    tolerance,
    forgetfulness,
    erratic
  )

  def withSamplingRate(samplingRate: Frequency.Value) = new ConfigBuilder(
    samplingRate,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness,
    config.erratic
  )

  def withSimulations(simulations: Int) = new ConfigBuilder(
    config.samplingRate,
    simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness,
    config.erratic
  )

  def withMaxIterations(maxIterations: Int) = new ConfigBuilder(
    config.samplingRate,
    config.simulations,
    maxIterations,
    config.tolerance,
    config.forgetfulness,
    config.erratic
  )

  def withTolerance(tolerance: Float) = new ConfigBuilder(
    config.samplingRate,
    config.simulations,
    config.maxIterations,
    tolerance,
    config.forgetfulness,
    config.erratic
  )

  def withForgetfulness(forgetfulness: Float) = new ConfigBuilder(
    config.samplingRate,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    forgetfulness,
    config.erratic
  )

  def withErratic(erratic: Float) = new ConfigBuilder(
    config.samplingRate,
    config.simulations,
    config.maxIterations,
    config.tolerance,
    config.forgetfulness,
    erratic
  )

  def build: Config = config

}
