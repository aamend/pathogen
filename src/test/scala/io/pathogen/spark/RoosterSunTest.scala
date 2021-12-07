package io.pathogen.spark

import java.text.SimpleDateFormat
import io.pathogen.spark.DateUtils.Frequency
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import io.pathogen.spark.DateUtils.Frequency
import scala.io.Source
import scala.math.BigDecimal


class RoosterSunTest extends FlatSpec with Matchers {

    //Function for rounding
    def round(n: Double): Double = {
        BigDecimal(n).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    //Reading CSV
    var events = Array[Event]()
    val eventsFile = Source.fromInputStream(this.getClass.getResourceAsStream("events_test.csv"), "UTF-8")
    for (line <- eventsFile.getLines.drop(1)) {
        val cols = line.split(",").map(_.trim)
        val currentEvent = new Event(conceptId=cols(0).toLong, eventStart=cols(1).toLong, eventStop=cols(2).toLong, amplitude=cols(3).toLong)
        events = events :+ currentEvent
    }
    eventsFile.close

    val spark:SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
    spark.sparkContext.setLogLevel("OFF");
    val rdd:RDD[Event] = spark.sparkContext.parallelize(events)

    //Defining objects needed
    val config = new Config(samplingRate=Frequency.MINUTE,maxIterations=75)
    val roosterObject = new Rooster(config)
    val sunObject = new Sun(config)

    //Calling function from rooster
    val g = roosterObject.crow(rdd)

    //Check the correct format of the output of Rooster and the input of Sun
    "Rooster" should "return correct class" in {
        g shouldBe a [Graph[_,_]]
        g.edges.collect()(0) shouldBe a [Edge[_]]
    }

    
    //Check the correct values of the edges
    "Rooster" should "return correct edges values" in {
        for ( edge <-g.edges.collect() )
        {
            //spark.graphx.Edge.srcId = id of origin node, spark.graphx.Edge.dstId = id of destination node, spark.graphx.Edge.attr = attribute
            if (edge.srcId == 1 && edge.dstId == 4)
            {
                round(edge.attr) shouldBe 0.2959
            }
            else if (edge.srcId == 2 && edge.dstId == 5)
            {
                edge.attr shouldBe 1.0
            }
            else if (edge.srcId == 1 && edge.dstId == 2)
            {
                round(edge.attr) shouldBe 0.3085
            }
            else if (edge.srcId == 4 && edge.dstId == 1)
            {
                round(edge.attr) shouldBe 0.2959
            }
            else if (edge.srcId == 1 && edge.dstId == 5)
            {
                round(edge.attr) shouldBe 0.2749
            }
            else if (edge.srcId == 3 && edge.dstId == 1)
            {
                round(edge.attr) shouldBe 0.7785
            }
        }
    }

    //Calling function for sun
    val s = sunObject.rise(g)

    //Check the correct format of the output of Sun
    "Sun" should "return correct class" in {
        s shouldBe a [Graph[_,_]]
        s.vertices.collect()(0)._1 shouldBe a [java.lang.Long]
        s.vertices.collect()(0)._2 shouldBe a [Pathogen]
    }

    //Check the correct values of the vertices
    "Sun" should "return correct vertices values" in {
        val result = s.vertices.collect().toMap
    
        round(result.get(1).get.aggressiveness) shouldBe 0.3717
        round(result.get(1).get.sensitivity) shouldBe 0.4265
        round(result.get(2).get.aggressiveness) shouldBe 0.1239
        round(result.get(2).get.sensitivity) shouldBe 0.1421
        round(result.get(3).get.aggressiveness) shouldBe 0.3655
        round(result.get(3).get.sensitivity) shouldBe 0.4193
        round(result.get(4).get.aggressiveness) shouldBe 0.1389
        round(result.get(4).get.sensitivity) shouldBe 0.1594
        result.get(5).get shouldBe Pathogen(0.0,0.0)
        result.get(6).get shouldBe Pathogen(0.0,0.0)
        result.get(7).get shouldBe Pathogen(0.0,0.0)
        result.get(8).get shouldBe Pathogen(0.0,0.0)
    }
}
