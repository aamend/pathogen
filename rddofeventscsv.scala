import io.pathogen.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Graph
import io.pathogen.spark.DateUtils.Frequency
import scala.io.Source
import org.apache.spark.sql.types._

/* 
    The csv file cointains:

    12 events: 6 pairs of events of the objects sun and moon, and animals: rooster, dog and owl. 
    Some of these events are overlapped, other are not.
    Events 13,14 and 15 are random events.
    Once the file is read, the events are stored in an array of class Event.
*/


var events = Array[Event]()
val eventsFile = Source.fromFile("src/test/resources/io/pathogen/spark/events_test.csv")
for (line <- eventsFile.getLines.drop(1)) {
    val cols = line.split(",").map(_.trim)
    val currentEvent = new Event(conceptId=cols(0).toLong, eventStart=cols(1).toLong, eventStop=cols(2).toLong, amplitude=cols(3).toLong)
    events = events :+ currentEvent
}
eventsFile.close

// The following cell creates an RDD of the previous events (array).
val spark:SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
val rdd:RDD[Event] = spark.sparkContext.parallelize(events)

// Objects definitions
val config = new Config(samplingRate=Frequency.MINUTE,maxIterations=75)
val roosterObject = new Rooster(config)
val sunObject = new Sun(config)

/* Rooster.Scala takes as input the initial time related events (an RDD of events) 
   and outputs the causal effects explained as a graph (graph[Pathogen, double]).
*/
val g = roosterObject.crow(rdd)

/* Sun.Scala takes as input a graph[Pathogen, double] of observed (and normalized)
   correlations and returns a graph where vertices contain both a sensitivity and aggressiveness scores.
   
   Class Pathogen contains sensitiviy and aggressiveness.
   Aggressiveness: measures how likely an event could explain downstreams effects (it causes other events)
   Sensitivity: measures how likely an event results from an upstream event (it is caused by other ovents)
*/

val s = sunObject.rise(g)
val result = s.vertices.collect()
