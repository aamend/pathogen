import io.pathogen.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Graph
import io.pathogen.spark.DateUtils.Frequency
import scala.io.Source

/*
val sun = 1L
val moon = 2L
val chicken = 3L
val dog = 4L
val owl = 5L

random events: 6L, 7L, 8L
*/


var events = Array[Event]()
val eventsFile = Source.fromFile("src/test/resources/io/pathogen/spark/events.csv")
for (line <- eventsFile.getLines.drop(1)) {
    val cols = line.split(",").map(_.trim)
    val currentEvent = new Event(conceptId=cols(0).toLong, eventStart=cols(1).toLong, eventStop=cols(2).toLong, amplitude=cols(3).toLong)
    events = events :+ currentEvent
}
eventsFile.close

val spark:SparkSession = SparkSession.builder().master("local[1]").appName("test").getOrCreate()
val rdd:RDD[Event] = spark.sparkContext.parallelize(events)


//Need a config to run the rooster
val config = new Config(samplingRate=Frequency.MINUTE,maxIterations=75)
val roosterObject = new Rooster(config)
val sunObject = new Sun(config)

//Calling function from rooster
val g = roosterObject.crow(rdd)

//calling function for sun
val s = sunObject.rise(g)


val result = s.vertices.collect()
