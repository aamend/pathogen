![Pathogen](/images/logo.png)

_The rooster crows immediately before sunrise, the rooster causes the sun to rise._

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark/pathogen/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.aamend.spark/pathogen)
[![Build Status](https://travis-ci.org/aamend/pathogen.svg?branch=master)](https://travis-ci.org/aamend/pathogen) 

Using GraphX to detect possible causes and effects between time related events. We observe a true 
causation signal by generating random correlations over same events at different time and back propagate 
these scores to their most connected events. Finally, we extract the most probable causes and effects 
together with a score of aggressiveness (how likely an event could explain downstream effects) and 
sensitivity (how likely an event results from an upstream cause).

### Getting Started

_Pathogen_ project is built for __Scala 2.11.x__ and __Spark 2.1.0__. 

#### Maven

_Pathogen_ is available on Maven Central. Add below dependency to your `pom.xml` file

```xml
<dependency>
  <groupId>io.pathogen</groupId>
  <artifactId>pathogen-spark</artifactId>
  <version>x.y.z</version>
</dependency>
```

#### SBT

If you are using SBT, simply add the following to your `build.sbt` file

```scala
libraryDependencies ++= Seq(
  "io.pathogen" % "pathogen-spark" % "x.y.z"
)
```

#### Spark Packages

Available as a [spark package](https://spark-packages.org/package/aamend/pathogen), include this package in your Spark Applications as follows

```bash
> $SPARK_HOME/bin/spark-shell --packages io.pathogen:pathogen-spark:x.y.z
```

## Authors

Antoine Amend - [[antoine.amend@gmail.com]](antoine.amend@gmail.com)

## License

Apache License, version 2.0