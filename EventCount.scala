/**
  * Edited by Farooq Shaikh-19200161
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ie.ucd.csl.comp47470

import java.io._
import java.io.File
import java.io.PrintWriter
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext, Time, StateSpec, State}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

import geotrellis.vector.{Point, Polygon}
import geotrellis.proj4._


class EventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object EventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))
      .map(tuple => ("all", 1))
      .reduceByKeyAndWindow(
       (x: Int, y: Int) => x + y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val latLongToWebMercator = Transform(LatLng, WebMercator)
    val p1 : Polygon = Polygon(latLongToWebMercator(-74.0141012, 40.7152191), latLongToWebMercator(-74.013777, 40.7152275), latLongToWebMercator(-74.0141027, 40.7138745), latLongToWebMercator(-74.0144185, 40.7140753), latLongToWebMercator(-74.0141012, 40.7152191))
    val p2 : Polygon = Polygon(latLongToWebMercator(-74.011869, 40.7217236), latLongToWebMercator(-74.009867, 40.721493), latLongToWebMercator(-74.010140,40.720053), latLongToWebMercator(-74.012083, 40.720267), latLongToWebMercator(-74.011869, 40.7217236))

    val wc = stream.map(_.split(","))
          .map(tuple=>
          if(tuple(0).equalsIgnoreCase("yellow"))
                {
                val p3 : Point = Point(latLongToWebMercator(tuple(10).toDouble, tuple(11).toDouble))
                        if (p3.within(p1)){
                        ("goldman", 1)
                }
                else if (p3.within(p2)){
                ("citigroup", 1)
                }
                else {("NotDefined",1)}
                }
         else{
         val p3 : Point = Point(latLongToWebMercator(tuple(8).toDouble,tuple(9).toDouble))
         if (p3.within(p1)){
                        ("goldman", 1)
                }
         else if (p3.within(p2)){
                ("citigroup", 1)
                }
                else {("NotDefined",1)}
         }

          ).filter(pair => pair._1 != "NotDefined").reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, Minutes(60), Minutes(60))

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
    ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val latLongToWebMercator = Transform(LatLng, WebMercator)
    val p1 : Polygon = Polygon(latLongToWebMercator(-74.0141012, 40.7152191), latLongToWebMercator(-74.013777, 40.7152275), latLongToWebMercator(-74.0141027, 40.7138745), latLongToWebMercator(-74.0144185, 40.7140753), latLongToWebMercator(-74.0141012, 40.7152191))
    val p2 : Polygon = Polygon(latLongToWebMercator(-74.011869, 40.7217236), latLongToWebMercator(-74.009867, 40.721493), latLongToWebMercator(-74.010140,40.720053), latLongToWebMercator(-74.012083, 40.720267), latLongToWebMercator(-74.011869, 40.7217236))

    // Code for q3, uncomment the following lines

        case class stateOp(newCount: Int, time_stamp: String, oldCount: Int) extends Serializable

            def stateUpdateFunction(currTime: Time, key : String, countInWindow : Option[Int], state : State[stateOp]) : Option[(String, stateOp)] = {
            var oldCount : Int = 0
            if (state.exists()){
                            oldCount = state.get().newCount
                        }
            var newCount = countInWindow.getOrElse(0)
            var batchTime = currTime.milliseconds
           
            if (newCount > 10  && newCount > (2*oldCount))
                        {
                        println(s"Number of arrivals to $key has doubled from $oldCount to $newCount at $batchTime!")
                        }

            var op = stateOp(newCount=newCount, time_stamp = "%08d".format(batchTime), oldCount=oldCount)
            state.update(op)
            Some((key, op))
            }

            val TaxiCountWithState = StateSpec.function(stateUpdateFunction _)

         val wc = stream.map(_.split(","))
          .map(tuple=>
          if(tuple(0).equalsIgnoreCase("yellow"))
                {
                val p3 : Point = Point(latLongToWebMercator(tuple(10).toDouble, tuple(11).toDouble))
                        if (p3.within(p1)){
                        ("goldman", 1)
                }
                else if (p3.within(p2)){
                ("citigroup", 1)
                }
                else {("NotDefined",1)}
                }
         else{
         val p3 : Point = Point(latLongToWebMercator(tuple(8).toDouble,tuple(9).toDouble))
         if (p3.within(p1)){
                        ("goldman", 1)
                }
         else if (p3.within(p2)){
                ("citigroup", 1)
                }
                else {("NotDefined",1)}
         }

          ).filter(pair => pair._1 != "NotDefined").reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, Minutes(10), Minutes(10))
                .mapWithState(TaxiCountWithState)

var outputDir =  new File("output")
outputDir.mkdirs()

        def writeFile(filename: String, lines: Seq[(String, (Int, String, Int))]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
        bw.write(line.toString + "\n")
    }
    bw.close()
}

         wc.foreachRDD((rdd, time)  => {
        var _newRDD = rdd.map(line => (line._1,(line._2.newCount, line._2.time_stamp, line._2.oldCount))).map(x => x).collect().toSeq
         writeFile(outputDir+"/part-"+"%08d".format(time.milliseconds), _newRDD)
    })


    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

