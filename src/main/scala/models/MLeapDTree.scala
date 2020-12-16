package models

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer, Row => MleapRow}
import resource._

import scala.collection.mutable

class MLeapDTree(val bundleLoc: String) {
  //  val bundleLoc = "jar:file:/home/samar/dtree-spark-pipeline.zip"
  val bundle: Bundle[Transformer] = (for (bf <- managed(BundleFile(bundleLoc))) yield {
    bf.loadMleapBundle().get
  }).tried.get

  val mlRFTransformer: Transformer = (for (bf <- managed(BundleFile(bundleLoc))) yield {
    bf.loadMleapBundle().get.root
  }).tried.get

  val mleapSchema: StructType = StructType(
    StructField("Elevation", ScalarType.Int),
    StructField("Aspect", ScalarType.Int),
    StructField("Slope", ScalarType.Int),
    StructField("Horizontal_Distance_To_Hydrology", ScalarType.Int),
    StructField("Vertical_Distance_To_Hydrology", ScalarType.Int),
    StructField("Horizontal_Distance_To_Roadways", ScalarType.Int),
    StructField("Hillshade_9am", ScalarType.Int),
    StructField("Hillshade_Noon", ScalarType.Int),
    StructField("Hillshade_3pm", ScalarType.Int),
    StructField("Horizontal_Distance_To_Fire_Points", ScalarType.Int),
    //    StructField("Cover_Type", ScalarType.Double),
    StructField("wilderness", ScalarType.Double),
    StructField("soil", ScalarType.Double)).get

  def transformLineToMLRow(line: String): MleapRow = {
    val tarr = line.split(",")
    MleapRow(
      tarr(0).toInt,
      tarr(1).toInt, tarr(2).toInt, tarr(3).toInt,
      tarr(4).toInt, tarr(5).toInt, tarr(6).toInt,
      tarr(7).toInt, tarr(8).toInt, tarr(9).toInt,
      //      tarr(tarr.length - 1).toDouble,
      tarr.slice(10, 14).indexOf("1").toDouble,
      tarr.slice(14, 54).indexOf("1").toDouble)
  }

  def makePredictionForOneRecord(
                                  line: String,
                                  mltransformer: ml.combust.mleap.runtime.frame.Transformer): DefaultLeapFrame = {
    val data = Seq(transformLineToMLRow(line))
    val frame = DefaultLeapFrame(mleapSchema, data)
    val frameRF = mltransformer.transform(frame).get
    frameRF
  }

  def makePredictionForMultipleRecords(
                                        listOfRecs: Seq[String],
                                        mltransformer: ml.combust.mleap.runtime.frame.Transformer):
  Seq[DefaultLeapFrame] = {
    val listOfMLFrames = listOfRecs.map { x =>
      mlRFTransformer.transform(
        DefaultLeapFrame(mleapSchema, Seq(transformLineToMLRow(x)))).get
    }
    listOfMLFrames
  }

  import java.io.File

  def makeAndTestPredictionsForFile(
                                     file: String,
                                     mltransformer: ml.combust.mleap.runtime.frame.Transformer):
  mutable.Seq[(Double, Double)] = {
    val fileSource = scala.io.Source.fromFile(new File(file))
    val lines = fileSource.getLines
    var recCount = 0
    var likeCount = 0
    val lbForPredictions = new scala.collection.mutable.ListBuffer[(Double, Double)]

    def getFeatureSet(line: String): String = line.substring(0, line.lastIndexOf(","))

    def getLabel(line: String) = line.substring(line.lastIndexOf(",") + 1, line.length)

    for (line <- lines) {
      val features = getFeatureSet(line)
      val label = getLabel(line)
      val predictionsFrame = makePredictionForOneRecord(features, mltransformer)
      val tupleForLB = (predictionsFrame.select("prediction").get.collect()(0).getDouble(0), label.toDouble)
      recCount += 1
      if (tupleForLB._1 == tupleForLB._2) likeCount += 1
      lbForPredictions.append(tupleForLB)
    }
    fileSource.close()
    println(
      s"""Total records processed $recCount ,
      records with label and predictions matching $likeCount,
      accuracy ${likeCount * 100 / recCount}%)""")
    lbForPredictions
  }
}
