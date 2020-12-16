package predictors

import models.MLeapDTree

object CovTypePredictor {
  val fileLoc = "jar:file:/home/samar/dtree-spark-pipeline.zip"
  //  val mldtree = scala.io.Source.fromResource("dtree-spark-pipeline.zip")
  val mldtree = new MLeapDTree(fileLoc)

  //val mldtree = new com.skk.training.models.MLeapDTree("jar:file:/home/samar/dtree-spark-pipeline.zip")
  def predictionsForOneItem(item: String): (String, Double) = {
    val df = mldtree.makePredictionForOneRecord(item, mldtree.mlRFTransformer)
    val mlFrame = df.select("prediction").get
    val prediction = mlFrame.collect().map(x => x.getAs[Double](0))
    println(s"Predicted Cover type for input $item is ${prediction.head}")
    (item, prediction.head)
  }

  def predictionsForMultipleItems(items: Array[String]): Seq[String] = {
    //    val seqItems = items.split("\\|")
    val framesWithPrediction = mldtree.makePredictionForMultipleRecords(items, mldtree.mlRFTransformer)
    framesWithPrediction.collect { case x => x }.foreach(x =>
      x.select("prediction").get.show)
    val zippedPredictions = framesWithPrediction.flatMap(x => x.select("prediction").get.collect().map(
      x => x.getAs[Double](0))).zipWithIndex.zip(items)
    zippedPredictions.map { case ((pred, idx), item) => "" + idx + " | " + item + " | " + pred }
  }

  def predictionsForItemsDelimited(item: String): Seq[String] = {
    val seqItems = item.split("\\|")
    val framesWithPrediction = mldtree.makePredictionForMultipleRecords(seqItems, mldtree.mlRFTransformer)
    //    framesWithPrediction.collect { case x => x }.foreach(x =>
    //      x.select("prediction").get.show)
    val zippedPredictions = framesWithPrediction.flatMap(x => x.select("prediction").get.collect().map(
      x => x.getAs[Double](0))).zipWithIndex.zip(seqItems)
    zippedPredictions.map { case (((pred, idx), item)) => "" + idx + " | " + item + " | " + pred }
  }

}

object CovTypePredictorCL {
  def main(args: Array[String]) {
    if (args.length == 0) {
      println("Please provide the data to estimate the cover type for")
      System.exit(1)
    }
    if (args.length > 1)
      CovTypePredictor.predictionsForMultipleItems(args)
    else if (args.length == 1 && args(0).indexOf("|") == -1)
      CovTypePredictor.predictionsForOneItem(args(0))
    else
      CovTypePredictor.predictionsForItemsDelimited(args(0))
  }
}

object CovTypePredictorTestFile extends App{
  val jarFileLoc = "jar:file:/home/samar/dtree-spark-pipeline.zip"
  //  val mldtree = scala.io.Source.fromResource("dtree-spark-pipeline.zip")
  val mldtree = new MLeapDTree(jarFileLoc)
  val dataFileLoc = "/home/samar/Downloads/covtype.data"
  mldtree.makeAndTestPredictionsForFile(dataFileLoc, mldtree.mlRFTransformer)
}