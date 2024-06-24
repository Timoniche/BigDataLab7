import org.scalatest.funsuite.AnyFunSuite

class DataMartTest extends AnyFunSuite {
  //  add to VM options
  //  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  test("Two DataFrames diff") {
        val marketPredictionsOld = getClass.getResource("market_predictions.csv").getPath
        val marketPredictionsNew = getClass.getResource("market_predictions2.csv").getPath
        val datamart = DataMart
        val dfOld = datamart.readDataFrameFromPath(marketPredictionsOld)
        val dfNew = datamart.readDataFrameFromPath(marketPredictionsNew)
        val predictionsDiff = datamart.dataFramesDiff(dfOld, dfNew)
        datamart.logDataFrame(predictionsDiff)
  }
}
