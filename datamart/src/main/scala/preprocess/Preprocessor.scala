package preprocess

import org.apache.spark.sql.DataFrame


object Preprocessor {
  private val inputCols: Array[String] = Array(
    "energy-kcal_100g",
    "sugars_100g",
    "energy_100g",
    "fat_100g",
    "saturated-fat_100g",
    "carbohydrates_100g"
  )

  def fillNa(df: DataFrame): DataFrame = df.na.fill(0.0)

  def leaveInputCols(df: DataFrame): DataFrame = df.select(inputCols.head, inputCols.tail: _*)
}
