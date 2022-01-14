import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler

object UDOLinearRegression {
  def getLinregValues(xy_values: Array[Double]): Array[Double] = {
    val x = xy_values(0)
    val y = xy_values(1)
    val x2 = x * x
    val x3 = x2 * x
    val x4 = x2 * x2
    val xy = x * y
    val x2y = x2 * y

    Array(1.0, x, x2, x3, x4, y, xy, x2y)
  }

  def addArrays(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    var result = new Array[Double](8)
    result(0) = arr1(0) + arr2(0)
    result(1) = arr1(1) + arr2(1)
    result(2) = arr1(2) + arr2(2)
    result(3) = arr1(3) + arr2(3)
    result(4) = arr1(4) + arr2(4)
    result(5) = arr1(5) + arr2(5)
    result(6) = arr1(6) + arr2(6)
    result(7) = arr1(7) + arr2(7)
    result
  }

  def linearRegression(dataset: RDD[Array[Double]]): Array[Double] = {
    var sums = dataset.map(getLinregValues).reduce(addArrays)

    var sum1 = sums(0)
    var sumx = sums(1)
    var sumx2 = sums(2)
    var sumx3 = sums(3)
    var sumx4 = sums(4)
    var sumy = sums(5)
    var sumxy = sums(6)
    var sumx2y = sums(7)

    var detInv = 1.0 / (
       sum1 * sumx2 * sumx4
       + 2 * sumx * sumx2 * sumx3
       - sumx2 * sumx2 * sumx2
       - sum1 * sumx3 * sumx3
       - sumx * sumx * sumx4
    )
    var a = detInv * (
       sumy * (sumx2 * sumx4 - sumx3 * sumx3)
       + sumxy * (sumx2 * sumx3 - sumx * sumx4)
       + sumx2y * (sumx * sumx3 - sumx2 * sumx2)
    )
    var b = detInv * (
       sumy * (sumx2 * sumx3 - sumx * sumx4)
       + sumxy * (sum1 * sumx4 - sumx2 * sumx2)
       + sumx2y * (sumx * sumx2 - sum1 * sumx3)
    )
    var c = detInv * (
       sumy * (sumx * sumx3 - sumx2 * sumx2)
       + sumxy * (sumx * sumx2 - sum1 * sumx3)
       + sumx2y * (sum1 * sumx2 - sumx * sumx)
    )

    Array(a, b, c)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("UDORegression").getOrCreate()

    val dataset = spark
      .read
      .option("sep", ",")
      .option("header", true)
      .schema("x DOUBLE, y DOUBLE")
      .csv(args(0))

    val dataset_rdd = dataset.rdd.map(row => Array(row.getDouble(0), row.getDouble(1))).cache()

    // Run once without timing to warm up spark
    linearRegression(dataset_rdd)

    for (i <- 0 until 10) {
      spark.time({
        linearRegression(dataset_rdd)
      })
    }

    spark.stop()
  }
}
