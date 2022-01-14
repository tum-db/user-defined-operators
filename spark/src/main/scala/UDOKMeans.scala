import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

object UDOKMeans {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("UDOKmeans").getOrCreate()

    val dataset = spark
      .read
      .option("sep", ",")
      .option("header", true)
      .schema("x DOUBLE, y DOUBLE, cluster_id INT")
      .csv(args(0))

    val assembler = new VectorAssembler()
      .setInputCols(Array("x", "y"))
      .setOutputCol("features")

    val dataset_features = assembler.transform(dataset).cache()

    val kmeans = new KMeans()
    kmeans
      .setInitMode("random")
      .setMaxIter(10)
      .setSeed(42)
      .setK(8)
      .setFeaturesCol("features")

    // Run k-means once without timing to warm up spark
    kmeans.fit(dataset_features)

    for (i <- 0 until 10) {
      spark.time({
        kmeans.fit(dataset_features)
      })
    }

    spark.stop()
  }
}
