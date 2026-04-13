package project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.rogach.scallop._
import org.apache.spark.graphx._

object BuiltInCountTriangle {
  val log = Logger.getLogger(getClass().getName())
  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val partitions = opt[Int](descr = "number of partitions", required = false, default = Some(8))
    verify()
  }

	def main(argv: Array[String]) {
		val conf = new SparkConf().setAppName("BuiltInCountTriangle")
		val sc = new SparkContext(conf)

		val args = new Conf(argv)
		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Partitions: " + args.partitions())


		val graph = GraphLoader.edgeListFile(sc, args.input(), false, args.partitions())
		val triangleCountGraph = graph.triangleCount()
		val triangleCount = triangleCountGraph.vertices.map(_._2).reduce(_ + _) / 3
		log.info(s"Total number of triangles: $triangleCount")

		val outputPath = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)
		sc.parallelize(Seq(s"$triangleCount"), 1).saveAsTextFile(args.output())
		sc.stop()
	}

}