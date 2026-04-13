package project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.rogach.scallop._

object OneThreePartition {
  val log = Logger.getLogger(getClass().getName())
  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, rho)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val rho = opt[Int](descr = "rho value for partitioning", required = true)
    val partitions = opt[Int](descr = "number of partitions", required = false, default = Some(8))
    verify()
  }

	def P(x : Int, rho : Int) : Int = {
		// Partition function for triangle type partitioning
		x % rho
	}

	sealed trait Keys extends Serializable
	case class Key1(a: Int) extends Keys
	case class Key3(a: Int, b: Int, c: Int) extends Keys

	def mapper(edge : (Int, Int), rho: Int): Seq[(Keys ,(Int, Int))] = {
		val (src, dst) = edge
		val srcPartition = P(src, rho)
		val dstPartition = P(dst, rho)

		val out = scala.collection.mutable.ArrayBuffer.empty[(Keys, (Int, Int))]
		out += ((Key1(srcPartition), (src, dst)))

		if (srcPartition != dstPartition) {
			out += ((Key1(dstPartition), (src, dst)))
			for {
				a <- 0 until rho-2
				b <- a+1 until rho-1
				c <- b+1 until rho
			} {
				if ((srcPartition == a || srcPartition == b || srcPartition == c) &&
					(dstPartition == a || dstPartition == b || dstPartition == c)) {
					out += ((Key3(a, b, c), (src, dst)))
				}
			}
		}
		out.toSeq
	}

	def triangleCounterReducer(edgeList: Iterable[(Int, Int)], rho: Int): Int= {
		val adj = scala.collection.mutable.Map.empty[Int, scala.collection.mutable.Set[Int]]
		edgeList foreach { case (src, dst) =>
			if (!adj.contains(src)) adj(src) = scala.collection.mutable.Set.empty[Int]
			if (!adj.contains(dst)) adj(dst) = scala.collection.mutable.Set.empty[Int]
			adj(src) += dst
		}

		var count = 0
		for ((u, neighbors) <- adj) {
			val neighborArray = neighbors.toArray.sorted
			for (i <- neighborArray.indices; j <- i + 1 until neighborArray.length) {
				val v = neighborArray(i)
				val w = neighborArray(j)
				if (adj(v).contains(w)) {
						count += 1	
				}
			}
		}
		count			
		
	}


	def main(argv: Array[String]) {
		val conf = new SparkConf().setAppName("OneThreePartition")
		val sc = new SparkContext(conf)

		val args = new Conf(argv)
		val rho = args.rho()
		val partitions = args.partitions()
		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Rho: " + rho)
		log.info("Partitions: " + args.partitions())


		// Map Stage
		val edges = sc.textFile(args.input(), args.partitions()).map { line =>
			val parts = line.split("\\s+")
			val src = parts(0).toInt
			val dst = parts(1).toInt
			if (src < dst) (src, dst) else (dst, src) // Ensure src < dst for consistency
		}

		val mapped = edges.flatMap(edge => mapper(edge, rho))

		// Reduce Stage
		val triangleCount = mapped.groupByKey(args.partitions()).map { case (key, edgeList) =>
			triangleCounterReducer(edgeList, rho)
		}.reduce(_ + _)
		log.info(s"Total number of triangles: $triangleCount")

		val outputPath = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)
		sc.parallelize(Seq(s"T$triangleCount"), 1).saveAsTextFile(args.output())
		sc.stop()
	}

}
