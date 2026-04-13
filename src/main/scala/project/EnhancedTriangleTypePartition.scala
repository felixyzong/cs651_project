package project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.rogach.scallop._

object EnhancedTriangleTypePartition {
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
	case class Key2(a: Int, b: Int) extends Keys
	case class Key3(a: Int, b: Int, c: Int) extends Keys

	def mapper(edge : (Int, Int), rho: Int): Seq[(Keys ,(Int, Int))] = {
		val (src, dst) = edge
		val srcPartition = P(src, rho)
		val dstPartition = P(dst, rho)

		val out = scala.collection.mutable.ArrayBuffer.empty[(Keys, (Int, Int))]
		for {
			a <- 0 until rho-1
			b <- a+1 until rho
		} {
			if ((srcPartition == a || srcPartition == b) && (dstPartition == a || dstPartition == b)) {
				out += ((Key2(a, b), (src, dst)))
			}
		}

		if (srcPartition != dstPartition) {
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

	def triangleCounterReducer(key: Keys, edgeList: Iterable[(Int, Int)], rho: Int): Int = {
		val adj = scala.collection.mutable.Map.empty[Int, scala.collection.mutable.Set[Int]]
		edgeList foreach { case (src, dst) =>
			if (!adj.contains(src)) adj(src) = scala.collection.mutable.Set.empty[Int]
			if (!adj.contains(dst)) adj(dst) = scala.collection.mutable.Set.empty[Int]
			adj(src) += dst
		}
        val a = key match {
            case Key2(a, b) => a
            case Key3(a, b, c) => a
        }

        val b = key match {
            case Key2(a, b) => b
            case Key3(a, b, c) => b
        }

		var count = 0
		for ((u, neighbors) <- adj) {
			val neighborArray = neighbors.toArray.sorted
			for (i <- neighborArray.indices; j <- i + 1 until neighborArray.length) {
				val v = neighborArray(i)
				val w = neighborArray(j)
				if (adj(v).contains(w)) {
					if (P(u, rho) == P(v, rho) && P(v, rho) == P(w, rho)) {
						if ((P(u, rho) == a || P(u, rho) == rho-1) && b==a+1) {
                            count += 1
                        }
					} else {
						count += 1
					}
				}
			}
		}
		count			
		
	}
		
	

	def main(argv: Array[String]) {
		val conf = new SparkConf().setAppName("TriangleTypePartition")
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
		}.filter { case (src, dst) => src != dst } // Filter self-edges

		val mapped = edges.flatMap(edge => mapper(edge, rho))

		// Reduce Stage
		val triangleCount = mapped.groupByKey(args.partitions()).map { case (key, edgeList) =>
			triangleCounterReducer(key, edgeList, rho)
		}.reduce(_ + _)
		log.info(s"Total number of triangles: $triangleCount")

		val outputPath = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)
		sc.parallelize(Seq(s"$triangleCount"), 1).saveAsTextFile(args.output())
		sc.stop()
	}

}
