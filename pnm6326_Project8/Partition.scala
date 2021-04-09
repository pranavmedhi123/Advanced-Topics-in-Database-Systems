import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Partition {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)

    val my_edges: RDD[Edge[Long]] = sc
      .textFile(args(0))
      .map(line => {
        val (my_nodes, connected_nodes) = line.split(",").splitAt(1)
        var vertexid = my_nodes(0).toLong
        (vertexid, connected_nodes.toList.map(_.toLong))
      })
      .flatMap(x => x._2.map(y => (x._1, y)))
      .map(n => {
        Edge(n._1, n._2, (0).toLong)
      })

    
    var count = 0
    val InitialPartitions = sc
      .textFile(args(0))
      .map(line => {
        val (my_nodes, _) = line.split(",").splitAt(1)
        var vertices = (-1).toLong
        if (count < 5) {
          vertices = my_nodes(0).toLong
          count += 1
        }
        vertices 
      })

    val InitialPartitionsonly = InitialPartitions.filter(_ != -1).collect().toList

    val graph: Graph[Long, Long] = Graph
      .fromEdges(my_edges, (0).toLong)
      .mapVertices((id, _) => {
        var my_centroids = (-1).toLong
        if (InitialPartitionsonly.contains(id)) {
          my_centroids = id
        }
        my_centroids
      })

    val pregel_instance = graph.pregel(Long.MinValue, 6)(
      (vertexid, my_vertices, my_Cluster) => {
        if (my_vertices == -1) {
          math.max(my_vertices, my_Cluster)
        } else {
          my_vertices
        }
      },
      triplet => {
        Iterator((triplet.dstId, triplet.srcAttr))
      },
      (first_vertex, second_vertex) => math.max(first_vertex, second_vertex)
    )

    var partition_Lengths = pregel_instance.vertices
      .map {
        case (id, my_centroids) =>
          (my_centroids, 1)
      }
      .reduceByKey(_ + _)

    partition_Lengths.collect.foreach(println)
  }
}