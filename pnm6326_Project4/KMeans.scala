import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()
  def distance (c:Point , p :Point): Double={
    
    // Calculating Euclidean distance [(x1 - x2)square + (y1 - y2)square]square root 
    var distance=Math.sqrt(Math.pow((c._1 - p._1),2) + Math.pow((c._2 - p._2),2));
    
    distance
  }

  def main(args: Array[ String ]): Unit ={
    val conf=new SparkConf().setAppName("KMeans")
    val sc=new SparkContext(conf)

    /* ... */

    centroids = sc.textFile(args(1)).collect.map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)})
    var points= sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})
    /* read initial centroids from centroids.txt */

    for ( i <- 1 to 5 ){
      val cents=sc.broadcast(centroids)
        centroids=points.map { p => (cents.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(c,point_values)=>
        var counter=0
        var sum_X_values=0.0
        var sum_Y_values=0.0
        for (points<- point_values){
          counter+=1
          sum_X_values+=points._1
          sum_Y_values+=points._2
        }
        (sum_X_values/counter,sum_Y_values/counter)
      }.collect
    
    }
    centroids.foreach(println)
  }
}
