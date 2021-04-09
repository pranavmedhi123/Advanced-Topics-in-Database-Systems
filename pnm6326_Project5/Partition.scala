import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.math

object Partition {
  val depth = 6

	def my_nodes( node: (Long, Long, List[Long] ) ) = {

	  var buffer_cluster = new ListBuffer[ ( Long, Long) ]
	  var temporary_cluster = ( node._1, node._2 )
	  buffer_cluster += temporary_cluster
	  if ( node._2 > 0 ) {
	    for ( line <- node._3 ) {
	      temporary_cluster = ( line, node._2 )
	      buffer_cluster += temporary_cluster
	    }
	  }
	  buffer_cluster

	}



	def my_join( node: ( Long, Long, List[Long] ) ): ( Long, ( Long, List[ Long ] ) ) = {

	  var my_join_operation = ( node._1, ( node._2, node._3 ) )
	  my_join_operation
	
	}



	def my_cents(id_number: ( Long, ( Long, ( Long, List[ Long ] ) ) ) ) : ( Long, Long, List[ Long ] ) = {

	  var centroids= ( id_number._1,  id_number._2._1,  id_number._2._2._2 )
	  if ( id_number._1 != -1 ) {
	  	if( id_number._2._2._1 != -1 ){

	    centroids= ( id_number._1,  id_number._2._2._1,  id_number._2._2._2 )
		}
	  
	  }

	  if ( id_number._1 != -1  ) {
	  	if( id_number._2._2._1 == -1 ){



	    centroids= ( id_number._1,  id_number._2._1,  id_number._2._2._2 )
		}
	  
	  }
	  centroids
	
	}




  def main( args: Array[ String ] ): Unit = {

    val conf = new SparkConf().setAppName( "Partition" )
    val sc = new SparkContext( conf )
    var counter = 0
    var graph = sc.textFile( args( 0 ) ).map( line => line.split( "," ) ).map ( line => line.map( _.toLong ) ) .map( line => if ( counter < 5 ) {

    	counter += 1
    	( line( 0 ),  line( 0 ),  line.drop( 1 ).toList )
    	
    	} else {

    		counter += 1
    		( line( 0 ), -1.toLong, line.drop( 1 ).toList )
    	} )
    for (i <- 1 to depth) {

      graph = graph.flatMap{my_nodes( _ )}.reduceByKey( math.max( _ ,  _ ) ).join( graph.map( line => my_join( line ) ) ) .map( line => my_cents( line ) )

    }
    graph.map( line => ( line._2,  1) ).reduceByKey( _  +  _ ).collect( ).foreach ( println )

  }
}
