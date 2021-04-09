import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.lang.Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                                        // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
    public long centroid;                                  // the id of the centroid in which this vertex belongs to
    public short depth;                                    // the BFS depth
    /* ... */

    Vertex () {}
    
    public Vertex (long id_new, Vector<Long> adjacent1, long cent_new, short depth_new){
        adjacent = new Vector<>();
        this.id = id_new;
        this.adjacent = adjacent1;
        this.centroid = cent_new;
        this.depth = depth_new;
    }

    @Override
    public void write (DataOutput out) throws IOException {
        
        out.writeLong(id);
        out.writeLong(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
            out.writeLong(adjacent.get(i));
        
        out.writeLong(centroid);
        out.writeShort(depth);
    }

    @Override
    public void readFields (DataInput in) throws IOException {
        adjacent = new Vector<Long>(); 
        this.id = in.readLong();
        long vector_size = in.readLong();
        for(int i=0; i<vector_size; i++)
            this.adjacent.addElement(in.readLong());

        this.centroid = in.readLong();
        this.depth = in.readShort();

    }
    
}

public class GraphPartition {
    static Vector<Long> cents = new Vector<Long>();
    final static short maximum_depth = 8;
    static short BFS_depth = 0;
    static int cent_counter = 0;
    /* ... */
    public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex> {
        //Job1 mapper1
        @Override
        public void map (Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            
            
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long id = s.nextLong(); 
            Vector<Long> adjacent = new Vector<Long>();
            while (s.hasNext()){
                adjacent.add(s.nextLong());
            }
            long centroid = -1;
            if (cent_counter < 10){
                centroid = id;
                cent_counter++;
                cents.add(id);
                
                context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,BFS_depth));
            }else{
                
                short depth = 0;
                context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,BFS_depth));
            }
            
            s.close();
        }
    }
    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        //Job1 Mapper2
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            Vector <Long> i = new Vector <Long>(); 
            context.write(new LongWritable(value.id), value);
            if (value.centroid > 0){
                for (long n: value.adjacent){
                    context.write(new LongWritable(n), new Vertex(n,i, value.centroid, BFS_depth));
                }
            }
        }
    }
    public static class Reducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        //Job2 Reducer1
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            short minimum_depth = 1000;
            long centroid = -1;
            short depth = 0;
            Vertex new_key = new Vertex (key.get(),  new Vector <Long>(),centroid, depth);
            for (Vertex v: values){
                if(!v.adjacent.isEmpty()){
                    new_key.adjacent = v.adjacent;
                }
                if (v.centroid > 0 && v.depth < minimum_depth){
                    minimum_depth = v.depth;
                    new_key.centroid = v.centroid;
                }
            }
            new_key.depth = minimum_depth;
            context.write(key,new_key);
        }
    }
    public static class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{
        //Job3 Mapper3
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                    throws IOException, InterruptedException{
            
            long i = 1;
            if (value.centroid >= 0)
                context.write(new LongWritable (value.centroid), new LongWritable(i));
            
        }
    }

    public static class Reducer2 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        // Job3 Reducer1
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long new_key = 0;
            for (LongWritable v: values) {
                new_key = new_key + v.get();
            }   
            context.write(key,new LongWritable (new_key));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* ... First Map-Reduce job to read the graph */
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(Mapper1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        
        job.waitForCompletion(true);

        
        for ( short i = 0; i < maximum_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJarByClass(GraphPartition.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(args[1]  +"/i"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]  +"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[1] + "/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
