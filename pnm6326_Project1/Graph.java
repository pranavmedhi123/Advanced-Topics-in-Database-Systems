import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {
	public static class MyMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = s.nextInt();
            int y = s.nextInt();
            context.write(new IntWritable(x),new IntWritable(y));
            s.close();
        }
    }
    public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int neighbour_count = 0;
            for (IntWritable v: values) {
                neighbour_count++;
            };
            context.write(key,new IntWritable(neighbour_count));
        }
    }
    public static class MyMapper2 extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int x= s.nextInt();
            int y=s.nextInt();
            context.write(new IntWritable(y),new IntWritable(1));
            s.close();
        }
    }
    public static class MyReducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            int number_of_nodes = 0;
            for (IntWritable v: values) {
                number_of_nodes+=v.get();
            };
            context.write(key,new IntWritable(number_of_nodes));
        }
    }




    public static void main ( String[] args ) throws Exception {
    	Job job = Job.getInstance();
    	
        job.setJobName("MyJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path("./intermediate_output"));
        job.waitForCompletion(true);

        Job job2=Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("./intermediate_output"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);

    }

     
}
    
// #I want number of keys that have number of neighbour_count==x, (sum of number of keys ,neighbour_count)
// neighbour count,sum of number of keys that have the neighbour_count as neighbour_count
// neighbour_count, sum of number of keys