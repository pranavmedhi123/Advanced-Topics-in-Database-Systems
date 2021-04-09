// import java.io.IOException;
// import java.util.Scanner;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.mapreduce.lib.input.*;
// import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import java.lang.Integer;
import java.io.*;

class Elem implements Writable{
    public short tag;
    public int index;
    public double value;
    Elem () {}
    Elem(short a,int b,double v){
        this.tag=a;
        this.index=b;
        this.value=v;
    }
    public void write(DataOutput out) throws IOException{
        out.writeShort(this.tag);
        out.writeInt(this.index);
        out.writeDouble(this.value);
    }
    public void readFields (DataInput in) throws IOException{
        this.tag=in.readShort();
        this.index=in.readInt();
        this.value=in.readDouble();
    }
    
    public String toString(){
        String str1 = Short.toString(this.tag); 
        String str2 = Integer.toString(this.index);
        String str3 = Double.toString(this.value);
        return str1+"\t "+str2+"\t "+str3;}
     }



class Pair implements WritableComparable<Pair>{
    public int i;
    public int j;
    Pair () {}
    Pair(int k,int l){
        this.i=k;
        this.j=l;
    }
    public void write(DataOutput out) throws IOException{
        out.writeInt(this.i);
        out.writeInt(this.j);
    }
    public void readFields(DataInput in) throws IOException{
        this.i=in.readInt();
        this.j=in.readInt();
    }
    public String toString(){
        String str1=Integer.toString(this.i);
        String str2=Integer.toString(this.j);
        return str1+"\t"+str2;}
    
    public int compareTo(Pair o) {
        int thisValue = this.i;
        int thatValue = o.i;
        int difference = thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1);
         if  (difference == 0){
             thisValue=this.j;
             thatValue=o.j;
             return thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1);
         }
         else{
         return thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1);}
      }


}

public class Multiply {
    public static class MyMapper1 extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short k=0;
            int i = s.nextInt();
            int j = s.nextInt();
            Double v = s.nextDouble();
            context.write(new IntWritable(j),(new Elem(k,i,v)));
            s.close();
        }
    }
    public static class MyMapper2 extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short k=1;
            int i = s.nextInt();
            int j = s.nextInt();
            Double v= s.nextDouble();
            context.write(new IntWritable(i),new Elem(k,j,v));
            s.close();
        }
    }
    public static class MyReducer1 extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        static Vector<Elem> elems=new Vector<Elem>();
        static Vector<Elem> pairs= new Vector<Elem>();
        
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            elems.clear();
            pairs.clear();
            for (Elem v: values)
                if (v.tag==0)
                    elems.add(new Elem(v.tag,v.index,v.value));
                else pairs.add(new Elem(v.tag,v.index,v.value));
            System.out.println("Vector A:"+elems);
            System.out.println("Vector B:"+pairs);
            for ( Elem e: elems )
                for ( Elem d: pairs )
                    context.write(new Pair(e.index,d.index),new DoubleWritable(e.value*d.value));

            
        }
    }
    public static class MyMapper3 extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    public static class MyReducer2 extends Reducer<Pair,DoubleWritable,Text,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double number_of_nodes = 0;
            for (DoubleWritable v: values) {
                number_of_nodes+=v.get();
            };
            context.write(new Text (key.toString()),new DoubleWritable(number_of_nodes));
        }
    }
    

    public static void main ( String[] args ) throws Exception {
        Job job=Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        job.setReducerClass(MyReducer1.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MyMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MyMapper2.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

        Job job2=Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MyMapper3.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
