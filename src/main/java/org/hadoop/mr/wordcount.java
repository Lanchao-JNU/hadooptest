package org.hadoop.mr;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class wordcount {
    public static class WordcountMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable>{
        private final static IntWritable one  = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                output.collect(word,one);
            }

        }
    }
    public static class WordcountReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()){
                sum+=values.next().get();
            }
            result.set(sum);
            output.collect(key,result);
        }
    }
}
