package com.zgc.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author codingXcong
 * @date 2019-06-22
 */
public class WordcountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    Text outKey = new Text();
    IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words =  line.split(" ");
        for (String word : words) {
            outKey.set(word);
            context.write(outKey,outValue);
        }
    }
}
