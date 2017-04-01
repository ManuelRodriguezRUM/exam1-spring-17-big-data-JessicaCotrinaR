package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jessica on 03-31-17.
 */
public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {
    //private IntWritable result = new IntWritable();

    @Override
   protected void reduce(Text key, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {


        String result = "";
        for (Text value : values){

            result = result.concat(value.toString() + "" );

        }
       context.write(key, new Text(result));

   }
}
