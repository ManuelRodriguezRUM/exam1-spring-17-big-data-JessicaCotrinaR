package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by jessica on 03-31-17.
 */
public class KeywordToTweetsMapper  extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String rawTweet = value.toString();

        try {
            Status tweetStatus = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = tweetStatus.getText().toUpperCase();
            long id = tweetStatus.getId();
            String ids = Long.toString(id);

            if (tweet.contains("TRUMP")){
                context.write(new Text("TRUMP"), new Text(ids));
            }
         if (tweet.contains("DICTATOR")){
               context.write(new Text("DICTATOR"), new Text(ids));

           }
           if (tweet.contains("IMPEACH")){
               context.write(new Text("IMPEACH"), new Text(ids));
            }

           if (tweet.contains("DRAIN")){
               context.write(new Text("DRAIN"), new Text(ids));
           }
           if (tweet.contains("SWAMP")){
                context.write(new Text("SWAMP"), new Text(ids));
            }
           if (tweet.contains("CHANGE")){
               context.write(new Text("CHANGE"), new Text(ids));
           }

        }
        // Convert to Status
        catch(TwitterException e){
            // ignore bad tweets
            Logger logger = LogManager.getRootLogger();
            logger.trace("Bad Tweet: " + rawTweet);
        }
    }
}
