import java.io.IOException;

import java.util.*;

import java.sql.Timestamp;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class selfThetaJoin {

    public static class LongArrayWritable extends ArrayWritable {
        public LongArrayWritable() {
            super(LongWritable.class);
        }
        public String toString() {
            Writable[] values = this.get();
            return "," + values[0].toString() + "," + values[1].toString() + "," + values[2].toString();
        }
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongArrayWritable> {
        private Text timeStamp = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] tokens = line.split(",");
            LongWritable[] atts = new LongWritable[2];
            String click = tokens[3];

            if (click.equals("1") && tokens[1] != null && tokens[1].length > 0) {
                long userId = Long.valueOf(tokens[1]);
                atts[0] = new LongWritable(userId);
                
                Timestamp ts = Timestamp.valueOf(tokens[0]);
                long currTime;
                LongArrayWritable res = new LongArrayWritable();

                // Output key-value paires: <timeStamp + 1, [userid, 1 sec]>, 
                // <timeStamp, [userid, 0 sec]> and <timeStamp - 1, [userid, -1 sec]> 
                for (int timeShift = -1; timeShift <= 1; timeShift++) {
                    atts[1] = new LongWritable(timeShift);
                    // Shifting timestamp by +1/0/-1 sec
                    currTime = ts.getTime() + timeShift*1000;
                    timeStamp.set(new Timestamp(currTime).toString());
                    res.set(atts);
                    output.collect(timeStamp, res);
                }                
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
        private Text timeStamp = new Text();

        public void reduce(Text key, Iterator<LongArrayWritable> values, OutputCollector<Text, LongArrayWritable> output, Reporter reporter) throws IOException {

            LongArrayWritable tmp = new LongArrayWritable();
            LongArrayWritable res = new LongArrayWritable();
            LongWritable[] result = new LongWritable[2];
            List<LongArrayWritable> allValues = new ArrayList<>();
            long zero = 0L;

            // Store all Values in the form of [user_id, time_shift]
            while (values.hasNext()) {
                tmp = values.next();
                allValues.add(tmp);
            }

            // Traverse all possible pairs of user_id s
            for (int i=0; i<allValues.size(); i++) {
                Writable[] a = allValues.get(i).get();
                long aId = ((LongWritable) a[0]).get();
                long aShift = ((LongWritable) a[1]).get();

                for (int j=i+1; j<allValues.size(); j++) {
                    Writable[] b = allValues.get(j).get();
                    long bId = ((LongWritable) b[0]).get();

                    // Find a satisfying pair of user_id s, and output it
                    if (aId < bId && aShift == zero) {
                        result[0] = new LongWritable(aId);
                        result[1] = new LongWritable(bId);
                        res.set(result);
                        output.collect(key, res);
                    }
                }
            }    
        }
    }

    public static void main(String[] args) throws Exception {
        // Setting config
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("selfThetaJoin");
        
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJar("~/mapredJobs/selfThetaJoin.jar");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongArrayWritable.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongArrayWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
