import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.sql.Timestamp;

public class mp1t2 {

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


                for (int timeShift = -1; timeShift <= 1; timeShift++) {
                    atts[1] = new LongWritable(timeShift);
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


            Writable[] a;
            Writable[] b;
            long aId;
            long aShift;
            long bId;
            long bShift;
            LongArrayWritable tmp = new LongArrayWritable();
            LongArrayWritable res = new LongArrayWritable();
            LongWritable[] result = new LongWritable[2];
            List<LongArrayWritable> allValues = new ArrayList<>();

            while (values.hasNext()) {
                tmp = values.next();
                if (!allValues.isEmpty()) {
                    a = tmp.get();
                    aId = ((LongWritable) a[0]).get();
                    aShift = ((LongWritable) a[1]).get();
                    for (LongArrayWritable iaw:allValues) {
                        b = iaw.get();
                        bId = ((LongWritable) b[0]).get();
                        bShift = ((LongWritable) b[1]).get();
                        if (aId < bId && Math.abs(aShift - bShift) < 2) {
                            Timestamp ts = Timestamp.valueOf(key.toString());
                            long realTime = ts.getTime() + aShift*1000;
                            timeStamp.set(new Timestamp(realTime).toString());
                            result[0] = new LongWritable(aId);
                            result[1] = new LongWritable(bId);
                            res.set(result);
                            output.collect(timeStamp, res);
                        }
                        else if (aId > bId && Math.abs(aShift - bShift) < 2) {
                            Timestamp ts = Timestamp.valueOf(key.toString());
                            long realTime = ts.getTime() + bShift*1000;
                            timeStamp.set(new Timestamp(realTime).toString());
                            result[0] = new LongWritable(bId);
                            result[1] = new LongWritable(aId);
                            res.set(result);
                            output.collect(timeStamp, res);
                        }
                    }
                }
                allValues.add(tmp);
            }  
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("mp1t2");
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJar("/root/mp1t2/mp1t2.jar");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongArrayWritable.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(LongArrayWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
