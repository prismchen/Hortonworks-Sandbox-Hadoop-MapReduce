import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class mp1t1 {

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public String toString() {
            Writable[] values = this.get();
            return "," + values[0].toString() + "," + values[1].toString() + "," + values[2].toString();
        }
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntArrayWritable> {
        // private final static IntWritable one = new IntWritable(1);
        private Text campaignId = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            // StringTokenizer tokenizer = new StringTokenizer(line, ",");
            String[] tokens = line.split(",");
            IntWritable[] atts = new IntWritable[3];

            int impression = Integer.parseInt(tokens[2]);
            int click = Integer.parseInt(tokens[3]);
            int conversion = Integer.parseInt(tokens[4]);
            campaignId.set(tokens[5]);

            atts[0] = new IntWritable(impression);
            atts[1] = new IntWritable(click);
            atts[2] = new IntWritable(conversion);

            IntArrayWritable res = new IntArrayWritable();
            res.set(atts);
            output.collect(campaignId, res);
 
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
        public void reduce(Text key, Iterator<IntArrayWritable> values, OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
            int sumOfImpression = 0;
            int sumOfClick = 0;
            int sumOfConversion = 0;

            Writable[] tmp;
            IntWritable[] result = new IntWritable[3];
            IntArrayWritable res = new IntArrayWritable();

            while (values.hasNext()) {
                    tmp = values.next().get();
                    sumOfImpression += ((IntWritable) tmp[0]).get();
                    sumOfClick += ((IntWritable) tmp[1]).get();
                    sumOfConversion += ((IntWritable) tmp[2]).get();
            }

            result[0] = new IntWritable(sumOfImpression);
            result[1] = new IntWritable(sumOfClick);
            result[2] = new IntWritable(sumOfConversion);

            res.set(result);
            output.collect(key, res);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("mp1t1");
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJar("/root/mp1/mp1t1.jar");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntArrayWritable.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntArrayWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
