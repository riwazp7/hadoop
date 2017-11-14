/**
 * Created by Riwaz on 11/13/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BatchAdsProcessor {

    public static class UrlMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class UrlReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "random");
        job.setJarByClass(BatchAdsProcessor.class);
        job.setMapperClass(BatchAdsProcessor.UrlMapper.class);
        job.setReducerClass(BatchAdsProcessor.UrlReducer.class);
        FileInputFormat.addInputPath(job, new Path(Path.CUR_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Path.CUR_DIR));
    }

}
