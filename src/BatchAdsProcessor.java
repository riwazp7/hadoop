/**
 * Created by Riwaz on 11/13/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BatchAdsProcessor {

    public static void main (String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "random");
        job.setJarByClass(BatchAdsProcessor.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(UrlReducer.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UrlMapper.class);
        FileInputFormat.addInputPath(job, new Path(Path.CUR_DIR));
        FileOutputFormat.setOutputPath(job, new Path(Path.CUR_DIR));
    }
}
