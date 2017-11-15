/**
 * BatchAdsProcessor.java
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BatchAdsProcessor {

    static final String separator = " ";
    static final String clickMarker = "@-@CLCK@-@"; /////////

    public static void main (String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Args: [impression log path] [clicks log path] [output path]");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ClickRateProcessor");
        job.setJarByClass(BatchAdsProcessor.class);
        job.setReducerClass(AdIdReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mappers.ClicksMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mappers.ImpressionsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
