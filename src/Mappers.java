import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class Mappers {

    private static final String referrer = "referrer";
    private static final String adId = "adId";
    private static final String impressionId = "impressionId";

    public static class ImpressionsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject jsonObject = new JsonParser().parse(value.toString()).getAsJsonObject();
            String emitValue = String.format(
                    "%s%s%s",
                    jsonObject.get(impressionId).getAsString(),
                    BatchAdsProcessor.separator,
                    jsonObject.get(referrer).getAsString());
            context.write(new Text(jsonObject.get(adId).getAsString()), new Text(emitValue));
        }
    }

    public static class ClicksMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject jsonObject = new JsonParser().parse(value.toString()).getAsJsonObject();
            String emitValue = String.format(
                    "%s%s%s",
                    jsonObject.get(impressionId).getAsString(),
                    BatchAdsProcessor.separator,
                    BatchAdsProcessor.clickMarker);
            context.write(new Text(jsonObject.get(adId).getAsString()), new Text(emitValue));
        }
    }
}
