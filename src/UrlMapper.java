import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UrlMapper extends Mapper<Text, Text, Text, Text> {

    private static String referrer = "referrer";
    private static String adId = "adId";

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String jsonString = value.toString();
        JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
        context.write(new Text(jsonObject.get(referrer).getAsString()), new Text(jsonObject.get(adId).getAsString()));
    }
}