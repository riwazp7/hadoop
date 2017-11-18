import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AdIdReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, String> impressionToReferrer = new HashMap<>();
        Map<String, Integer> referrerImpressionCount = new HashMap<>();
        Map<String, Integer> referrerClickCount = new HashMap<>();
        ArrayList<String> clicksList = new ArrayList<>();
	
	
        for (Text value : values) {
           String[] tokens = value.toString().split(BatchAdsProcessor.separator);
           if (tokens[1].equals(BatchAdsProcessor.clickMarker)) {
               clicksList.add(tokens[0]);
           } else {
               impressionToReferrer.put(tokens[0], tokens[1]);
               referrerImpressionCount.merge(tokens[1], 1, (a, b) -> a + b);
           }
        }

        for (String clickedImpression : clicksList) {
            referrerClickCount.merge(impressionToReferrer.get(clickedImpression), 1, (a, b) -> a + b);
        }

        for (Map.Entry<String, Integer> entry : referrerImpressionCount.entrySet()) {
            Integer clickCount = referrerClickCount.get(entry.getKey());
            String outputKey = String.format("[%s, %s]", entry.getKey() /* referrer */, key.toString() /* adId */);
            String output = (clickCount == null) ?
                    "0" : String.valueOf((double) clickCount / (double) entry.getValue() /* total impressions */);
            context.write(
                    new Text(outputKey),
                    new Text(output));
        }
    }
}
