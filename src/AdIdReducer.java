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
               Integer referrerCount = referrerImpressionCount.get(tokens[1]);
	       if (referrerCount == null) referrerImpressionCount.put(tokens[1], 1);
	       else referrerImpressionCount.put(tokens[1], referrerCount + 1);
           }
        }

        for (String clickedImpression : clicksList) {
            String referrer = impressionToReferrer.get(clickedImpression);
            Integer clickCount = referrerClickCount.get(referrer);
	    if (clickCount == null) referrerClickCount.put(referrer, 1);
	    else referrerClickCount.put(referrer, clickCount + 1);
        }

        String adId = key.toString();
        for (Map.Entry<String, Integer> entry : referrerImpressionCount.entrySet()) {
            String referrer = entry.getKey();
            Integer totalImpressions = entry.getValue();
            Integer clickCount = referrerClickCount.get(referrer);
            String outputKey = String.format("[%s, %s]", referrer, adId);
            String output = (clickCount == null) ?
                    "0" : String.valueOf((double) clickCount / (double) totalImpressions);
            context.write(
                    new Text(outputKey),
                    new Text(output));
        }
    }
}
