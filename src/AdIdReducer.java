import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AdIdReducer extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> impressionToReferrer;
    private Map<String, Integer> referrerImpressionCount;
    private Map<String, Integer> referrerClickCount;
    private ArrayList<String> clicksList;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        impressionToReferrer = new HashMap<>();
        referrerImpressionCount = new HashMap<>();
        referrerClickCount = new HashMap<>();
        clicksList = new ArrayList<>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
            String referrer = impressionToReferrer.get(clickedImpression);
            referrerClickCount.merge(referrer, 1, (a, b) -> a + b);
        }

        String adId = key.toString();
        for (Map.Entry<String, Integer> entry : referrerImpressionCount.entrySet()) {
            String referrer = entry.getKey();
            Integer totalImpressions = entry.getValue();
            Integer clickCount = referrerClickCount.get(referrer);
            String outputKey = String.format("%s, %s", adId, referrer);
            context.write(
                    new Text(outputKey),
                    new Text((String.valueOf((double) clickCount/ (double) totalImpressions))));
        }
        System.out.println("1 Reduce Finis");
    }
}