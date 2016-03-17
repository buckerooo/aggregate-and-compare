package buckerooo.ag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataAggregator<T> {
    private final List<T> data;

    public DataAggregator(List<T> data) {
        this.data = data;
    }

    public <V> Map<String, V> aggregate(Aggregator<T, V> aggregator) {
        return aggregate(aggregator.key(), aggregator.value(), aggregator.initialValue());
    }

    public <V> Map<String, V> aggregate(AggregationKey<T> aggregationKey, AggregationValue<T, V> aggregationValue, V initialValue) {

        Map<String, V> aggregatedData = new HashMap<>();

        for (T row : data) {
            String key = aggregationKey.key(row);

            if(aggregatedData.containsKey(key)) {
                aggregatedData.computeIfPresent(key, (key1, oldValue) -> aggregationValue.key(row, oldValue));
            } else {
                aggregatedData.putIfAbsent(key, aggregationValue.key(row, initialValue));
            }
        }

        return aggregatedData;
    }

    public interface AggregationKey<T> {
        String key(T key);
    }

    public interface AggregationValue<T, V> {
        V key(T order, V key);
    }

}
