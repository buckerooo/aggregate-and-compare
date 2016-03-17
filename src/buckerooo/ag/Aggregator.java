package buckerooo.ag;

import buckerooo.ag.DataAggregator.AggregationKey;
import buckerooo.ag.DataAggregator.AggregationValue;

public interface Aggregator<T, V> {
    AggregationKey<T> key();

    AggregationValue<T, V> value();

    V initialValue();
}
