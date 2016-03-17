package buckerooo.ag;

public class CountAggregator<T> implements Aggregator<T, Integer> {

    private final DataAggregator.AggregationKey<T> aggregationKey;

    public CountAggregator(DataAggregator.AggregationKey<T> aggregationKey) {
        this.aggregationKey = aggregationKey;
    }

    @Override
    public DataAggregator.AggregationKey<T> key() {
        return aggregationKey;
    }

    @Override
    public DataAggregator.AggregationValue<T, Integer> value() {
        return (item, count) -> count + 1;
    }

    @Override
    public Integer initialValue() {
        return 0;
    }
}
