package buckerooo.ag;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static buckerooo.ag.DataAggregatorTest.Order.order;
import static java.time.Duration.between;
import static java.time.LocalDateTime.now;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class DataAggregatorTest {

    @Test
    public void canAggregateOnASingleFieldReturningCountOfEachKey () {
        List<Order> orders = Arrays.asList(
                order("order-123", "F6678", "Thin red line", "Film", "London", now(), now()),
                order("order-456", "B9909", "The girl on the train", "Book", "London", now(), now()),
                order("order-789", "F6678", "Thin red line", "Film", "London", now(), now())
        );

        Map aggregatedOrders = new DataAggregator<>(orders)
                .aggregate(key -> key.itemCode,
                        (order, count) -> count + 1,
                        0);

        assertThat(aggregatedOrders.size(), equalTo(2));
        assertThat(aggregatedOrders.get("B9909"), equalTo(1));
        assertThat(aggregatedOrders.get("F6678"), equalTo(2));
    }

    @Test
    public void canAggregateToGetAListOfAssociatedItems () {
        Order order1 = order("order-123", "F6678", "Thin red line", "Film", "London", now(), now());
        Order order2 = order("order-456", "B9909", "The girl on the train", "Book", "London", now(), now());
        Order order3 = order("order-789", "F6678", "Thin red line", "Film", "London", now(), now());

        List<Order> orders = Arrays.asList(order1, order2, order3);

        Map<String, List<Order>> aggregatedOrders = new DataAggregator<>(orders)
                .aggregate(key -> key.itemCode,
                        (order, ordersForKey) -> {ordersForKey.add(order); return ordersForKey;},
                        new ArrayList<>());

        assertThat(aggregatedOrders.size(), equalTo(2));
        assertThat(aggregatedOrders.get("B9909"), hasItems(order2));
        assertThat(aggregatedOrders.get("F6678"), hasItems(order1, order3));
    }

    @Test
    public void canAggregateOnASingleFieldReturingACalculateField () {
        LocalDateTime now = now();

        List<Order> orders = Arrays.asList(
                order("order-123", "F6678", "Thin red line", "Film", "London", now, now.plusMinutes(20)),
                order("order-456", "B9909", "The girl on the train", "Book", "London", now, now.plusMinutes(10)),
                order("order-789", "F6678", "Thin red line", "Film", "London", now, now.plusHours(1).plusMinutes(30))
        );

        Map aggregatedOrders = new DataAggregator<>(orders)
                .aggregate(
                        key -> key.itemCode,
                        (order, currentDuration) -> between(order.orderCreationTime, order.orderDispatchTime).plus(currentDuration),
                        Duration.ZERO
                );

        assertThat(aggregatedOrders.size(), equalTo(2));
        assertThat(aggregatedOrders.get("B9909"), equalTo(Duration.ofMinutes(10)));
        assertThat(aggregatedOrders.get("F6678"), equalTo(Duration.ofMinutes(110)));
    }

    @Test
    public void canAggregateOnMultipleKeys () {
        List<Order> orders = Arrays.asList(
                order("order-1", "F6678", "Thin red line", "Film", "London", now(), now()),
                order("order-2", "B9909", "The girl on the train", "Book", "London", now(), now()),
                order("order-3", "B9909", "The girl on the train", "Film", "Leeds", now(), now()),
                order("order-4", "F6678", "Thin red line", "Film", "London", now(), now()),
                order("order-5", "B9909", "The girl on the train", "Film", "London", now(), now())
        );

        /* aggregate all order items to where they were dispatched */
        Map aggregatedOrders = new DataAggregator<>(orders)
                .aggregate(key -> key.itemName + "|" + key.dispatchLocation,
                        (order, count) -> count + 1,
                        0);

        assertThat(aggregatedOrders.size(), equalTo(3));
        assertThat(aggregatedOrders.get("Thin red line|London"), equalTo(2));
        assertThat(aggregatedOrders.get("The girl on the train|London"), equalTo(2));
        assertThat(aggregatedOrders.get("The girl on the train|Leeds"), equalTo(1));
    }

    static class Order {
        public final String id;
        public final String itemCode;
        public final String itemName;
        public final String itemType;
        public final String dispatchLocation;
        public final LocalDateTime orderCreationTime;
        public final LocalDateTime orderDispatchTime;

        public Order(String id, String itemCode, String itemName, String itemType, String dispatchLocation,
                     LocalDateTime orderCreationTime, LocalDateTime orderDispatchTime) {
            this.id = id;
            this.itemCode = itemCode;
            this.itemName = itemName;
            this.itemType = itemType;
            this.dispatchLocation = dispatchLocation;
            this.orderCreationTime = orderCreationTime;
            this.orderDispatchTime = orderDispatchTime;
        }

        public static Order order(String id, String itemCode, String itemName, String itemType, String dispatchLocation,
                                  LocalDateTime orderCreationTime, LocalDateTime orderDispatchTime) {
            return new Order(id, itemCode, itemName, itemType, dispatchLocation, orderCreationTime, orderDispatchTime);
        }
    }
}
