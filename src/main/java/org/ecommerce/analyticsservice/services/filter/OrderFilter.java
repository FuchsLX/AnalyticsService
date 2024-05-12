package org.ecommerce.analyticsservice.services.filter;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class OrderFilter {

    public static class CancelledOrderFilter implements FilterFunction<Row> {
        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("status").toString().equalsIgnoreCase("cancelled");
        }
    }

    public static class CompletedOrderFilter implements FilterFunction<Row> {
        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("status").toString().equalsIgnoreCase("completed");
        }
    }

    public static class ProcessingOrderFilter implements FilterFunction<Row> {
        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("status").toString().equalsIgnoreCase("processing");
        }
    }

    public static class DeliveredOrderFilter implements FilterFunction<Row> {
        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("status").toString().equalsIgnoreCase("delivered");
        }
    }
}
