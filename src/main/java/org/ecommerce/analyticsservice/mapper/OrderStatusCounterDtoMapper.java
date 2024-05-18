package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.OrderStatusCounterDto;

public final class OrderStatusCounterDtoMapper implements MapFunction<Row, OrderStatusCounterDto> {

    @Override
    public OrderStatusCounterDto call(Row row) throws Exception {
        return OrderStatusCounterDto.builder()
                .status(row.getAs("order_status"))
                .order_quantity(row.getAs("quantity"))
                .build();
    }
}
