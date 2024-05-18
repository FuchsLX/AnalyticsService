package org.ecommerce.analyticsservice.mapper;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.OrderStatusRevenueDto;

public final class OrderStatusRevenueDtoMapper implements MapFunction<Row, OrderStatusRevenueDto> {

    @Override
    public OrderStatusRevenueDto call(Row row) throws Exception {
        return OrderStatusRevenueDto.builder()
                .orderStatus(row.getAs("order_status"))
                .revenue(row.getAs("total_price_after_discount"))
                .totalPriceBeforeDiscount(row.getAs("total_price_before_discount"))
                .build();
    }
}
