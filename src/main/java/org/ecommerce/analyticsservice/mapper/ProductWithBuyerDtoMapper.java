package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductWithBuyerDto;

public final class ProductWithBuyerDtoMapper implements MapFunction<Row, ProductWithBuyerDto> {

    @Override
    public ProductWithBuyerDto call(Row row) throws Exception {
        return ProductWithBuyerDto.builder()
                .productId(row.getAs("product_id"))
                .productName(row.getAs("product_name"))
                .uniqueBuyersCount(row.getAs("unique_buyers_count"))
                .repeatBuyersCount(row.getAs("repeat_buyers_count"))
                .reorderRate(row.getAs("reorder_rate"))
                .build();
    }
}
