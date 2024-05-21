package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductWithBuyerDto;

public final class ProductWithBuyerDtoMapper implements MapFunction<Row, ProductWithBuyerDto> {

    @Override
    public ProductWithBuyerDto call(Row row) throws Exception {
        String productName = row.getAs("product_name");
        if (productName.length() > 20) productName = productName.substring(0, 20) + "...";
        return ProductWithBuyerDto.builder()
                .productId(row.getAs("product_id"))
                .productName(productName)
                .uniqueBuyersCount(row.getAs("unique_buyers_count"))
                .repeatBuyersCount(row.getAs("repeat_buyers_count"))
                .reorderRate(row.getAs("reorder_rate"))
                .build();
    }
}
