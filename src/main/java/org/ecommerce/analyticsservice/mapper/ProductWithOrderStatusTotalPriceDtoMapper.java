package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductWithOrderStatusRevDto;

public final class ProductWithOrderStatusTotalPriceDtoMapper implements MapFunction<Row, ProductWithOrderStatusRevDto> {
    @Override
    public ProductWithOrderStatusRevDto call(Row row) throws Exception {
        return ProductWithOrderStatusRevDto.builder()
                .productId(row.getAs("product_id"))
                .productName(row.getAs("product_name"))
                .processingTotalPrice(row.getAs("processing_total_price"))
                .cancelledTotalPrice(row.getAs("cancelled_total_price"))
                .deliveredTotalPrice(row.getAs("delivered_total_price"))
                .completedTotalPrice(row.getAs("completed_total_price"))
                .build();
    }
}
