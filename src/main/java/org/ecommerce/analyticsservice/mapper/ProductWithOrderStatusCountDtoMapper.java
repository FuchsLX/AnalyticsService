package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductWithOrderStatusCountDto;

public final class ProductWithOrderStatusCountDtoMapper implements MapFunction<Row, ProductWithOrderStatusCountDto> {

    @Override
    public ProductWithOrderStatusCountDto call(Row row) throws Exception {
        return ProductWithOrderStatusCountDto.builder()
                .productId(row.getAs("product_id"))
                .productName(row.getAs("product_name"))
                .processingCount(row.getAs("processing_quantity"))
                .cancelledCount(row.getAs("cancelled_quantity"))
                .completedCount(row.getAs("completed_quantity"))
                .deliveredCount(row.getAs("delivered_quantity"))
                .build();
    }
}
