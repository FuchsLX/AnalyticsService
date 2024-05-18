package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryWithOrderStatusRevDto;

public final class CategoryWithOrderStatusRevDtoMapper implements MapFunction<Row, CategoryWithOrderStatusRevDto> {
    @Override
    public CategoryWithOrderStatusRevDto call(Row row) throws Exception {
        return CategoryWithOrderStatusRevDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
                .processingTotalPrice(row.getAs("processing_total_price"))
                .cancelledTotalPrice(row.getAs("cancelled_total_price"))
                .deliveredTotalPrice(row.getAs("delivered_total_price"))
                .completedTotalPrice(row.getAs("completed_total_price"))
                .build();
    }
}
