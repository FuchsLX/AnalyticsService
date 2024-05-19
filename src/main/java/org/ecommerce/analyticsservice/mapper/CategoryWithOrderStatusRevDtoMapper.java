package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryWithOrderStatusRevDto;

import java.math.BigDecimal;

public final class CategoryWithOrderStatusRevDtoMapper implements MapFunction<Row, CategoryWithOrderStatusRevDto> {
    @Override
    public CategoryWithOrderStatusRevDto call(Row row) throws Exception {
        return CategoryWithOrderStatusRevDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
                .processingTotalPrice(((BigDecimal) row.getAs("processing_total_price")).doubleValue())
                .cancelledTotalPrice(((BigDecimal) row.getAs("cancelled_total_price")).doubleValue())
                .deliveredTotalPrice(((BigDecimal) row.getAs("delivered_total_price")).doubleValue())
                .completedTotalPrice(((BigDecimal) row.getAs("completed_total_price")).doubleValue())
                .build();
    }
}
