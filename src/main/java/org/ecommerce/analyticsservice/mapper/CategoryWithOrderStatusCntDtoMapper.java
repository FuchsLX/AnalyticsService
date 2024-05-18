package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryWithOrderStatusCntDto;

public final class CategoryWithOrderStatusCntDtoMapper implements MapFunction<Row, CategoryWithOrderStatusCntDto> {

    @Override
    public CategoryWithOrderStatusCntDto call(Row row) throws Exception {
        return CategoryWithOrderStatusCntDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
                .processingCount(row.getAs("processing_quantity"))
                .cancelledCount(row.getAs("cancelled_quantity"))
                .completedCount(row.getAs("completed_quantity"))
                .deliveredCount(row.getAs("delivered_quantity"))
                .build();
    }
}
