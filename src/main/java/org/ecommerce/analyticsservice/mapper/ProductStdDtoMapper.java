package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductStdDto;

public final class ProductStdDtoMapper implements MapFunction<Row, ProductStdDto> {

    @Override
    public ProductStdDto call(Row row) throws Exception {
        return ProductStdDto.builder()
                .productId(row.getAs("product_id"))
                .productName(row.getAs("product_name"))
                .price(row.getAs("price"))
                .discount(row.getAs("discount"))
                .quantity(row.getAs("quantity"))
                .build();
    }
}
