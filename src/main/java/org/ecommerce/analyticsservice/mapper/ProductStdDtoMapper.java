package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductStdDto;

import java.math.BigDecimal;

public final class ProductStdDtoMapper implements MapFunction<Row, ProductStdDto> {

    @Override
    public ProductStdDto call(Row row) throws Exception {
        String productName = row.getAs("product_name");
        if (productName.length() > 20) productName = productName.substring(0, 20) + "...";
        return ProductStdDto.builder()
                .productId(row.getAs("product_id"))
                .productName(productName)
                .price(((BigDecimal) row.getAs("price")).doubleValue())
                .discount(((BigDecimal) row.getAs("discount")).doubleValue())
                .quantity(row.getAs("quantity"))
                .build();
    }
}
