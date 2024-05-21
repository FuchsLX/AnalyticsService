package org.ecommerce.analyticsservice.dtos;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class ProductStdDto {
    private String productId;
    private String productName;
    private double price;
    private double discount;
    private Long quantity;
}
