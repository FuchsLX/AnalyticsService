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
    private BigDecimal price;
    private BigDecimal discount;
    private Long quantity;
}
