package org.ecommerce.analyticsservice.dtos;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class ProductWithOrderStatusRevDto {
    private String productId;
    private String productName;
    private double completedTotalPrice;
    private double cancelledTotalPrice;
    private double processingTotalPrice;
    private double deliveredTotalPrice;
}
