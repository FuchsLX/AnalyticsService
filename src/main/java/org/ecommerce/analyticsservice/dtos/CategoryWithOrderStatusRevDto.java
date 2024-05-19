package org.ecommerce.analyticsservice.dtos;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class CategoryWithOrderStatusRevDto {
    private String categoryId;
    private String categoryName;
    private double completedTotalPrice;
    private double cancelledTotalPrice;
    private double processingTotalPrice;
    private double deliveredTotalPrice;
}
