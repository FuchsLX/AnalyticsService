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
    private BigDecimal completedTotalPrice;
    private BigDecimal cancelledTotalPrice;
    private BigDecimal processingTotalPrice;
    private BigDecimal deliveredTotalPrice;
}
