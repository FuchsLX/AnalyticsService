package org.ecommerce.analyticsservice.dtos;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class OrderStatusRevenueDto {
    private String orderStatus;
    private BigDecimal revenue;
    private BigDecimal totalPriceBeforeDiscount;
}
