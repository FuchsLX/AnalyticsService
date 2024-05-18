package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class OrderStatusCounterDto {
    private String status;
    private long order_quantity;
}
