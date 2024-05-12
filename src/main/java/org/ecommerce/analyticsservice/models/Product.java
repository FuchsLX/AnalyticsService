package org.ecommerce.analyticsservice.models;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Product {
    private String id;
    private String title;
    private boolean enabled;
    private BigDecimal discount;
    private BigDecimal price;
    private long quantity;
    private float ratingAvg;
}
