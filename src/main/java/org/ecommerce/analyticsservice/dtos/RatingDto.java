package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class RatingDto {
    private String productId;
    private String productName;
    private Double ratingAvg;
    private Long ratingCount;
    private Long oneStarRatingCount;
    private Long twoStarRatingCount;
    private Long threeStarRatingCount;
    private Long fourStarRatingCount;
    private Long fiveStarRatingCount;
}
