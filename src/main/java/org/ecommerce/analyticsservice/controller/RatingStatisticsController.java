package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.models.Rating;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/rating-analytic")
public class RatingStatisticsController {

    private final ProductAnalyticsService productAnalyticsService;

    @GetMapping("/{productId}")
    public ResponseEntity<Rating> getRating(@PathVariable String productId) {
        return ResponseEntity.ok(productAnalyticsService.getRating(productId));
    }

}
