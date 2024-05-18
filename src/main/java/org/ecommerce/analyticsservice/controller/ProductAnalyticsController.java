package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.constants.router.ProductAnalyticsRouter.*;
import org.ecommerce.analyticsservice.dtos.RatingDto;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.ecommerce.analyticsservice.constants.router.ProductAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class ProductAnalyticsController {

    private final ProductAnalyticsService productAnalyticsService;

    @GetMapping(RATING_WITH_ID_ROUTER)
    public ResponseEntity<RatingDto> getRatingByProductId(@PathVariable("productId") String productId) {
        try {
            RatingDto ratingDto = productAnalyticsService.getRatingByProductId(productId);
            return ResponseEntity.ok(ratingDto);
        } catch (NotFoundException exception) {
            return ResponseEntity.notFound().build();
        }
    }


}
