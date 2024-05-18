package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.ecommerce.analyticsservice.constants.router.ProductAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class ProductAnalyticsController {

    private final ProductAnalyticsService productAnalyticsService;

    @GetMapping(GET_RATING_WITH_ID_ROUTER)
    public ResponseEntity<RatingDto> getRatingByProductId(@PathVariable("productId") String productId) {
        try {
            RatingDto ratingDto = productAnalyticsService.getRatingByProductId(productId);
            return ResponseEntity.ok(ratingDto);
        } catch (NotFoundException exception) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_RATING_AVG_ROUTER)
    public ResponseEntity<List<RatingDto>> getTopHighestRatedProducts(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(productAnalyticsService.getTopHighestRatedProducts(numRecords));
    }

    @GetMapping(GET_TOP_LOWEST_RATING_AVG_ROUTER)
    public ResponseEntity<List<RatingDto>> getTopLowestRatedProducts(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(productAnalyticsService.getTopLowestRatedProducts(numRecords));
    }

    @GetMapping(GET_PRODUCT_WITH_CATEGORY_AND_TAG_BY_ID_ROUTER)
    public ResponseEntity<ProductWithCategoryTagDto> getProductWithCategoryTagByProductId(@PathVariable("productId") String productId) {
        try {
            var dto = productAnalyticsService.getProductWithCategoryTagByProductId(productId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_ALL_PRODUCT_WITH_CATEGORY_AND_TAG_ROUTER)
    public ResponseEntity<List<ProductWithCategoryTagDto>> getAllProductWithCategoryTag() {
        return ResponseEntity.ok(productAnalyticsService.getAllProductWithCategoryTag());
    }

    @GetMapping(GET_PRODUCT_STD_BY_ID_ROUTER)
    public ResponseEntity<ProductStdDto> getProductStdDtoByProductId(@PathVariable("productId") String productId) {
        try {
            var dto = productAnalyticsService.getProductStdDtoByProductId(productId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_ALL_PRODUCT_STD_ROUTER)
    public ResponseEntity<List<ProductStdDto>> getAllProductStd() {
        return ResponseEntity.ok(productAnalyticsService.getAllProductStd());
    }

    @GetMapping(GET_PRODUCT_WITH_ORDER_STATUS_COUNT_BY_ID_ROUTER)
    public ResponseEntity<ProductWithOrderStatusCountDto> getProductWithOrderStatusCountByProductId(@PathVariable("productId") String productId) {
        try {
            var dto = productAnalyticsService.getProductWithOrderStatusCountByProductId(productId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_COMPLETED_COUNT_PRODUCT_ROUTER)
    public ResponseEntity<List<ProductWithOrderStatusCountDto>> getTopHighestCompletedCountProduct(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(productAnalyticsService.getTopHighestCompletedCountProduct(numRecords));
    }

    @GetMapping(GET_PRODUCT_WITH_ORDER_STATUS_REV_BY_ID_ROUTER)
    public ResponseEntity<ProductWithOrderStatusRevDto> getProductWithOrderStatusRevByProductId(@PathVariable("productId") String productId) {
        try {
            var dto = productAnalyticsService.getProductWithOrderStatusRevByProductId(productId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_REV_PRODUCT_ROUTER)
    public ResponseEntity<List<ProductWithOrderStatusRevDto>> getTopHighestRevenueProduct(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(productAnalyticsService.getTopHighestRevenueProduct(numRecords));
    }

    @GetMapping(GET_PRODUCT_WITH_BUYER_COUNT_BY_ID_ROUTER)
    public ResponseEntity<ProductWithBuyerDto> getProductWithBuyerByProductId(@PathVariable("productId") String productId) {
        try {
            var dto = productAnalyticsService.getProductWithBuyerByProductId(productId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_REORDER_RATE_PRODUCT_ROUTER)
    public ResponseEntity<List<ProductWithBuyerDto>> getTopHighestReorderRateProduct(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(productAnalyticsService.getTopHighestReorderRateProduct(numRecords));
    }
}
