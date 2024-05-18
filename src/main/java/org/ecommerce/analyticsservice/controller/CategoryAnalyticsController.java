package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.services.CategoryAnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.ecommerce.analyticsservice.constants.router.CategoryAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class CategoryAnalyticsController {
    private final CategoryAnalyticsService categoryAnalyticsService;
    private static final Logger logger = LoggerFactory.getLogger(CategoryAnalyticsController.class);

    @GetMapping(GET_CATEGORY_RATING_WITH_ID_ROUTER)
    public ResponseEntity<CategoryRatingDto> getCategoryRatingByCategoryId(@PathVariable("categoryId") String categoryId) {
           try {
               var dto = categoryAnalyticsService.getCategoryRatingByCategoryId(categoryId);
               return ResponseEntity.ok(dto);
           } catch (NotFoundException ex) {
               return ResponseEntity.notFound().build();
           }
    }

    @GetMapping(GET_TOP_HIGHEST_RATING_AVG_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryRatingDto>> getTopHighestRatedCategory(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(categoryAnalyticsService.getTopHighestRatedCategory(numRecords));
    }

    @GetMapping(GET_TOP_LOWEST_RATING_AVG_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryRatingDto>> getTopLowestRatedCategory(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(categoryAnalyticsService.getTopLowestRatedCategory(numRecords));
    }

    @GetMapping(GET_CATEGORY_PRODUCT_CNT_WITH_ID_ROUTER)
    public ResponseEntity<CategoryProductCountDto> getCategoryProductCountByCategoryId(@PathVariable("categoryId") String categoryId) {
        try {
            var dto = categoryAnalyticsService.getCategoryProductCountByCategoryId(categoryId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_PRODUCT_COUNT_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryProductCountDto>> getTopHighestCategoryProductCount(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(categoryAnalyticsService.getTopHighestCategoryProductCount(numRecords));
    }

    @GetMapping(GET_ALL_CATEGORY_PRODUCT_CNT_ROUTER)
    public ResponseEntity<List<CategoryProductCountDto>> getAllCategoryProductCount() {
        return ResponseEntity.ok(categoryAnalyticsService.getAllCategoryProductCount());
    }

    @GetMapping(GET_CATEGORY_WITH_ORDER_STATUS_COUNT_BY_ID_ROUTER)
    public ResponseEntity<CategoryWithOrderStatusCntDto> getCategoryWithOSCntByCategoryId(@PathVariable("categoryId") String categoryId) {
        try {
            var dto = categoryAnalyticsService.getCategoryWithOSCntByCategoryId(categoryId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    };

    @GetMapping(GET_TOP_HIGHEST_COMPLETED_COUNT_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryWithOrderStatusCntDto>> getTopHighestCompletedCategoryOSCnt(@PathVariable("numRecords") int numRecords) {
        return ResponseEntity.ok(categoryAnalyticsService.getTopHighestCompletedCategoryOSCnt(numRecords));
    }

    @GetMapping(GET_CATEGORY_WITH_ORDER_STATUS_REV_BY_ID_ROUTER)
    public ResponseEntity<CategoryWithOrderStatusRevDto> getCategoryWithOSErvByCategoryId(@PathVariable("categoryId") String categoryId){
        try {
            var dto = categoryAnalyticsService.getCategoryWithOSErvByCategoryId(categoryId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_REV_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryWithOrderStatusRevDto>> getTopHighestCompletedCategoryOSRev(@PathVariable("numRecords") int numRecords){
        return ResponseEntity.ok(categoryAnalyticsService.getTopHighestCompletedCategoryOSRev(numRecords));
    }

    @GetMapping(GET_CATEGORY_WITH_BUYER_COUNT_BY_ID_ROUTER)
    public ResponseEntity<CategoryWithBuyerCntDto> getCategoryWithBuyerCntByCategoryId(@PathVariable("categoryId") String categoryId){
        try {
            var dto = categoryAnalyticsService.getCategoryWithBuyerCntByCategoryId(categoryId);
            return ResponseEntity.ok(dto);
        } catch (NotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(GET_TOP_HIGHEST_REORDER_RATE_CATEGORY_ROUTER)
    public ResponseEntity<List<CategoryWithBuyerCntDto>> getTopHighestReorderRateCategory(@PathVariable("numRecords") int numRecords){
        return ResponseEntity.ok(categoryAnalyticsService.getTopHighestReorderRateCategory(numRecords));
    }
}
