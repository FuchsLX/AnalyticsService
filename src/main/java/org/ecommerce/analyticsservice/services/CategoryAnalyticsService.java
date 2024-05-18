package org.ecommerce.analyticsservice.services;

import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;

import java.util.List;

public interface CategoryAnalyticsService {

    CategoryRatingDto getCategoryRatingByCategoryId(String categoryId) throws NotFoundException;
    List<CategoryRatingDto> getTopHighestRatedCategory(int numRecords);
    List<CategoryRatingDto> getTopLowestRatedCategory(int numRecords);

    CategoryProductCountDto getCategoryProductCountByCategoryId(String categoryId) throws NotFoundException;
    List<CategoryProductCountDto> getTopHighestCategoryProductCount(int numRecords);
    List<CategoryProductCountDto> getAllCategoryProductCount();

    CategoryWithOrderStatusCntDto getCategoryWithOSCntByCategoryId(String categoryId) throws NotFoundException;
    List<CategoryWithOrderStatusCntDto> getTopHighestCompletedCategoryOSCnt(int numRecords);

    CategoryWithOrderStatusRevDto getCategoryWithOSErvByCategoryId(String categoryId) throws NotFoundException;
    List<CategoryWithOrderStatusRevDto> getTopHighestCompletedCategoryOSRev(int numRecords);

    CategoryWithBuyerCntDto getCategoryWithBuyerCntByCategoryId(String categoryId) throws NotFoundException;
    List<CategoryWithBuyerCntDto> getTopHighestReorderRateCategory(int numRecords);
}
