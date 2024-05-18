package org.ecommerce.analyticsservice.services;

import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;

import java.util.List;

public interface ProductAnalyticsService {

    ProductRatingDto getRatingByProductId(String productId) throws NotFoundException;
    List<ProductRatingDto> getTopHighestRatedProducts(int numRecords);
    List<ProductRatingDto> getTopLowestRatedProducts(int numRecords);

    ProductWithCategoryTagDto getProductWithCategoryTagByProductId(String productId) throws NotFoundException;
    List<ProductWithCategoryTagDto> getAllProductWithCategoryTag();

    ProductStdDto getProductStdDtoByProductId(String productId) throws NotFoundException;
    List<ProductStdDto> getAllProductStd();

    ProductWithOrderStatusCountDto getProductWithOrderStatusCountByProductId(String productId) throws NotFoundException;
    List<ProductWithOrderStatusCountDto> getTopHighestCompletedCountProduct(int numRecords);

    ProductWithOrderStatusRevDto getProductWithOrderStatusRevByProductId(String productId) throws NotFoundException;
    List<ProductWithOrderStatusRevDto> getTopHighestRevenueProduct(int numRecords);

    ProductWithBuyerDto getProductWithBuyerByProductId(String productId) throws NotFoundException;
    List<ProductWithBuyerDto> getTopHighestReorderRateProduct(int numRecords);
}
