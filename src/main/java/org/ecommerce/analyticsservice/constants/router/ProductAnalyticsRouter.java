package org.ecommerce.analyticsservice.constants.router;

public final class ProductAnalyticsRouter extends BaseRouter{
    private static final String ROUTER_PREFIX = apiPrefix + "/product";
    public static final String RATING_WITH_ID_ROUTER = ROUTER_PREFIX + "/rating/{productId}";
}
