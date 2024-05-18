package org.ecommerce.analyticsservice.constants.router;

public final class OrderAnalyticsRouter extends BaseRouter{
    private static final String ROUTER_PREFIX = apiPrefix + "/order";
    public static final String ORDER_STATUS_COUNT_ROUTER = ROUTER_PREFIX + "/counter";
    public static final String ORDER_STATUS_REVENUE_ROUTER = ROUTER_PREFIX + "/revenue";
}
