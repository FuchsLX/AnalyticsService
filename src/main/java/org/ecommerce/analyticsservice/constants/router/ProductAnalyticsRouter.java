package org.ecommerce.analyticsservice.constants.router;

public final class ProductAnalyticsRouter extends BaseRouter {
    private static final String ROUTER_PREFIX = apiPrefix + "/product";

    public static final String GET_RATING_WITH_ID_ROUTER = ROUTER_PREFIX + "/rating/{productId}";
    public static final String GET_TOP_HIGHEST_RATING_AVG_ROUTER = ROUTER_PREFIX + "/rating/{numRecords}/desc";
    public static final String GET_TOP_LOWEST_RATING_AVG_ROUTER = ROUTER_PREFIX + "/rating/{numRecords}/asc";

    public static final String GET_PRODUCT_WITH_CATEGORY_AND_TAG_BY_ID_ROUTER = ROUTER_PREFIX + "/ct/{productId}";
    public static final String GET_ALL_PRODUCT_WITH_CATEGORY_AND_TAG_ROUTER = ROUTER_PREFIX + "/ct";

    public static final String GET_PRODUCT_STD_BY_ID_ROUTER = ROUTER_PREFIX + "/std/{productId}";
    public static final String GET_ALL_PRODUCT_STD_ROUTER = ROUTER_PREFIX + "/std";

    public static final String GET_PRODUCT_WITH_ORDER_STATUS_COUNT_BY_ID_ROUTER = ROUTER_PREFIX + "/osc/{productId}";
    public static final String GET_TOP_HIGHEST_COMPLETED_COUNT_PRODUCT_ROUTER = ROUTER_PREFIX + "/osc/{numRecords}/desc";

    public static final String GET_PRODUCT_WITH_ORDER_STATUS_REV_BY_ID_ROUTER = ROUTER_PREFIX + "/osr/{productId}";
    public static final String GET_TOP_HIGHEST_REV_PRODUCT_ROUTER = ROUTER_PREFIX + "/osr/{numRecords}/desc";

    public static final String GET_PRODUCT_WITH_BUYER_COUNT_BY_ID_ROUTER = ROUTER_PREFIX + "/bc/{productId}";
    public static final String GET_TOP_HIGHEST_REORDER_RATE_PRODUCT_ROUTER = ROUTER_PREFIX + "/bc/{numRecords}/desc";
}
