package org.ecommerce.analyticsservice.constants.router;

public final class CategoryAnalyticsRouter extends BaseRouter {
    private static final String ROUTER_PREFIX = apiPrefix + "/category";

    public static final String GET_CATEGORY_RATING_WITH_ID_ROUTER = ROUTER_PREFIX + "/rating/{categoryId}";
    public static final String GET_TOP_HIGHEST_RATING_AVG_CATEGORY_ROUTER = ROUTER_PREFIX + "/rating/{numRecords}/desc";
    public static final String GET_TOP_LOWEST_RATING_AVG_CATEGORY_ROUTER = ROUTER_PREFIX + "/rating/{numRecords}/asc";

    public static final String GET_CATEGORY_PRODUCT_CNT_WITH_ID_ROUTER = ROUTER_PREFIX + "/pc/{categoryId}";
    public static final String GET_TOP_HIGHEST_PRODUCT_COUNT_CATEGORY_ROUTER = ROUTER_PREFIX + "/pc/{numRecords}/desc";
    public static final String GET_ALL_CATEGORY_PRODUCT_CNT_ROUTER = ROUTER_PREFIX + "/pc";

    public static final String GET_CATEGORY_WITH_ORDER_STATUS_COUNT_BY_ID_ROUTER = ROUTER_PREFIX + "/osc/{categoryId}";
    public static final String GET_TOP_HIGHEST_COMPLETED_COUNT_CATEGORY_ROUTER = ROUTER_PREFIX + "/osc/{numRecords}/desc";

    public static final String GET_CATEGORY_WITH_ORDER_STATUS_REV_BY_ID_ROUTER = ROUTER_PREFIX + "/osr/{categoryId}";
    public static final String GET_TOP_HIGHEST_REV_CATEGORY_ROUTER = ROUTER_PREFIX + "/osr/{numRecords}/desc";

    public static final String GET_CATEGORY_WITH_BUYER_COUNT_BY_ID_ROUTER = ROUTER_PREFIX + "/bc/{categoryId}";
    public static final String GET_TOP_HIGHEST_REORDER_RATE_CATEGORY_ROUTER = ROUTER_PREFIX + "/bc/{numRecords}/desc";
}
