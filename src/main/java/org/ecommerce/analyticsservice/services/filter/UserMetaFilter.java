package org.ecommerce.analyticsservice.services.filter;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class UserMetaFilter {

    public static class MaleGenderFilter implements FilterFunction<Row> {
        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("user_meta_gender").toString().equalsIgnoreCase("male");
        }
    }


}
