package org.ecommerce.analyticsservice.filter;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class RoleFilter  {

    public static class CustomerRoleFilter implements FilterFunction<Row> {

        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("name").toString().equalsIgnoreCase("customer");
        }
    }

    public static class ManagerRoleFilter implements FilterFunction<Row> {

        @Override
        public boolean call(Row value) throws Exception {
            return value.getAs("name").toString().equalsIgnoreCase("staff") ||
                    value.getAs("name").toString().equalsIgnoreCase("admin");
        }
    }


}
