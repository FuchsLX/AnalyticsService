package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class CustomerCountDto {
    private long totalCustomer;
    private long totalCustomerEnabled;
    private long totalCustomersUpdatedProfile;
    private long totalCustomersBoughtAtLeastOnce;
    private long totalRepeatCustomers;
}
