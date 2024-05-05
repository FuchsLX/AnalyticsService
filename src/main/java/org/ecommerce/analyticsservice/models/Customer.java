package org.ecommerce.analyticsservice.models;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Customer {
    private String id;
    private String firstName;
    private String lastName;
    private String middleName;
    private String email;
    private String mobileNumber;
    private String gender;
    private String address;
}
