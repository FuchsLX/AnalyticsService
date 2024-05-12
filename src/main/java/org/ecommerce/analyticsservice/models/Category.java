package org.ecommerce.analyticsservice.models;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Category {
    private String id;
    private String name;
    private String parentId;
}
