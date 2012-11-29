package com.hotels.sps.client;

import java.util.Set;
import java.util.UUID;

public interface DataModel {
    
    public Set<PaymentInfo> readModel(String userId);
    public Set<UUID> writeModel(String userId, Set<PaymentInfo> paymentInfos);

}
