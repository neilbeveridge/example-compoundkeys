package com.hotels.sps.client;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class CqlDataModelTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(CqlDataModelTest.class);
    private static final DataModel DATA_MODEL = new CqlDataModel();
    private static final String USERID = "neil";

    //@Test
    public void write () {
        Set<PaymentInfo> payments = new LinkedHashSet<>();
        payments.add(new PaymentInfo("123", "Barclaycard", "XXXXX789012XXXX", "0315"));
        payments.add(new PaymentInfo("456", "Visa", "XXXXX789013XXXX", "0316"));
        payments.add(new PaymentInfo("789", "Current Account", "XXXXX789014XXXX", "0317"));
        
        Set<UUID> paymentUUIDs = DATA_MODEL.writeModel(USERID, payments);
        
        LOG.error("written payment UUIDs: {}", paymentUUIDs);
    }
    
    @Test
    public void read () {
        Set<PaymentInfo> payments = DATA_MODEL.readModel(USERID);
        
        LOG.error("read payments: {}", payments);
    }
    
}
