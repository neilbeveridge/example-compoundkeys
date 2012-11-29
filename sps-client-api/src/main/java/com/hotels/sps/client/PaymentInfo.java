package com.hotels.sps.client;

import java.util.UUID;

public class PaymentInfo {

    public UUID paymentUUID;
    public String pvToken;
    public String name;
    public String number;
    public String expiry;
    
    public PaymentInfo () {
    }
    
    public PaymentInfo(String pvToken, String name, String number, String expiry) {
        this.pvToken = pvToken;
        this.name = name;
        this.number = number;
        this.expiry = expiry;
    }

    @Override
    public String toString() {
        return "PaymentInfo [paymentUUID=" + paymentUUID + ", pvToken=" + pvToken + ", name=" + name + ", number=" + number + ", expiry=" + expiry
                + "]";
    }
    
}
