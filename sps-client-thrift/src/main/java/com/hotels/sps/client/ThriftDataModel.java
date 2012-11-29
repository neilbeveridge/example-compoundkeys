package com.hotels.sps.client;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.annotations.Component;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AnnotatedCompositeSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class ThriftDataModel implements DataModel {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftDataModel.class);

    private static final Keyspace PAYMENTS_KEYSPACE;
    private static final ColumnFamily<String, Composite> PAYMENTS_CF;
    private static final AnnotatedCompositeSerializer<Composite> PAYMENTS_SERIALIZER = new AnnotatedCompositeSerializer<Composite>(Composite.class);

    static {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace("sps")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1).setSeeds("127.0.0.1:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        PAYMENTS_KEYSPACE = context.getEntity();

        PAYMENTS_CF = new ColumnFamily<String, Composite>("paymentinfo_thrift", StringSerializer.get(), PAYMENTS_SERIALIZER);
    }

    @Override
    public Set<PaymentInfo> readModel(String userId) {
        OperationResult<ColumnList<Composite>> result;
        try {
            result = PAYMENTS_KEYSPACE.prepareQuery(PAYMENTS_CF).getKey(userId).execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
        
        ColumnList<Composite> columns = result.getResult();

        Set<PaymentInfo> payments = new LinkedHashSet<>();
        
        String lastSeen = null;
        PaymentInfo payment = new PaymentInfo();
        
        if (columns.size() > 0) {
            for (Column<Composite> column : columns) {
                //handle the payment info boundary
                if (lastSeen != null && !column.getName().getPaymentUuid().equals(lastSeen)) {
                    payments.add(payment);
                    payment = new PaymentInfo();
                    payment.paymentUUID = UUID.fromString(column.getName().paymentUuid);
                }
                lastSeen = column.getName().getPaymentUuid();
                
                LOG.info("column name: {}, value: {}", column.getName(), column.getStringValue());
    
                switch (column.getName().field) {
                case "pvtoken":
                    payment.pvToken = column.getStringValue();
                    break;
                case "name":
                    payment.name = column.getStringValue();
                    break;
                case "number":
                    payment.number = column.getStringValue();
                    break;
                case "expiry":
                    payment.expiry = column.getStringValue();
                    break;
                }
            }
            payments.add(payment);
        }
        
        return payments;
    }

    @Override
    public Set<UUID> writeModel(String userId, Set<PaymentInfo> paymentInfos) {
        Set<UUID> paymentUUIDs = new LinkedHashSet<>();
        for (PaymentInfo paymentInfo : paymentInfos) {
            paymentUUIDs.add(writeSingle(userId, paymentInfo));
        }
        
        return paymentUUIDs;
    }
    
    private UUID writeSingle(String userId, PaymentInfo paymentInfo) {
        MutationBatch batch = PAYMENTS_KEYSPACE.prepareMutationBatch();

        UUID paymentUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        String sPaymentUUID = paymentUUID.toString();

        batch.withRow(PAYMENTS_CF, userId)
                .putColumn(new Composite(sPaymentUUID, "pvtoken"), paymentInfo.pvToken, null)
                .putColumn(new Composite(sPaymentUUID, "name"), paymentInfo.name, null)
                .putColumn(new Composite(sPaymentUUID, "number"), paymentInfo.number, null)
                .putColumn(new Composite(sPaymentUUID, "expiry"), paymentInfo.expiry, null);
        
        try {
            batch.execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }

        return paymentUUID;
    }

    public static final class Composite {
        private @Component(ordinal = 0)
        String paymentUuid;
        private @Component(ordinal = 1)
        String field;

        public Composite() {
        }

        public Composite(String paymentUUID, String field) {
            this.paymentUuid = paymentUUID;
            this.field = field;
        }

        public String getPaymentUuid() {
            return paymentUuid;
        }

        public void setPaymentUuid(String paymentUuid) {
            this.paymentUuid = paymentUuid;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((field == null) ? 0 : field.hashCode());
            result = prime * result + ((paymentUuid == null) ? 0 : paymentUuid.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Composite other = (Composite) obj;
            if (field == null) {
                if (other.field != null)
                    return false;
            } else if (!field.equals(other.field))
                return false;
            if (paymentUuid == null) {
                if (other.paymentUuid != null)
                    return false;
            } else if (!paymentUuid.equals(other.paymentUuid))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "Composite [paymentUuid=" + paymentUuid + ", field=" + field + "]";
        }
    }

}
