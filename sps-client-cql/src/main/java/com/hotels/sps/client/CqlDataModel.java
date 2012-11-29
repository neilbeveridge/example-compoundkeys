package com.hotels.sps.client;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.mortbay.log.Log;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.TimeUUIDUtils;

public class CqlDataModel implements DataModel {
    //private static final Logger LOG = LoggerFactory.getLogger(CqlDataModel.class);

    private static final Keyspace PAYMENTS_KEYSPACE;
    private static final ColumnFamily<String, String> PAYMENTS_CF;

    /*
     * create table paymentinfo_cql (
     *  user text,
     *  paymentid timeuuid,
     *  name text,
     *  number text,
     *  expiry text,
     *  pvtoken text,
     *  primary key (user,paymentid)
     * );
     */
    private static final String CQL_WRITE = "insert into paymentinfo_cql (user, paymentid, name, number, expiry, pvtoken) values ('%1$s','%2$s','%3$s','%4$s','%5$s','%6$s')";
    private static final String CQL_READ = "select * from paymentinfo_cql where user='%s'";

    static {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster("ClusterName")
                .forKeyspace("sps")
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE).setCqlVersion("3.0.0"))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setMaxConnsPerHost(1).setSeeds("127.0.0.1:9160"))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        PAYMENTS_KEYSPACE = context.getEntity();

        PAYMENTS_CF = new ColumnFamily<>("paymentinfo_cql", StringSerializer.get(), StringSerializer.get());

    }

    @Override
    public Set<PaymentInfo> readModel(String userId) {
        String cql = String.format(CQL_READ, userId);
        Log.info("CQL: {}", cql);
        
        OperationResult<CqlResult<String, String>> result;
        try {
            result = PAYMENTS_KEYSPACE.prepareQuery(PAYMENTS_CF).withCql(cql).execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }

        Set<PaymentInfo> payments = new LinkedHashSet<>();

        for (Row<String, String> row : result.getResult().getRows()) {
            PaymentInfo payment = new PaymentInfo();

            for (Column<String> column : row.getColumns()) {
                switch (column.getName()) {
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
                case "paymentid":
                    payment.paymentUUID = column.getUUIDValue();
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

        UUID paymentUUID = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
        String sPaymentUUID = paymentUUID.toString();

        String cql = String.format(CQL_WRITE, userId, sPaymentUUID, paymentInfo.name, paymentInfo.number, paymentInfo.expiry, paymentInfo.pvToken);
        Log.info("CQL: {}", cql);

        try {
            PAYMENTS_KEYSPACE.prepareQuery(PAYMENTS_CF).withCql(cql).execute();
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }

        return paymentUUID;
    }

}
