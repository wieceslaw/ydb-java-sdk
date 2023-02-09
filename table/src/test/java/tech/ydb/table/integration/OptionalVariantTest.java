package tech.ydb.table.integration;

import java.time.Duration;

import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.table.Session;
import tech.ydb.table.impl.SimpleTableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.test.junit4.GrpcTransportRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class OptionalVariantTest {
    private final static Logger logger = LoggerFactory.getLogger(OptionalVariantTest.class);

    private final static String QUERY =
            "$vt4 = ParseType(\"Variant<String, Int32??>\");\n " +
            "select\n" +
            "    just(just(Variant(just(just(177)), \"1\", $vt4))),\n" +
            "    just(just(Variant(nothing(int??), \"1\", $vt4)))";

    @ClassRule
    public final static GrpcTransportRule ydbTransport = new GrpcTransportRule();

    private final SimpleTableClient tableClient = SimpleTableClient.newClient(
            GrpcTableRpc.useTransport(ydbTransport)
    ).build();

    @Test
    public void testPartitioningSettings() {
        try (Session session = tableClient.createSession(Duration.ofSeconds(2)).join().getValue()) {
            DataQueryResult result = session.executeDataQuery(QUERY, TxControl.onlineRo()).join().getValue();

            ResultSetReader rs = result.getResultSet(0);
            while (rs.next()) {
                for (int column = 0; column < rs.getColumnCount(); column += 1) {
                    logger.info("READ COLUMN {} -> TYPE {}, VALUE {}", column,
                            rs.getColumn(column).getType(), rs.getColumn(column).getValue());
                }
            }
        }
    }
}
