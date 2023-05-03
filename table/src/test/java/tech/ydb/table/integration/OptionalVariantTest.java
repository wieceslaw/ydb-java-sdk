package tech.ydb.table.integration;

import java.time.Duration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.ValueProtos;
import tech.ydb.core.Operations;
import tech.ydb.core.Result;
import tech.ydb.core.grpc.GrpcRequestSettings;
import tech.ydb.table.Session;
import tech.ydb.table.YdbTable;
import tech.ydb.table.impl.SimpleTableClient;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.test.junit4.GrpcTransportRule;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class OptionalVariantTest {
    private final static Logger logger = LoggerFactory.getLogger(OptionalVariantTest.class);

    @ClassRule
    public final static GrpcTransportRule ydbTransport = new GrpcTransportRule();

    private final GrpcTableRpc tableRpc = GrpcTableRpc.useTransport(ydbTransport);
    private final SimpleTableClient tableClient = SimpleTableClient.newClient(tableRpc).build();

    private Session session;

    @Before
    public void initSession() {
        session = tableClient.createSession(Duration.ofSeconds(2)).join().getValue();
    }

    @After
    public void closeSession() {
        session.close();
        session = null;
    }

    @Test
    public void testOptionals() {
        // Optional<Optional<Optional<Int32>>>
        String extepectedType = ""
                + "optional_type {"
                + "  item {"
                + "    optional_type {"
                + "      item {"
                + "        optional_type {"
                + "          item {"
                + "            type_id: INT32"
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        executeRawQuery("select Just(Just(Just(177)));")
                .isType(extepectedType)
                .isValue("int32_value: 177");

        executeRawQuery("select Just(Just(Nothing(Int32?)));")
                .isType(extepectedType)
                .isValue("nested_value { nested_value { null_flag_value: NULL_VALUE } }");

        executeRawQuery("select Just(Nothing(Int32??));")
                .isType(extepectedType)
                .isValue("nested_value { null_flag_value: NULL_VALUE }");

        executeRawQuery("select Nothing(Int32???);")
                .isType(extepectedType)
                .isValue("null_flag_value: NULL_VALUE");
    }

    @Test
    public void testVariants() {
        // Optional<Variant(String, Optional<Optional<Int32>>)>
        String extepectedType = ""
                + "optional_type {"
                + "  item {"
                + "    variant_type {"
                + "      tuple_items {"
                + "        elements {"
                + "          type_id: STRING"
                + "        }"
                + "        elements {"
                + "          optional_type {"
                + "            item {"
                + "              optional_type {"
                + "                item {"
                + "                  type_id: INT32"
                + "                }"
                + "              }"
                + "            }"
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        executeRawQuery("select Nothing(ParseType('Variant<String, Int32??>?'));")
                .isType(extepectedType)
                .isValue("null_flag_value: NULL_VALUE");

        executeRawQuery("select Just(Variant(Nothing(Int??), '1', ParseType('Variant<String, Int32??>')));")
                .isType(extepectedType)
                .isValue("nested_value { null_flag_value: NULL_VALUE } variant_index: 1");

        executeRawQuery("select Just(Variant(Just(Nothing(Int?)), '1', ParseType('Variant<String, Int32??>')));")
                .isType(extepectedType)
                .isValue("nested_value { nested_value { null_flag_value: NULL_VALUE } } variant_index: 1");

        executeRawQuery("select Just(Variant(Just(Just(127)), '1', ParseType('Variant<String, Int32??>')));")
                .isType(extepectedType)
                .isValue("nested_value { int32_value:127 } variant_index: 1");
    }

    @Test
    public void testOptionalVariants() {
        // Optional<Optional<Variant<Uint32, Optional<Uint64>>>>
        String extepectedType = ""
                + "optional_type {"
                + "  item {"
                + "    optional_type {"
                + "      item {"
                + "        variant_type {"
                + "          tuple_items {"
                + "            elements {"
                + "              type_id: UINT32"
                + "            }"
                + "            elements {"
                + "              optional_type {"
                + "                item {"
                + "                  type_id: UINT64"
                + "                }"
                + "              }"
                + "            }"
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        executeRawQuery("select Nothing(ParseType('Variant<Uint32, Uint64?>??'));")
                .isType(extepectedType)
                .isValue("null_flag_value:NULL_VALUE");

        executeRawQuery("select Just(Nothing(ParseType('Variant<Uint32, Uint64?>?')));")
                .isType(extepectedType)
                .isValue("nested_value { null_flag_value:NULL_VALUE }");

        executeRawQuery("select Just(Just(Variant(127, '0', ParseType('Variant<Uint32, Uint64?>'))));")
                .isType(extepectedType)
                .isValue("nested_value { uint32_value: 127 }");

        executeRawQuery("select Just(Just(Variant(Just(127), '1', ParseType('Variant<Uint32, Uint64?>'))));")
                .isType(extepectedType)
                .isValue("nested_value { uint64_value:127 } variant_index:1");

        executeRawQuery("select Just(Just(Variant(Nothing(Uint64?), '1', ParseType('Variant<Uint32, Uint64?>'))));")
                .isType(extepectedType)
                .isValue("nested_value { null_flag_value:NULL_VALUE } variant_index:1");
    }

    @Test
    public void testSuperVariants() {
        // Optional<Optional<Variant(Optional<String>, Optional<Int32>)>>
        String expectedType = ""
                + "optional_type {"
                + "  item {"
                + "    optional_type {"
                + "      item {"
                + "        variant_type {"
                + "          tuple_items {"
                + "            elements {"
                + "              optional_type {"
                + "                item {"
                + "                  type_id: STRING"
                + "                }"
                + "              }"
                + "            }"
                + "            elements {"
                + "              optional_type {"
                + "                item {"
                + "                  type_id: INT32"
                + "                }"
                + "              }"
                + "            }"
                + "          }"
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}";

        executeRawQuery("select Nothing(ParseType('Variant<String?, Int32?>??'));")
                .isType(expectedType)
                .isValue("null_flag_value: NULL_VALUE");

        executeRawQuery("select Just(Nothing(ParseType('Variant<String?, Int32?>?')));")
                .isType(expectedType)
                .isValue("nested_value { null_flag_value: NULL_VALUE }");

        executeRawQuery("select Just(Just(Variant(Nothing(Int?), '1', ParseType('Variant<String?, Int32?>'))));")
                .isType(expectedType)
                .isValue("nested_value { null_flag_value: NULL_VALUE } variant_index:1 ");

        executeRawQuery("select Just(Just(Variant(Nothing(String?), '0', ParseType('Variant<String?, Int32?>'))));")
                .isType(expectedType)
                .isValue("nested_value { null_flag_value: NULL_VALUE }");

        executeRawQuery("select Just(Just(Variant(Just(127), '1', ParseType('Variant<String?, Int32?>'))));")
                .isType(expectedType)
                .isValue("nested_value { int32_value: 127 } variant_index:1 ");

        executeRawQuery("select Just(Just(Variant(Just('Hello'), '0', ParseType('Variant<String?, Int32?>'))));")
                .isType(expectedType)
                .isValue("nested_value { bytes_value:\"Hello\" } ");
    }

    private ResultChecker executeRawQuery(String query) {
        logger.debug("execute query {}", query);
        YdbTable.ExecuteDataQueryRequest request = YdbTable.ExecuteDataQueryRequest.newBuilder()
                .setSessionId(session.getId())
                .setOperationParams(Operations.createParams(new ExecuteDataQuerySettings().toOperationSettings()))
                .setTxControl(TxControl.onlineRo().toPb())
                .setQuery(YdbTable.Query.newBuilder().setYqlText(query))
                .setCollectStats(YdbTable.QueryStatsCollection.Mode.STATS_COLLECTION_NONE)
                .build();

        Result<YdbTable.ExecuteQueryResult> executeResult = tableRpc.executeDataQuery(
                request, GrpcRequestSettings.newBuilder().build()
        ).join();

        Assert.assertTrue(executeResult.toString(), executeResult.isSuccess());
        YdbTable.ExecuteQueryResult queryResult = executeResult.getValue();
        if (queryResult == null) {
            throw new AssertionError(executeResult.toString());
        }

        ValueProtos.ResultSet rs = queryResult.getResultSets(0);
        return new ResultChecker(rs.getColumns(0).getType(), rs.getRows(0).getItems(0));

    }

    private class ResultChecker {
        private final ValueProtos.Type protoType;
        private final ValueProtos.Value protoValue;

        public ResultChecker(ValueProtos.Type type, ValueProtos.Value value) {
            this.protoType = type;
            this.protoValue = value;

            logger.debug("got type\n{}", type.toString());
            logger.debug("got value\n{}", value.toString());
        }

        public ResultChecker isType(String expected) {
            String cleanedType = protoType.toString().replaceAll("\\s+", "");
            String cleanedExpected = expected.replaceAll("\\s+", "");

            Assert.assertEquals(cleanedExpected, cleanedType);
            return this;
        }

        public ResultChecker isValue(String expected) {
            String cleanedValue = protoValue.toString().replaceAll("\\s+", "");
            String cleanedExpected = expected.replaceAll("\\s+", "");

            Assert.assertEquals(cleanedExpected, cleanedValue);
            return this;
        }
    }
}
