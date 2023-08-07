package de.claasklar.benchmark;

import static de.claasklar.primitives.document.ArrayValue.array;
import static de.claasklar.primitives.document.IntValue.integer;
import static de.claasklar.primitives.document.NestedObjectValue.object;
import static de.claasklar.primitives.document.StringValue.string;

import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.ContextlessDocumentGeneratorBuilder;
import de.claasklar.generation.SameAggregationGenerator;
import de.claasklar.generation.VariableAggregationGenerator;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.id.IdDistributionFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LogisticBenchmark {

  private static final int WAREHOUSE_NUM = 50;
  private static final int STOCK_ITEM_NUM = 4_000_000;
  private static final int STOCK_ITEM_NUM_UNORDERED = 1_000_000;

  // number of new unordered stock items during 6h
  private static final int STOCK_ITEM_NUM_UNORDERED_TRANSACTION = 685;
  // 20000 orders per day
  // simulate 1 year worth of orders
  private static final int ORDERS_NUM = 7_300_000;
  private static final int CUSTOMER_NUM = 730_000;

  // assumption: 20000 orders/day
  // transaction phase runs for 6h -> 5000 orders need to be created
  // 10 orders per customer -> 500 customers need to be created during the transaction phase
  private static final int CUSTOMER_NUM_TRANSACTION = 500;
  private static final int PRODUCT_NUM = 100_000;
  private static final double SIMULATED_WORKLOAD_DURATION_HOURS = 6;

  private final RandomNumberGenerator random = new StdRandomNumberGenerator();

  public Benchmark createLogisticBenchmark() {
    return BenchmarkBuilder.builder()
        .writeSpecification(
            config ->
                config
                    .collectionName("warehouses")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("city", s -> s.ssbRandomLengthString(5, 15))
                            .field("street", s -> s.ssbRandomLengthString(7, 30))
                            .field(
                                "postal_code",
                                () ->
                                    new StringValue(String.format("%5d", random.nextInt(1, 99999))))
                            .build()))
        .writeSpecification(
            config ->
                config
                    .collectionName("stock_items")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .fieldFromPipe(
                                "warehouse_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("warehouses"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .fieldFromPipe(
                                "product_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("products"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .constantNumber(1)
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("warehouses")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    idDistributionFactory.uniform(WAREHOUSE_NUM))))
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .constantNumber(1)
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("products")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    idDistributionFactory.uniform(PRODUCT_NUM)))))
        .primaryWriteSpecification(
            "primary_stock_items",
            config ->
                config
                    .collectionName("stock_items")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .fieldFromPipe(
                                "warehouse_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("warehouses"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .fieldFromPipe(
                                "product_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("products"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .constantNumber(1)
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("warehouses")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    idDistributionFactory.uniform(WAREHOUSE_NUM))))
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .constantNumber(1)
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("products")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    idDistributionFactory.uniform(PRODUCT_NUM)))))
        .writeSpecification(
            config ->
                config
                    .collectionName("products")
                    .documentGenerator(
                        ContextlessDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.ssbRandomLengthString(10, 100))
                            .field("price", s -> s.uniformIntSupplier(1, 2000))
                            .build()))
        .writeSpecification(
            config ->
                config
                    .collectionName("orders")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.ssbRandomLengthString(10, 30))
                            .field("city", s -> s.ssbRandomLengthString(5, 15))
                            .field("street", s -> s.ssbRandomLengthString(7, 30))
                            .field(
                                "postal_code",
                                () ->
                                    new StringValue(String.format("%5d", random.nextInt(1, 99999))))
                            .fieldFromPipe(
                                "stock_item_ids",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("stock_items"),
                                        p -> p.selectByPath("$.[*]._id").toArray()))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .countDistribution(cf -> cf.uniform(1, 11))
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("stock_items")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    idDistributionFactory.offset(
                                                        (long) (STOCK_ITEM_NUM_UNORDERED * 1.2),
                                                        IdDistributionFactory::unique))
                                            .existing()
                                            .bufferSize(300))))
        .primaryWriteSpecification(
            "write_customer",
            config ->
                config
                    .collectionName("customers")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.ssbRandomLengthString(10, 30))
                            .fieldFromPipe(
                                "order_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("orders"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                // E[X]=10
                                .countDistribution(df -> df.shift(1, () -> df.geometric(1 / 10.0)))
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("orders")
                                            .idDistribution(IdDistributionFactory::unique))))
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("find_one_customer")
                    .queryGenerator(
                        (v, i) ->
                            new VariableAggregationGenerator(
                                new CollectionName("customers"),
                                AggregationOptions.aggregate("customers")
                                    .pipeline(
                                        List.of(
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$eq",
                                                        array(
                                                            string("$_id"),
                                                            string("$$customer_id"))))),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("orders"),
                                                        "localField", string("order_id"),
                                                        "foreignField", string("_id"),
                                                        "as", string("orders")))))),
                                v.existingId(
                                    "customer_id",
                                    new CollectionName("customers"),
                                    i.uniform(CUSTOMER_NUM)))))
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("find_warehouses_for_order")
                    .queryGenerator(
                        (v, i) ->
                            new VariableAggregationGenerator(
                                new CollectionName("orders"),
                                AggregationOptions.aggregate("orders")
                                    .pipeline(
                                        List.of(
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$eq",
                                                        array(
                                                            string("$_id"),
                                                            string("$$order_id"))))),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("stock_items"),
                                                        "localField", string("stock_item_ids"),
                                                        "foreignField", string("_id"),
                                                        "as", string("stock_items")))),
                                            object("$unwind", string("$stock_items")),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("warehouses"),
                                                        "localField",
                                                            string("stock_items.warehouse_id"),
                                                        "foreignField", string("_id"),
                                                        "as", string("warehouse")))),
                                            object(
                                                "$group",
                                                object(
                                                    "_id",
                                                    object(
                                                        Map.of(
                                                            "city", string("$warehouse.city"),
                                                            "street", string("$warehouse.street"),
                                                            "postal_code",
                                                                string(
                                                                    "$warehouse.postal_code"))))))),
                                v.existingId(
                                    "order_id",
                                    new CollectionName("orders"),
                                    i.uniform(ORDERS_NUM)))))
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("product_availability")
                    .queryGenerator(
                        (v, i) ->
                            new VariableAggregationGenerator(
                                new CollectionName("stock_items"),
                                AggregationOptions.aggregate("stock_items")
                                    .pipeline(
                                        List.of(
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$eq",
                                                        array(
                                                            string("$product_id"),
                                                            string("$$product_id"))))),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("orders"),
                                                        "localField", string("_id"),
                                                        "foreignField", string("stock_item_ids"),
                                                        "as", string("order")))),
                                            object(
                                                "$match",
                                                object("order", object("$size", integer(0)))),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("warehouses"),
                                                        "localField", string("warehouse_id"),
                                                        "foreignField", string("_id"),
                                                        "as", string("warehouse")))),
                                            object(
                                                "$group",
                                                object(
                                                    "_id",
                                                    object(
                                                        Map.of(
                                                            "city", string("$warehouse.city"),
                                                            "street", string("$warehouse.street"),
                                                            "postal_code",
                                                                string("$warehouse.postal_code"))),
                                                    "in_stock_count",
                                                    object(
                                                        "$count",
                                                        object(Collections.emptyMap())))))),
                                v.existingId(
                                    "product_id",
                                    new CollectionName("products"),
                                    i.uniform(PRODUCT_NUM)))))
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("warehouse_revenue")
                    .queryGenerator(
                        new SameAggregationGenerator(
                            new CollectionName("stock_items"),
                            AggregationOptions.aggregate("stock_items")
                                .pipeline(
                                    List.of(
                                        object(
                                            "$lookup",
                                            object(
                                                Map.of(
                                                    "from",
                                                    string("stock_items"),
                                                    "localField",
                                                    string("stock_item_ids"),
                                                    "foreignField",
                                                    string("_id"),
                                                    "as",
                                                    string("stock_items")))),
                                        object("$unwind", string("$stock_items")),
                                        object(
                                            "$lookup",
                                            object(
                                                Map.of(
                                                    "from", string("products"),
                                                    "localField", string("stock_items.product_id"),
                                                    "foreignField", string("_id"),
                                                    "as", string("product")))),
                                        object(
                                            "$lookup",
                                            object(
                                                Map.of(
                                                    "from", string("warehouses"),
                                                    "localField",
                                                        string("stock_items.warehouse_id"),
                                                    "foreignField", string("_id"),
                                                    "as", string("warehouse")))),
                                        object("$unwind", string("$warehouse")),
                                        object("$unwind", string("$product")),
                                        object(
                                            "$group",
                                            object(
                                                "_id",
                                                object(
                                                    Map.of(
                                                        "city", string("$warehouse.city"),
                                                        "street", string("$warehouse.street"),
                                                        "postal_code",
                                                            string("$warehouse.postal_code"))),
                                                "revenue",
                                                object("$sum", string("$product.price")))))))))
        .loadPhase(
            loadPhaseConfig ->
                loadPhaseConfig
                    .primaryWriteSpecification(
                        (long) CUSTOMER_NUM - CUSTOMER_NUM_TRANSACTION, "write_customer")
                    .primaryWriteSpecification(
                        (long) STOCK_ITEM_NUM_UNORDERED - STOCK_ITEM_NUM_UNORDERED_TRANSACTION,
                        "primary_stock_items")
                    .numThreads(30))
        .database(config -> {})
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("orders"))
                .keys(object("stock_item_ids", integer(1))))
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("stock_items"))
                .keys(object("product_id", integer(1))))
        .transactionPhase(
            config ->
                config.weightedRandom(
                    weightedRandom -> {
                      var customerWeight = 10;
                      var stockItemsWeight = 10;
                      double totalCount = (2000.0 / customerWeight) * CUSTOMER_NUM_TRANSACTION;
                      var productAvailabilityWeight = 1279;
                      var targetOpsPerMilli =
                          totalCount
                              * (1 / SIMULATED_WORKLOAD_DURATION_HOURS)
                              * (1 / 60.0) // hours
                              * (1 / 60.0) // minutes
                              * (1 / 1000.0); // milliseconds
                      weightedRandom
                          .weightedSpecification(stockItemsWeight, "primary_stock_items")
                          .weightedSpecification(customerWeight, "write_customer")
                          .weightedSpecification(1, "warehouse_revenue")
                          .weightedSpecification(productAvailabilityWeight, "product_availability")
                          .weightedSpecification(350, "find_one_customer")
                          .weightedSpecification(350, "find_warehouses_for_order")
                          .totalCount((int) totalCount)
                          .targetOps(targetOpsPerMilli)
                          .threadCount(15);
                    }))
        .build();
  }
}
