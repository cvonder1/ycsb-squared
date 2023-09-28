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
import java.util.function.Consumer;

/**
 * LogisticBenchmark provides two benchmark configuration for simulating the operation of a
 * nationwide warehouse chain. One configuration is using a data model with referencing between
 * documents, the other one uses embedding.
 */
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

  /**
   * Creates a benchmark, which simulates the operation of a national warehouse chain.
   *
   * <p>The database consists of five collections: customers, orders, stock_items, products,
   * warehouses. Stock items are concrete instances of a product stored in one warehouse. Customers
   * can place orders, which in turn reference stock items. If stock items are not sold, they are
   * not referenced by any order.
   *
   * <p>Collections are linked by referencing.
   *
   * @return Benchmark instance
   */
  public Benchmark createLogisticBenchmark() {
    return BenchmarkBuilder.builder()
        // configure the write specification for the "warehouses" collection
        .writeSpecification(
            config ->
                config
                    .collectionName("warehouses")
                    // The document generator is invoked every time a new warehouse document is
                    // required.
                    // The generated documents have the fields city, street and postal_code
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("city", s -> s.alphaNumRandomLengthString(5, 15))
                            .field("street", s -> s.alphaNumRandomLengthString(7, 30))
                            .field(
                                "postal_code",
                                () ->
                                    new StringValue(String.format("%5d", random.nextInt(1, 99999))))
                            .build()))
        // configure the write specification for the stock_items collection. Stock items are
        // instances of a single product stored in a warehouse.
        .writeSpecification(
            config ->
                config
                    .collectionName("stock_items")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            // Pipes are the mechanism to convert from one data representation to
                            // another.
                            // Here documents are selected from the warehouses collection and
                            // the first id is selected.
                            // The id of the warehouse is stored in the field warehouse_id.
                            // Below is configured how the documents are selected from the
                            // warehouses collection with the referenceDistributionConfig option.
                            // The referenceDistributionConfig is configured in a way, that ensures
                            // the document generator selects exactly one warehouse.
                            .fieldFromPipe(
                                "warehouse_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("warehouses"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            // The product_id is configured similarly to the warehouse_id field.
                            .fieldFromPipe(
                                "product_id",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("products"),
                                        p -> p.selectByPath("$.[0]._id").toId()))
                            .build())
                    // referenceDistributionConfig determines how many and which warehouse documents
                    // are selected for referencing in the stock_items document.
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                // For every stock_item exactly one warehouse is selected.
                                .constantNumber(1)
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            // Documents get selected from the "warehouses"
                                            // collection.
                                            .collectionName("warehouses")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    // The warehouse instance is selected according
                                                    // to a uniform distribution over all possible
                                                    // warehouses.
                                                    idDistributionFactory.uniform(WAREHOUSE_NUM))))
                    // For products, we want the same configuration as for warehouses.
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
        // In contrast to write specifications, primary write specifications are on top of a
        // document hierarchy and can be called in the load and transaction phase.
        // For stock items we need both a write specification and a primary write specification.
        // Stock items, which were sold, are referenced by an order and therefore have to be
        // "simple" write specifications.
        // Stock items, which were not sold and are still in a warehouse, are not referenced.
        // Because of this, we also need a primary write specification for the stock items.
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
        // Write specification for products.
        .writeSpecification(
            config ->
                config
                    .collectionName("products")
                    // The product documents have two fields.
                    .documentGenerator(
                        ContextlessDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.alphaNumRandomLengthString(10, 100))
                            .field("price", s -> s.uniformIntSupplier(1, 2000))
                            .build()))
        // write specification for orders.
        .writeSpecification(
            config ->
                config
                    .collectionName("orders")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.alphaNumRandomLengthString(10, 30))
                            .field("city", s -> s.alphaNumRandomLengthString(5, 15))
                            .field("street", s -> s.alphaNumRandomLengthString(7, 30))
                            .field(
                                "postal_code",
                                () ->
                                    new StringValue(String.format("%5d", random.nextInt(1, 99999))))
                            // stock_item_ids selects documents from the stock_items collection.
                            // The array of stock item documents is in turn mapped to an array of
                            // ids and stored in the stock_item_ids field.
                            .fieldFromPipe(
                                "stock_item_ids",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("stock_items"),
                                        p -> p.selectByPath("$.[*]._id").toArray()))
                            .build())
                    // configure benchmark to select 1 to 11 unique stock items per order.
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                // Draw the number of selected stock items from a uniform
                                // distribution from 1 to 10(inclusive).
                                .countDistribution(cf -> cf.uniform(1, 11))
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("stock_items")
                                            .idDistribution(
                                                idDistributionFactory ->
                                                    // Each selected stock item is unique. The ids
                                                    // are offset, so that no stock items, which are
                                                    // unordered, are selected.
                                                    idDistributionFactory.offset(
                                                        (long) (STOCK_ITEM_NUM_UNORDERED * 1.2),
                                                        IdDistributionFactory::unique)))))
        // configuration for the "customers" collection. The customers are also a primary write
        // specification.
        .primaryWriteSpecification(
            "write_customer",
            config ->
                config
                    .collectionName("customers")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.alphaNumRandomLengthString(10, 30))
                            // Map the order documents to an array of ids and store them in the
                            // field order_ids.
                            .fieldFromPipe(
                                "order_ids",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("orders"),
                                        p -> p.selectByPath("$.[*]._id").toArray()))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                // Draw the number of selected orders per customer from a geometric
                                // distribution with p = 0.1.
                                // Add 1 to the random variable to have at least one document.
                                // E[X]=10
                                .countDistribution(df -> df.shift(1, () -> df.geometric(1 / 10.0)))
                                .documentDistribution(
                                    documentDistributionConfig ->
                                        documentDistributionConfig
                                            .collectionName("orders")
                                            // Each selected order is unique
                                            .idDistribution(IdDistributionFactory::unique))))
        // read specification for reading one customer.
        // read specifications can be invoked in the transaction phase, to simulate read workloads.
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("find_one_customer")
                    // query generator for an aggregate query with one variable.
                    .queryGenerator(
                        (v, i) ->
                            new VariableAggregationGenerator(
                                new CollectionName("customers"),
                                AggregationOptions.aggregate("customers")
                                    // each object in the list is one pipeline stage
                                    .pipeline(
                                        List.of(
                                            // find the customer matching the variable customer_id
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$eq",
                                                        array(
                                                            string("$_id"),
                                                            string("$$customer_id"))))),
                                            // lookup ("join") the customer with its orders.
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("orders"),
                                                        "localField", string("order_ids"),
                                                        "foreignField", string("_id"),
                                                        "as", string("orders")))))),
                                // configure variable customer_id to be one randomly chosen existing
                                // customer id.
                                v.existingId(
                                    "customer_id",
                                    new CollectionName("customers"),
                                    i.uniform(CUSTOMER_NUM)))))
        // Read specification for finding an order's warehouses.
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("find_warehouses_for_order")
                    .queryGenerator(
                        (v, i) ->
                            // use MongoDB's aggregate pipeline
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
                                // the variable with the name order_id is randomly chosen from all
                                // existing order ids.
                                v.existingId(
                                    "order_id",
                                    new CollectionName("orders"),
                                    i.uniform(ORDERS_NUM)))))
        // Read specification for generating queries, which find availability information on one
        // product.
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
        // Read specification for calculating each warehouse's revenue.
        .readSpecification(
            readSpecificationConfig ->
                readSpecificationConfig
                    .name("warehouse_revenue")
                    .queryGenerator(
                        // use a pipeline aggregation without any variables.
                        new SameAggregationGenerator(
                            new CollectionName("orders"),
                            AggregationOptions.aggregate("orders")
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
        // We are happy with the default database configuration.
        .database(config -> {})
        // create an ascending index on the field orders.stock_item_ids
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("orders"))
                .keys(object("stock_item_ids", integer(1))))
        // create an ascending index on the field stock_items.product_id
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("stock_items"))
                .keys(object("product_id", integer(1))))
        // In the load phase the database can be populated with data. The data is obtained by
        // executing the write specifications.
        .loadPhase(loadPhaseConfig())
        // The load phase mimics a realistic mixture of read and write queries.
        .transactionPhase(transactionPhaseConfig())
        .build();
  }

  /**
   * Creates a benchmark, which simulates the operation of a national warehouse chain.
   *
   * <p>The database consists of five collections: customers, orders, stock_items, products,
   * warehouses. Stock items are concrete instances of a product stored in one warehouse. Customers
   * can place orders, which in turn reference stock items. If stock items are not sold, they are
   * not referenced by any order.
   *
   * <p>Collections are linked by embedding all documents into customers or stock items.
   *
   * @return Benchmark instance
   */
  public Benchmark createLogisticDenormalizedBenchmark() {
    // The benchmark configuration is very similar to the configuration using referencing above.
    return BenchmarkBuilder.builder()
        .writeSpecification(
            config ->
                config
                    .collectionName("warehouses")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("city", s -> s.alphaNumRandomLengthString(5, 15))
                            .field("street", s -> s.alphaNumRandomLengthString(7, 30))
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
                            // The pipe selects one document from the warehouse collection and
                            // embeds the complete document into the stock_items.warehouse field
                            .fieldFromPipe(
                                "warehouse",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("warehouses"),
                                        p -> p.selectByPath("$.[0]").toObject()))
                            // The pipe selects one document from the warehouse collection and
                            // embeds the complete document into the stock_items.warehouse field
                            .fieldFromPipe(
                                "product",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("products"),
                                        p -> p.selectByPath("$.[0]").toObject()))
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
                            // The pipe selects one document from the warehouse collection and
                            // embeds the complete document into the stock_items.warehouse field
                            .fieldFromPipe(
                                "warehouse",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("warehouses"),
                                        p -> p.selectByPath("$.[0]").toObject()))
                            // The pipe selects one document from the warehouse collection and
                            // embeds the complete document into the stock_items.warehouse field
                            .fieldFromPipe(
                                "product",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("products"),
                                        p -> p.selectByPath("$.[0]").toObject()))
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
                            .field("name", s -> s.alphaNumRandomLengthString(10, 100))
                            .field("price", s -> s.uniformIntSupplier(1, 2000))
                            .build()))
        .writeSpecification(
            config ->
                config
                    .collectionName("orders")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.alphaNumRandomLengthString(10, 30))
                            .field("city", s -> s.alphaNumRandomLengthString(5, 15))
                            .field("street", s -> s.alphaNumRandomLengthString(7, 30))
                            .field(
                                "postal_code",
                                () ->
                                    new StringValue(String.format("%5d", random.nextInt(1, 99999))))
                            // The pipe takes all selected documents from the stock_items collection
                            // and embeds them into the orders document.
                            .fieldFromPipe(
                                "stock_items",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("stock_items"),
                                        p -> p.selectByPath("$.[*]").toArray()))
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
                                                        IdDistributionFactory::unique)))))
        .primaryWriteSpecification(
            "write_customer",
            config ->
                config
                    .collectionName("customers")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            .field("name", s -> s.alphaNumRandomLengthString(10, 30))
                            // The pipe embeds all selected order documents into the customer
                            // document.
                            .fieldFromPipe(
                                "orders",
                                f ->
                                    f.selectCollection(
                                        new CollectionName("orders"),
                                        p -> p.selectByPath("$.[*]").toArray()))
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
                            // With the embedded data model the lookup stage can be dropped.
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
                                                            string("$$customer_id"))))))),
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
                                            object("$unwind", string("$stock_items")),
                                            object(
                                                "$replaceRoot",
                                                object("newRoot", string("$stock_items"))),
                                            object(
                                                "$group",
                                                object(
                                                    "_id",
                                                    object(
                                                        Map.of(
                                                            "city",
                                                            string("$warehouse.city"),
                                                            "street",
                                                            string("$warehouse.street"),
                                                            "postal_code",
                                                            string("$warehouse.postal_code"))))))),
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
                                                            string("$product._id"),
                                                            string("$$product_id"))))),
                                            object(
                                                "$lookup",
                                                object(
                                                    Map.of(
                                                        "from", string("orders"),
                                                        "localField", string("_id"),
                                                        "foreignField", string("stock_items._id"),
                                                        "as", string("order")))),
                                            object(
                                                "$match",
                                                object("order", object("$size", integer(0)))),
                                            object(
                                                "$group",
                                                object(
                                                    "_id",
                                                    object(
                                                        Map.of(
                                                            "city",
                                                            string("$warehouse.city"),
                                                            "street",
                                                            string("$warehouse.street"),
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
                            new CollectionName("customers"),
                            AggregationOptions.aggregate("customers")
                                .pipeline(
                                    List.of(
                                        object("$unwind", string("$orders")),
                                        object(
                                            "$replaceRoot", object("newRoot", string("$orders"))),
                                        object("$unwind", string("$stock_items")),
                                        object(
                                            "$replaceRoot",
                                            object("newRoot", string("$stock_items"))),
                                        object(
                                            "$group",
                                            object(
                                                "_id",
                                                object(
                                                    Map.of(
                                                        "city",
                                                        string("$warehouse.city"),
                                                        "street",
                                                        string("$warehouse.street"),
                                                        "postal_code",
                                                        string("$warehouse.postal_code"))),
                                                "revenue",
                                                object("$sum", string("product.price")))))))))
        .loadPhase(loadPhaseConfig())
        .database(config -> {})
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("orders"))
                .keys(object("stock_items._id", integer(1))))
        .indexConfiguration(
            IndexConfiguration.of(new CollectionName("stock_items"))
                .keys(object("product._id", integer(1))))
        .transactionPhase(transactionPhaseConfig())
        .build();
  }

  private static Consumer<BenchmarkBuilder.LoadPhaseConfig> loadPhaseConfig() {
    return loadPhaseConfig ->
        loadPhaseConfig
            // The customers collection should be populated with 729500 customers during the load
            // phase.
            // The later transaction phase creates on average the remaining 500 customers.
            .primaryWriteSpecification(
                (long) CUSTOMER_NUM - CUSTOMER_NUM_TRANSACTION, "write_customer")
            // The stock_items collection should be populated with 999315 unordered(unsold) stock
            // items.
            // The later transaction phase create on average the remaining 685 unordered stock
            // items.
            .primaryWriteSpecification(
                (long) STOCK_ITEM_NUM_UNORDERED - STOCK_ITEM_NUM_UNORDERED_TRANSACTION,
                "primary_stock_items")
            // Run 30 threads executing the primary write specifications in parallel during the load
            // phase.
            // The actual number of threads is much higher as all operations acting on referenced
            // documents run also in parallel.
            .numThreads(30);
  }

  private static Consumer<BenchmarkBuilder.TransactionPhaseConfig> transactionPhaseConfig() {
    return config ->
        // configure specifications to be run according to assigned weights
        config.weightedRandom(
            weightedRandom -> {
              var customerWeight = 10;
              var stockItemsWeight = 10;
              double totalCount = (2000.0 / customerWeight) * CUSTOMER_NUM_TRANSACTION;
              var productAvailabilityWeight = 1279;
              // We want to simulate 100000 (totalCount) operations in
              // 6h(SIMULATED_WORKLOAD_DURATION_HOURS).
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
                  // run in total 100000 transactions
                  .totalCount((int) totalCount)
                  // run this many operations per millisecond
                  .targetOps(targetOpsPerMilli)
                  // Run fifteen client threads concurrently.
                  // Each client thread is responsible for running 1/15th of the total count.
                  .threadCount(15);
            });
  }
}
