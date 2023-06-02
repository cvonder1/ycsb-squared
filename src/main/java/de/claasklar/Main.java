package de.claasklar;

import static com.mongodb.client.model.Filters.*;
import static de.claasklar.util.BsonUtil.asOurBson;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Aggregates;
import de.claasklar.database.mongodb.MongoDatabaseBuilder;
import de.claasklar.generation.*;
import de.claasklar.generation.suppliers.ValueSuppliers;
import de.claasklar.generation.suppliers.VariableSuppliers;
import de.claasklar.idStore.FileIdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.primitives.query.FindOptions;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.document.ExistingDocumentDistribution;
import de.claasklar.random.distribution.document.SimpleDocumentDistribution;
import de.claasklar.random.distribution.id.UniformIdDistribution;
import de.claasklar.random.distribution.reference.ConstantNumberReferencesDistribution;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.specification.PrimaryWriteSpecification;
import de.claasklar.specification.ReadSpecification;
import de.claasklar.specification.WriteSpecification;
import de.claasklar.specification.WriteSpecificationRegistry;
import de.claasklar.util.BsonUtil;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws InterruptedException {
    var openTelemetry = TelemetryConfig.buildOpenTelemetry();
    LoggerFactory.getLogger(Main.class).atInfo().log("Hello, we started!");
    var transactionDurationHistogram =
        openTelemetry
            .meterBuilder(TelemetryConfig.METRIC_SCOPE_NAME)
            .setInstrumentationVersion(TelemetryConfig.version())
            .build()
            .histogramBuilder("transaction_duration")
            .ofLongs()
            .setUnit("us")
            .setDescription(
                "Tracks duration of transactions across all specifications. Attributes give more detail about collection and operation.")
            .build();
    var tracer =
        openTelemetry.getTracer(
            TelemetryConfig.INSTRUMENTATION_SCOPE_NAME, TelemetryConfig.version());
    var applicationSpan = tracer.spanBuilder(TelemetryConfig.APPLICATION_SPAN_NAME).startSpan();

    var idStore = new FileIdStore();
    var allCollections =
        List.of(new CollectionName("test_collection"), new CollectionName("primary"));
    var database =
        MongoDatabaseBuilder.builder()
            .databaseName(TelemetryConfig.version())
            .connectionString(new ConnectionString("mongodb://mongodb"))
            .openTelemetry(openTelemetry)
            .tracer(tracer)
            .collections(allCollections)
            .databaseReadConcern(ReadConcern.DEFAULT)
            .databaseWriteConcern(WriteConcern.JOURNALED)
            .databaseReadPreference(ReadPreference.nearest())
            .collectionReadConcern(new CollectionName("test_collection"), ReadConcern.AVAILABLE)
            .build();
    var collectionName = new CollectionName("test_collection");
    var threadExecutor = Context.current().wrap(Executors.newFixedThreadPool(5));
    var bufferedThreadExecutor = Context.current().wrap(Executors.newFixedThreadPool(20));

    var registry = new WriteSpecificationRegistry();
    var documentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field("number", (ValueSuppliers s) -> s.uniformIntSupplier(0, 1000))
            .build();
    var writeSpec =
        new WriteSpecification(
            collectionName,
            documentGenerator,
            database,
            idStore,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());
    registry.register(writeSpec);

    var idDistribution = new UniformIdDistribution(5000, new StdRandomNumberGenerator());
    var documentDistribution =
        new SimpleDocumentDistribution(
            collectionName, idDistribution, idStore, database, registry, tracer);
    var existingDocumentDistribution =
        new ExistingDocumentDistribution(
            50, documentDistribution, database, bufferedThreadExecutor, tracer);
    var referencesDistribution =
        new ConstantNumberReferencesDistribution(5, existingDocumentDistribution);

    var primaryDocumentGenerator =
        ContextDocumentGeneratorBuilder.builder()
            .field("s1", (s) -> s.uniformLengthStringSupplier(5, 10))
            .fieldFromPipe(
                "secondary",
                p ->
                    p.selectCollection(
                        collectionName,
                        pipeBuilder -> pipeBuilder.selectByPath("$.[*]._id").toArray()))
            .build();
    var primaryWriteSpecification =
        new PrimaryWriteSpecification(
            new CollectionName("primary"),
            new ReferencesDistribution[] {referencesDistribution},
            primaryDocumentGenerator,
            database,
            threadExecutor,
            transactionDurationHistogram,
            idStore,
            tracer,
            Clock.systemUTC());

    var queryGenerator =
        new SameFindGenerator(
            new CollectionName("test_collection"),
            FindOptions.find().filter(asOurBson(gt("number", 900))));
    var readSpecification =
        new ReadSpecification(
            "big_number_query",
            queryGenerator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var variableSuppliers = new VariableSuppliers(idStore);
    var idQueryGenerator =
        new VariableFindGenerator(
            new CollectionName("primary"),
            FindOptions.find()
                .filter(
                    asOurBson(
                        expr(
                            new BsonDocument(
                                "$eq",
                                new BsonArray(
                                    List.of(
                                        new BsonString("$_id"),
                                        new BsonString("$$primary_id"))))))),
            variableSuppliers.existingId(
                "primary_id",
                new CollectionName("primary"),
                new UniformIdDistribution(1000, new StdRandomNumberGenerator())));
    var findOneReadSpecification =
        new ReadSpecification(
            "find_one_primary",
            idQueryGenerator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var countAggregationQueryGenerator =
        new SameAggregationGenerator(
            new CollectionName("test_collection"),
            AggregationOptions.aggregate("test_collection")
                .pipeline(List.of(BsonUtil.asOurBson(Aggregates.count("num_test_collection")))));
    var countAggregationReadSpecification =
        new ReadSpecification(
            "count_test_collection",
            countAggregationQueryGenerator,
            database,
            transactionDurationHistogram,
            tracer,
            Clock.systemUTC());

    var ids = new HashSet<>();

    try {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        var runnable = primaryWriteSpecification.runnable();
        logger.atDebug().log("running primaryWriteSpecification");
        runnable.run();
        ids.add(runnable.getDocument().getId());
        logger.atDebug().log("running readRunnable");
        var readRunnable = readSpecification.runnable();
        readRunnable.run();
        countAggregationReadSpecification.runnable().run();
        if (i > 800) {
          logger.atDebug().log("running findOneReadSpecification");
          findOneReadSpecification.runnable().run();
        }
      }
      System.out.println(System.currentTimeMillis() - start);
      System.out.println("Total num ids: " + ids.size());
      System.out.println("version: " + TelemetryConfig.version());
    } catch (Exception e) {
      applicationSpan.recordException(e);
    } finally {
      threadExecutor.shutdown();
      bufferedThreadExecutor.shutdown();
      applicationSpan.end();
      database.close();
      Thread.sleep(Duration.ofSeconds(10));
    }
  }
}
