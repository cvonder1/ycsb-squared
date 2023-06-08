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
import de.claasklar.phase.load.LoadPhase;
import de.claasklar.phase.load.TransactionPhase;
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
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
    var threadExecutor = Context.current().wrap(Executors.newVirtualThreadPerTaskExecutor());
    var bufferedThreadExecutor =
        Context.current().wrap(Executors.newVirtualThreadPerTaskExecutor());

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

    var loadPhase = new LoadPhase(primaryWriteSpecification, 800, applicationSpan, tracer);
    var transactionPhase =
        new TransactionPhase(
            1000,
            2,
            20,
            List.of(
                new Pair<>(0.2, primaryWriteSpecification),
                new Pair<>(0.6, readSpecification),
                new Pair<>(0.1, countAggregationReadSpecification),
                new Pair<>(0.1, findOneReadSpecification)),
            new StdRandomNumberGenerator(),
            applicationSpan,
            tracer);

    try {
      loadPhase.load();
      transactionPhase.run();
      logger.atInfo().log(() -> "version: " + TelemetryConfig.version());
      Thread.sleep(Duration.ofSeconds(10));
      threadExecutor.shutdown();
      bufferedThreadExecutor.shutdown();
      var threadExecutorShutDown = threadExecutor.awaitTermination(1, TimeUnit.MINUTES);
      if (!threadExecutorShutDown) {
        logger.atWarn().log("could not terminate threadExecutor withing one minute");
      }
      var bufferedExecutorShutDown = bufferedThreadExecutor.awaitTermination(1, TimeUnit.MINUTES);
      if (!bufferedExecutorShutDown) {
        logger.atWarn().log("could not terminate bufferedExecutor withing one minute");
      }
    } catch (Exception e) {
      logger.atError().log(e.getMessage());
      applicationSpan.recordException(e);
    } finally {
      threadExecutor.shutdownNow();
      bufferedThreadExecutor.shutdownNow();
      applicationSpan.end();
      database.close();
      Thread.sleep(Duration.ofSeconds(10));
    }
  }
}
