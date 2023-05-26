package de.claasklar;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import de.claasklar.database.mongodb.MongoDatabaseBuilder;
import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.ContextlessDocumentGeneratorBuilder;
import de.claasklar.generation.suppliers.Suppliers;
import de.claasklar.idStore.FileIdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.document.ExistingDocumentDistribution;
import de.claasklar.random.distribution.document.SimpleDocumentDistribution;
import de.claasklar.random.distribution.id.UniformIdDistribution;
import de.claasklar.random.distribution.reference.ConstantNumberReferencesDistribution;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.specification.PrimaryWriteSpecification;
import de.claasklar.specification.WriteSpecification;
import de.claasklar.specification.WriteSpecificationRegistry;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import org.slf4j.LoggerFactory;

public class Main {

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
    var allCollections = List.of(new CollectionName("test_collection"), new CollectionName("primary"));
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
            .field("number", (Suppliers s) -> s.uniformIntSupplier(0, 1000))
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
            tracer,
            Clock.systemUTC());

    var ids = new HashSet<>();

    try {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 1000; i++) {
        var runnable = primaryWriteSpecification.runnable();
        runnable.run();
        ids.add(runnable.getDocument().getId());
      }
      System.out.println(System.currentTimeMillis() - start);
      System.out.println("Total num ids: " + ids.size());
      System.out.println("version: " + TelemetryConfig.version());
    } catch(Exception e) {
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
