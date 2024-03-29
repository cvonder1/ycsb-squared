package de.claasklar.benchmark;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import de.claasklar.database.Database;
import de.claasklar.database.mongodb.MongoDatabaseBuilder;
import de.claasklar.generation.ContextDocumentGenerator;
import de.claasklar.generation.DocumentGenerator;
import de.claasklar.generation.QueryGenerator;
import de.claasklar.generation.suppliers.VariableSuppliers;
import de.claasklar.idStore.IdStore;
import de.claasklar.idStore.SparseInMemoryIdStore;
import de.claasklar.phase.*;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.random.distribution.Distribution;
import de.claasklar.random.distribution.LongDistributionFactory;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.document.ComputeDocumentDistribution;
import de.claasklar.random.distribution.document.DocumentDistribution;
import de.claasklar.random.distribution.document.ExistingDocumentDistribution;
import de.claasklar.random.distribution.document.SimpleDocumentDistribution;
import de.claasklar.random.distribution.id.IdDistribution;
import de.claasklar.random.distribution.id.IdDistributionFactory;
import de.claasklar.random.distribution.reference.DecoratorReferencesDistribution;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.specification.*;
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.extension.incubator.metrics.ExtendedLongHistogramBuilder;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

/** Class for building a benchmark, which can be run. */
public class BenchmarkBuilder {

  private Function<List<CollectionName>, Database> databaseSupplier;
  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final LongHistogram transactionDurationHistogram;
  private final DocumentGenerationSpecificationRegistry registry;
  private final List<DocumentGenerationSpecificationConfig> documentGenerationSpecificationConfigs;
  private final Map<String, PrimaryWriteSpecificationConfig> primaryWriteSpecificationConfigs;
  private final List<ReadSpecificationConfig> readSpecifications;
  private final List<IndexConfiguration> indexConfigurations;
  private final PhaseTopic phaseTopic;
  @NotNull private LoadPhaseConfig loadPhaseConfig;
  @NotNull private TransactionPhaseConfig transactionPhaseConfig;

  private Database database;
  private IdStore idStore;
  private ExecutorService executorService;
  private Clock clock = Clock.systemUTC();
  private Span applicationSpan;

  private BenchmarkBuilder() {
    this.openTelemetry = TelemetryConfig.buildOpenTelemetry();
    this.tracer =
        openTelemetry.getTracer(
            TelemetryConfig.INSTRUMENTATION_SCOPE_NAME, TelemetryConfig.version());
    this.transactionDurationHistogram =
        ((ExtendedLongHistogramBuilder)
                openTelemetry
                    .meterBuilder(TelemetryConfig.METRIC_SCOPE_NAME)
                    .setInstrumentationVersion(TelemetryConfig.version())
                    .build()
                    .histogramBuilder("transaction_duration")
                    .ofLongs())
            .setAdvice(
                advice -> advice.setExplicitBucketBoundaries(TelemetryConfig.bucketBoundaries()))
            .setUnit("ms")
            .setDescription(
                "Tracks duration of transactions across all specifications. Attributes give more detail about collection and operation.")
            .build();
    this.registry = new DocumentGenerationSpecificationRegistry();
    documentGenerationSpecificationConfigs = new LinkedList<>();
    primaryWriteSpecificationConfigs = new HashMap<>();
    readSpecifications = new LinkedList<>();
    indexConfigurations = new LinkedList<>();
    phaseTopic = new PhaseTopic();
  }

  public static BenchmarkBuilder builder() {
    return new BenchmarkBuilder();
  }

  public Benchmark build() {
    try (ValidatorFactory factory =
        Validation.byDefaultProvider()
            .configure()
            .messageInterpolator(new ParameterMessageInterpolator())
            .buildValidatorFactory()) {
      var validator = factory.getValidator();
      var validationResult =
          validator.validate(validator).stream()
              .map(ConstraintViolation::getMessage)
              .collect(Collectors.joining("\n"));
      if (!validationResult.isBlank()) {
        throw new IllegalArgumentException(validationResult);
      }
    }

    var allCollections =
        Stream.concat(
                documentGenerationSpecificationConfigs.stream()
                    .map(DocumentGenerationSpecificationConfig::getCollectionName),
                primaryWriteSpecificationConfigs.values().stream().map(it -> it.collectionName))
            .toList();
    database = databaseSupplier.apply(allCollections);
    phaseTopic.register(database);
    idStore = new SparseInMemoryIdStore();
    executorService = Executors.newVirtualThreadPerTaskExecutor();
    clock = Clock.systemUTC();
    applicationSpan = tracer.spanBuilder(TelemetryConfig.APPLICATION_SPAN_NAME).startSpan();
    var executorServices = new HashSet<ExecutorService>();

    var indexPhase =
        new IndexPhase(
            indexConfigurations.toArray(IndexConfiguration[]::new),
            database,
            applicationSpan,
            tracer);

    var allComputeDocumentDistributions = allComputeDocumentDistributions();
    var intersection = allWriteDocumentDistributions();
    intersection.retainAll(allComputeDocumentDistributions);
    if (!intersection.isEmpty()) {
      var violatingCollection = intersection.stream().findAny().get();
      throw new IllegalArgumentException(
          violatingCollection
              + " is configured to be both stored in the database and recomputed. This is not possible");
    }

    for (var computeSpecificationConfig : documentGenerationSpecificationConfigs) {
      DocumentGenerationSpecification specification;
      if (allComputeDocumentDistributions.contains(
          computeSpecificationConfig.getCollectionName())) {
        specification =
            new ComputeSpecification(
                computeSpecificationConfig.getCollectionName(),
                computeSpecificationConfig.getDocumentGenerator(),
                computeSpecificationConfig.getReferencesDistributionConfigs().stream()
                    .map(
                        it -> {
                          var distribution = this.buildReferencesDistribution(it);
                          executorServices.add(distribution.second());
                          return distribution.first();
                        })
                    .toArray(ReferencesDistribution[]::new),
                idStore,
                executorService,
                tracer);
      } else {
        specification =
            new WriteSpecification(
                computeSpecificationConfig.getCollectionName(),
                computeSpecificationConfig.getDocumentGenerator(),
                computeSpecificationConfig.getReferencesDistributionConfigs().stream()
                    .map(
                        it -> {
                          var distribution = this.buildReferencesDistribution(it);
                          executorServices.add(distribution.second());
                          return distribution.first();
                        })
                    .toArray(ReferencesDistribution[]::new),
                database,
                idStore,
                executorService,
                transactionDurationHistogram,
                tracer,
                clock);
      }
      registry.register(specification);
    }

    var topLevelSpecifications = new HashMap<String, TopSpecification>();
    var primaryWriteSpecificaitons = new HashMap<String, PrimaryWriteSpecification>();
    for (var primaryConfigEntry : primaryWriteSpecificationConfigs.entrySet()) {
      var primaryConfig = primaryConfigEntry.getValue();
      var specification =
          new PrimaryWriteSpecification(
              primaryConfig.collectionName,
              primaryConfig.idShift,
              primaryConfig.referencesDistributionConfigs.stream()
                  .map(
                      it -> {
                        var distribution = this.buildReferencesDistribution(it);
                        executorServices.add(distribution.second());
                        return distribution.first();
                      })
                  .toArray(ReferencesDistribution[]::new),
              primaryConfig.documentGenerator,
              database,
              executorService,
              transactionDurationHistogram,
              idStore,
              tracer,
              clock);
      topLevelSpecifications.put(primaryConfigEntry.getKey(), specification);
      primaryWriteSpecificaitons.put(primaryConfigEntry.getKey(), specification);
    }

    var variableSuppliers = new VariableSuppliers(idStore);
    for (var readSpecificationConfig : readSpecifications) {
      var specification =
          new ReadSpecification(
              readSpecificationConfig.name,
              readSpecificationConfig.queryGeneratorFunction.apply(
                  variableSuppliers, new IdDistributionFactory()),
              database,
              transactionDurationHistogram,
              tracer,
              clock);
      topLevelSpecifications.put(readSpecificationConfig.name, specification);
    }

    topLevelSpecifications.values().forEach(phaseTopic::register);

    var loadPhase =
        new LoadPhase(
            loadPhaseConfig.primaryWriteSpecifications.stream()
                .map(it -> new Pair<>(it.first(), primaryWriteSpecificaitons.get(it.second())))
                .collect(Collectors.toList()),
            loadPhaseConfig.numThreads,
            applicationSpan,
            tracer);

    var transactionPhase = buildTransactionPhase(topLevelSpecifications);

    return new Benchmark(
        indexPhase,
        loadPhase,
        transactionPhase,
        database,
        executorServices.stream().filter(Objects::nonNull).toList(),
        applicationSpan,
        phaseTopic);
  }

  private Set<@NotNull CollectionName> allWriteDocumentDistributions() {
    return Stream.concat(
            primaryWriteSpecificationConfigs.values().stream()
                .flatMap(it -> it.referencesDistributionConfigs.stream()),
            documentGenerationSpecificationConfigs.stream()
                .flatMap(it -> it.referencesDistributionConfigs.stream()))
        .map(it -> it.documentDistributionConfig)
        .filter(it -> it.recomputableDocumentDistributionConfig == null)
        .map(it -> it.collectionName)
        .collect(Collectors.toSet());
  }

  private Set<@NotNull CollectionName> allComputeDocumentDistributions() {
    return Stream.concat(
            primaryWriteSpecificationConfigs.values().stream()
                .flatMap(it -> it.referencesDistributionConfigs.stream()),
            documentGenerationSpecificationConfigs.stream()
                .flatMap(it -> it.referencesDistributionConfigs.stream()))
        .map(it -> it.documentDistributionConfig)
        .filter(it -> it.recomputableDocumentDistributionConfig != null)
        .map(it -> it.collectionName)
        .collect(Collectors.toSet());
  }

  private Pair<ReferencesDistribution, ExecutorService> buildReferencesDistribution(
      ReferencesDistributionConfig config) {
    var documentDistribution = buildDocumentDistribution(config.documentDistributionConfig);
    return new Pair<>(
        new DecoratorReferencesDistribution(
            config.countDistribution, documentDistribution.first(), executorService),
        documentDistribution.second());
  }

  private Pair<DocumentDistribution, ExecutorService> buildDocumentDistribution(
      DocumentDistributionConfig config) {
    if (config.existingDocumentDistributionConfig != null) {
      var simpleDocumentDistribution =
          new SimpleDocumentDistribution(
              config.collectionName, config.idDistribution, idStore, database, registry, tracer);
      ExecutorService existingExecutorService =
          config.existingDocumentDistributionConfig.executorService;
      if (existingExecutorService == null) {
        existingExecutorService = executorService;
      }
      var existingDocumentDistribution =
          new ExistingDocumentDistribution(
              config.existingDocumentDistributionConfig.bufferSize,
              simpleDocumentDistribution,
              database,
              existingExecutorService,
              tracer);
      phaseTopic.register(existingDocumentDistribution);
      return new Pair<>(existingDocumentDistribution, existingExecutorService);
    } else if (config.recomputableDocumentDistributionConfig != null) {
      return new Pair<>(
          new ComputeDocumentDistribution(config.collectionName, config.idDistribution, registry),
          null);
    }
    return new Pair<>(
        new SimpleDocumentDistribution(
            config.collectionName, config.idDistribution, idStore, database, registry, tracer),
        null);
  }

  private TransactionPhase buildTransactionPhase(Map<String, TopSpecification> topSpecifications) {
    if (transactionPhaseConfig.powerTestTransactionPhaseConfig != null) {
      return new PowerTestTransactionPhase(
          transactionPhaseConfig.powerTestTransactionPhaseConfig.specificationNames.stream()
              .map(
                  name -> {
                    var spec = topSpecifications.get(name);
                    if (spec == null) {
                      throw new IllegalArgumentException(
                          "cannot find specification with the name " + name);
                    }
                    return spec;
                  })
              .toList(),
          applicationSpan,
          tracer);
    } else if (transactionPhaseConfig.weightedRandomTransactionPhaseConfig != null) {
      var config = transactionPhaseConfig.weightedRandomTransactionPhaseConfig;
      return new WeightedRandomTransactionPhase(
          config.totalCount,
          config.threadCount,
          config.targetOps,
          config.weightedSpecifications.stream()
              .map(
                  weightAndName -> {
                    var spec = topSpecifications.get(weightAndName.second());
                    if (spec == null) {
                      throw new IllegalArgumentException(
                          "cannot find specification with the name " + weightAndName.second());
                    }
                    return weightAndName.mapSecond(it -> spec);
                  })
              .toList(),
          new StdRandomNumberGenerator(),
          applicationSpan,
          tracer);
    } else {
      throw new IllegalArgumentException("must specify type of transaction phase");
    }
  }

  /**
   * Configure MongoDB Database. Configuration includes read/write concerns and connection settings.
   *
   * @param config applied to the mongo configuration
   * @return this
   */
  public BenchmarkBuilder database(Consumer<MongoConfiguration> config) {
    var mongoConfiguration = new MongoConfiguration();
    config.accept(mongoConfiguration);
    this.databaseSupplier =
        (allCollections) -> {
          var builder =
              MongoDatabaseBuilder.builder()
                  .databaseName(TelemetryConfig.version())
                  .connectionString(mongoConfiguration.connectionString)
                  .collections(allCollections)
                  .databaseReadConcern(mongoConfiguration.databaseReadConcern)
                  .databaseWriteConcern(mongoConfiguration.databaseWriteConcern)
                  .databaseReadPreference(mongoConfiguration.databaseReadPreference)
                  .tracer(tracer)
                  .openTelemetry(openTelemetry);
          for (var collectionReadPreference :
              mongoConfiguration.collectionsReadPreferences.entrySet()) {
            builder.collectionReadPreference(
                collectionReadPreference.getKey(), collectionReadPreference.getValue());
          }
          for (var collectionReadConcern : mongoConfiguration.collectionsReadConcerns.entrySet()) {
            builder.collectionReadConcern(
                collectionReadConcern.getKey(), collectionReadConcern.getValue());
          }
          for (var collectionWriteConcern :
              mongoConfiguration.collectionsWriteConcerns.entrySet()) {
            builder.collectionWriteConcerns(
                collectionWriteConcern.getKey(), collectionWriteConcern.getValue());
          }
          return builder.build();
        };
    return this;
  }

  public static class MongoConfiguration {
    private final Map<CollectionName, ReadPreference> collectionsReadPreferences = new HashMap<>();
    private final Map<CollectionName, ReadConcern> collectionsReadConcerns = new HashMap<>();
    private final Map<CollectionName, WriteConcern> collectionsWriteConcerns = new HashMap<>();
    private ReadPreference databaseReadPreference = ReadPreference.nearest();
    private ReadConcern databaseReadConcern = ReadConcern.DEFAULT;
    private WriteConcern databaseWriteConcern = WriteConcern.JOURNALED;
    private ConnectionString connectionString = new ConnectionString("mongodb://mongodb");

    /**
     * <a href="https://www.mongodb.com/docs/manual/core/read-preference/">MongoDB Doc</a>
     *
     * @param collectionName collection the config is applied to
     * @param readPreference selected read preference
     * @return this
     */
    public MongoConfiguration collectionReadPreference(
        CollectionName collectionName, ReadPreference readPreference) {
      collectionsReadPreferences.put(collectionName, readPreference);
      return this;
    }

    /**
     * "The readConcern option allows you to control the consistency and isolation properties of the
     * data read from replica sets and replica set shards." from <a
     * href="https://www.mongodb.com/docs/manual/reference/read-concern/">MongoDB Doc</a>.
     *
     * @param collectionName collection the config is applied to
     * @param readConcern selected read concern
     * @return this
     */
    public MongoConfiguration collectionReadConcern(
        CollectionName collectionName, ReadConcern readConcern) {
      collectionsReadConcerns.put(collectionName, readConcern);
      return this;
    }

    /**
     * "Write concern describes the level of acknowledgment requested from MongoDB for write
     * operations to a standalone mongod or to Replica sets or to sharded clusters." from <a
     * href="https://www.mongodb.com/docs/manual/reference/write-concern/">MongoDB Doc</a>.
     *
     * @param collectionName collection the config is applied to
     * @param writeConcern selected write concern
     * @return this
     */
    public MongoConfiguration collectionWriteConcern(
        CollectionName collectionName, WriteConcern writeConcern) {
      collectionsWriteConcerns.put(collectionName, writeConcern);
      return this;
    }

    /**
     * Set default read preference for database.
     *
     * @param readPreference selected read preference
     * @return this
     */
    public MongoConfiguration databaseReadPreference(ReadPreference readPreference) {
      this.databaseReadPreference = readPreference;
      return this;
    }

    /**
     * Set default read concern for database.
     *
     * @param readConcern selected read concern
     * @return this
     */
    public MongoConfiguration databaseReadConcern(ReadConcern readConcern) {
      this.databaseReadConcern = readConcern;
      return this;
    }

    /**
     * Set default write concern for database
     *
     * @param writeConcern selected write concern
     * @return this
     */
    public MongoConfiguration databaseWriteConcern(WriteConcern writeConcern) {
      this.databaseWriteConcern = writeConcern;
      return this;
    }

    /**
     * Connection string for connecting to database. Default: "mongodb://mongodb"
     *
     * @param connectionString address to connect to
     * @return this
     */
    public MongoConfiguration connectionString(ConnectionString connectionString) {
      this.connectionString = connectionString;
      return this;
    }
  }

  /**
   * Configure a {@link de.claasklar.specification.WriteSpecification WriteSpecification} to be used
   * in this benchmark.
   *
   * @see de.claasklar.specification.WriteSpecification
   * @see DocumentGenerationSpecificationConfig
   * @param configConsumer applied to the DocumentGenerationSpecificationConfig
   * @return this
   */
  public BenchmarkBuilder writeSpecification(
      Consumer<DocumentGenerationSpecificationConfig> configConsumer) {
    var config = new DocumentGenerationSpecificationConfig();
    configConsumer.accept(config);
    documentGenerationSpecificationConfigs.add(config);
    return this;
  }

  /**
   * Configure primary write specification. In contrast to a {@link WriteSpecification} a {@link
   * PrimaryWriteSpecification} marks the top of a document hierarchy. The primary write
   * specification can be called in both the load and transaction phase.
   *
   * @param name name of the specification
   * @param configConsumer applies configuration
   * @return this
   */
  public BenchmarkBuilder primaryWriteSpecification(
      String name, Consumer<PrimaryWriteSpecificationConfig> configConsumer) {
    var config = new PrimaryWriteSpecificationConfig();
    configConsumer.accept(config);
    primaryWriteSpecificationConfigs.put(name, config);
    return this;
  }

  /** Configure a {@link WriteSpecification} */
  public static class DocumentGenerationSpecificationConfig {
    @NotNull private CollectionName collectionName;
    @NotNull private DocumentGenerator documentGenerator;
    private List<ReferencesDistributionConfig> referencesDistributionConfigs;

    private DocumentGenerationSpecificationConfig() {
      referencesDistributionConfigs = new LinkedList<>();
    }

    /**
     * Set the specification's collection name.
     *
     * @param collectionName collection this specificaiton is for
     * @return this
     */
    public DocumentGenerationSpecificationConfig collectionName(String collectionName) {
      this.collectionName = new CollectionName(collectionName);
      return this;
    }

    /**
     * Set the document generator used for generating the document. The generator is invoked each
     * time a new document for this specification is requested.
     *
     * @see de.claasklar.generation.ContextDocumentGeneratorBuilder
     * @see de.claasklar.generation.ContextlessDocumentGeneratorBuilder
     * @param documentGenerator generates the documents
     * @return this
     */
    public DocumentGenerationSpecificationConfig documentGenerator(
        DocumentGenerator documentGenerator) {
      this.documentGenerator = documentGenerator;
      return this;
    }

    /**
     * Configure which and how many documents are selected from another collection.
     *
     * @param configConsumer configuration
     * @return this
     */
    public DocumentGenerationSpecificationConfig referenceDistributionConfig(
        Consumer<ReferencesDistributionConfig> configConsumer) {
      var config = new ReferencesDistributionConfig();
      configConsumer.accept(config);
      this.referencesDistributionConfigs.add(config);
      return this;
    }

    public CollectionName getCollectionName() {
      return collectionName;
    }

    public DocumentGenerator getDocumentGenerator() {
      return documentGenerator;
    }

    public List<ReferencesDistributionConfig> getReferencesDistributionConfigs() {
      return referencesDistributionConfigs;
    }
  }

  public static class PrimaryWriteSpecificationConfig {
    @NotNull private CollectionName collectionName;
    @NotNull private ContextDocumentGenerator documentGenerator;
    private List<ReferencesDistributionConfig> referencesDistributionConfigs;
    @Positive private long idShift = 0;

    private PrimaryWriteSpecificationConfig() {
      referencesDistributionConfigs = new LinkedList<>();
    }

    /**
     * Set the specification's collection name.
     *
     * @param collectionName collection this specificaiton is for
     * @return this
     */
    public PrimaryWriteSpecificationConfig collectionName(String collectionName) {
      this.collectionName = new CollectionName(collectionName);
      return this;
    }

    /**
     * Set the document generator used for generating the document. The generator is invoked each
     * time a new document for this specification is requested.
     *
     * @see de.claasklar.generation.ContextDocumentGeneratorBuilder
     * @see de.claasklar.generation.ContextlessDocumentGeneratorBuilder
     * @param documentGenerator generates the documents
     * @return this
     */
    public PrimaryWriteSpecificationConfig documentGenerator(
        ContextDocumentGenerator documentGenerator) {
      this.documentGenerator = documentGenerator;
      return this;
    }

    /**
     * Configure which and how many documents are selected from another collection.
     *
     * @param configConsumer configuration
     * @return this
     */
    public PrimaryWriteSpecificationConfig referenceDistributionConfig(
        Consumer<ReferencesDistributionConfig> configConsumer) {
      var config = new ReferencesDistributionConfig();
      configConsumer.accept(config);
      this.referencesDistributionConfigs.add(config);
      return this;
    }

    /**
     * Shift the ids by the given value.
     *
     * @param idShift value added to ids
     * @return this
     */
    public PrimaryWriteSpecificationConfig idShift(long idShift) {
      this.idShift = idShift;
      return this;
    }
  }

  public static class ReferencesDistributionConfig {

    @NotNull private Distribution<Long> countDistribution;

    private final DocumentDistributionConfig documentDistributionConfig =
        new DocumentDistributionConfig();

    private ReferencesDistributionConfig() {}

    /**
     * Always select the same number of documents from the target collection.
     *
     * @param constantNumber number of documents to select
     * @return this
     */
    public ReferencesDistributionConfig constantNumber(int constantNumber) {
      this.countDistribution = new LongDistributionFactory().constant(constantNumber);
      return this;
    }

    /**
     * Configure which documents get selected
     *
     * @param configConsumer configuration
     * @return this
     */
    public ReferencesDistributionConfig documentDistribution(
        Consumer<DocumentDistributionConfig> configConsumer) {
      configConsumer.accept(documentDistributionConfig);
      return this;
    }

    /**
     * Determine amount of selected documents by sampling the provided {@link Distribution}.
     *
     * @see LongDistributionFactory
     * @param countDistributionFactory factory for creating Distribution
     * @return this
     */
    public ReferencesDistributionConfig countDistribution(
        Function<LongDistributionFactory, Distribution<Long>> countDistributionFactory) {
      this.countDistribution = countDistributionFactory.apply(new LongDistributionFactory());
      return this;
    }
  }

  public static class DocumentDistributionConfig {
    @NotNull private CollectionName collectionName;
    @NotNull private IdDistribution idDistribution;

    private ExistingDocumentDistributionConfig existingDocumentDistributionConfig;
    private RecomputableDocumentDistributionConfig recomputableDocumentDistributionConfig;

    private DocumentDistributionConfig() {}

    /**
     * Set the collection name to select documents from.
     *
     * @param collectionName target collection's name
     * @return this
     */
    public DocumentDistributionConfig collectionName(String collectionName) {
      this.collectionName = new CollectionName(collectionName);
      return this;
    }

    /**
     * Configure which documents are chosen from the target collection.
     *
     * @param factory creates IdDistribution
     * @return this
     */
    public DocumentDistributionConfig idDistribution(
        Function<IdDistributionFactory, IdDistribution> factory) {
      idDistribution = factory.apply(new IdDistributionFactory());
      return this;
    }

    @Deprecated
    public ExistingDocumentDistributionConfig existing() {
      recomputableDocumentDistributionConfig = null;
      existingDocumentDistributionConfig = new ExistingDocumentDistributionConfig();
      return existingDocumentDistributionConfig;
    }

    /**
     * Recompute the target documents on demand instead of storing the documents in the database.
     * This can be done deterministically by seeding the RNG with the target document's id.
     *
     * @return recomputable config
     */
    public RecomputableDocumentDistributionConfig recomputable() {
      existingDocumentDistributionConfig = null;
      recomputableDocumentDistributionConfig = new RecomputableDocumentDistributionConfig();
      return recomputableDocumentDistributionConfig;
    }
  }

  public static class ExistingDocumentDistributionConfig {
    private ExecutorService executorService;

    @NotNull @Min(1)
    private int bufferSize = 50;

    public ExistingDocumentDistributionConfig executorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    public ExistingDocumentDistributionConfig bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }
  }

  public static class RecomputableDocumentDistributionConfig {}

  /**
   * Configure a {@link ReadSpecification}. A read specification can either resemble a find or
   * aggregate query. The read specifications can be called during the transaction phase.
   *
   * @param configConsumer specification config
   * @return this
   */
  public BenchmarkBuilder readSpecification(Consumer<ReadSpecificationConfig> configConsumer) {
    var config = new ReadSpecificationConfig();
    configConsumer.accept(config);
    readSpecifications.add(config);
    return this;
  }

  public static class ReadSpecificationConfig {
    @NotNull private String name;

    @NotNull private BiFunction<VariableSuppliers, IdDistributionFactory, QueryGenerator>
        queryGeneratorFunction;

    /**
     * Set the read specification's name
     *
     * @param name name for later reference
     * @return this
     */
    public ReadSpecificationConfig name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the query generator.
     *
     * @param queryGenerator
     * @return this
     */
    public ReadSpecificationConfig queryGenerator(QueryGenerator queryGenerator) {
      this.queryGeneratorFunction = (v, i) -> queryGenerator;
      return this;
    }

    /**
     * Set the query generator.
     *
     * @param config factory for a query generator
     * @return this
     */
    public ReadSpecificationConfig queryGenerator(
        BiFunction<VariableSuppliers, IdDistributionFactory, QueryGenerator> config) {
      queryGeneratorFunction = config;
      return this;
    }
  }

  /**
   * Add index configuration, which should be applied to the database.
   * @param indexConfiguration index to be created
   * @return this
   */
  public BenchmarkBuilder indexConfiguration(IndexConfiguration indexConfiguration) {
    indexConfigurations.add(indexConfiguration);
    return this;
  }

  /**
   * Configure the load phase.
   * During the load phase a share of the total data is loaded into the database.
   * @param configConsumer configuration
   * @return this
   */
  public BenchmarkBuilder loadPhase(Consumer<LoadPhaseConfig> configConsumer) {
    this.loadPhaseConfig = new LoadPhaseConfig();
    configConsumer.accept(this.loadPhaseConfig);
    return this;
  }

  public static class LoadPhaseConfig {
    @NotEmpty
    private final List<Pair<Long, String>> primaryWriteSpecifications = new LinkedList<>();

    @Min(1)
    private int numThreads = 10;

    /**
     * Add primary write specification to the load phase and set the number of invocations.
     * @param targetCount number of invocations
     * @param primaryWriteSpecificationName name set by {@link BenchmarkBuilder#primaryWriteSpecification(String, Consumer)}
     * @return this
     */
    public LoadPhaseConfig primaryWriteSpecification(
        long targetCount, String primaryWriteSpecificationName) {
      primaryWriteSpecifications.add(new Pair<>(targetCount, primaryWriteSpecificationName));
      return this;
    }

    /**
     * Number of concurrent client threads.
     * Each client thread executes one primary write specification at a time.
     * However, the number of concurrent threads can be greater, as each write specification calls other write specification and executes them in parallel.
     * @param numThreads number of client threads
     * @return this
     */
    public LoadPhaseConfig numThreads(int numThreads) {
      this.numThreads = numThreads;
      return this;
    }
  }

  /**
   * Configure the transaction phase.
   * During the transaction phase a mixture of primary write and read specifications are called, to simulate a given workload.
   * @param configConsumer transaction phase configuration
   * @return this
   */
  public BenchmarkBuilder transactionPhase(Consumer<TransactionPhaseConfig> configConsumer) {
    this.transactionPhaseConfig = new TransactionPhaseConfig();
    configConsumer.accept(this.transactionPhaseConfig);
    return this;
  }

  public static class TransactionPhaseConfig {
    private PowerTestTransactionPhaseConfig powerTestTransactionPhaseConfig;
    private WeightedRandomTransactionPhaseConfig weightedRandomTransactionPhaseConfig;

    private TransactionPhaseConfig() {}

    /**
     * Run a power test during the transaction phase.
     * @param configConsumer power test config
     * @return this
     */
    public TransactionPhaseConfig powerTest(
        Consumer<PowerTestTransactionPhaseConfig> configConsumer) {
      this.weightedRandomTransactionPhaseConfig = null;
      this.powerTestTransactionPhaseConfig = new PowerTestTransactionPhaseConfig();
      configConsumer.accept(this.powerTestTransactionPhaseConfig);
      return this;
    }

    /**
     * Run specifications according to their assigned weights.
     * @param configConsumer config with weights
     * @return this
     */
    public TransactionPhaseConfig weightedRandom(
        Consumer<WeightedRandomTransactionPhaseConfig> configConsumer) {
      this.powerTestTransactionPhaseConfig = null;
      this.weightedRandomTransactionPhaseConfig = new WeightedRandomTransactionPhaseConfig();
      configConsumer.accept(this.weightedRandomTransactionPhaseConfig);
      return this;
    }
  }

  public static class PowerTestTransactionPhaseConfig {
    private final List<String> specificationNames = new LinkedList<>();

    private PowerTestTransactionPhaseConfig() {}

    /**
     * Append a single specification to the list of specifications, that should be run.
     * The specification can either be a read specification or a primary write specification.
     * The name is the same as in {@link ReadSpecificationConfig#name(String)} or {@link BenchmarkBuilder#primaryWriteSpecification(String, Consumer)}
     * @param topLevelSpecificationName specification name
     * @return this
     */
    public PowerTestTransactionPhaseConfig specification(String topLevelSpecificationName) {
      specificationNames.add(topLevelSpecificationName);
      return this;
    }

    /**
     * Append a multiple specification to the list of specifications, that should be run.
     * The specification can either be a read specification or a primary write specification.
     * The name is the same as in {@link ReadSpecificationConfig#name(String)} or {@link BenchmarkBuilder#primaryWriteSpecification(String, Consumer)}
     * @param topLevelSpecificationNames specification names
     * @return this
     */
    public PowerTestTransactionPhaseConfig specification(String... topLevelSpecificationNames) {
      specificationNames.addAll(Arrays.asList(topLevelSpecificationNames));
      return this;
    }
  }

  public static class WeightedRandomTransactionPhaseConfig {
    @NotNull @Positive private Long totalCount;
    @NotNull @Positive private Integer threadCount;
    @NotNull @Positive private Double targetOps;
    private final List<Pair<@Positive Double, String>> weightedSpecifications;

    private WeightedRandomTransactionPhaseConfig() {
      weightedSpecifications = new LinkedList<>();
    }

    /**
     * Set the number of total operations.
     * In each iteration one operation is chosen from the weighted specification list and executed.
     * @param totalCount number of total iterations
     * @return this
     */
    public WeightedRandomTransactionPhaseConfig totalCount(long totalCount) {
      this.totalCount = totalCount;
      return this;
    }

    /**
     * Set number of concurrent client threads.
     * Each client thread executes one specification at a time.
     * However, the number of concurrent threads can be larger, because any {@link WriteSpecification} can run multiple other WriteSpecifications concurrently.
     * @param threadCount number of client threads
     * @return this
     */
    public WeightedRandomTransactionPhaseConfig threadCount(int threadCount) {
      this.threadCount = threadCount;
      return this;
    }

    /**
     * Set number of operations per millisecond.
     * @param targetOps number of operations per millisecond.
     * @return this
     */
    public WeightedRandomTransactionPhaseConfig targetOps(double targetOps) {
      this.targetOps = targetOps;
      return this;
    }

    /**
     * Add one primary write specification or read specification with weight to the list of specifications.
     * The name is the same as in {@link ReadSpecificationConfig#name(String)} or {@link BenchmarkBuilder#primaryWriteSpecification(String, Consumer)}
     * @param weight assigned weight
     * @param specificationName read or primary write specification name
     * @return this
     */
    public WeightedRandomTransactionPhaseConfig weightedSpecification(
        double weight, String specificationName) {
      weightedSpecifications.add(new Pair<>(weight, specificationName));
      return this;
    }
  }
}
