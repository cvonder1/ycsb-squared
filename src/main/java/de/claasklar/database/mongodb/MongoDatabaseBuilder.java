package de.claasklar.database.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import de.claasklar.database.mongodb.codec.OurDocumentCodecRegistry;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.util.MapCollector;
import de.claasklar.util.Pair;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MongoDatabaseBuilder {

  private ConnectionString connectionString;
  private String databaseName;
  private Tracer tracer;
  private OpenTelemetry openTelemetry;
  private final Map<CollectionName, ReadPreference> collectionsReadPreferences;
  private final Map<CollectionName, ReadConcern> collectionsReadConcerns;
  private final Map<CollectionName, WriteConcern> collectionsWriteConcerns;
  private ReadPreference databaseReadPreference;
  private ReadConcern databaseReadConcern;
  private WriteConcern databaseWriteConcern;
  private List<CollectionName> collections;

  private MongoDatabaseBuilder() {
    collectionsReadPreferences = new HashMap<>();
    collectionsReadConcerns = new HashMap<>();
    collectionsWriteConcerns = new HashMap<>();
  }

  public static MongoDatabaseBuilder builder() {
    return new MongoDatabaseBuilder();
  }

  public MongoDatabaseBuilder connectionString(ConnectionString connectionString) {
    this.connectionString = connectionString;
    return this;
  }

  public MongoDatabaseBuilder databaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  public MongoDatabaseBuilder tracer(Tracer tracer) {
    this.tracer = tracer;
    return this;
  }

  public MongoDatabaseBuilder openTelemetry(OpenTelemetry openTelemetry) {
    this.openTelemetry = openTelemetry;
    return this;
  }

  public MongoDatabaseBuilder collectionReadPreference(
      CollectionName collectionName, ReadPreference readPreference) {
    collectionsReadPreferences.put(collectionName, readPreference);
    return this;
  }

  public MongoDatabaseBuilder collectionReadConcern(
      CollectionName collectionName, ReadConcern readConcern) {
    collectionsReadConcerns.put(collectionName, readConcern);
    return this;
  }

  public MongoDatabaseBuilder collectionWriteConcerns(
      CollectionName collectionName, WriteConcern writeConcern) {
    collectionsWriteConcerns.put(collectionName, writeConcern);
    return this;
  }

  public MongoDatabaseBuilder databaseReadPreference(ReadPreference readPreference) {
    this.databaseReadPreference = readPreference;
    return this;
  }

  public MongoDatabaseBuilder databaseReadConcern(ReadConcern readConcern) {
    this.databaseReadConcern = readConcern;
    return this;
  }

  public MongoDatabaseBuilder databaseWriteConcern(WriteConcern writeConcern) {
    this.databaseWriteConcern = writeConcern;
    return this;
  }

  public MongoDatabaseBuilder collections(List<CollectionName> collections) {
    this.collections = collections;
    return this;
  }

  public MongoDatabase build() {
    Objects.requireNonNull(connectionString, "ConnectionString cannot be null");
    Objects.requireNonNull(databaseName, "DatabaseName cannot be null");
    Objects.requireNonNull(tracer, "Tracer cannot be null");
    var client = MongoClients.create(connectionString);
    var database =
        client.getDatabase(databaseName).withCodecRegistry(new OurDocumentCodecRegistry());
    if (databaseReadPreference != null) {
      database = database.withReadPreference(databaseReadPreference);
    }
    if (databaseReadConcern != null) {
      database = database.withReadConcern(databaseReadConcern);
    }
    if (databaseWriteConcern != null) {
      database = database.withWriteConcern(databaseWriteConcern);
    }
    com.mongodb.client.MongoDatabase finalDatabase = database;
    var mongoCollections =
        collections.stream()
            .map(
                collectionName -> {
                  var collection =
                      finalDatabase.getCollection(collectionName.name(), OurDocument.class);
                  if (collectionsReadPreferences.containsKey(collectionName)) {
                    collection =
                        collection.withReadPreference(
                            collectionsReadPreferences.get(collectionName));
                  }
                  if (collectionsReadConcerns.containsKey(collectionName)) {
                    collection =
                        collection.withReadConcern(collectionsReadConcerns.get(collectionName));
                  }
                  if (collectionsWriteConcerns.containsKey(collectionName)) {
                    collection =
                        collection.withWriteConcern(collectionsWriteConcerns.get(collectionName));
                  }
                  return new Pair<>(collectionName, collection);
                })
            .collect(new MapCollector<>());

    var histogram =
        openTelemetry
            .meterBuilder(TelemetryConfig.METRIC_SCOPE_NAME)
            .setInstrumentationVersion(TelemetryConfig.version())
            .build()
            .histogramBuilder("database_duration")
            .ofLongs()
            .setUnit("us")
            .setDescription("Tracks duration of all database operations.")
            .build();
    return new MongoDatabase(client, mongoCollections, tracer, histogram, Clock.systemUTC());
  }
}
