package de.claasklar.database.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import de.claasklar.database.mongodb.codec.OurDocumentCodecRegistry;
import de.claasklar.util.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.util.Objects;

public class MongoDatabaseBuilder {

  private ConnectionString connectionString;
  private String databaseName;
  private Tracer tracer;
  private OpenTelemetry openTelemetry;

  private MongoDatabaseBuilder() {}

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

  public MongoDatabase build() {
    Objects.requireNonNull(connectionString, "ConnectionString cannot be null");
    Objects.requireNonNull(databaseName, "DatabaseName cannot be null");
    Objects.requireNonNull(tracer, "Tracer cannot be null");
    var client = MongoClients.create(connectionString);
    var database =
        client.getDatabase(databaseName).withCodecRegistry(new OurDocumentCodecRegistry());
    var histogram =
        openTelemetry
            .meterBuilder(TelemetryConfig.METRIC_SCOPE_NAME)
            .setInstrumentationVersion(TelemetryConfig.METRIC_SCOPE_NAME)
            .build()
            .histogramBuilder("database_duration")
            .ofLongs()
            .setUnit("us")
            .setDescription("Tracks duration of all database operations.")
            .build();
    return new MongoDatabase(client, database, tracer, histogram, Clock.systemUTC());
  }
}
