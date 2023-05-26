package de.claasklar.database.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

public class MongoDatabase implements Database, AutoCloseable {

  private final MongoClient client;
  private final Tracer tracer;
  private final LongHistogram histogram;
  private final Clock clock;
  private final Map<CollectionName, MongoCollection<OurDocument>> collections;

  MongoDatabase(
      MongoClient client,
      Map<CollectionName, MongoCollection<OurDocument>> collections,
      Tracer tracer,
      LongHistogram histogram,
      Clock clock) {
    this.client = client;
    this.tracer = tracer;
    this.histogram = histogram;
    this.clock = clock;
    this.collections = collections;
  }

  @Override
  public OurDocument write(CollectionName collectionName, OurDocument document, Span span) {
    var writeSpan =
        tracer
            .spanBuilder("write document to database")
            .setParent(Context.current().with(span))
            .setAllAttributes(
                new TelemetryUtil()
                    .attributes(collectionName, document.getId()).toBuilder()
                        .put("document", document.toString())
                        .build())
            .startSpan();
    try (var ignored = writeSpan.makeCurrent()) {
      var start = clock.instant();
      collections.get(collectionName).insertOne(document);
      histogram.record(
          start.until(clock.instant(), ChronoUnit.MICROS),
          Attributes.of(
              stringKey("collection"), collectionName.toString(), stringKey("operation"), "WRITE"));
      return document;
    } catch (Exception e) {
      writeSpan.recordException(e);
      writeSpan.setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      writeSpan.end();
    }
  }

  @Override
  public Optional<OurDocument> read(CollectionName collectionName, Id id, Span span) {
    var readSpan =
        tracer
            .spanBuilder("read document from database")
            .setParent(Context.current().with(span))
            .setAllAttributes(new TelemetryUtil().attributes(collectionName, id))
            .startSpan();
    try (var ignored = readSpan.makeCurrent()) {
      var start = clock.instant();
      var document =
          collections.get(collectionName)
              .find(eq("_id", id))
              .first();
      histogram.record(
          start.until(clock.instant(), ChronoUnit.MICROS),
          Attributes.of(
              stringKey("collection"), collectionName.toString(), stringKey("operation"), "READ"));
      if (document == null) {
        readSpan.addEvent("found no document");
        return Optional.empty();
      } else {
        readSpan.addEvent(
            "found document", Attributes.of(stringKey("document"), document.toString()));
        return Optional.of(document);
      }
    } catch (Exception e) {
      readSpan.recordException(e);
      readSpan.setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      readSpan.end();
    }
  }

  @Override
  public void close() {
    this.client.close();
  }
}
