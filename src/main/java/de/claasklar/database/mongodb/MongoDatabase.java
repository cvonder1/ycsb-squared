package de.claasklar.database.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.query.Find;
import de.claasklar.primitives.query.FindOptions;
import de.claasklar.primitives.query.Query;
import de.claasklar.util.TelemetryUtil;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.configuration.CodecRegistry;

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
      var document = collections.get(collectionName).find(eq("_id", id)).first();
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
  public void executeQuery(Query query, Span span) {
    var executeSpan =
        tracer
            .spanBuilder("execute query " + query.getQueryName())
            .setParent(Context.current().with(span))
            .setAllAttributes(
                new TelemetryUtil()
                    .executeQueryAttributes(query.getCollectionName(), query.getQueryName()))
            .startSpan();
    try {
      if (query instanceof Find find) {
        executeFind(find, executeSpan);
      }
    } catch (Exception e) {
      executeSpan.recordException(e);
      executeSpan.setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      executeSpan.end();
    }
  }

  private void executeFind(Find find, Span span) {
    var collection = collections.get(find.getCollectionName());
    var result = collection.find(NestedObjectValue.class);
    applyFindOptions(result, find.getFindOptions(), collection.getCodecRegistry());
    var start = clock.instant();
    try (var iterator = result.iterator()) {
      var list = new LinkedList<>();
      iterator.forEachRemaining(list::add);
      span.addEvent("query executed", Attributes.of(stringKey("result"), list.toString()));
    }
    histogram.record(
        start.until(clock.instant(), ChronoUnit.MICROS),
        Attributes.of(
            stringKey("collection"),
            find.getCollectionName().toString(),
            stringKey("operation"),
            find.getQueryName()));
  }

  private <T> void applyFindOptions(
      FindIterable<T> findIterable, FindOptions findOptions, CodecRegistry codecRegistry) {
    if (findOptions.getFilter() != null) {
      findIterable.filter(
          BsonDocumentWrapper.asBsonDocument(findOptions.getFilter(), codecRegistry));
    }
    if (findOptions.getBatchSize() != null) {
      findIterable.batchSize(findOptions.getBatchSize());
    }
    if (findOptions.getLimit() != null) {
      findIterable.limit(findOptions.getLimit());
    }
    if (findOptions.getProjection() != null) {
      findIterable.projection(
          BsonDocumentWrapper.asBsonDocument(findOptions.getProjection(), codecRegistry));
    }
    if (findOptions.getMaxTime() != null) {
      findIterable.maxTime(findOptions.getMaxTime().toMillis(), TimeUnit.MILLISECONDS);
    }
    if (findOptions.getMaxAwaitTime() != null) {
      findIterable.maxAwaitTime(findOptions.getMaxAwaitTime().toMillis(), TimeUnit.MILLISECONDS);
    }
    if (findOptions.getSkip() != null) {
      findIterable.skip(findOptions.getSkip());
    }
    if (findOptions.getSort() != null) {
      findIterable.sort(BsonDocumentWrapper.asBsonDocument(findOptions.getSort(), codecRegistry));
    }
    if (findOptions.getNoCursorTimeout() != null) {
      findIterable.noCursorTimeout(findOptions.getNoCursorTimeout());
    }
    if (findOptions.getPartial() != null) {
      findIterable.partial(findOptions.getPartial());
    }
    if (findOptions.getHint() != null) {
      findIterable.hint(BsonDocumentWrapper.asBsonDocument(findOptions.getHint(), codecRegistry));
    }
    if (findOptions.getVariables() != null) {
      findIterable.let(
          BsonDocumentWrapper.asBsonDocument(findOptions.getVariables(), codecRegistry));
    }
    if (findOptions.getMax() != null) {
      findIterable.max(BsonDocumentWrapper.asBsonDocument(findOptions.getMax(), codecRegistry));
    }
    if (findOptions.getMin() != null) {
      findIterable.min(BsonDocumentWrapper.asBsonDocument(findOptions.getMin(), codecRegistry));
    }
    if (findOptions.getReturnKey() != null) {
      findIterable.returnKey(findOptions.getReturnKey());
    }
    if (findOptions.getShowRecordId() != null) {
      findIterable.showRecordId(findOptions.getShowRecordId());
    }
    if (findOptions.getAllowDiskUse() != null) {
      findIterable.allowDiskUse(findOptions.getAllowDiskUse());
    }
  }

  @Override
  public void close() {
    this.client.close();
  }
}
