package de.claasklar.database.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.primitives.query.Aggregation;
import de.claasklar.primitives.query.AggregationOptions;
import de.claasklar.primitives.query.Find;
import de.claasklar.primitives.query.FindOptions;
import de.claasklar.primitives.query.Query;
import de.claasklar.util.Pair;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class MongoDatabase implements Database, AutoCloseable {

  private final MongoClient client;
  private final com.mongodb.client.MongoDatabase database;
  private final Tracer tracer;
  private final LongHistogram histogram;
  private final Clock clock;
  private final Map<CollectionName, MongoCollection<OurDocument>> collections;

  MongoDatabase(
      MongoClient client,
      com.mongodb.client.MongoDatabase database,
      Map<CollectionName, MongoCollection<OurDocument>> collections,
      Tracer tracer,
      LongHistogram histogram,
      Clock clock) {
    this.client = client;
    this.database = database;
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
      } else if (query instanceof Aggregation aggregation) {
        executeAggregate(aggregation, executeSpan);
      }
    } catch (Exception e) {
      executeSpan.recordException(e);
      executeSpan.setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      executeSpan.end();
    }
  }

  @Override
  public void createIndex(IndexConfiguration indexConfiguration, Span span) {
    var indexSpan =
        tracer
            .spanBuilder("create index")
            .setAttribute("collection", indexConfiguration.getCollectionName().name())
            .setAttribute("keys", indexConfiguration.getKeys().toString())
            .setParent(Context.current().with(span))
            .startSpan();

    try {
      var collection = collections.get(indexConfiguration.getCollectionName());
      var index = mapIndexConfiguration(indexConfiguration, collection.getCodecRegistry());
      collection.createIndex(index.first(), index.second());
    } catch (Exception e) {
      indexSpan.setStatus(StatusCode.ERROR);
      indexSpan.recordException(e);
      throw e;
    } finally {
      indexSpan.end();
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

  private void executeAggregate(Aggregation aggregation, Span span) {
    var iterable = aggregateIterableFromOptions(aggregation.getAggregationOptions());
    var start = clock.instant();
    try (var iterator = iterable.iterator()) {
      var list = new LinkedList<>();
      iterator.forEachRemaining(list::add);
      span.addEvent("query executed", Attributes.of(stringKey("result"), list.toString()));
    }
    histogram.record(
        start.until(clock.instant(), ChronoUnit.MICROS),
        Attributes.of(
            stringKey("collection"),
            aggregation.getCollectionName().toString(),
            stringKey("operation"),
            aggregation.getQueryName()));
  }

  private AggregateIterable<NestedObjectValue> aggregateIterableFromOptions(
      AggregationOptions aggregationOptions) {
    var codecRegistry = database.getCodecRegistry();
    AggregateIterable<NestedObjectValue> iterable;
    List<BsonDocument> pipeline =
        aggregationOptions.getPipeline().stream()
            .map(it -> BsonDocumentWrapper.asBsonDocument(it, codecRegistry))
            .toList();
    // 1 stands for an aggregation without collection
    if (aggregationOptions.getAggregate().equals("1")) {
      iterable = database.aggregate(pipeline, NestedObjectValue.class);
    } else {
      iterable =
          collections
              .get(new CollectionName(aggregationOptions.getAggregate()))
              .aggregate(pipeline, NestedObjectValue.class);
    }

    if (aggregationOptions.getAllowDiskUse() != null) {
      iterable.allowDiskUse(aggregationOptions.getAllowDiskUse());
    }
    if (aggregationOptions.getMaxTime() != null) {
      iterable.maxTime(aggregationOptions.getMaxTime().toMillis(), TimeUnit.MILLISECONDS);
    }
    if (aggregationOptions.getMaxAwaitTime() != null) {
      iterable.maxAwaitTime(aggregationOptions.getMaxAwaitTime().toMillis(), TimeUnit.MILLISECONDS);
    }
    if (aggregationOptions.getBypassDocumentValidation() != null) {
      iterable.bypassDocumentValidation(aggregationOptions.getBypassDocumentValidation());
    }
    if (aggregationOptions.getCollation() != null) {
      iterable.collation(mapCollation(aggregationOptions.getCollation()));
    }
    if (aggregationOptions.getHint() != null) {
      iterable.hint(
          BsonDocumentWrapper.asBsonDocument(aggregationOptions.getHint(), codecRegistry));
    }
    if (aggregationOptions.getHintString() != null) {
      iterable.hintString(aggregationOptions.getHintString());
    }
    if (aggregationOptions.getVariables() != null) {
      iterable.let(
          BsonDocumentWrapper.asBsonDocument(aggregationOptions.getVariables(), codecRegistry));
    }
    return iterable;
  }

  private Collation mapCollation(de.claasklar.primitives.query.Collation ourCollation) {
    if (ourCollation == null) {
      return null;
    }
    return Collation.builder()
        .locale(ourCollation.getLocale())
        .caseLevel(ourCollation.getCaseLevel())
        .collationCaseFirst(mapCollationCaseFirst(ourCollation.getCaseFirst()))
        .collationStrength(mapCollationStrength(ourCollation.getStrength()))
        .numericOrdering(ourCollation.getNumericOrdering())
        .collationAlternate(mapCollationAlternate(ourCollation.getAlternate()))
        .collationMaxVariable(mapCollationMaxVariable(ourCollation.getMaxVariable()))
        .normalization(ourCollation.getNormalization())
        .backwards(ourCollation.getBackwards())
        .build();
  }

  private CollationCaseFirst mapCollationCaseFirst(
      de.claasklar.primitives.query.CollationCaseFirst ourCollationCaseFirst) {
    if (ourCollationCaseFirst == null) {
      return null;
    }
    return switch (ourCollationCaseFirst) {
      case OFF -> CollationCaseFirst.OFF;
      case LOWER -> CollationCaseFirst.LOWER;
      case UPPER -> CollationCaseFirst.UPPER;
    };
  }

  private CollationStrength mapCollationStrength(
      de.claasklar.primitives.query.CollationStrength ourCollationStrength) {
    if (ourCollationStrength == null) {
      return null;
    }
    return switch (ourCollationStrength) {
      case PRIMARY -> CollationStrength.PRIMARY;
      case SECONDARY -> CollationStrength.SECONDARY;
      case TERTIARY -> CollationStrength.TERTIARY;
      case QUATERNARY -> CollationStrength.QUATERNARY;
      case IDENTICAL -> CollationStrength.IDENTICAL;
    };
  }

  private CollationAlternate mapCollationAlternate(
      de.claasklar.primitives.query.CollationAlternate ourCollationAlternate) {
    if (ourCollationAlternate == null) {
      return null;
    }
    return switch (ourCollationAlternate) {
      case SHIFTED -> CollationAlternate.SHIFTED;
      case NON_IGNORABLE -> CollationAlternate.NON_IGNORABLE;
    };
  }

  private CollationMaxVariable mapCollationMaxVariable(
      de.claasklar.primitives.query.CollationMaxVariable ourCollationMaxVariable) {
    if (ourCollationMaxVariable == null) {
      return null;
    }
    return switch (ourCollationMaxVariable) {
      case PUNCT -> CollationMaxVariable.PUNCT;
      case SPACE -> CollationMaxVariable.SPACE;
    };
  }

  private Pair<Bson, IndexOptions> mapIndexConfiguration(
      IndexConfiguration indexConfiguration, CodecRegistry codecRegistry) {
    var indexOptions = new IndexOptions();
    if (indexConfiguration.getBackground() != null) {
      indexOptions.background(indexConfiguration.getBackground());
    }
    if (indexConfiguration.getUnique() != null) {
      indexOptions.unique(indexConfiguration.getUnique());
    }
    indexOptions.name(indexConfiguration.getName());
    if (indexConfiguration.getSparse() != null) {
      indexOptions.sparse(indexConfiguration.getSparse());
    }
    if (indexConfiguration.getExpireAfterDuration() != null) {
      indexOptions.expireAfter(
          indexConfiguration.getExpireAfterDuration().toSeconds(), TimeUnit.SECONDS);
    }
    indexOptions.version(indexConfiguration.getVersion());
    indexOptions.weights(
        BsonDocumentWrapper.asBsonDocument(indexConfiguration.getWeights(), codecRegistry));
    indexOptions.defaultLanguage(indexConfiguration.getDefaultLanguage());
    indexOptions.languageOverride(indexConfiguration.getLanguageOverride());
    indexOptions.textVersion(indexConfiguration.getTextVersion());
    indexOptions.sphereVersion(indexConfiguration.getSphereVersion());
    indexOptions.bits(indexConfiguration.getBits());
    indexOptions.min(indexConfiguration.getMin());
    indexOptions.max(indexConfiguration.getMax());
    indexOptions.storageEngine(
        BsonDocumentWrapper.asBsonDocument(indexConfiguration.getStorageEngine(), codecRegistry));
    indexOptions.partialFilterExpression(
        BsonDocumentWrapper.asBsonDocument(
            indexConfiguration.getPartialFilterExpression(), codecRegistry));
    indexOptions.collation(mapCollation(indexConfiguration.getCollation()));
    indexOptions.wildcardProjection(
        BsonDocumentWrapper.asBsonDocument(
            indexConfiguration.getWildcardProjection(), codecRegistry));
    if (indexConfiguration.getHidden() != null) {
      indexOptions.hidden(indexConfiguration.getHidden());
    }
    return new Pair<>(
        BsonDocumentWrapper.asBsonDocument(indexConfiguration.getKeys(), codecRegistry),
        indexOptions);
  }

  @Override
  public void close() {
    this.client.close();
  }
}
