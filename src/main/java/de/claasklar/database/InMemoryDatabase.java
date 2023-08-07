package de.claasklar.database;

import de.claasklar.phase.PhaseTopic;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.primitives.query.Query;
import de.claasklar.util.Subject;
import io.opentelemetry.api.trace.Span;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDatabase implements Database {

  private final Map<CollectionName, Map<Id, OurDocument>> data;

  public InMemoryDatabase() {
    this.data = new ConcurrentHashMap<>();
  }

  @Override
  public OurDocument write(CollectionName collectionName, OurDocument document, Span span) {
    var collection = this.data.get(collectionName);
    if (collection == null) {
      collection = new ConcurrentHashMap<>();
      this.data.put(collectionName, collection);
    }
    collection.put(document.getId(), document);
    return document;
  }

  @Override
  public Optional<OurDocument> read(CollectionName collectionName, Id id, Span span) {
    var collection = this.data.get(collectionName);
    if (collection == null) {
      return Optional.empty();
    }
    var document = collection.get(id);
    if (document == null) {
      return Optional.empty();
    } else {
      return Optional.of(document);
    }
  }

  @Override
  public void executeQuery(Query query, Span span) {}

  @Override
  public void createIndex(IndexConfiguration indexConfiguration, Span span) {}

  @Override
  public void close() throws Exception {}

  @Override
  public void update(PhaseTopic.BenchmarkPhase update) {}

  @Override
  public void setSubject(Subject<PhaseTopic.BenchmarkPhase> subject) {
    subject.unregister(this);
  }
}
