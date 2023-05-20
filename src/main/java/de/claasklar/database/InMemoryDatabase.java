package de.claasklar.database;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import io.opentelemetry.api.trace.Span;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDatabase implements Database {

  private final Map<CollectionName, Map<Id, Document>> data;

  public InMemoryDatabase() {
    this.data = new ConcurrentHashMap<>();
  }

  @Override
  public Document write(CollectionName collectionName, Document document, Span span) {
    var collection = this.data.get(collectionName);
    if (collection == null) {
      collection = new ConcurrentHashMap<>();
      this.data.put(collectionName, collection);
    }
    collection.put(document.id(), document);
    return document;
  }

  @Override
  public Optional<Document> read(CollectionName collectionName, Id id, Span span) {
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
}
