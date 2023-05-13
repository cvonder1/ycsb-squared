package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.idStore.IdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.StringValue;
import de.claasklar.primitives.span.Span;
import java.util.Map;

public class WriteSpecificationRunnable implements Runnable {

  private final CollectionName collectionName;
  private final IdLong idLong;
  private final Span span;
  private final Database database;
  private final IdStore idStore;
  private Document document;
  private boolean done = false;

  public WriteSpecificationRunnable(
      CollectionName collectionName, IdLong idLong, Span span, Database database, IdStore idStore) {
    this.collectionName = collectionName;
    this.idLong = idLong;
    this.span = span;
    this.database = database;
    this.idStore = idStore;
  }

  @Override
  public void run() {
    var document = new Document(this.idLong.toId(), Map.of("ello", new StringValue("world")));
    var runSpan = new Span(this.getClass(), this.collectionName.toString() + this.idLong);
    try (var ignored = span.register(runSpan).enter()) {
      this.document = database.write(collectionName, document, span);
      this.done = true;
      this.idStore.store(collectionName, idLong);
    }
  }

  public IdLong getIdLong() {
    if (!done) {
      throw new IllegalStateException("cannot access id before runnable is finished");
    }
    return this.idLong;
  }

  public Document getDocument() {
    if (!done) {
      throw new IllegalStateException("cannot access document before runnable is finished");
    }
    return this.document;
  }
}
