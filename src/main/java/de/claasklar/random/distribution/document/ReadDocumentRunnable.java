package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.span.Span;

/** For existing documents */
public final class ReadDocumentRunnable implements DocumentRunnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final Span span;
  private final Database database;
  private boolean wasRun = false;
  private Document document;

  public ReadDocumentRunnable(
      CollectionName collectionName, IdLong id, Span span, Database database) {
    this.collectionName = collectionName;
    this.id = id;
    this.span = span;
    this.database = database;
  }

  @Override
  public Document getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("ReadDocumentRunnable must first be run");
    }
    return this.document;
  }

  @Override
  public boolean wasRun() {
    return this.wasRun;
  }

  @Override
  public void run() {
    if (this.wasRun) {
      throw new IllegalStateException("ReadDocumentRunnable can only be executed once");
    }
    var runSpan = new Span(this.getClass(), this.id.toString());
    try (var ignored = this.span.register(runSpan).enter()) {
      this.document =
          database
              .read(this.collectionName, this.id.toId(), this.span)
              .orElseThrow(() -> new NoSuchDocumentException(this.collectionName, this.id.toId()));
      this.wasRun = true;
    }
  }

  public IdLong getId() {
    return id;
  }
}
