package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.span.Span;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.util.MapCollector;
import de.claasklar.util.Pair;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class PrimaryWriteSpecificationRunnable implements Runnable {

  private final CollectionName collectionName;
  private final IdLong id;
  private final ReferencesDistribution[] referencesDistributions;
  private final Database database;
  private final ExecutorService executor;
  private boolean wasRun = false;
  private Document document;

  public PrimaryWriteSpecificationRunnable(
      CollectionName collectionName,
      IdLong id,
      ReferencesDistribution[] referencesDistributions,
      Database database,
      ExecutorService executor) {
    this.collectionName = collectionName;
    this.id = id;
    this.referencesDistributions = referencesDistributions;
    this.database = database;
    this.executor = executor;
  }

  @Override
  public void run() {
    try (var span = new Span(PrimaryWriteSpecificationRunnable.class, id.toString()).enter()) {
      var references =
          Arrays.stream(referencesDistributions)
              .parallel()
              .map(dist -> new Pair<>(dist.getCollectionName(), dist.next(span)))
              .map(pair -> pair.mapSecond(runnable -> runnable.execute(executor)))
              .collect(new MapCollector<>());
      var document = new Document(id.toId(), Collections.emptyMap());
      try (var writeSpan =
          span.newChild(PrimaryWriteSpecificationRunnable.class, "writing document").enter()) {
        database.write(collectionName, document, writeSpan);
      }
      this.document = document;
      this.wasRun = true;
    }
  }

  public boolean wasRun() {
    return this.wasRun;
  }

  public Document getDocument() {
    if (!wasRun) {
      throw new IllegalStateException("cannot access document before runnable was run");
    }
    return this.document;
  }
}
