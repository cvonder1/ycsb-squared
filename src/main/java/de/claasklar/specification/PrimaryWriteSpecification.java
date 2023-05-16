package de.claasklar.specification;

import de.claasklar.database.Database;
import de.claasklar.generation.ContextDocumentGenerator;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class PrimaryWriteSpecification implements Specification {

  private final CollectionName collectionName;
  private final ReferencesDistribution[] referencesDistributions;
  private final ContextDocumentGenerator generator;
  private final Database database;
  private final ExecutorService executor;

  private final AtomicLong currentId;

  public PrimaryWriteSpecification(
      CollectionName collectionName,
      ReferencesDistribution[] referencesDistributions,
      ContextDocumentGenerator generator,
      Database database,
      ExecutorService executor) {
    this.collectionName = collectionName;
    this.referencesDistributions = referencesDistributions;
    this.generator = generator;
    this.database = database;
    this.executor = executor;
    this.currentId = new AtomicLong(1);
  }

  public PrimaryWriteSpecificationRunnable runnable() {
    return new PrimaryWriteSpecificationRunnable(
        collectionName,
        new IdLong(currentId.getAndIncrement()),
        referencesDistributions,
        generator,
        database,
        executor);
  }
}
