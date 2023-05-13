package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.span.Span;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExistingDocumentDistribution implements DocumentDistribution {

  private final Queue<IdLong> queue;
  private final int bufferSize;
  private final AtomicInteger currentSize;
  private final CollectionName collectionName;
  private final DocumentDistribution documentDistribution;
  private final Database database;
  private final Executor executor;

  public ExistingDocumentDistribution(
    int bufferSize,
    DocumentDistribution documentDistribution,
    Database database,
    Executor executor) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.bufferSize = bufferSize;
    this.currentSize = new AtomicInteger(0);
    this.collectionName = documentDistribution.getCollectionName();
    this.documentDistribution = documentDistribution;
    this.database = database;
    this.executor = executor;
  }

  /**
   * @param span
   * @return only futures for existing documents (ReadDocumentFuture)
   */
  @Override
  public DocumentRunnable next(Span span) {
    var queueSize = currentSize.get();
    if (queueSize < bufferSize) {
      this.fillQueue(50);
    }
    IdLong nextId = queue.poll();

    for (int i = 0; i < 10 && nextId == null; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IllegalStateException("could not obtain new id, thread was interrupted");
      }
      nextId = queue.poll();
    }
    if (nextId == null) {
      throw new IllegalStateException("could not obtain new id, no id in queue");
    }
    currentSize.decrementAndGet();
    return new ReadDocumentRunnable(collectionName, nextId, span, database);
  }

  private void fillQueue(int numElements) {
    var queueSize = currentSize.get();
    for (int i = 0; i < Math.min(Math.max((bufferSize - queueSize), 0), numElements); i++) {
      try (var bufferSpan =
        new Span(ExistingDocumentDistribution.class, "buffering").enter()) {
        var nextRunnable = documentDistribution.next(bufferSpan);
        if (nextRunnable instanceof ReadDocumentRunnable r) {
          queue.add(r.getId());
          currentSize.incrementAndGet();
        } else if (nextRunnable instanceof WriteDocumentRunnable w) {
          CompletableFuture.supplyAsync(() -> w, executor)
            .thenAccept(
              supplied -> {
                supplied.run();
                queue.add(supplied.getId());
                currentSize.incrementAndGet();
              });
        } else {
          throw new IllegalArgumentException("unknown class " + nextRunnable.getClass());
        }
      }
    }
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
