package de.claasklar.random.distribution.document;

import de.claasklar.database.Database;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExistingDocumentDistribution implements DocumentDistribution {

  private static final Logger logger = LoggerFactory.getLogger(ExistingDocumentDistribution.class);
  private final Queue<IdLong> queue;
  private final int bufferSize;
  private final AtomicInteger currentSize;
  private final AtomicInteger waitingSize;
  private final CollectionName collectionName;
  private final DocumentDistribution documentDistribution;
  private final Database database;
  private final Executor executor;
  private final Tracer tracer;

  public ExistingDocumentDistribution(
      int bufferSize,
      DocumentDistribution documentDistribution,
      Database database,
      Executor executor,
      Tracer tracer) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.bufferSize = bufferSize;
    this.currentSize = new AtomicInteger(0);
    this.waitingSize = new AtomicInteger(0);
    this.collectionName = documentDistribution.getCollectionName();
    this.documentDistribution = documentDistribution;
    this.database = database;
    this.executor = executor;
    this.tracer = tracer;
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

    for (int i = 0; i < 20 && nextId == null; i++) {
      logger
          .atWarn()
          .log("No id in queue for the {}-nth time for the collection {}", i, collectionName);
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
    return new ReadDocumentRunnable(collectionName, nextId, span, database, tracer);
  }

  private void fillQueue(int numElements) {
    var numBuffering =
        Math.min(Math.max((bufferSize - currentSize.get() - waitingSize.get()), 0), numElements);
    waitingSize.updateAndGet((old) -> old + numBuffering);
    var bufferSpan = tracer.spanBuilder("Buffering next documents").setNoParent().startSpan();
    var writeFutures = new LinkedList<>();
    var numReading = 0;
    for (int i = 0; i < numBuffering; i++) {
      var nextRunnable = documentDistribution.next(bufferSpan);
      if (nextRunnable instanceof ReadDocumentRunnable r) {
        queue.add(r.getId());
        currentSize.incrementAndGet();
        numReading++;
        waitingSize.decrementAndGet();
      } else if (nextRunnable instanceof WriteDocumentRunnable w) {
        var future =
            CompletableFuture.supplyAsync(() -> w, executor)
                .thenAccept(
                    supplied -> {
                      supplied.run();
                      queue.add(supplied.getId());
                      currentSize.incrementAndGet();
                    })
                .whenComplete(
                    (it, e) -> {
                      waitingSize.decrementAndGet();
                      if (e != null) {
                        bufferSpan.recordException(e);
                      }
                    });
        writeFutures.add(future);
      } else {
        throw new IllegalArgumentException("unknown class " + nextRunnable.getClass());
      }
    }
    var numReadingF = numReading;
    CompletableFuture.allOf(writeFutures.toArray(CompletableFuture[]::new))
        .whenComplete(
            (it1, it2) -> {
              bufferSpan.addEvent(
                  "Buffered Elements",
                  Attributes.builder()
                      .put("numBuffering", numBuffering)
                      .put("numWriting", writeFutures.size())
                      .put("numReading", numReadingF)
                      .build());
              bufferSpan.end();
            });
  }

  @Override
  public CollectionName getCollectionName() {
    return collectionName;
  }
}
