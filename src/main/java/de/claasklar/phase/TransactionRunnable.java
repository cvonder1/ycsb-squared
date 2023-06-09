package de.claasklar.phase;

import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.specification.TopSpecification;
import de.claasklar.util.Pair;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionRunnable implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(TransactionRunnable.class);
  private final long targetOps;
  /** Duration of a time slot for one operation in nanoseconds. */
  private final long opDurationNs;

  private long opsCount = 0;
  private final List<Pair<Double, TopSpecification>> weightedSpecifications;
  private final RandomNumberGenerator random;
  private final Span span;
  private final Tracer tracer;

  private long startTimeNs = System.nanoTime();

  public TransactionRunnable(
      long targetOps,
      double targetOpsPerMs,
      List<Pair<Double, TopSpecification>> weightedSpecifications,
      RandomNumberGenerator random,
      Span span,
      Tracer tracer) {
    if (targetOps <= 0) {
      throw new IllegalArgumentException(
          "cannot execute less than or equal to 0 operations in total");
    }
    this.targetOps = targetOps;
    if (targetOpsPerMs >= 1_000_000) {
      throw new IllegalArgumentException("cannot execute more than 1000000 operations per thread");
    }
    if (targetOpsPerMs <= 0.0) {
      throw new IllegalArgumentException(
          "cannot execute less or equal than zero operations per thread per ms");
    }
    this.opDurationNs = (long) (1_000_000 / targetOpsPerMs);
    var sumWeight =
        weightedSpecifications.stream().map(Pair::first).reduce(Double::sum).orElse(1.0);
    if ((sumWeight - 1.0) > 0.0001) {
      throw new IllegalArgumentException("weights do not add up to 1");
    }
    AtomicReference<Double> acc = new AtomicReference<>(0.0);
    this.weightedSpecifications =
        weightedSpecifications.stream()
            .sorted(Comparator.comparing(Pair::first))
            .map(
                it ->
                    it.mapFirst(
                        weight -> {
                          acc.updateAndGet(v -> v + weight);
                          return acc.get();
                        }))
            .toList();
    this.random = random;
    this.span = span;
    this.tracer = tracer;
  }

  @Override
  public void run() {
    var runSpan = newSpan();
    try {
      defer();
      startTimeNs = System.nanoTime();
      while (opsCount < targetOps) {
        try {
          var spec = selectOneSpecification();
          spec.runnable().run();
          opsCount++;
          throttleNanos();
        } catch (Exception e) {
          logger.atError().log(e.getMessage());
          runSpan.recordException(e);
        }
      }
    } catch (Exception e) {
      runSpan.recordException(e);
      runSpan.setStatus(StatusCode.ERROR);
    } finally {
      runSpan.end();
    }
  }

  private void defer() {
    sleepUntil(System.nanoTime() + random.nextLong(0, opDurationNs));
  }

  private TopSpecification selectOneSpecification() {
    var randomSelection = random.nextDouble(0, 1);
    for (var spec : weightedSpecifications) {
      if (spec.first() < randomSelection) {
        return spec.second();
      }
    }
    return weightedSpecifications.get(weightedSpecifications.size() - 1).second();
  }

  private void throttleNanos() {
    var deadline = startTimeNs + opDurationNs * opsCount;
    sleepUntil(deadline);
  }

  private void sleepUntil(long deadlineNs) {
    LockSupport.parkNanos(deadlineNs - System.nanoTime());
  }

  private Span newSpan() {
    return tracer
        .spanBuilder("run transaction runnable")
        .setAttribute("thread", Thread.currentThread().getName())
        .setParent(Context.current().with(span))
        .startSpan();
  }
}
