package de.claasklar.specification;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import de.claasklar.database.Database;
import de.claasklar.generation.QueryGenerator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;

public final class ReadSpecification implements TopSpecification {

  private final String name;
  private final QueryGenerator queryGenerator;
  private final Database database;
  private final LongHistogram histogram;
  private final Tracer tracer;
  private final Clock clock;
  private final Attributes attributes;

  public ReadSpecification(
      String name,
      QueryGenerator queryGenerator,
      Database database,
      LongHistogram histogram,
      Tracer tracer,
      Clock clock) {
    this.name = name;
    this.queryGenerator = queryGenerator;
    this.database = database;
    this.histogram = histogram;
    this.tracer = tracer;
    this.clock = clock;
    this.attributes =
        Attributes.of(
            stringKey("collection"),
            queryGenerator.getCollectionName().toString(),
            stringKey("operation"),
            name);
  }

  @Override
  public ReadSpecificationRunnable runnable() {
    return new ReadSpecificationRunnable(
        queryGenerator, name, database, attributes, tracer, clock, histogram);
  }

  @Override
  public String getName() {
    return name;
  }
}
