package de.claasklar.database;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.index.IndexConfiguration;
import de.claasklar.primitives.query.Query;
import io.opentelemetry.api.trace.Span;
import java.util.Optional;

public interface Database {

  OurDocument write(CollectionName collectionName, OurDocument document, Span span);

  Optional<OurDocument> read(CollectionName collectionName, Id id, Span span);

  void executeQuery(Query query, Span span);

  void createIndex(IndexConfiguration indexConfiguration, Span span);
}
