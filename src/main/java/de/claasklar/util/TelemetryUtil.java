package de.claasklar.util;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import io.opentelemetry.api.common.Attributes;

public class TelemetryUtil {
  public Attributes putId(Attributes attributes, IdLong id) {
    return attributes.toBuilder()
        .put("id_long", id.toString())
        .put("id", id.toId().toString())
        .build();
  }

  public Attributes attributes(CollectionName collectionName, IdLong idLong) {
    return Attributes.builder()
        .put("collection", collectionName.toString())
        .put("id_long", idLong.toString())
        .put("id", idLong.toId().toString())
        .build();
  }
}
