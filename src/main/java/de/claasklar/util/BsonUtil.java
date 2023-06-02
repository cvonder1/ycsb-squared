package de.claasklar.util;

import de.claasklar.database.mongodb.codec.OurDocumentCodecRegistry;
import de.claasklar.primitives.document.NestedObjectValue;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

public class BsonUtil {

  private static final CodecRegistry codecRegistry = new OurDocumentCodecRegistry();

  public static NestedObjectValue asOurBson(Bson bson) {
    return codecRegistry
        .get(NestedObjectValue.class)
        .decode(bson.toBsonDocument().asBsonReader(), DecoderContext.builder().build());
  }
}
