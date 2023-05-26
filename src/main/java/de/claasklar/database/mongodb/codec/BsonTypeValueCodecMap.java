package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.Value;
import org.bson.BsonType;
import org.bson.codecs.Codec;

public class BsonTypeValueCodecMap {

  private final BsonTypeValueClassMap map;
  private final OurDocumentCodecRegistry codecRegistry;

  public BsonTypeValueCodecMap(BsonTypeValueClassMap map, OurDocumentCodecRegistry codecRegistry) {
    this.map = map;
    this.codecRegistry = codecRegistry;
  }

  public Codec<? extends Value> get(BsonType key) {
    return codecRegistry.get(map.get(key));
  }
}
