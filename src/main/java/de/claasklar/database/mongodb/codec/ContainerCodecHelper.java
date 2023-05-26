package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.Value;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.codecs.DecoderContext;

public class ContainerCodecHelper {
  static Value readValue(
      BsonReader reader, DecoderContext decoderContext, BsonTypeValueCodecMap codecMap) {
    var bsonType = reader.getCurrentBsonType();
    if (bsonType == BsonType.NULL) {
      reader.readNull();
      return null;
    } else {
      var codec = codecMap.get(bsonType);
      return codec.decode(reader, decoderContext);
    }
  }
}
