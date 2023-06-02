package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.Value;
import org.bson.BsonReader;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerCodecHelper {
  private static final Logger logger = LoggerFactory.getLogger(ContainerCodecHelper.class);

  static Value readValue(
      BsonReader reader, DecoderContext decoderContext, BsonTypeValueCodecMap codecMap) {
    var bsonType = reader.getCurrentBsonType();
    try {
      var codec = codecMap.get(bsonType);
      return codec.decode(reader, decoderContext);
    } catch (CodecConfigurationException e) {
      throw new CodecConfigurationException("Can't find codec for type " + bsonType, e);
    }
  }
}
