package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.NestedObjectValue;
import de.claasklar.primitives.document.Value;
import java.util.HashMap;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class NestedObjectValueCodec implements Codec<NestedObjectValue> {

  private final ObjectValueEncoder encoder;
  private final BsonTypeValueCodecMap codecMap;

  public NestedObjectValueCodec(CodecRegistry codecRegistry, BsonTypeValueCodecMap codecMap) {
    this.encoder = new ObjectValueEncoder(codecRegistry);
    this.codecMap = codecMap;
  }

  @Override
  public NestedObjectValue decode(BsonReader reader, DecoderContext decoderContext) {
    var values = new HashMap<String, Value>();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      String fieldName = reader.readName();
      values.put(fieldName, ContainerCodecHelper.readValue(reader, decoderContext, codecMap));
    }
    reader.readEndDocument();
    return new NestedObjectValue(values);
  }

  @Override
  public void encode(BsonWriter writer, NestedObjectValue value, EncoderContext encoderContext) {
    encoder.encode(writer, value, encoderContext);
  }

  @Override
  public Class<NestedObjectValue> getEncoderClass() {
    return NestedObjectValue.class;
  }
}
