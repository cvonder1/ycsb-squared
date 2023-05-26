package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.ObjectValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class ObjectValueEncoder implements Encoder<ObjectValue> {

  private final CodecRegistry codecRegistry;

  public ObjectValueEncoder(CodecRegistry codecRegistry) {
    this.codecRegistry = codecRegistry;
  }

  @Override
  public void encode(BsonWriter writer, ObjectValue value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    value
        .entrySet()
        .forEach(
            entry -> {
              writer.writeName(entry.getKey());
              writeValue(writer, entry.getValue(), encoderContext);
            });
    writer.writeEndDocument();
  }

  @Override
  public Class<ObjectValue> getEncoderClass() {
    return ObjectValue.class;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void writeValue(BsonWriter writer, Object value, EncoderContext encoderContext) {
    if (value == null) {
      writer.writeNull();
    } else {
      Codec codec = codecRegistry.get(value.getClass());
      encoderContext.encodeWithChildContext(codec, writer, value);
    }
  }
}
