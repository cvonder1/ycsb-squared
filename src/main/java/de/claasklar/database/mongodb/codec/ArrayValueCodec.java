package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.Value;
import java.util.LinkedList;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class ArrayValueCodec implements Codec<ArrayValue> {

  private final CodecRegistry codecRegistry;
  private final BsonTypeValueCodecMap codecMap;

  public ArrayValueCodec(CodecRegistry codecRegistry, BsonTypeValueCodecMap codecMap) {
    this.codecRegistry = codecRegistry;
    this.codecMap = codecMap;
  }

  @Override
  public ArrayValue decode(BsonReader reader, DecoderContext decoderContext) {
    var values = new LinkedList<Value>();
    reader.readStartArray();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      values.add(ContainerCodecHelper.readValue(reader, decoderContext, codecMap));
    }
    reader.readEndArray();
    return new ArrayValue(values);
  }

  @Override
  public void encode(BsonWriter writer, ArrayValue value, EncoderContext encoderContext) {
    writer.writeStartArray();
    value.iterator().forEachRemaining(it -> writeValue(writer, it, encoderContext));
    writer.writeEndArray();
  }

  @Override
  public Class<ArrayValue> getEncoderClass() {
    return ArrayValue.class;
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
