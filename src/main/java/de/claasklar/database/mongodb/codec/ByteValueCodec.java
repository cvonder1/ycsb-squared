package de.claasklar.database.mongodb.codec;

import de.claasklar.primitives.document.ByteValue;
import org.bson.BsonBinary;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class ByteValueCodec implements Codec<ByteValue> {

  @Override
  public ByteValue decode(BsonReader reader, DecoderContext decoderContext) {
    return new ByteValue(reader.readBinaryData().getData());
  }

  @Override
  public void encode(BsonWriter writer, ByteValue value, EncoderContext encoderContext) {
    writer.writeBinaryData(new BsonBinary(value.value()));
  }

  @Override
  public Class<ByteValue> getEncoderClass() {
    return ByteValue.class;
  }
}
