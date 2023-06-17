package de.claasklar.database.mongodb.codec;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

public class OurDocumentCodecRegistry implements CodecRegistry {

  private final CodecRegistry delegate;

  public OurDocumentCodecRegistry() {
    var bsonTypeValueCodecMap = new BsonTypeValueCodecMap(BsonTypeValueClassMap.DEFAULT_MAP, this);
    this.delegate =
        fromCodecs(
            new OurDocumentCodec(this, bsonTypeValueCodecMap),
            new IdCodec(),
            new DoubleValueCodec(),
            new BoolValueCodec(),
            new ArrayValueCodec(this, bsonTypeValueCodecMap),
            new StringValueCodec(),
            new NestedObjectValueCodec(this, bsonTypeValueCodecMap),
            new ByteValueCodec(),
            new NullValueCodec(),
            new IntValueCodec(),
            new LongValueCodec());
  }

  @Override
  public <T> Codec<T> get(Class<T> clazz) {
    return this.delegate.get(clazz);
  }

  @Override
  public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
    return this.delegate.get(clazz, registry);
  }
}
