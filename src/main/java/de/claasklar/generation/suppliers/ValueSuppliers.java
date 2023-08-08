package de.claasklar.generation.suppliers;

import de.claasklar.primitives.document.*;
import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.random.distribution.SampleNonRepeating;
import de.claasklar.random.distribution.UniformDistribution;
import de.claasklar.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ValueSuppliers {

  private final RandomNumberGenerator random;

  public ValueSuppliers(RandomNumberGenerator randomNumberGenerator) {
    this.random = randomNumberGenerator;
  }

  public ValueSupplier uniformIntSupplier(int lower, int upper) {
    return () -> new IntValue(random.nextInt(lower, upper));
  }

  public ValueSupplier uniformLongSupplier(long lower, long upper) {
    return () -> new LongValue(random.nextLong(lower, upper));
  }

  public ValueSupplier uniformLengthStringSupplier(int lower, int upper) {
    return () -> {
      var length = random.nextInt(lower, upper);
      var sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        sb.append((char) random.nextLong(0, Character.MAX_VALUE));
      }
      return new StringValue(sb.toString());
    };
  }

  public ValueSupplier repeat(String key, Supplier<Integer> times, ValueSupplier singleSupplier) {
    return () -> {
      var length = times.get();
      var array = new ArrayValue();
      for (int i = 0; i < length; i++) {
        array.add(singleSupplier.get());
      }
      return array;
    };
  }

  public ValueSupplier objectSupplier(Consumer<Map<String, ValueSupplier>> config) {
    Map<String, ValueSupplier> map = new HashMap<>();
    config.accept(map);
    return this.objectSupplier(map);
  }

  public ValueSupplier objectSupplier(Map<String, ValueSupplier> fields) {
    return () -> {
      var object = new NestedObjectValue();
      fields.entrySet().stream()
          .forEach(entry -> object.put(entry.getKey(), entry.getValue().get()));
      return object;
    };
  }

  public ValueSupplier prefixedRandomString(String prefix, Supplier<String> stringSupplier) {
    return () -> new StringValue(prefix + stringSupplier.get());
  }

  public ValueSupplier selectNonRepeating(
      String prefix, String[] options, int k, BiFunction<String, String, String> combiner) {
    var sourceDist = new UniformDistribution<>(options);
    var sampleDist = new SampleNonRepeating<>(sourceDist);
    return () -> {
      var selections = sampleDist.select(k);
      var agg = prefix;
      for (var selection : selections) {
        agg = combiner.apply(agg, selection);
      }
      return new StringValue(agg);
    };
  }

  public ValueSupplier selectNonRepeating(
      String[] options, int k, BiFunction<String, String, String> combiner) {
    return selectNonRepeating("", options, k, combiner);
  }

  public ValueSupplier alphaNumRandomLengthString(int min, int max) {
    var alphaNum = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
    return () -> {
      var length = random.nextInt(min, max);
      long rChar = 0;
      var sb = new StringBuilder();
      for (int i = 0; i < length; i++) {
        if (i % 5 == 0) {
          rChar = random.nextLong(0, Long.MAX_VALUE);
        }
        sb.append(alphaNum.charAt((int) (rChar % alphaNum.length())));
        rChar >>= 6;
      }
      return new StringValue(sb.toString());
    };
  }

  public ValueSupplier uniformSelection(String[] choices) {
    var distribution = new UniformDistribution<>(choices);
    return () -> new StringValue(distribution.sample());
  }

  public ValueSupplier fixedString(String value) {
    var stringValue = new StringValue(value);
    return () -> stringValue;
  }

  public ValueSupplier weightedSelection(List<Pair<Double, String>> options) {
    var sum = options.stream().map(Pair::first).reduce(Double::sum);
    var acc =
        new Object() {
          double val = 0;
        };
    var accumulatedWeights =
        options.stream()
            .map(
                it -> {
                  acc.val += it.first();
                  return it.mapFirst(prev -> acc.val);
                })
            .toArray(Pair[]::new);
    return () -> {
      var choice = random.nextDouble(0, acc.val);
      for (var option : accumulatedWeights) {
        if ((double) option.first() < choice) {
          return new StringValue((String) option.second());
        }
      }
      return new StringValue((String) accumulatedWeights[accumulatedWeights.length - 1].second());
    };
  }
}
