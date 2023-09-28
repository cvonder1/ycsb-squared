package de.claasklar.benchmark;

import static de.claasklar.primitives.document.ArrayValue.array;
import static de.claasklar.primitives.document.NestedObjectValue.object;
import static de.claasklar.primitives.document.StringValue.string;

import com.mongodb.ConnectionString;
import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.VariableAggregationGenerator;
import de.claasklar.generation.pipes.Pipes;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.query.AggregationOptions;
import java.util.Arrays;
import java.util.function.Consumer;

public class CruditBenchmark {

  private static final String[] names = {
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "burnished",
    "chartreuse",
    "chiffon",
    "chocolate",
    "coral",
    "cornflower",
    "cornsilk",
    "cream",
    "cyan",
    "dark",
    "deep",
    "dim",
    "dodger",
    "drab",
    "firebrick",
    "floral",
    "forest",
    "frosted",
    "gainsboro",
    "ghost",
    "goldenrod",
    "green",
    "grey",
    "honeydew",
    "hot",
    "indian",
    "ivory",
    "khaki",
    "lace",
    "lavender",
    "lawn",
    "lemon",
    "light",
    "lime",
    "linen",
    "magenta",
    "maroon",
    "medium",
    "metallic",
    "midnight",
    "mint",
    "misty",
    "moccasin",
    "navajo",
    "navy",
    "olive",
    "orange",
    "orchid",
    "pale",
    "papaya",
    "peach",
    "peru",
    "pink",
    "plum",
    "powder",
    "puff",
    "purple",
    "red",
    "rose",
    "rosy",
    "royal",
    "saddle",
    "salmon",
    "sandy",
    "seashell",
    "sienna",
    "sky",
    "slate",
    "smoke",
    "snow",
    "spring",
    "steel",
    "tan",
    "thistle",
    "tomato",
    "turquoise",
    "violet",
    "wheat",
    "white",
    "yellow"
  };

  public Benchmark createCruditBenchmark() {
    return BenchmarkBuilder.builder()
        .writeSpecification(
            prototypeResourceWriteConfig(
                "prototype_resources", "prototype_resources_first_level", 0.6))
        .writeSpecification(
            prototypeResourceWriteConfig(
                "prototype_resources_first_level", "prototype_resources_second_level", 0.99))
        .writeSpecification(
            prototypeResourceWriteConfig(
                "prototype_resources_second_level", "prototype_resources_no_level", 1))
        .primaryWriteSpecification(
            "prototype_api",
            config ->
                config
                    .collectionName("prototype_api")
                    .documentGenerator(
                        ContextDocumentGeneratorBuilder.builder()
                            // 30 days window
                            .field(
                                "last_updated",
                                s ->
                                    s.uniformLongSupplier(
                                        1693584560200L, 1693584560200L + 2592000000L))
                            .fieldFromPipe(
                                "prototype_resources",
                                fieldPipeBuilder ->
                                    fieldPipeBuilder.selectCollection(
                                        new CollectionName("prototype_resources"),
                                        Pipes.PipeBuilder::toArray))
                            .build())
                    .referenceDistributionConfig(
                        referencesDistributionConfig ->
                            referencesDistributionConfig
                                .documentDistribution(
                                    d ->
                                        d.collectionName("prototype_resources")
                                            .idDistribution(i -> i.uniform(Integer.MAX_VALUE - 16))
                                            .recomputable())
                                .countDistribution(c -> c.geometric(0.01))))
        .readSpecification(
            config ->
                config
                    .name("find_one_resource")
                    .queryGenerator(
                        (s, i) ->
                            new VariableAggregationGenerator(
                                new CollectionName("prototype_api"),
                                AggregationOptions.aggregate("prototype_api")
                                    .pipeline(
                                        Arrays.asList(
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$in",
                                                        array(
                                                            string("$$resource_id"),
                                                            string("$prototype_resources._id"))))),
                                            object("$unwind", string("$prototype_resources")),
                                            object(
                                                "$replaceRoot",
                                                object("newRoot", string("$prototype_resources"))),
                                            object(
                                                "$match",
                                                object(
                                                    "$expr",
                                                    object(
                                                        "$eq",
                                                        array(
                                                            string("$_id"),
                                                            string("$$resource_id"))))))),
                                s.existingId(
                                    "resource_id",
                                    new CollectionName("prototype_resources"),
                                    i.uniform(Integer.MAX_VALUE - 16)))))
        .loadPhase(config -> config.primaryWriteSpecification(1000 / 10, "prototype_api"))
        .database((d) -> d.connectionString(new ConnectionString("mongodb://localhost:27017")))
        .transactionPhase(
            config ->
                config.weightedRandom(
                    w ->
                        w.threadCount(40)
                            .totalCount(4000 / 10)
                            .targetOps(10)
                            .weightedSpecification(0.5, "find_one_resource")
                            .weightedSpecification(0.5, "prototype_api")))
        .build();
  }

  private static Consumer<BenchmarkBuilder.DocumentGenerationSpecificationConfig>
      prototypeResourceWriteConfig(
          String collectionName, String nextLevelName, double pNextLevelZero) {
    var documentContextGenerator =
        ContextDocumentGeneratorBuilder.builder()
            .field(
                "name",
                v -> v.selectNonRepeating(names, 3, (first, second) -> first + "_" + second))
            .fieldObjectInserters(
                o -> o.maybeField("field1", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field2", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field3", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field4", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field5", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field6", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field7", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field8", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field9", 0.8, v -> v.alphaNumRandomLengthString(10, 40)))
            .fieldObjectInserters(
                o -> o.maybeField("field10", 0.8, v -> v.alphaNumRandomLengthString(10, 40)));
    if (pNextLevelZero < 1) {
      documentContextGenerator.fieldFromPipe(
          "children",
          f -> f.selectCollection(new CollectionName(nextLevelName), Pipes.PipeBuilder::toArray));
    }
    return documentConfig ->
        documentConfig
            .collectionName(collectionName)
            .documentGenerator(documentContextGenerator.build())
            .referenceDistributionConfig(
                referencesDistributionConfig ->
                    referencesDistributionConfig
                        .countDistribution(
                            c -> c.zeroOrElse(pNextLevelZero, () -> c.geometric(0.1)))
                        .documentDistribution(
                            distConfig ->
                                distConfig
                                    .idDistribution(i -> i.uniform(Integer.MAX_VALUE - 16))
                                    .collectionName(nextLevelName)
                                    .recomputable()));
  }
}
