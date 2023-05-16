package de.claasklar.generation.inserters;

import de.claasklar.primitives.document.Value;
import java.util.function.Function;

public interface InserterFactory<T extends Value> extends Function<Context, Inserter<T>> {}
