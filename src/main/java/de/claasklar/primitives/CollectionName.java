package de.claasklar.primitives;

import java.util.regex.Pattern;

public record CollectionName(String name) {

  private static final Pattern legalCharacters = Pattern.compile("[^a-zA-Z0-9_]");

  public CollectionName {
    if (name.isBlank()) {
      throw new IllegalArgumentException("name cannot be blank");
    }
    if (legalCharacters.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "name contains illegal characters. [a-zA-Z0-9_] are legal characters");
    }
  }
}
