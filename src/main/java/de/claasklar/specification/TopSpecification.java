package de.claasklar.specification;

public interface TopSpecification extends Specification {
  Runnable runnable();

  String getName();
}
