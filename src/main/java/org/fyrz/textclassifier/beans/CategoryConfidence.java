package org.fyrz.textclassifier.beans;

public class CategoryConfidence implements Comparable<CategoryConfidence>
{
  private final double category;
  private final double confidence;

  public CategoryConfidence(final double category, final double confidence){
    this.category = category;
    this.confidence = confidence;
  }

  public double getCategory() {
    return category;
  }

  public double getConfidence() {
    return confidence;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CategoryConfidence that = (CategoryConfidence) o;
    if (Double.compare(that.category, category) != 0) {
      return false;
    }
    if (Double.compare(that.confidence, confidence) != 0) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(category);
    result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(confidence);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public int compareTo(final CategoryConfidence o) {
    return ((Double) o.confidence).compareTo(((Double) confidence));
  }
}
