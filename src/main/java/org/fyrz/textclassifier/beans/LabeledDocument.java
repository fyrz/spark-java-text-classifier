package org.fyrz.textclassifier.beans;

import java.io.Serializable;

public class LabeledDocument implements Serializable {

  private final Double label;
  private final String text;

  public LabeledDocument(final Double pLabel, final String pText) {
    label = pLabel;
    text = pText;
  }

  public Double getLabel() {
    return label;
  }

  public String getText() {
    return text;
  }
}
