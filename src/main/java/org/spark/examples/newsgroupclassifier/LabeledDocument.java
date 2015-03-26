package org.spark.examples.newsgroupclassifier;

import java.io.Serializable;


public class LabeledDocument implements Serializable {
  public final Double label;
  public final String text;

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
