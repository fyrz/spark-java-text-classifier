package org.fyrz.textclassifier.beans;

import java.io.Serializable;

public class Document implements Serializable {
  private final String text;

  public Document(final String text) {
    this.text = text;
  }

  public String getText() {
    return text;
  }
}
