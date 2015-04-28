package org.fyrz.textclassifier;

import java.util.TreeSet;

import org.fyrz.textclassifier.beans.CategoryConfidence;


public class TopNSet<Z extends Comparable<?>> extends TreeSet<Z> {
    private final int size;

    public TopNSet(final int size) {
      super();
      this.size = size;
    }

    @Override
    public boolean add(final Z e) {
      if (size() >= this.size) {
        remove(last());
      }
      return super.add(e);
    }
}
