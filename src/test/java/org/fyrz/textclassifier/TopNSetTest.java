package org.fyrz.textclassifier;

import org.fyrz.textclassifier.beans.CategoryConfidence;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


public class TopNSetTest {

    @Test
    public void topNSet() {

      CategoryConfidence[] confidences = new CategoryConfidence[]{
          new CategoryConfidence(1, 10),
          new CategoryConfidence(2, 11),
          new CategoryConfidence(3,  9),
          new CategoryConfidence(4, 15)
      };


      TopNSet<CategoryConfidence> topNSet = new TopNSet<CategoryConfidence>(3);
      for (CategoryConfidence categoryConfidence : confidences) {
        topNSet.add(categoryConfidence);
      }

      assertThat(topNSet.size()).isEqualTo(3);
      assertThat(topNSet).
          extracting("confidence").
          contains(10.0, 11.0, 15.0);
        assertThat(topNSet).
            extracting("confidence").
            containsExactly(15.0, 11.0, 10.0);


    }
}
