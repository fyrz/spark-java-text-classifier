package org.fyrz.textclassifier.evaluation;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.fyrz.textclassifier.TopNSet;
import org.fyrz.textclassifier.beans.CategoryConfidence;


public class NaiveBayesConfidenceHelper
{
    public static double calculateTopScoreForAllCategoriesMultiNominal(NaiveBayesModel model, Vector testData) {
        double predictedCategory = -1;
        double scoreSum = 0;
        double maxScore = Double.NEGATIVE_INFINITY;

        double[] testArray = testData.toArray();
        Map<Integer, Double> sparseMap = new HashMap<Integer, Double>();
        for (int i = 0; i < testArray.length; i++) {
            if (testArray[i] != 0) {
                sparseMap.put(i, testArray[i]);
            }
        }

        for (int i = 0; i < model.pi().length; i++) {
            double score = 0.0;
            for(Map.Entry<Integer, Double> entry : sparseMap.entrySet()) {
                score += (entry.getValue().doubleValue() * model.theta()[i][entry.getKey().intValue()]);
            }

            score += model.pi()[i];
            scoreSum += score;
            if (maxScore < score) {
                predictedCategory = model.labels()[i];
                maxScore = score;
            }
        }

        System.out.println("Percent: " + maxScore / (scoreSum / 100));
        return predictedCategory;
    }

    public static TopNSet<CategoryConfidence> calculateScoresForAllCategoriesMultiNominal(
        final NaiveBayesModel model, final Vector testData) {
      return calculateScoresForAllCategoriesMultiNominal(model, testData, -1);
    }

    public static TopNSet<CategoryConfidence> calculateScoresForAllCategoriesMultiNominal(
        final NaiveBayesModel model, final Vector testData, final double threshold) {
      TopNSet<CategoryConfidence> topNSet = new TopNSet<>(3);
      double scoreSum = 0;

      double[] testArray = testData.toArray();
      Map<Integer, Double> sparseMap = new HashMap<Integer, Double>();
      for (int i = 0; i < testArray.length; i++) {
        if (testArray[i] != 0) {
          sparseMap.put(i, testArray[i]);
        }
      }

      for (int i = 0; i < model.pi().length; i++) {
        double score = 0.0;
        for (Map.Entry<Integer, Double> entry : sparseMap.entrySet()) {
          score += (entry.getValue().doubleValue() * model.theta()[i][entry.getKey().intValue()]);
        }

        score += model.pi()[i];
        scoreSum += score;
        topNSet.add(new CategoryConfidence(model.labels()[i], score));
      }

      // filter topN set using threshold
      if (threshold != -1) {
          TopNSet<CategoryConfidence> tmpTopNSet = new TopNSet<>(3);
          for(CategoryConfidence categoryConfidence : topNSet) {
            if ((categoryConfidence.getConfidence() / (scoreSum / 100)) > threshold) {
              tmpTopNSet.add(new CategoryConfidence(
                  categoryConfidence.getCategory(),
                  (categoryConfidence.getConfidence() / (scoreSum / 100))));
            }
          }
          topNSet = tmpTopNSet;
      }
      return topNSet;
    }
}
