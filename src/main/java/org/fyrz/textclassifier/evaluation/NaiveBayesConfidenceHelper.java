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
                score += (entry.getValue() * model.theta()[i][entry.getKey()]);
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

    public static TopNSet calculateScoresForAllCategoriesMultiNominal(NaiveBayesModel model, Vector testData) {
        final TopNSet topNSet = new TopNSet(3);

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
                score += (entry.getValue() * model.theta()[i][entry.getKey()]);
            }
            score += model.pi()[i];
            topNSet.add(new CategoryConfidence(model.labels()[i], score));
        }
        return topNSet;
    }
}
