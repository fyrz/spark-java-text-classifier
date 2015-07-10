package org.fyrz.textclassifier.classifcation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.collections.TopNSet;
import org.fyrz.textclassifier.beans.CategoryConfidence;
import scala.Tuple2;

public class NaiveBayesClassifier
    implements PairFlatMapFunction<LabeledPoint, String, Integer>
{
    private final NaiveBayesModel model;

    public NaiveBayesClassifier(final SparkContext sparkContext, final String naiveBayesModelPath)
    {
        model = NaiveBayesModel.load(sparkContext, naiveBayesModelPath);
    }

    @Override
    public List<Tuple2<String, Integer>> call(final LabeledPoint labeledPoint)
        throws Exception
    {
        List<Tuple2<String, Integer>> results = new ArrayList<>(2);

        double expectedLabel = labeledPoint.label();
        TopNSet<CategoryConfidence> topNSet = calculateScoresForAllCategoriesMultiNominal(model,
            labeledPoint.features(), 0.32);

        // FN
        int matchType = 0;
        String key = "FN";
        for (CategoryConfidence categoryConfidence : topNSet) {
            if (categoryConfidence.getCategory() == expectedLabel) {
                // TP
                matchType = 1;
                key = "TP";
                results.add(new Tuple2<String, Integer>("TP " + String.format("%.2f",
                    categoryConfidence.getConfidence()), 1));
                break;
            }
            else {
                // FP
                matchType = 2;
                key = "FP";
                results.add(new Tuple2<String, Integer>("FP " + String.format("%.2f",
                    categoryConfidence.getConfidence()), 1));
            }
        }
        return results;
    }

    private static double calculateTopScoreForAllCategoriesMultiNominal(NaiveBayesModel model, Vector testData) {
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

    private static TopNSet<CategoryConfidence> calculateScoresForAllCategoriesMultiNominal(
            final NaiveBayesModel model, final Vector testData) {
        return calculateScoresForAllCategoriesMultiNominal(model, testData, -1);
    }

    private static TopNSet<CategoryConfidence> calculateScoresForAllCategoriesMultiNominal(
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

