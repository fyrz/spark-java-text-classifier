package org.fyrz.textclassifier.classifcation;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.collections.TopNSet;
import org.fyrz.textclassifier.beans.CategoryConfidence;
import scala.Tuple2;
import static org.fyrz.textclassifier.evaluation.NaiveBayesConfidenceHelper.calculateScoresForAllCategoriesMultiNominal;


public class NaiveBayesClassifier
    implements PairFlatMapFunction<LabeledPoint, String, Integer>
{
    private final NaiveBayesModel model;

    public NaiveBayesClassifier(final SparkContext sparkContext, final String naiveBayesModelPath)
    {
        this.model = NaiveBayesModel.load(sparkContext, naiveBayesModelPath);
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
}

