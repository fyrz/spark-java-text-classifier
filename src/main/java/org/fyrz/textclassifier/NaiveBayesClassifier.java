package org.fyrz.textclassifier;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.beans.CategoryConfidence;
import org.fyrz.textclassifier.tokenizer.NgramTokenizer;
import scala.Tuple2;
import static org.fyrz.textclassifier.evaluation.NaiveBayesConfidenceHelper.calculateScoresForAllCategoriesMultiNominal;


public class NaiveBayesClassifier {

  static class LabeledTextToRDDTransformerFunction implements Function<String, LabeledPoint>, Serializable {

    final HashingTF hashingTF = new HashingTF(10000);

    @Override
    public LabeledPoint call(String s) throws Exception {
      String[] parts = s.split("[|]{3}");
      double label = Double.valueOf(parts[0]).doubleValue();

      List<String> tokenList = new ArrayList<>();
      NgramTokenizer ngramTokenizer = new NgramTokenizer();
      Reader reader = new StringReader(s);
      try {
        TokenStream tokenStream = ngramTokenizer.tokenStream("contents", reader);
        CharTermAttribute term = tokenStream.getAttribute(CharTermAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
          tokenList.add(term.toString());
        }
      } catch (IOException e) {
      }
      return new LabeledPoint(label, hashingTF.transform(tokenList));
    }
  }

  static class NaiveBayesPredictionFunction implements PairFlatMapFunction<LabeledPoint, String, Integer> {

    private final NaiveBayesModel model;

    public NaiveBayesPredictionFunction(final NaiveBayesModel model) {
      this.model = model;
    }

    @Override
    public List<Tuple2<String, Integer>> call(final LabeledPoint labeledPoint)
      throws Exception {
      List<Tuple2<String, Integer>> results = new ArrayList<>(2);

        double expectedLabel = labeledPoint.label();
        TopNSet<CategoryConfidence> topNSet = calculateScoresForAllCategoriesMultiNominal(model, labeledPoint.features(), 0.32);


        // FN
        int matchType = 0;
        String key = "FN";
        for (CategoryConfidence categoryConfidence : topNSet) {
            if (categoryConfidence.getCategory() == expectedLabel) {
                // TP
                matchType = 1;
                key = "TP";
                results.add(new Tuple2<String, Integer>("TP "+String.format("%.2f", categoryConfidence.getConfidence()), 1));
                break;
            } else {
                // FP
                matchType = 2;
                key = "FP";
                results.add(new Tuple2<String, Integer>("FP "+String.format("%.2f", categoryConfidence.getConfidence()), 1));
            }
        }
        return results;
      }
  }

  public static void main(String[] args) {
    final String path = "/vagrant/input.txt";
    final String validationPath = "/vagrant/validation.txt";

    SparkConf conf = new SparkConf().setAppName("Naive bayes classifier.");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rawData = sc.textFile(path).cache();
    double[] splitRatios = {0.7d, 0.3d};
    JavaRDD<String>[] splitData = rawData.randomSplit(splitRatios, 42l);

    JavaRDD<LabeledPoint> trainingData = splitData[0].map(
        new LabeledTextToRDDTransformerFunction());

    final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd());

    JavaRDD<LabeledPoint> testData = splitData[1].map(new LabeledTextToRDDTransformerFunction());
    testData.flatMapToPair(new NaiveBayesPredictionFunction(model)).
    reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(final Integer v1, final Integer v2)
          throws Exception {
        return v1.intValue()+v2.intValue();
      }
    }).saveAsTextFile("testOut");

    JavaRDD<String> validationData = sc.textFile(validationPath).cache();
    JavaRDD<LabeledPoint> valData = validationData.map(new LabeledTextToRDDTransformerFunction());

    valData.flatMapToPair(new NaiveBayesPredictionFunction(model)).
      reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(final Integer v1, final Integer v2)
          throws Exception {
        return v1.intValue()+v2.intValue();
      }
    }).saveAsTextFile("validationOut");

    sc.close();
  }
}
