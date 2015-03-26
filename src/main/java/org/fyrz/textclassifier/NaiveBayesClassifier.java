package org.fyrz.textclassifier;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.tokenizer.LowercaseWhitespaceTokenizer;
import org.fyrz.textclassifier.tokenizer.NgramTokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class NaiveBayesClassifier {

  static class LabeledTextToRDDTransformerFunction implements Function<String, LabeledPoint>, Serializable {

    final HashingTF hashingTF = new HashingTF(10000);

    @Override
    public LabeledPoint call(String s) throws Exception {
      String[] parts = s.split("[|]{3}");
      Double label = Double.valueOf(parts[0]);

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
        // TODO handle java.io.IOException
      }
      return new LabeledPoint(label, hashingTF.transform(tokenList));
    }
  }


  public static void main(String[] args) {
    final String path = "/vagrant/20_newsgroups/out";

    SparkConf conf = new SparkConf().setAppName("Naive bayes classifier.");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rawData = sc.textFile(path).cache();
    double[] splitRatios = {0.7d, 0.3d};
    JavaRDD<String>[] splitData = rawData.randomSplit(splitRatios, 42l);

    JavaRDD<LabeledPoint> trainingData = splitData[0].map(
        new LabeledTextToRDDTransformerFunction()).cache();

    ChiSqSelector chiSqSelector = new ChiSqSelector(500);
    final ChiSqSelectorModel chiSqSelectorModel = chiSqSelector.fit(trainingData.rdd());

    // reduce feature set
    JavaRDD<LabeledPoint> reducedTrainingData = trainingData.map(new Function<LabeledPoint, LabeledPoint>() {
      @Override
      public LabeledPoint call(LabeledPoint labeledPoint) throws Exception {
        return new LabeledPoint(labeledPoint.label(), chiSqSelectorModel.transform(labeledPoint.features()));
      }
    });

    final NaiveBayesModel model = NaiveBayes.train(reducedTrainingData.rdd());

    JavaRDD<LabeledPoint> testData = splitData[0].map(
        new LabeledTextToRDDTransformerFunction()).cache();

    testData.map(new Function<LabeledPoint, String>(){
      @Override
      public String call(LabeledPoint labeledPoint) throws Exception {
        return String.format("retrieved %f, expected: %f",
            model.predict(labeledPoint.features()),
            labeledPoint.label());
      }
    }).saveAsTextFile("out.txt");

    sc.close();
  }
}
