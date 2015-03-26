package org.fyrz.textclassifier;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.tokenizer.TextAnalyzer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class NaiveBayesClassifier {
  public static void main(String[] args) {
    final String path = "/vagrant/20_newsgroups/out";

    SparkConf conf = new SparkConf().setAppName("Newsgroup classifier.");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> rawData = sc.textFile(path).cache();
    double[] splitRatios = {0.7d, 0.3d};
    JavaRDD<String>[] splitData = rawData.randomSplit(splitRatios, 42l);

    final HashingTF hashingTF = new HashingTF(10000);


    JavaRDD<LabeledPoint> labeledPointJavaRDD = splitData[0].map(
        new Function<String, LabeledPoint>() {
          public LabeledPoint call(String s) {

            String[] parts = s.split("[|]{3}");
            Double label = Double.valueOf(parts[0]);

            List<String> tokenList = new ArrayList<>();
            TextAnalyzer sTextAnalyzer = new TextAnalyzer();
            Reader reader = new StringReader(s);
            try {
              TokenStream tokenStream = sTextAnalyzer.tokenStream("contents", reader);
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
        }).cache();

    NaiveBayesModel model = NaiveBayes.train(labeledPointJavaRDD.rdd());
    model.save(sc.sc(), "model.txt");
    sc.close();
  }
}
