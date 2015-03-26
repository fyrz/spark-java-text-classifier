package org.spark.examples.newsgroupclassifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;


public class NewsGroupCrossValidation {
  public static void main(String[] args)
      throws IOException {
    final String path = "/vagrant/20_newsgroups/out";

    SparkConf conf = new SparkConf().setAppName("Newsgroup classifier.");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(sc);


    JavaRDD<String> rawData = sc.textFile(path).cache();
    double[] splitRatios = {0.7d, 0.3d};
    JavaRDD<String>[] splitData = rawData.randomSplit(splitRatios, 42l);

    JavaRDD<LabeledDocument> labeledData = splitData[0].map(
        new Function<String, LabeledDocument>() {
          public LabeledDocument call(String s) {
            String[] parts = s.split("[|]{3}");

            Double label = 0d;
            if (Double.valueOf(parts[0]).equals(1d)) {
              label = 1d;
            }

            if (parts.length == 1) {
              return new LabeledDocument(label, "");
            }
            return new LabeledDocument(label, parts[1]);
          }
        }
    ).cache();

    DataFrame trainingData = jsql.createDataFrame(labeledData, LabeledDocument.class);

    Tokenizer tokenizer = new TextTokenizer().
        setInputCol("text").
        setOutputCol("words");
    HashingTF hashingTF = new HashingTF()
        .setNumFeatures(1000)
        .setInputCol(tokenizer.getOutputCol())
        .setOutputCol("features");
    LogisticRegression lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01);

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

    int[] featureNumber = {50000, 10000, 5000};
    double[] regparams = {0.01, 0.1, 1.0};
    ParamGridBuilder paramGridBuilder = new ParamGridBuilder().
        addGrid(hashingTF.numFeatures(), featureNumber).
        addGrid(lr.regParam(), regparams);

    // Perform cross validation
    CrossValidator cv = new CrossValidator().
        setNumFolds(3).
        setEstimator(pipeline).
        setEstimatorParamMaps(paramGridBuilder.build()).
        setEvaluator(new BinaryClassificationEvaluator());

    // Trained model
    Model model = cv.fit(trainingData);

    JavaRDD<Document> unlabeledData = splitData[1].map(
        new Function<String, Document>() {
          public Document call(String s) {
            String[] parts = s.split("[|]{3}");
            if (parts.length == 1) {
              return new Document("");
            }
            return new Document(parts[1]);
          }
        }
    ).cache();
    DataFrame testData = jsql.createDataFrame(unlabeledData, Document.class);

    DataFrame predictions = model.transform(testData);
    for (Row r : predictions.select("text", "probability", "prediction").collect()) {
      System.out.println("( --> prob=" + r.get(1) + ", prediction=" + r.get(2));
    }
  }
}
