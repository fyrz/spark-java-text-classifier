package org.fyrz.textclassifier.experimental;

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
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.fyrz.textclassifier.beans.Document;
import org.fyrz.textclassifier.beans.LabeledDocument;
import org.fyrz.textclassifier.classifcation.ClassifierUtilities;
import org.fyrz.textclassifier.tokenizer.SparkLuceneTokenizer;

import java.io.IOException;

/**
 * Logistic regression evaluation application
 *
 * <p>Uses new Pipeline API & cross validation.</p>
 *
 * <p>Experimental: Models cannot be saved at the moment.</p>
 */
public class LrEvaluationApplicationWithCV
{
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
        new ClassifierUtilities.LabeledTextToLabeledDocumentRDDFunction()).cache();

    DataFrame trainingData = jsql.createDataFrame(labeledData, LabeledDocument.class);

    Tokenizer tokenizer = new SparkLuceneTokenizer().
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
    double[] regParams = {0.01, 0.1, 1.0};
    ParamGridBuilder paramGridBuilder = new ParamGridBuilder().
        addGrid(hashingTF.numFeatures(), featureNumber).
        addGrid(lr.regParam(), regParams);

    // Perform cross validation
    CrossValidator cv = new CrossValidator().
        setNumFolds(3).
        setEstimator(pipeline).
        setEstimatorParamMaps(paramGridBuilder.build()).
        setEvaluator(new BinaryClassificationEvaluator());

    // Trained model
    Model model = cv.fit(trainingData).bestModel();

    // Classify data using the model
    JavaRDD<Document> unlabeledData = splitData[1].map(
        new ClassifierUtilities.LabeledTextToUnlabeledDocumentRDDFunction()).cache();
    DataFrame testData = jsql.createDataFrame(unlabeledData, Document.class);

    DataFrame predictions = model.transform(testData);
    for (Row r : predictions.select("text", "probability", "prediction").collect()) {
      System.out.println("( --> prob=" + r.get(1) + ", prediction=" + r.get(2));
    }
  }
}
