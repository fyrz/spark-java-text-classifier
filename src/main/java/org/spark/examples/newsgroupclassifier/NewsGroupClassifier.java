package org.spark.examples.newsgroupclassifier;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class NewsGroupClassifier
{
    public static void main(String[] args)
        throws IOException
    {
        final String path = "/vagrant/20_newsgroups/out";

        SparkConf conf = new SparkConf().setAppName("Newsgroup classifier.");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext jsql = new SQLContext(sc);


        JavaRDD<String> rawData = sc.textFile(path).cache();

        JavaRDD<LabeledDocument> labeledData = rawData.map(
            new Function<String, LabeledDocument>() {
              public LabeledDocument call(String s) {
                String[] parts = s.split("|||");
                return new LabeledDocument(Double.valueOf(parts[0]), parts[1]);
              }
        }).cache();

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
            .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

        // Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(trainingData);
    }
}
