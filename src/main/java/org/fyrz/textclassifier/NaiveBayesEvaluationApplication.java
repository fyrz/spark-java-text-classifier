package org.fyrz.textclassifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.classifcation.ClassifierUtilities;
import org.fyrz.textclassifier.classifcation.NaiveBayesClassifier;
import org.fyrz.textclassifier.classifcation.NaiveBayesModelTrainer;

/**
 *
 */
public class NaiveBayesEvaluationApplication {

    public static void main(String[] args) {

        // -- Prepare
        final String trainingDataPath = "/vagrant/input.txt";
        final String validationDataPath = "/vagrant/validation.txt";
        final String modelPath = "/vagrant/testModel";

        SparkConf conf = new SparkConf().setAppName("Naive bayes classifier.");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // -- Train model
        JavaRDD<String> trainingData = sc.textFile(trainingDataPath).cache();
        final NaiveBayesModelTrainer naiveBayesModelTrainer = new NaiveBayesModelTrainer();
        naiveBayesModelTrainer.train(modelPath, trainingData);

        // -- Classification of data
        JavaRDD<String> validationData = sc.textFile(validationDataPath).cache();
        JavaRDD<LabeledPoint> valData = validationData.map(new ClassifierUtilities.LabeledTextToRDDTransformerFunction());

        JavaPairRDD<String, Integer> classifiedData = valData.flatMapToPair(new NaiveBayesClassifier(valData.context(), modelPath));

        // -- Aggregate classification results for presentation
        classifiedData.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer v1, final Integer v2)
                    throws Exception {
                return v1.intValue() + v2.intValue();
            }
        }).saveAsTextFile("validationOut");

        sc.close();
    }
}
