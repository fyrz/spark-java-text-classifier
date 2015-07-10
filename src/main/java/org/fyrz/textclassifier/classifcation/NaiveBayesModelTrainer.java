package org.fyrz.textclassifier.classifcation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;


public class NaiveBayesModelTrainer
{
    public void train(final String modelPath, final JavaRDD<String> trainingData)
    {
        JavaRDD<LabeledPoint> labeledTrainingData = trainingData.map(
            new ClassifierUtilities.LabeledTextToRDDTransformerFunction());
        final NaiveBayesModel model = NaiveBayes.train(labeledTrainingData.rdd());
        model.save(trainingData.context(), modelPath);
    }
}
