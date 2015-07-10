package org.fyrz.textclassifier.classifcation;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.beans.Document;
import org.fyrz.textclassifier.beans.LabeledDocument;
import org.fyrz.textclassifier.tokenizer.NgramTokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


public class ClassifierUtilities {

    final static String labeledTextSeparatorRegex = "[|]{3}";

    public static class LabeledTextToLabeledPointRDDFunction implements Function<String, LabeledPoint>, Serializable {
        final HashingTF hashingTF = new HashingTF(10000);

        @Override
        public LabeledPoint call(String s) throws Exception {
            String[] parts = s.split(labeledTextSeparatorRegex);
            double label = Double.valueOf(parts[0]).doubleValue();

            List<String> tokenList = new ArrayList<>();
            NgramTokenizer ngramTokenizer = new NgramTokenizer();
            Reader reader = new StringReader(parts[1]);
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

    public static class LabeledTextToLabeledDocumentRDDFunction implements Function<String, LabeledDocument> {

        @Override
        public LabeledDocument call(String s) {
            String[] parts = s.split(labeledTextSeparatorRegex);
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

    public static class LabeledTextToUnlabeledDocumentRDDFunction implements Function<String, Document> {

        @Override
        public Document call(String s) {
            String[] parts = s.split(labeledTextSeparatorRegex);
            if (parts.length == 1) {
                return new Document("");
            }
            return new Document(parts[1]);
        }
    }
}
