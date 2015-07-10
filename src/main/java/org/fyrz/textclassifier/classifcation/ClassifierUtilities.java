package org.fyrz.textclassifier.classifcation;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.fyrz.textclassifier.tokenizer.NgramTokenizer;


public class ClassifierUtilities
{
    public static class LabeledTextToRDDTransformerFunction implements Function<String, LabeledPoint>, Serializable
    {
        final HashingTF hashingTF = new HashingTF(10000);

        @Override
        public LabeledPoint call(String s) throws Exception {
            String[] parts = s.split("[|]{3}");
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
}
