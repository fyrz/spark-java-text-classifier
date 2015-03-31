package org.fyrz.textclassifier.tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


public class NgramTokenizerTest
{
    private static List<String> tokenize(String text)
    {
        List<String> tokenList = new ArrayList<>();
        NgramTokenizer ngramTokenizer = new NgramTokenizer();
        Reader reader = new StringReader(text);
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
        return tokenList;
    }

    @Test
    public void simpleNgram() {
      List<String> result = tokenize("EQUIVALENT DELL PSU FOR DELL LATITUDE E5430");
      assertThat(result).contains("psu", "dell", "e 5430");
    }
}
