package org.fyrz.textclassifier.tokenizer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class TextTokenizer extends Tokenizer {

  @Override
  public Function1<String, Seq<String>> createTransformFunc(final ParamMap paramMap) {
    return new TextTokenizerFunction();
  }

  public static class TextTokenizerFunction extends AbstractFunction1<String, Seq<String>> implements Serializable {
    @Override
    public Seq<String> apply(final String s) {
      List<String> tokenList = new ArrayList<String>();
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
      return JavaConversions.asScalaBuffer(tokenList);
    }
  }
}
