package org.fyrz.textclassifier.tokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.Version;

import java.io.Reader;

public class TextAnalyzer extends Analyzer {

  @Override
  protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_44, reader);
    TokenStream filter = new LowerCaseFilter(Version.LUCENE_44, source);
    return new TokenStreamComponents(source, filter);
  }
}
