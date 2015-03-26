package org.spark.examples.newsgroupclassifier;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.Version;


public class TextAnalyzer extends Analyzer
{
    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader)
    {
        Tokenizer source = new WhitespaceTokenizer(Version.LUCENE_44, reader);
        TokenStream filter = new LowerCaseFilter(Version.LUCENE_44, source);
        return new TokenStreamComponents(source, filter);
    }
}
