package org.spark.examples.newsgroupclassifier;

import java.io.Serializable;


public class Document
    implements Serializable
{
    public final String text;

    public Document(final String pText)
    {
        text = pText;
    }


    public String getText()
    {
        return text;
    }
}
