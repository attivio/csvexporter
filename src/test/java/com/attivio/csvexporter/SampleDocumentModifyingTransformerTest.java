/**
 * Copyright 2017 Attivio Inc., All rights reserved.
 */
package com.attivio.csvexporter;

import org.junit.Test;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.test.DocumentAssert;
import com.attivio.csvexporter.SampleDocumentModifyingTransformer;

/** The simplest unit test of a document transformer. */
public class SampleDocumentModifyingTransformerTest {

  @Test
  public void testTransformer() throws AttivioException {
    // Initialize a transformer object
    SampleDocumentModifyingTransformer xformer = new SampleDocumentModifyingTransformer();
    xformer.setField("foo");
    xformer.setValue("bar");

    IngestDocument doc = new IngestDocument("1234");
    xformer.processDocument(doc);

    DocumentAssert.assertFieldValue(doc, "foo", "bar");
  }

}
