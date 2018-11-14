/**
* Copyright 2018 Attivio Inc., All rights reserved.
*/
package com.attivio.csvexporter;

import com.attivio.sdk.scanner.DataSourceScanner;

public class SampleCustomHttpDataSourceScannerTest extends SampleHttpDataSourceScannerTest {
  @Override
  protected DataSourceScanner createScanner() {
    DataSourceScanner scanner = new SampleCustomHttpDataSourceScanner();
    ((SampleCustomHttpDataSourceScanner)scanner).setContentUrl(TEST_URL);
    return scanner;
  }
}
