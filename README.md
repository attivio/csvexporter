# csvexporter

## Overview
Includes a custom connector, built using the Attivio 5.6 Connector SDK, which executes a configurable streaming query and outputs the results as a CSV file, including delimited multi-value fields.

## System Requirements
* Same as Attivio installation

## Build and Installation
See https://github.com/attivio/sdk/blob/5.5/attivio_module_sdk.md for instructions on building and installing modules created using the Attivio SDK.

Once the module has been added to your Attivio installation, execute the following steps to create an instance of it and add it to a workflow.

1. Click **System Management > Connectors**
2. Click **New**
3. Click **CSV Export Scanner** and click **OK**
4.In the popup that appears enter the following:

| Field	| Value |
| --- | --- |
| Name	| cvs-exporter |
| queryString | table:city |

5. Click **Save**

## Configuration
The **queryString** can be populated with any simple query langauge syntax. The matching documents will be exported.

## Usage Example
At times, it may be desirable to pull data out of an Attivio index in CSV format to, for example, use within integration or end-to-end tests or just to share with someone. This connector can be run in an ad hoc manner or scheduled to produce a CSV containing the targted content. 

## Release History
* 0.1.0-SNAPSHOT - Initial release

## Author
Anthony Paquette
Director, Quality Assurance and Customer Enablement
Attivio, Inc.
