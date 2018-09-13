## Amazon Web Services (AWS) plugin for Cloudera Altus Director

* [Introduction](#introduction)
* [Getting started](#getting-started)
* [Building the plugin](#building-the-plugin)
* [Running the live tests](#running-the-live-tests)
* [Implementation details](#implementation-details)
* [Important notice](#important-notice)

### Introduction

This project defines an [open source](http://www.apache.org/licenses/LICENSE-2.0) plugin implementing the [Cloudera Altus Director Service Provider Interface](https://github.com/cloudera/director-spi) (Director SPI) for [Amazon Web Services](http://aws.amazon.com) (AWS).

The plugin implements a compute provider that allocates [Amazon Elastic Compute Cloud](http://aws.amazon.com/ec2/) (EC2) instances, and a database server provider that allocates [Amazon Relational Database Service](http://aws.amazon.com/rds/) (RDS) instances.

### Getting started

Because the AWS plugin ships with Cloudera Altus Director, these instructions are primarily geared towards plugin authors, who may be interested in using this plugin as an example or a base for their own plugins. They assume familiarity with git, github, maven, and other software development technologies; AWS concepts and terminology; and some familiarity with Cloudera Altus Director and the Director SPI.

Additional details can be found in the source code, in the [AWS Plugin javadoc](http://cloudera.github.io/director-aws-plugin/apidocs/), and in the [Cloudera Altus Director User Guide](http://www.cloudera.com/content/cloudera/en/documentation/cloudera-director/latest/PDF/cloudera-director.pdf).

Prior to using the plugin, you will need to have AWS credentials.

### Building the plugin

You can build the plugin with `mvn clean install`. Maven will need access to the Director SPI, either via the Cloudera Repository referenced in `pom.xml`, or via a local build.

### Running the live tests

By default, the unit tests do not require communication with AWS. To run live tests that allocate and destroy EC2 and RDS instances, you must specify a system property (`test.aws.live`), and arrange for your AWS credentials to be available to the default credential provider chain via one of the methods described [here](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html).

The live test also requires some AWS network configurations to be specified in a property file, see the [sample](./tests/src/test/resources/livetest.sample.properties) file. The default location to look for this property file should be overridden and is specified as a system property (`test.aws.live.file`).

Example command to run live tests : `mvn -Dtest.aws.live=true -Dtest.aws.live.file=/path/to/livetest.properties clean install`

### Implementation details

The plugin follows the implementation pattern described in the [Director SPI documentation](https://github.com/cloudera/director-spi).

The major classes are:

* The launcher ([AWSLauncher](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/AWSLauncher.html))
* The cloud provider ([AWSProvider](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/AWSProvider.html))
* The resource providers ([EC2Provider](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2Provider.html) and [RDSProvider](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/rds/RDSProvider.html))
* The corresponding resource templates ([EC2InstanceTemplate](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2InstanceTemplate.html) and [RDSInstanceTemplate](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/rds/RDSInstanceTemplate.html))
* The corresponding resources ([EC2Instance](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2Instance.html) and [RDSInstance](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/rds/RDSInstance.html))

The configuration properties are implemented as nested enums of configuration property tokens. A representative example is [EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2InstanceTemplate.EC2InstanceTemplateConfigurationPropertyToken.html).

The display properties are implemented as nested enums of display property tokens. A representative example is [EC2Instance.EC2InstanceDisplayPropertyToken](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2Instance.EC2InstanceDisplayPropertyToken.html).

Validation is implemented via composite validators, each containing a default validator and a custom validator. A representative custom validator example is [EC2InstanceTemplateConfigurationValidator](http://cloudera.github.io/director-aws-plugin/apidocs/index.html?com/cloudera/director/aws/ec2/EC2InstanceTemplateConfigurationValidator.html).

Localization uses the default localization support provided by the Director SPI.

Logging uses the [SLF4J](http://www.slf4j.org) logging API, as required by the Director SPI.

Additional filesystem configuration is specified in HOCON and loaded using the [typesafe config](https://github.com/typesafehub/config/) library.

### Important notice

Copyright &copy; 2015 Cloudera, Inc. Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

Cloudera, the Cloudera logo, and any other product or service names or slogans contained in this document are trademarks of Cloudera and its suppliers or licensors, and may not be copied, imitated or used, in whole or in part, without the prior written permission of Cloudera or the applicable trademark holder.

Hadoop and the Hadoop elephant logo are trademarks of the Apache Software Foundation. Amazon Web Services, the "Powered by Amazon Web Services" logo, Amazon Elastic Compute Cloud, EC2, Amazon Relational Database Service, and RDS are trademarks of Amazon.com, Inc. or its affiliates in the United States and/or other countries. All other trademarks, registered trademarks, product names and company names or logos mentioned in this document are the property of their respective owners. Reference to any products, services, processes or other information, by trade name, trademark, manufacturer, supplier or otherwise does not constitute or imply endorsement, sponsorship or recommendation thereof by us.

Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this document may be reproduced, stored in or introduced into a retrieval system, or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, or otherwise), or for any purpose, without the express written permission of Cloudera.

Cloudera may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this document. Except as expressly provided in any written license agreement from Cloudera, the furnishing of this document does not give you any license to these patents, trademarks, copyrights, or other intellectual property. For information about patents covering Cloudera products, see http://tiny.cloudera.com/patents.

The information in this document is subject to change without notice. Cloudera shall not be liable for any damages resulting from technical errors or omissions which may be present in this document, or from use of this document.
