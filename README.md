# Event Management Agent

The Event Management Agent is a tool for architects and developers working with event-driven architectures (EDAs) to discover event streams flowing through event brokers as well as the related event broker configuration information. It can also discover schemas from registries. The Event Management Agent can be used in two different ways:

* As the Event Management Agent component of Solace PubSub+ Cloud Event Portal to:
    - discover runtime event data from event brokers and schema registries
    - populate Event Portal with the discovered runtime data to enable the management and reuse of EDA assets
    - audit the runtime data and flag discrepancies between the runtime and the design time intent for event data governance purposes, and to ensure that the runtime and design time configurations stay in-sync
* As a standalone tool that discovers runtime event data from event or message brokers and schema registries.

Our plan is to open source the Event Management Agent to enable architects and developers to contribute to it as well as to build new plugins so that:

* the agent can discover data from different event broker types
* existing plugins can be extended to discover additional data
* EDA data can be discovered from other systems, e.g. schemas from schema registries

## Available today

* Users can discover Solace PubSub+ and Apache and Confluent Kafka brokers event flow data:
    - Users can discover Solace PubSub+ queues and subscriptions
    - Users can discover Apache and Confluent Kafka topics and consumer groups
* Users can discover Confluent Schema Registry schemas
* Users get discovered data in the form of JSON files separated by entity types
* The Event Management Agent architecture is currently in the form of Java packages

## On the roadmap

* The Event Management Agent has an open source plugin framework
* Support additional Solace PubSub+ and Apache Kafka event broker authentication types in the form of plugins such as Kerberos, etc.
* Collection of topics from events flowing though Solace PubSub+ brokers
* Export discovered data as AsyncAPI specifications
* Addition of the infrastructure needed for the Event Management Agent to be a true open source project
* Discovery of Apache Kafka connectors
* Introduction of a UI for the Event Management Agent
* Additional support to more event broker types
* Event Management Agent executables

# Running the Event Management Agent in connected mode

In this mode, the Event Management Agent connects to the Solace PubSub+ Event Portal. Scans are initiated from Event Portal and are automatically uploaded.

## Minimum hardware requirements

The Event Management Agent was tested to run with

* 1 CPU
* 1 GB RAM

## Prerequisites

Docker is required for any of the steps below that use the event-management-agent DockerHub image. A working JRE 17 environment is required for any of the steps below that use the event-management-agent jar file.

## Downloading the Event Management Agent

The Event Management Agent must be installed in a location that has access to your event brokers and schema registries.

Pull the Event Management Agent Docker image from Docker Hub:
```
docker pull solace/event-management-agent
```

## Generating the Event Management Agent Connection File

1. Log in the Solace Cloud Console: https://console.solace.cloud/login/
2. Follow the steps for generating the connection file described in the Solace documentation: https://docs.solace.com/Cloud/Event-Portal/event-portal-collect-runtime-data.htm#creating_connection_file
    - For security considerations, passwords should be configured as environment variables. Therefore, provide the environment variable names that will contain the passwords when generating the connection file.
3. Download the connection file next to the Event Management Agent
4. Create the environment variables containing the actual passwords you provided in place of the passwords when generating the connection file

## Running the Event Management Agent

Specify:
* the location of the connection file (e.g. /path/to/file/AcmeRetail.yml)
* the environment variables containing the authentication details of your event brokers and schema registries (e.g. KAFKA_PASSWORD)
* the name of the Event Management Agent container (e.g. event-management-agent)
* the Event Management Agent Docker image (e.g. solace/event-management-agent:latest)

```
docker run -d -p 8180:8180 -v /path/to/file/AcmeRetail.yml:/config/ema.yml --env KAFKA_PASSWORD=myKafkaPassword --name event-management-agent solace/event-management-agent:latest
```

> **_NOTE:_**  Depending on your OS, the container may fail to start if your connection filename contains spaces or special characters. If you see such failures, make sure your filenames are within quotes or that they do not contain spaces or special characters.

The Event Management Agent takes a couple of minutes to start. The Event Management Agent logs are available via Docker:

```
docker logs -f event-management-agent
```

## Running a Discovery Scan

The Event Management Agent is now connected to the Solace Cloud Console.
Follow the steps in the documentation to run a discovery scan: https://docs.solace.com/Cloud/Event-Portal/event-portal-collect-runtime-data.htm#collecting_runtime_data


# Running the Event Management Agent in Standalone Mode

## Getting the Event Management Agent

The Event Management Agent must have access to your event brokers and schema registries.

* pull the Event Management Docker image
```
docker pull solace/event-management-agent
```

* clone the Event Management Agent GitHub repository
```
git clone https://github.com/SolaceProducts/event-management-agent.git
cd event-management-agent
```

## Generating the Connection File

1. duplicate the connect file found in:
```
service/application/src/main/resources/application.yml
```
2. uncomment the sections relevant to your event brokers and schema registries
3. provide the authentication details required to connect to your event brokers and schema registries

## Running the Event Management Agent in a Docker Container

Specify:
* the location of the connection file (e.g. /path/to/file/AcmeRetail.yml)
* the name of the Event Management Agent container (e.g. event-management-agent)
* the Event Management Agent Docker image (e.g. solace/event-management-agent:latest)

```
docker run -d -p 8180:8180 -v /path/to/file/AcmeRetail.yml:/config/ema.yml --name event-management-agent solace/event-management-agent:latest
```

> **_NOTE:_**  Depending on your OS, the container may fail to start if your connection filename contains spaces or special characters. If you see such failures, make sure your filenames are within quotes or that they do not contain spaces or special characters.

The Event Management Agent takes a couple of minutes to start. The Event Management Agent logs are available via Docker:

```
docker logs -f event-management-agent
```

## Running Discovery Scans using REST

### REST Interface

The Event Management Agent includes REST APIs that allow users to interact with the agent. See [REST Documentation](docs/rest.md) for additional information.

To run a scan:
* provide the id of the event broker you specified in the connection file, e.g. mykafkacluster
* provide the scan type, e.g. KAFKA_ALL
* provide the destination: FILE_WRITER

```
curl -X 'POST' \
  'http://localhost:8180/api/v2/ema/resources/mykafkacluster/scan' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "scanTypes": [
    "KAFKA_ALL"
  ],
  "destinations": [
    "FILE_WRITER"
  ]
}'
```

The agent will return a scan id.

## Exporting Discovery Scans

The Event Management Agent can export discovery scans as zip files containing the entity type JSON files.

* provide the scan id, e.g. ih9z9lwwv5r
* provide the name of the zip file, e.g. ih9z9lwwv5r.zip 

```
curl -X 'GET' \
  'http://localhost:8180/api/v2/ema/resources/export/ih9z9lwwv5r/zip' \
  -H 'accept: */*' \
  --output ih9z9lwwv5r.zip
```

## Running a Discovery Scan and File Export Using the CLI

A discovery scan and file export can be run as a single command from a terminal using the event-management-agent CLI. The CLI can be accessed via either the Event Management Agent jar file or a docker container using the instructions below. Use the `Upload Scan File` mode when [creating the Event Management agent in Event Portal](https://docs.solace.com/Cloud/Event-Portal/event-portal-collect-runtime-data.htm#creating_connection_file) to create and download the standalone connection file. Set the resource passwords as environment variables that match the variables set in the connection file.

### Discovery Scan and File Export from CLI Using the Jar File

The Event Management Agent jar file can be obtained by either:
* downloading the file from the [releases page](https://github.com/SolaceProducts/event-management-agent/releases). A release of `1.5.1` or greater is required.
* running a [build of the source code](#installing-maven-dependencies-and-building-the-event-management-agent-jar-file)

To run a scan:
* provide the id of the event broker specified in the connection file. (e.g. mysolacebroker)
* provide the event-management-agent jar file. (e.g. event-management-agent-1.5.1-SNAPSHOT.jar)
* provide the path and filename for the resulting scan data zip file. (e.g. /path/to/scan.zip)
* provide the location of the connection file (e.g. /path/to/file/AcmeRetailStandalone.yml)

The command is as follows:

```
java -jar event-management-agent-1.5.1-SNAPSHOT.jar scan mysolacebroker /path/to/scan.zip --springdoc.api-docs.enabled=false --spring.main.web-application-type=none --spring.config.location=/path/to/file/AcmeRetailStandalone.yml
```

### Discovery Scan and File Export From CLI Using Docker

To run a scan and export operation using the CLI from docker:
* provide the id of the event broker specified in the connection file. (e.g. mysolacebroker)
* provide the path and filename for the resulting scan data zip file. (e.g. /path/to/scan.zip)
* provide the location of the connection file (e.g. /path/to/file/AcmeRetailStandalone.yml)
* provide the name of the Event Management Agent container (e.g. event-management-agent)
* provide the Event Management Agent Docker image (e.g. solace/event-management-agent:latest)
* add all password environment variables to the environment of the docker container (example BROKER_PASSWORD)

The commands are as follows:

```
export CMD_LINE_ARGS="scan mysolacebroker /path/to/scan.zip  --springdoc.api-docs.enabled=false --spring.main.web-application-type=none"
```

```
docker run -d -v /path/to/file/AcmeRetailStandalone.yml:/config/ema.yml -v /path/to/scandir:/tmp  --env CMD_LINE_ARGS=${CMD_LINE_ARGS} --env BROKER_PASSWORD=${BROKER_PASSWORD} --name event-management-agent event-management-agent:latest
```

## Running a Discovery File Upload Using the CLI

A discovery scan zip file upload can be run as a single command from the terminal using either the Event Management Agent jar file or a docker container.
See [Generating the Event Management Agent connection file](#generating-the-event-management-agent-connection-file) for instructions on how to generate a connection file and set up password environment variables.

### Discovery File Upload From CLI Using the Jar File

The Event Management Agent jar file can be obtained by either:
* downloading the file from the [releases page](https://github.com/SolaceProducts/event-management-agent/releases). A release of `1.5.1` or greater is required.
* running a [build of the source code](#installing-maven-dependencies-and-building-the-event-management-agent-jar-file)

To run a scan file upload:
* provide the event-management-agent jar file. (e.g. event-management-agent-1.5.1-SNAPSHOT.jar)
* provide the path and filename of the scan data zip file. (e.g. /path/to/scan.zip)
* provide the location of the connection file (e.g. /path/to/file/AcmeRetail.yml)

The command is as follows:

```
java -jar event-management-agent-1.5.1-SNAPSHOT.jar upload /path/to/scan.zip --springdoc.api-docs.enabled=false --spring.main.web-application-type=none --spring.config.location=/path/to/file/AcmeRetail.yml
```

### Discovery Scan and File Export From CLI Using Docker

To run a scan file upload using the CLI from docker:
* provide the path and filename of the scan data zip file. (e.g. /path/to/scan.zip)
* provide the location of the connection file (e.g. /path/to/file/AcmeRetail.yml)
* provide the name of the Event Management Agent container (e.g. event-management-agent)
* provide the Event Management Agent Docker image (e.g. solace/event-management-agent:latest)

The commands are as follows:

```
export CMD_LINE_ARGS="upload /tmp/scan.zip --springdoc.api-docs.enabled=false --spring.main.web-application-type=none"
```

```
docker run -d -v /path/to/file/AcmeRetail.yml:/config/ema.yml -v /path/to/scan.zip:/tmp  --env CMD_LINE_ARGS=${CMD_LINE_ARGS} --name event-management-agent event-management-agent:latest
```

# Manually Uploading Scans to Event Portal

You can manually upload discovery scan zip files to Event Portal. To do so:
1. Scan and export discovery scans following with a first Event Management Agent running in standalone mode: [Running the Event Management Agent in standalone mode](#running-the-event-management-agent-in-standalone-mode)
2. Set up a second agent in connected mode following: [Running the Event Management Agent in connected mode](#running-the-event-management-agent-in-connected-mode)
3. Upload your Discovery zip file with the following curl command, providing the name of the zip file, e.g. scan.zip:
```
curl -X 'POST' \
  'http://localhost:8180/api/v2/ema/resources/import' \
  -H 'accept: */*' \
  -H 'Content-Type: multipart/form-data' \
  -F 'file=@scan.zip;type=application/zip'
```

# Solace PubSub+ Event Broker Configuration Push

The Event Management Agent enables the Solace PubSub+ Cloud Event Portal to directly deploy configuration to Solace PubSub+ Event Brokers. This functionality is available when the Event Management Agent is deployed as a Docker container or (for development purposes) executed from the jar file. Configuration push utilizes [OpenTofu](https://opentofu.org/) (a fork of the [Terraform](https://developer.hashicorp.com/terraform/) infrastructure management tool) along with the [Solace Terraform Provider](https://github.com/SolaceProducts/terraform-provider-solacebroker).

## Configuration Push With Docker (Recommended)

The Docker image of the Event Management Agent includes all necessary library dependencies for deploying configurations to Solace PubSub+ Event Brokers and is the recommended option for performing configuration push. Set up the agent by following the instructions for [running the Event Management Agent in connected mode](#running-the-event-management-agent-in-connected-mode).

## Configuration Push With The Jar File (Development)

The Event Management Agent configuration push functionality is dependent on the [OpenTofu](https://github.com/opentofu/opentofu) and [Solace Terraform Provider](https://github.com/SolaceProducts/terraform-provider-solacebroker) libraries. These dependencies must be installed before [running the Event Management Agent as a process](#running-the-event-management-agent-as-a-process).

### OpenTofu

Follow [these installation instructions](https://opentofu.org/docs/intro/install/) to install OpenTofu on Linux or MacOS.

The Event Management Agent requires the `terraform` executable to be available and aliased to `tofu`. To accomplish this, follow the instructions below to create an executable script:
```
printf '#!/bin/bash\ntofu $*' > terraform
chmod +x terraform
```
The `terraform` script mentioned above needs to be added to the system's PATH.

### Solace Terraform Provider

The [Solace Terraform Provider](https://github.com/SolaceProducts/terraform-provider-solacebroker) can be installed on a variety of operating systems and machine architectures.

1. Visit the [latest software release](https://github.com/SolaceProducts/terraform-provider-solacebroker/releases) page to select and download the asset that fits your installation requirements. For example, if you are installing on MacOS with Apple silicon, choose the terraform-provider-solacebroker_0.9.2_darwin_arm64.zip file (assuming 0.9.2 is the most recent version).

2. Unzip the file. This will yield a LICENSE file, a README.md file, and the provider plugin file (`terraform-provider-solacebroker_0.9.2` in the example case).

3. Create or update your `${HOME}/.tofurc` (MacOS and Linux) configuration file with a `provider_installation` section that contains the following `dev_overrides`:
```
provider_installation {
  dev_overrides {
    "registry.terraform.io/solaceproducts/solacebroker" = "/path/containing/terraform-provider-solacebroker"
  }

  direct {}
}
```
where "/path/containing/terraform-provider-solacebroker" is replaced with the directory location of the `terraform-provider-solacebroker_0.9.2` file (not including the filename).


# Building the Event Management Agent

## Prerequisites

* Java 17 (JDK 17.0.13_11+)
* Maven

## Cloning the GitHub Event Management Agent Repository

```
git@github.com:SolaceProducts/event-management-agent.git
```

### Installing Maven Dependencies and Building the Event Management Agent Jar File

```
cd event-management-agent/service
mvn clean install
```

The generated Event Management Agent jar file is found in:

```
application/target/event-management-agent-1.0.0-SNAPSHOT.jar
```

### Running the Event Management Agent As a Process

You can run the Event Management Agent as a process in both connected and standalone modes.

Specify:
* the Event Management Agent jar file (e.g. event-management-agent-1.0.0-SNAPSHOT.jar)
* the location of the connection file (e.g. /path/to/file/AcmeRetail.yml) - See [Generating the Event Management Agent connection file](#generating-the-event-management-agent-connection-file) to generate a connection file

```
java -jar application/target/event-management-agent-1.0.0-SNAPSHOT.jar --spring.config.location=/path/to/file/AcmeRetail.yml
```

The Event Management Agent is ready.

# Deploying the Event Management Agent in a Kubernetes Cluster

## ConfigMap example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: ema-example1
  namespace: ema
data:
  ema-properties: |
    springdoc:
      packages-to-scan: com.solace.maas.ep.event.management.agent.scanManager.rest
      api-docs:
        path: /docs/event-management-agent
      swagger-ui:
        path: /event-management-agent/swagger-ui.html
    server:
      port: 8180
    spring:
      h2:
        console:
          path: /h2
          enabled: true
          settings:
            web-allow-others: true
      datasource:
        password: password
        driver-class-name: org.h2.Driver
        username: sa
        url: jdbc:h2:file:./data/cache;DB_CLOSE_ON_EXIT=FALSE
      jpa:
        hibernate:
          ddl-auto: create-drop
        database-platform: org.hibernate.dialect.H2Dialect
        defer-datasource-initialization: true
      servlet:
        multipart:
          max-request-size: ${MAX_REQUEST_SIZE:5MB}
          max-file-size: ${MAX_FILE_SIZE:5MB}
      main:
        allow-bean-definition-overriding: true
    camel:
      springboot:
        use-mdc-logging: true
    kafka:
      client:
        config:
          reconnections:
            max-backoff:
              unit: milliseconds
              value: 1000
            backoff:
              unit: milliseconds
              value: 50
          connections:
            max-idle:
              unit: milliseconds
              value: 10000
            request-timeout:
              unit: milliseconds
              value: 5000
            timeout:
              unit: milliseconds
              value: 60000
    eventPortal:
      gateway:
        id: TOFILL
        name: TOFILL
        messaging:
          standalone: false
          rtoSession: false
          enableHeartbeats: true
          testHeartbeats: true
          connections:
            - name: eventPortalGateway
              authenticationType: ${EP_GATEWAY_AUTH:basicAuthentication}
              msgVpn: ${EP_GATEWAY_MSGVPN:perf-evmr}
              url: ${EP_GATEWAY_URL:TOFILL}
              users:
                - name: TOFILL
                  password: TOFILL
                  username: ${EP_GATEWAY_USERNAME:TOFILL}
                  clientName: TOFILL
      organizationId: ${EP_ORGANIZATION_ID:TOFILL}
      runtimeAgentId: ${EP_RUNTIME_AGENT_ID:TOFILL}
      topicPrefix: ${EP_TOPIC_PREFIX:sc/ep/runtime}
    plugins:
      resources:
      - id: TOFILL
        type: kafka
        name: TOFILL
        connections:
          - name: kafkaNoAuthConnection
            url: ${KAFKA_BOOTSTRAP_SERVERS:TOFILL}
            authentication:
              - protocol: PLAINTEXT
                credentials:
                  - type: noAuth
                    source: ENVIRONMENT_VARIABLE
                    operations:
                      - name: ALL
                    properties: [
                    ]
      - id: TOFILL
        type: CONFLUENT_SCHEMA_REGISTRY
        name: TOFILL
        relatedServices:
          - TOFILL
        connections:
          - name: TOFILL
            url: ${CONFLUENT_SCHEMA_REGISTRY_URL:http://TOFILL}
```

## Secret Example

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: broker-passwords
  namespace: ema
type: Opaque
data:
  MY_KAFKA_PASSWORD: bXlTdXBlclNlY3JldFBhc3N3b3JkCg==
```

## Deployment Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  labels:
    test: deployment
  name: ema-example1
  namespace: ema
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ema
  template:
    metadata:
      labels:
        app: ema
    spec:
      containers:
      - image: docker.io/solace/event-mangement-agent:1.1.1
        imagePullPolicy: IfNotPresent
        name: event-management-agent
        volumeMounts:
        - mountPath: /config
          name: ema-example1
        env: 
            - name: MY_KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: broker-passwords
                  key: MY_KAFKA_PASSWORD
        ports:
        - containerPort: 8180
      restartPolicy: Always
      volumes:
      - configMap:
          defaultMode: 420
          items:
          - key: ema-properties
            mode: 420
            path: ema.yml
          name: ema-example1
          optional: true
        name: ema-example1
```

## Service Example

If users wish to run the EMA in offline mode, they will need to create a service in order to make the pod reachable for REST commands outside of the cluster.

# Event Broker Plugins

The Event Management Agent comes with the following event or message broker plugins included:

* Apache and Confluent Kafka
* Solace PubSub+
* Confluent Schema Registry
* MSK
* IBM MQ

The default application.yml provides various plugin examples. For Kafka, the properties section under credentials is passthrough. For example a property in ConsumerConfig or SSLConfigs classes.

If using AWS IAM, the AWS Access Key Id and AWS Secret Access Key need to be present. This can be done via a credentials file or environment variables as shown below:

Put a file with the following contents into a ~/.aws/credentials file

```
[default]
aws_access_key_id = <aws_access_key>
aws_secret_access_key = <aws_secret_key>
```

Alternatively, you can make these environment variables (these will also override the credentials file if present)

```
export AWS_ACCESS_KEY_ID=<aws_access_key>
export AWS_SECRET_ACCESS_KEY=<aws_secret_key>
```

## Motivations

See [motivations](./docs/motivations.md)

## Testing

There are several interesting scenarios to test the Event Management Agent. These scenarios can be divided into two main
categories according to the deployment mode.

* Testing the Event Management Agent as standalone service (stand-alone deployment).
* Testing the end-to-end flow in Solace PubSub+ Console connected mode (From the frontend to the Event Portal, then to
  the Event Management Agent)

### Testing the Event Management Agent in Standalone Mode

The most important test in standalone mode is to ensure that the Event Management Agent runs and collects data properly.
To that end, the test includes the steps below:

1. Update the `plugins` section of the `application.yml` with the details of the resource you want to scan.
2. Start the Event Management Agent either from the IDE or by running the JAR file.
3. Examine the on-console logs for a log from `ResourceConfig` class indicating that the resource(s)
   has been created. **Note**: The logs may differ according to the resource(s) specified in the
   `application.yml` file.

```
c.s.m.e.r.a.c.ResourceConfig : Created [kafka] resource with id:[sakdjf] and name: [some-name1]
c.s.m.e.r.a.c.ResourceConfig : Created [solace] resource with id:[hdfgkdjf] and name: [some-name2]
```

4. View the local Swagger documentation to learn about the available REST endpoints for the Event Management Agent. To access
   the Swagger documentation, use the link `http://localhost:8180/event-management-agent/swagger-ui/index.html` (Note:
   The Event Management Agent is under continuous development. Therefore, please check the Swagger documentation to make
   sure that you are using the recent endpoint schema).
5. Initiate a scan against a resource by sending a POST request to the endpoint that triggers the data
   collection `/api/v2/ema/resources/{resourceId}/scan`. The request can be sent using either Postman or
   a curl command.
6. Ensure that the `destinations` in the request body contains `FILE_WRITER`, i.e., `"destinations":["FILE_WRITER"]`,
   then send the request.
7. Confirm that you receive a scan id, e.g., `3a41a0f5-cd85-455c-a863-9636f69dc7b2`
8. Examine the Event Management Agent console logs to make sure that individual scan types are complete. e.g.,
   `Route subscriptionConfiguration completed for scanId 3a41a0f5-cd85-455c-a863-9636f69dc7b2`
9. Examine the collected data by browsing to the directory `data_collection`. This directory is organized as
   {schedule_id}/{scan_id}/{scan_type.json}
10. Verify that the collected data contains a separate JSON file for each scan type.
11. Verify the contents of each JSON file.
12. Check the logs by browsing to `data_collection/logs/{scan_id}.log` and `general-logs.log` to make sure no exceptions
    or errors occurred.
13. Finally, if you have added the `EVENT_PORTAL` as a destination, check the Event Portal tables to confirm they
    contain the scanned data.

## Contributions

Contributions are encouraged! If you have ideas to improve an existing plugin, create a new plugin, or improve/extend
the agent framework then please contribute!

## License 

Event Management Agent is available under the Apache License V2.


## Contributors

@gregmeldrum @slunelsolace @AHabes @MichaelDavisSolace @helrac @moodiRealist
