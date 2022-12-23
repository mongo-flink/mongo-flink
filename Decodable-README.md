# Decodable Mongo Flink Fork

## Summary
This repo is a fork of https://github.com/mongo-flink/mongo-flink tracking version 1.0-SNAPSHOT (at least until 1.0 is officially released on Maven).

No changes made to the source code – this fork exists simply to be able to package and upload deps to CodeArtifact.

## Deployment

In `~/.m2/settings.xml`, add the following:

```text
<?xml version="1.0" encoding="UTF-8"?>

<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
		xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">
<servers>
	<server>
	<id>decodable-mvn-snapshots-local</id>
	<username>aws</username>
	<password>${env.CODEARTIFACT_AUTH_TOKEN}</password>
	</server>
	<server>
	<id>decodable-mvn-releases-local</id>
	<username>aws</username>
	<password>${env.CODEARTIFACT_AUTH_TOKEN}</password>
	</server>
</servers>
</settings>
```

### Run Deployment

To push JARs to AWS CodeArtifact, run the following:
```text
export CODEARTIFACT_AUTH_TOKEN=`aws codeartifact get-authorization-token --domain decodable --domain-owner 671293015970 --region us-west-2 --query authorizationToken --output text --profile prod`
mvn deploy -DskipTests -Pdecodable-mvn-snapshots-local
```
