#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
	echo "Executing deploy"
    mvn deploy -P sonatype-nexus-deployment --settings cd/mvnsettings.xml
else
	echo "Deploy not executed"
fi
