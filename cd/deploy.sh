#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
	echo "Executing deploy"
    mvn deploy --settings cd/mvnsettings.xml
else
	echo "Deploy not executed"
fi
