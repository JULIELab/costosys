#!/usr/bin/env bash
if [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    mvn deploy --settings cd/mvnsettings.xml
fi
