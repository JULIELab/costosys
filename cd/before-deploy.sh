#!/usr/bin/env bash
if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
	openssl aes-256-cbc -K $encrypted_d47fb4320ded_key -iv $encrypted_d47fb4320ded_iv -in cd/codesigning.asc.enc -out ~\/.gnupg/codesigning.asc -d
	gpg --fast-import cd/signingkey.asc
fi

