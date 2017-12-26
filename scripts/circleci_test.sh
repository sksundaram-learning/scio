#!/bin/bash
#
#  Copyright 2017 Spotify AB.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

set -e
set -x

DIR_OF_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$DIR_OF_SCRIPT/circleci_env.sh"

"$DIR_OF_SCRIPT/gen_schemas.sh"

if [ "$CI_PULL_REQUEST" = "" ]; then
  echo "Running tests for Scala $CI_SCALA_VERSION, branch: $CIRCLE_BRANCH"
  JSON_KEY=$(basename $GOOGLE_APPLICATION_CREDENTIALS)
  openssl aes-256-cbc -d -in "$DIR_OF_SCRIPT/$JSON_KEY.enc" -out "$DIR_OF_SCRIPT/$JSON_KEY" -k $ENCRYPTION_KEY
  PROPS="-Dbigquery.project=data-integration-test -Dbigquery.secret=$DIR_OF_SCRIPT/$JSON_KEY"
  TESTS="test it:test"
else
  echo "Running tests for Scala $CI_SCALA_VERSION, PR: $CI_PULL_REQUEST"
  PROPS="-Dbigquery.project=dummy-project"
  TESTS="test"
fi

if [ $CI_SCOVERAGE -eq 1 ]; then
  sbt $PROPS ++$CI_SCALA_VERSION "set every offline := true" scalastyle coverage $TESTS coverageReport coverageAggregate
else
  sbt $PROPS ++$CI_SCALA_VERSION "set every offline := true" scalastyle $TESTS
fi
