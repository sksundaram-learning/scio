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

echo "Running REPL integration tests for Scala $CI_SCALA_VERSION"
echo "Test scripts:"
find ./scio-repl/src/it/resources -type f

sbt ++$CI_SCALA_VERSION "set every offline := true" scio-repl/assembly

find ./scio-repl/src/it/resources -type f -exec sh -c "cat {} | java -jar ./scio-repl/target/scala-${CI_SCALA_VERSION%.*}/scio-repl-*.jar" \; | tee repl-$CI_SCALA_VERSION.log
grep 'SUCCESS: \[scio\]' repl-$CI_SCALA_VERSION.log
