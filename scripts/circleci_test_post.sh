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

if [ $CI_SCOVERAGE -eq 1 ]; then
  echo "Publishing code coverage for Scala $CI_SCALA_VERSION"
  bash <(curl -s https://codecov.io/bash)
fi

if ( [ $CIRCLE_BRANCH = "master" ] && $(grep -q SNAPSHOT version.sbt) ); then
  sbt ++$CI_SCALA_VERSION "set every offline := true" publish
fi
