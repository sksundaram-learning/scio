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
hostname

export CI_SCOVERAGE=0

# Reverse order node indices with Scala version. Node 0 is the main build.
case $CIRCLE_NODE_INDEX in
  0)
    export CI_SCALA_VERSION="2.12.4"
    export CI_SCOVERAGE=1
    ;;
  1)
    export CI_SCALA_VERSION="2.11.11"
    ;;
  *)
    exit 1
    ;;
esac
