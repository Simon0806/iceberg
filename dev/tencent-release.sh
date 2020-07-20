#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
set -e  # Exit immediately if a command exits with a non-zero status

if [ $# -ne 4 ]; then
  echo "Usage: $0 <apache-version> <tencent-version> <rc-num> <release-repo-not-snapshot?>"
  exit
fi

version=$1-$2-tencent  # <apache-version>-<tencent-version>-tencent, e.g. 0.7.0-1-tencent
rc=$3
release_repo=$4  # Y for release repo, others for snapshot repo

tag=apache-iceberg-$version
tagrc=${tag}-rc${rc}

echo "Preparing source for $tagrc"

# create version.txt for this release
echo $version > version.txt
git add version.txt
git commit -m "Add version.txt for release $version" version.txt

set_version_hash=`git rev-list HEAD 2> /dev/null | head -n 1 `

git tag -am "Apache Iceberg $version" $tagrc $set_version_hash
remote=$(git remote -v | grep http://git.code.oa.com/DataLake/iceberg.git | head -n 1 | awk '{print $1}')
git push ${remote} ${tagrc}

release_hash=`git rev-list $tagrc 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown git tag: $tag"
  exit
fi

echo "Using commit $release_hash"

rm -rf ${tag}*
tarball=$tag.tar.gz

# be conservative and use the release hash, even though git produces the same
# archive (identical hashes) using the scm tag
git archive $release_hash --prefix $tag/ -o $tarball .baseline api arrow bundled-guava common core data dev flink flink-1.9 flink-1.9-runtime gradle gradlew hive mr orc parquet pig spark spark2 spark-runtime spark3 spark3-runtime CHANGES LICENSE NOTICE README.md build.gradle baseline.gradle deploy.gradle deploy-tencent.gradle tasks.gradle jmh.gradle gradle.properties settings.gradle versions.props version.txt

# checksum
sha512sum $tarball > ${tarball}.sha512

# extract source tarball
tar xzf ${tarball}

# build and publish
cd ${tag}
sed -i "s/deploy.gradle/deploy-tencent.gradle/g" build.gradle  # use deploy-tencent.gradle instead

if [ ${release_repo} = "Y" ]; then
  ./gradlew -Prelease publishApachePublicationToMavenRepository
  echo "Published to release repo"
else
  ./gradlew publishApachePublicationToMavenRepository
  echo "Published to snapshot repo"
fi

# clean
rm -rf ../${tag}*

echo "Success! The release candidate [${tagrc}] is available"
echo "Commit SHA1: ${release_hash}"
