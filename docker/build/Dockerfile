#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM registry.access.redhat.com/ubi8/ubi

# who's your boss?
MAINTAINER "Brian Holman" <bholman@dezota.com>

ENV JAVA_HOME /usr/lib/jvm/zulu8
RUN \
    set -xeu && \
    yum -y -q install https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
    yum -y -q install python3 zulu8-jdk less && \
    yum -y -q install https://download.dremio.com/community-server/23.1.0-202211250121140756-a79618c7/dremio-community-23.1.0-202211250121140756_a79618c7_1.noarch.rpm && \
    yum -q clean all && \
    rm -rf /var/cache/yum && \
    alternatives --set python /usr/bin/python3
COPY ./startup.sh /opt/dremio/bin/startup.sh
COPY ./logback.xml /opt/dremio/conf/logback.xml
COPY ./dremio-clickhouse-plugin-23.1.0-202211250121140756-a79618c7.jar /opt/dremio/jars
RUN chmod 755 /opt/dremio/bin/startup.sh
RUN chown -R dremio:dremio /opt/dremio    
EXPOSE 2181 9047 31010 45678
USER dremio:dremio
ENV LANG en_US.UTF-8
ENTRYPOINT ["/opt/dremio/bin/startup.sh"]
