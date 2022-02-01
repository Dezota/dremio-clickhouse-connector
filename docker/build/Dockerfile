FROM dremio/dremio-oss:19.1.0

# who's your boss?
MAINTAINER "Brian Holman" <bholman@dezota.com>

# Switching to root to install the required packages
USER root
RUN apt update
RUN apt -y dist-upgrade
RUN usermod -d /opt/dremio dremio
COPY ./dremio-clickhouse-plugin-19.1.0-202111160130570172-0ee00450.jar /opt/dremio/jars
COPY ./startup.sh /opt/dremio/bin/startup.sh
RUN chmod 755 /opt/dremio/bin/startup.sh
RUN chown -R dremio:dremio /opt/dremio
USER dremio
ENTRYPOINT ["/opt/dremio/bin/startup.sh"]
