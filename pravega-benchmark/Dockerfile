# Building Container

FROM gradle:4.10-jdk8 as GradleBuilder
#FROM base-image:latest as GradleBuilder

USER 0

COPY ca-certificates/* /usr/local/share/ca-certificates/
RUN update-ca-certificates

RUN apt-get update \
    && apt-get install -y \
        maven \
    && rm -rf /var/lib/apt/lists/*

USER gradle

COPY --chown=gradle:gradle build.gradle /home/gradle/src/build.gradle
COPY --chown=gradle:gradle gradle /home/gradle/src/gradle
COPY --chown=gradle:gradle gradle.properties /home/gradle/src/gradle.properties
COPY --chown=gradle:gradle settings.gradle /home/gradle/src/settings.gradle
COPY --chown=gradle:gradle lib /home/gradle/src/lib
COPY --chown=gradle:gradle src /home/gradle/src/src
COPY --chown=gradle:gradle certs /home/gradle/certs

WORKDIR /home/gradle/src

ENV GRADLE_USER_HOME=/home/gradle

RUN gradle installDist \
--no-daemon --info --stacktrace

# Runtime Container

FROM openjdk:8-jre

ENV APP_NAME=pravega-benchmark

# NEW
ENV APP_NAME=pravega-benchmark
ENV pravega_client_auth_method=Bearer
ENV pravega_client_auth_loadDynamic=true
ENV KEYCLOAK_SERVICE_ACCOUNT_FILE=/keycloak.json

COPY --from=GradleBuilder /home/gradle/src/build/install/${APP_NAME} /opt/${APP_NAME}
COPY --from=GradleBuilder /home/gradle/certs /home/gradle/certs
# Uncomment this line if TLS is enabled. FYI there is a bug with pravega-benchmark ... it doesn't support TLS yet.
# For more details : https://github.com/pravega/pravega-benchmark/issues/105
# RUN keytool -import -trustcacerts -noprompt -file /home/gradle/certs/tls.crt -alias nautilus_cert -storepass changeit -keystore /usr/local/openjdk-8/lib/security/cacerts

ENTRYPOINT ["/opt/pravega-benchmark/bin/pravega-benchmark"]
