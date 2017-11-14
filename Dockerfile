FROM xorcio/orc:3146eb9aec551972679eba1fb5647af2128c6098 as orc

FROM openjdk:8u171-jdk
COPY --from=orc /usr/local/bin/testkit.exe /usr/local/bin/testkit.exe
COPY --from=orc /prelude /orc/prelude

COPY . /usr/oam

WORKDIR /usr/oam

RUN ./gradlew :bin:shadowJar

RUN echo "#!/bin/sh\ntestkit.exe exec java -- -jar ./bin/build/libs/jvm-oam-1.1-SNAPSHOT-all.jar repl" > /usr/oam-test && chmod +x /usr/oam-test