FROM maven:3.8.6-openjdk-18 AS builder

WORKDIR /aeron

#ENV AERON_VERSION=1.50.2
#RUN mvn org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
#    -DrepoUrl=https://repo1.maven.org/maven2/ \
#    -Dartifact=io.aeron:aeron-all:${AERON_VERSION} -DoutputDirectory=/aeron && \
#    mvn org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy \
#    -DrepoUrl=https://repo1.maven.org/maven2/ \
#    -Dartifact=io.aeron:aeron-archive:${AERON_VERSION} -DoutputDirectory=/aeron

RUN git clone https://github.com/real-logic/aeron.git . && ./gradlew :aeron-all:jar :aeron-agent:shadowJar

FROM eclipse-temurin:21

WORKDIR /aeron
COPY --from=builder /aeron/aeron-all/build/libs/aeron-all-*.jar /aeron
COPY --from=builder /aeron/aeron-agent/build/libs/aeron-agent-*.jar /aeron/agent/aeron-agent.jar

EXPOSE 8010 8011 8012

# uses grep&sed to convert AERON_XXX_YYY to -Daeron.xxx.yyy
CMD ["sh", "-c", "java -javaagent:/aeron/agent/aeron-agent.jar \
  -cp '/aeron/*' \
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
  $(env | grep ^AERON_ | awk '{print tolower($0)}' | sed 's/^/-D/g' | sed 's/_/./g') \
  io.aeron.archive.ArchivingMediaDriver"]
