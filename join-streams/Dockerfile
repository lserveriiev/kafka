FROM openjdk:11

ARG src="build/libs/joinstreams-0.0.1-SNAPSHOT.jar"
ARG target="kafka-join-streams.jar"
COPY ["${src}", "${target}"]

CMD ["java", "-jar", "kafka-join-streams.jar"]