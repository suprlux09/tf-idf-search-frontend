FROM openjdk:11
ARG JAR_FILE=build/libs/*jar-with-dependencies.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
