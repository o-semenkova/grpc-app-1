FROM maven:3.5.2-jdk-8 as build
COPY src /master-grpc/src/
COPY pom.xml master-grpc/pom.xml
RUN mvn -f /master-grpc/pom.xml clean install

FROM openjdk:8-jdk-alpine
LABEL author=osem
COPY --from=build /master-grpc/target/master-grpc-0.0.1-SNAPSHOT.jar master-grpc-0.0.1.jar
ENTRYPOINT ["java","-jar","/master-grpc-0.0.1.jar"]