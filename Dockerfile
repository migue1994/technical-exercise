FROM eclipse-temurin:17-jdk-focal
ADD target/my-app-1.0-SNAPSHOT.jar my-app-1.0-SNAPSHOT.jar
EXPOSE 4567
ENTRYPOINT ["java", "-jar", "my-app-1.0-SNAPSHOT.jar"]