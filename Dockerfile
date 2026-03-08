# Multi-stage build: compile with Maven, run with JRE
FROM eclipse-temurin:17-jdk AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

COPY pom.xml .
RUN mvn dependency:go-offline -B -q || true

COPY src ./src
RUN mvn package -B -q -DskipTests && \
    mv target/replay-*-all.jar target/replay-app.jar

# Runtime image
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /build/target/replay-app.jar ./replay-app.jar
EXPOSE 8080
USER 1000
ENTRYPOINT ["java", "-jar", "/app/replay-app.jar", "8080"]
