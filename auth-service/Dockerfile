# ---- BUILDER STAGE ----
FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Ensure CA certificates are installed and updated
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy only pom.xml first (for dependency caching)
COPY pom.xml .

# Pre-fetch dependencies (use verify instead of go-offline to avoid plugin resolution issues)
RUN mvn -B -DskipTests -Djava.net.preferIPv4Stack=true verify clean --fail-never

# Copy the source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests -Djava.net.preferIPv4Stack=true

# ---- RUNNER STAGE ----
FROM eclipse-temurin:21-jdk AS runner

WORKDIR /app

COPY --from=builder /app/target/auth-service-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 4005

ENTRYPOINT ["java", "-jar", "app.jar"]
