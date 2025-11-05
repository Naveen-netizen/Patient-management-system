# ---------- 1️⃣ Build Stage ----------
FROM maven:3.9.9-eclipse-temurin-21 AS builder

# Set working directory
WORKDIR /app

# Copy only pom.xml first (for dependency caching)
COPY pom.xml .

# Pre-download dependencies (retry on failure)
RUN mvn -B -Dmaven.wagon.http.retryHandler.count=5 dependency:go-offline || true

# Copy source code
COPY src ./src

# Build the application (skip tests for faster build)
RUN mvn clean package -DskipTests

# ---------- 2️⃣ Runtime Stage ----------
FROM openjdk:21-jdk-slim AS runner

# Set working directory
WORKDIR /app

# Copy the JAR from the builder stage
COPY --from=builder /app/target/*.jar app.jar

# Expose application port
EXPOSE 4004

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]
