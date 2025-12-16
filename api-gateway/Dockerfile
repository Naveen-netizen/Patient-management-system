# ---------- 1️⃣ Build Stage ----------
FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn -B -Dmaven.wagon.http.retryHandler.count=5 dependency:go-offline || true

COPY src ./src
RUN mvn clean package -DskipTests

# ---------- 2️⃣ Runtime Stage ----------
FROM eclipse-temurin:21-jdk AS runner

WORKDIR /app

COPY --from=builder /app/target/*.jar app.jar

EXPOSE 4004

ENTRYPOINT ["java", "-jar", "app.jar"]
