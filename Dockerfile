FROM eclipse-temurin:17-jdk

WORKDIR /app

# Install system dependencies: libfaketime for clock skew simulation, wget for connector download
RUN apt-get update && \
    apt-get install -y libfaketime wget && \
    rm -rf /var/lib/apt/lists/*

# Download MySQL Connector/J before COPY so Docker layer is cached independently of source changes
RUN mkdir -p /mysql-lib && \
    wget -q "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
    -O /mysql-lib/mysql-connector-j-8.0.33.jar

COPY . /app

# Compile all Java sources including DAO classes (picked up by directory wildcards)
# and DBConnectionHelper (root-level, listed explicitly)
RUN mkdir -p /app/bin && \
    javac -cp /mysql-lib/mysql-connector-j-8.0.33.jar \
    -d /app/bin \
    Clock/*.java \
    Common/*.java \
    ChargingStation/*.java \
    Reservation/*.java \
    ChargingSession/*.java \
    Pricing/*.java \
    Payment/*.java \
    DBConnectionHelper.java \
    EVClient.java MultithreadTest.java ReplicationTest.java \
    tests/*.java

# Runtime classpath: compiled classes + MySQL JDBC driver
ENV CLASSPATH=/app/bin:/mysql-lib/mysql-connector-j-8.0.33.jar
