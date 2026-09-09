FROM eclipse-temurin:17-jdk

WORKDIR /app

RUN apt-get update && \
    apt-get install -y libfaketime && \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN mkdir -p /app/bin && javac -d /app/bin Clock/*.java ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java ReplicationTest.java

ENV CLASSPATH=/app/bin
