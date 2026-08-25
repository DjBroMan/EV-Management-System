FROM eclipse-temurin:17-jdk

WORKDIR /app

COPY . /app

RUN mkdir -p /app/bin && javac -d /app/bin ChargingStation/*.java Reservation/*.java ChargingSession/*.java Pricing/*.java Payment/*.java EVClient.java MultithreadTest.java

ENV CLASSPATH=/app/bin
