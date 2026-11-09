# Throwaway image for the option B bridge measurement (PLAN.md 3.3, E7): Flink 2.0 + PyFlink, nothing else.
FROM flink:2.0-java17
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip python3-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/* && ln -sf /usr/bin/python3 /usr/bin/python
ENV JAVA_HOME=/opt/java/openjdk
RUN pip3 install --no-cache-dir "apache-flink==2.0.0" "numpy<2"
# Thread mode (pemja) does not start in this image: "Failed to find PemJa Library" (docs/results/e7-option-b-bridge.md).
