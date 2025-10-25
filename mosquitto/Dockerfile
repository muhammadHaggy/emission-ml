FROM eclipse-mosquitto:2.0
COPY config /mosquitto/config
RUN chown -R mosquitto:mosquitto /mosquitto
USER mosquitto
CMD ["/usr/sbin/mosquitto","-c","/mosquitto/config/mosquitto.conf"]
