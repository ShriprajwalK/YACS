FROM openjdk:8-jre
EXPOSE 5000
EXPOSE 5001

COPY master.py master.py 
COPY config.json config.json
RUN apt update &&\
    apt install -y wget python3 less &&\
    chmod +x master.py

ENTRYPOINT python3 master.py config.json RR
