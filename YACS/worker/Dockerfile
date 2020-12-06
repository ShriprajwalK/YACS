FROM openjdk:8-jre
EXPOSE 4000

COPY worker.py worker.py 

RUN apt update &&\
    apt install -y wget python3 less
    
ENTRYPOINT python3 worker.py 
