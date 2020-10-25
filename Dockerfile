FROM python:3.7-stretch

######################################################################
# Chromedriver vvv
######################################################################
RUN apt-get update && \
    apt-get install -y gnupg wget curl unzip --no-install-recommends && \
    wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list && \
    apt-get update -y && \
    apt-get install -y google-chrome-stable && \
    CHROMEVER=$(google-chrome --product-version | grep -o "[^\.]*\.[^\.]*\.[^\.]*") && \
    DRIVERVER=$(curl -s "https://chromedriver.storage.googleapis.com/LATEST_RELEASE_$CHROMEVER") && \
    wget -q --continue -P /chromedriver "http://chromedriver.storage.googleapis.com/$DRIVERVER/chromedriver_linux64.zip" 
RUN unzip /chromedriver/chromedriver_linux64.zip -d /bin
ENV CHROMEDRIVER_DIR /bin
ENV PATH $CHROMEDRIVER_DIR:$PATH
######################################################################
# Chromedriver ^^^
######################################################################

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt 

RUN mkdir microservice
RUN mkdir microservice/models
RUN mkdir microservice/serving
RUN mkdir microservice/webscraper

COPY models/. /microservice/models/
COPY serving/. /microservice/serving/
COPY webscraper/. /microservice/webscraper/

ENV PYTHONPATH "${PYTONPATH}:/microservice"

WORKDIR /microservice

ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:8080", "server:application", "--timeout", "300", "--chdir", "/microservice/serving", "--workers", "1", "--threads", "1"]
