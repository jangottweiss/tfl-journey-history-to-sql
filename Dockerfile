FROM python:3

# Install GDAL dependencies
RUN apt-get update && apt-get install -y \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

RUN mkdir /data
RUN mkdir /data/input
RUN mkdir /data/output
RUN mkdir /data/meta
VOLUME /data/input
VOLUME /data/output
VOLUME /data/meta

ADD requirements.txt /
RUN pip install -r requirements.txt
ADD src/main.py /

CMD [ "python", "./main.py" ]