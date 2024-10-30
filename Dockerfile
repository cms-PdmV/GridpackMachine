# Build dependencies
FROM python:3.11.10-alpine3.20@sha256:f089154eb2546de825151b9340a60d39e2ba986ab17aaffca14301b0b961a11c AS build
RUN apk update && apk upgrade

# Install Kerberos client and gcc for Python wrapper
RUN apk add build-base && apk add krb5-dev 

WORKDIR /usr/app
RUN python -m venv /usr/app/venv
ENV PATH="/usr/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Create image for deployment
FROM python:3.11.10-alpine3.20@sha256:f089154eb2546de825151b9340a60d39e2ba986ab17aaffca14301b0b961a11c AS backend
RUN apk update && apk upgrade
RUN pip install --upgrade pip setuptools wheel

# Install Git - Required to update the Gridpack files repository
RUN apk add git

# Install the Kerberos client
RUN apk add krb5-dev 

# User and application folder
RUN addgroup -g 1001 pdmv && adduser --disabled-password -u 1001 -G pdmv pdmv
RUN mkdir -p /usr/app/gridpacks && chown -R pdmv:pdmv /usr/app
WORKDIR /usr/app

COPY --chown=pdmv:pdmv --from=build /usr/app/venv ./venv
COPY --chown=pdmv:pdmv . .

# Temporal folder
RUN chown pdmv:0 /usr/app/gridpacks && chmod 770 /usr/app/gridpacks

USER 1001

ENV PATH="/usr/app/venv/bin:$PATH"
CMD [ "python", "main.py" ] 