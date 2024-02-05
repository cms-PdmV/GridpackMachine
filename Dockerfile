# Build dependencies
FROM python:3.11.7-alpine3.19@sha256:6aa46819a8ff43850e52f5ac59545b50c6d37ebd3430080421582af362afec97 AS build
RUN apk update && apk upgrade

WORKDIR /usr/app
RUN python -m venv /usr/app/venv
ENV PATH="/usr/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Create image for deployment
FROM python:3.11.7-alpine3.19@sha256:6aa46819a8ff43850e52f5ac59545b50c6d37ebd3430080421582af362afec97 AS backend
RUN apk update && apk upgrade
RUN pip install --upgrade pip setuptools wheel

# Install Git - Required to update the Gridpack files repository
RUN apk add git

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