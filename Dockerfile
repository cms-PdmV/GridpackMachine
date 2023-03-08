# Build dependencies
FROM python:3.11.2-slim-buster@sha256:8f827e9cc31e70c5bdbed516d7d6627b10afad9b837395ac19315685d13b40c2 AS build

WORKDIR /usr/app
RUN python -m venv /usr/app/venv
ENV PATH="/usr/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt

# Create image for deployment
FROM python:3.11.2-slim-buster@sha256:8f827e9cc31e70c5bdbed516d7d6627b10afad9b837395ac19315685d13b40c2 AS backend

# Install Git
# This is require to enable Repository refresh
RUN apt update && apt install -y git

RUN groupadd -g 999 python && useradd -r -u 999 -g python python

RUN mkdir /usr/app && chown python:python /usr/app
WORKDIR /usr/app

# Install the GridpackFiles repository inside the container
RUN git clone https://github.com/cms-PdmV/GridpackFiles.git && \
    chown -R python:python /usr/app/GridpackFiles

COPY --chown=python:python --from=build /usr/app/venv ./venv
COPY --chown=python:python . .

USER 999

ENV PATH="/usr/app/venv/bin:$PATH"
CMD [ "python", "main.py" ]