FROM docker.io/nodisbr/python

MAINTAINER Pedro Tonini <pedro.tonini@nodis.com.br>

WORKDIR /app

ARG PIP_INDEX_URL
ARG PIP_EXTRA_INDEX_URL

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

ENTRYPOINT ["python", "/app/run.py"]