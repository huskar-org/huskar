FROM python:2.7.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        libpq-dev \
        librabbitmq-dev \
        libncurses5-dev \
        mariadb-client \
        shellcheck \
        golang-go

ENV GOPATH "/opt/gopath"
RUN go get -u github.com/client9/misspell/cmd/misspell

RUN pip install -U virtualenv && virtualenv /opt/huskar_api
ENV VIRTUAL_ENV "/opt/huskar_api"

ENV PATH "$VIRTUAL_ENV/bin:$GOPATH/bin:$PATH"

ADD . /srv/huskar_api
WORKDIR /srv/huskar_api

RUN pip install --no-cache-dir -r requirements-dev.txt

ENTRYPOINT ["./manage.sh"]
