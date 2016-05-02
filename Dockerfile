FROM alpine:3.3

ADD . /composite-orgs-transformer/

RUN apk add --update bash \
  && apk --update add git bzr \
  && apk --update add go \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/composite-orgs-transformer" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && cp -r composite-orgs-transformer/* $GOPATH/src/${REPO_PATH} \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -t ./... \
  && go build \
  && mv composite-orgs-transformer /app \
  && apk del go git bzr \
  && rm -rf $GOPATH /var/cache/apk/*
CMD [ "/app" ]