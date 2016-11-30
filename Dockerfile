FROM alpine:3.4

ADD . /composite-orgs-transformer/

RUN apk --no-cache add bash ca-certificates\
  && apk --no-cache --virtual .build-dependencies add git bzr go \
  && cd composite-orgs-transformer \
  && git fetch origin 'refs/tags/*:refs/tags/*' \
  && BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && cd .. \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/composite-orgs-transformer" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && cp -r composite-orgs-transformer/* $GOPATH/src/${REPO_PATH} \
  && cd $GOPATH/src/${REPO_PATH} \
  && go get -t ./... \
  && cd $GOPATH/src/${REPO_PATH} \
  && echo ${LDFLAGS} \
  && go build -ldflags="${LDFLAGS}" \
  && rm -rf /composite-orgs-transformer \
  && mv composite-orgs-transformer /composite-orgs-transformer \
  && apk del .build-dependencies \
  && rm -rf $GOPATH /var/cache/apk/*
CMD [ "/composite-orgs-transformer" ]