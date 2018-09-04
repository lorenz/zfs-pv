FROM alpine:edge

RUN apk --update add zfs-libs go git musl-dev zfs-dev
ENV GOPATH /go
WORKDIR /go/src/git.dolansoft.org/dolansoft/zfs-csi-driver
COPY . .
RUN go get .
RUN go install .

FROM alpine:edge
RUN apk --update add zfs-libs
COPY --from=0 /go/bin/zfs-csi-driver /
ENTRYPOINT ["/zfs-csi-driver"]