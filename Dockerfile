FROM golang:1.24.10-alpine AS builder
LABEL stage=mgxbuilder

RUN apk add --no-cache alpine-sdk
WORKDIR /go/src/mgx
COPY go.mod go.sum ./
RUN go mod download

COPY Makefile .

ARG TARGETPLATFORM

COPY cmd ./cmd
COPY pkg ./pkg
RUN make build


FROM alpine:latest
LABEL maintainers="support@migrx.io" \
      description="Mgx CSI Plugin"

RUN apk update && \
    apk add nvme-cli e2fsprogs xfsprogs blkid xfsprogs-extra e2fsprogs-extra util-linux ca-certificates

WORKDIR /

COPY --from=builder /go/src/mgx/bin/mgxcsi mgxcsi

ENTRYPOINT ["/mgxcsi"]
