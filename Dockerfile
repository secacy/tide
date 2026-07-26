# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine AS build
ARG TARGET=gateway
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tide ./cmd/${TARGET}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tide /tide
USER nonroot:nonroot
ENTRYPOINT ["/tide"]
