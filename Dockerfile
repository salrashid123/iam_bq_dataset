FROM golang:1.16.6 as build

WORKDIR /app
COPY . .

RUN go mod download

RUN go build server.go

FROM gcr.io/distroless/base
COPY --from=build /app/server /

EXPOSE 8080

ENTRYPOINT ["/server"]