# Build stage
FROM golang:1.20-alpine3.17 AS builder
WORKDIR /app
COPY . .
RUN go build -o main main.go

# Run stage
#FROM alpine:3.17

#ADD https://github.com/golang/go/raw/master/lib/time/zoneinfo.zip /zoneinfo.zip
#ENV ZONEINFO /zoneinfo.zip
#WORKDIR /app
#COPY --from=builder /app/main .
#COPY app.env .
#COPY start.sh .

EXPOSE 8080
CMD [ "/app/main" ]
