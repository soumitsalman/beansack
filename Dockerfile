FROM golang:1.22.1-alpine
# alpine is not necessary. it can be 1.20 or other tags

WORKDIR /app

COPY . .

RUN go get

RUN go build -o bin .

EXPOSE 8080

ENTRYPOINT [ "/app/bin" ]