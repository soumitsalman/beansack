FROM golang:1.22.1-alpine
WORKDIR /app
COPY . .
RUN go get
RUN go build -o bin .
ENV EMBED_GENERATION_URL http://embedding-server:10000/embed
ENV PARROTBOX_URL https://parrotboxservice.orangeflower-f8e1f6b0.eastus.azurecontainerapps.io
EXPOSE 8080
ENTRYPOINT [ "/app/bin" ]
