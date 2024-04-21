FROM golang:1.22.1-alpine
WORKDIR /app
COPY . .
RUN go get
RUN go build -o bin .
ENV EMBED_GENERATION_URL https://embedding-server.purplesea-08c513a7.eastus.azurecontainerapps.io/embed
ENV PARROTBOX_URL https://parrotboxservice.orangeflower-f8e1f6b0.eastus.azurecontainerapps.io
EXPOSE 8080
ENTRYPOINT [ "/app/bin" ]
