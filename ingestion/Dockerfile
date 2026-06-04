FROM golang:1.22-alpine AS builder

RUN apk add --no-cache rsync openssh-client git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
RUN go mod tidy

COPY . .
RUN CGO_ENABLED=0 go build -o ingestion .

# -------------------------
FROM alpine:3.19

RUN apk add --no-cache rsync openssh-client ca-certificates su-exec

# Création de l'utilisateur avec son home pointant sur /app
RUN adduser -D -h /app -u 1000 app

WORKDIR /app
COPY --from=builder /app/ingestion .

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
