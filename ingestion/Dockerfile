FROM golang:1.22-alpine AS builder

RUN apk add --no-cache rsync openssh-client git

WORKDIR /app

# copy modules first
COPY go.mod go.sum ./

# ensure dependencies are clean + deterministic
RUN go mod download
RUN go mod tidy

# copy full source
COPY . .

# build static binary
RUN CGO_ENABLED=0 go build -o ingestion .

# -------------------------
FROM alpine:3.19

RUN apk add --no-cache rsync openssh-client ca-certificates

WORKDIR /app

COPY --from=builder /app/ingestion .

CMD ["./ingestion"]
