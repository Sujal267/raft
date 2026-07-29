FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/dashboard ./cmd/dashboard

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/dashboard ./dashboard
EXPOSE 8080
ENTRYPOINT ["./dashboard"]
