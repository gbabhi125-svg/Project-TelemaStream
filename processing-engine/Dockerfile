FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/processing-engine ./cmd

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/processing-engine /processing-engine
ENTRYPOINT ["/processing-engine"]
