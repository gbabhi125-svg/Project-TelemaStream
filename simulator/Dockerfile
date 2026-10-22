FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/simulator ./cmd

FROM alpine:3.19
COPY --from=build /out/simulator /simulator
ENTRYPOINT ["/simulator"]
