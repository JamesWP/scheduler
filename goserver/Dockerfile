# Alternative to image/Dockerfile: same GET /healthz + POST /run API, but
# no separate kube-scheduler binary or kubectl -- it's all one Go binary
# built from this module. See README.md for what that trades away.

FROM docker.io/library/golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /schedsim-go .

FROM gcr.io/distroless/static-debian12
COPY --from=build /schedsim-go /schedsim-go
EXPOSE 8080
ENTRYPOINT ["/schedsim-go"]
