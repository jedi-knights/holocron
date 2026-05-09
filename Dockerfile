# Multi-stage build for the holocrond broker and holocron-registry daemons.
#
# The workspace lives at /src so module-relative paths stay stable. Both
# binaries are built in one `build` stage; final images select between
# them via `--target holocrond` or `--target holocron-registry`.

FROM golang:1.23-alpine AS build
WORKDIR /src

# Copy the workspace and every module manifest first so dependency
# resolution caches independently of source changes.
COPY go.work ./
COPY proto/go.mod proto/
COPY broker/go.mod broker/
COPY sdk/go.mod sdk/
COPY cli/go.mod cli/
COPY connect/go.mod connect/
COPY registry/go.mod registry/
COPY streams/go.mod streams/
COPY examples/go.mod examples/

# Now copy the rest of the source. We do not copy go.sum (not checked
# into git for this learning project); `go build` will resolve as needed.
COPY proto/ proto/
COPY broker/ broker/
COPY sdk/ sdk/
COPY cli/ cli/
COPY connect/ connect/
COPY registry/ registry/
COPY streams/ streams/

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/holocrond ./broker/cmd/holocrond \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/holocron-registry ./registry/cmd/holocron-registry

# --- holocrond: the broker daemon --------------------------------------
FROM gcr.io/distroless/static:nonroot AS holocrond
COPY --from=build /out/holocrond /usr/local/bin/holocrond
USER nonroot:nonroot
EXPOSE 9092 9192
ENTRYPOINT ["/usr/local/bin/holocrond"]

# --- holocron-registry: the schema-registry daemon ---------------------
FROM gcr.io/distroless/static:nonroot AS holocron-registry
COPY --from=build /out/holocron-registry /usr/local/bin/holocron-registry
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/holocron-registry"]
