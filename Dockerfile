# --- Vue SPA build ----------------------------------------------------------
# Produces web/dist/, which the next stage embeds into tetra-brew.
FROM node:20-bookworm-slim AS webbuild

WORKDIR /web

COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY web/ ./
RUN npm run build

# --- Go build ---------------------------------------------------------------
FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Drop in the freshly built SPA so //go:embed all:dist (under -tags web_embed)
# picks it up. The package directory web/ already has its own Go files;
# overwriting the dist/ subdirectory is all we need.
COPY --from=webbuild /web/dist /src/web/dist

# CGO required for github.com/uber/h3-go/v4 (coverage-map H3 indexing).
# Static linking via -extldflags=-static keeps the binaries portable enough
# for the slim runtime image.
# tetra-brew is built with -tags web_embed so the Vue SPA is baked in and
# the binary serves it without --web-root. The other two daemons don't
# serve HTTP UIs, so they don't need the tag.
RUN CGO_ENABLED=1 GOOS=linux go build -tags web_embed -ldflags '-extldflags "-static"' -o /out/tetra-brew ./cmd/tetra-brew
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-webradio ./cmd/tetra-brew-webradio
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-echo ./cmd/tetra-brew-echo
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-proxy ./cmd/tetra-brew-proxy
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-soundboard ./cmd/tetra-brew-soundboard
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-link ./cmd/tetra-brew-link
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags '-extldflags "-static"' -o /out/tetra-brew-blechelse ./cmd/tetra-brew-blechelse

# Build ACELP encoder/decoder from included source.
# Only the non-main sources go in; encoder.c/encoder_stdio.c/decoder.c each
# have their own main() and are linked individually.
RUN gcc -Icodec/ -Ofast codec/encoder_stdio.c codec/tetra-codec.c codec/tetra-codec-impl.c -o /out/tetra-acelp-stdio
RUN gcc -Icodec/ -Ofast codec/decoder.c codec/tetra-codec.c codec/tetra-codec-impl.c -o /out/tetra-acelp-decoder

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /out/tetra-brew /app/tetra-brew
COPY --from=build /out/tetra-brew-webradio /app/tetra-brew-webradio
COPY --from=build /out/tetra-brew-echo /app/tetra-brew-echo
COPY --from=build /out/tetra-brew-proxy /app/tetra-brew-proxy
COPY --from=build /out/tetra-brew-soundboard /app/tetra-brew-soundboard
COPY --from=build /out/tetra-brew-link /app/tetra-brew-link
COPY --from=build /out/tetra-brew-blechelse /app/tetra-brew-blechelse
COPY --from=build /out/tetra-acelp-stdio /app/tetra-acelp-stdio
COPY --from=build /out/tetra-acelp-decoder /app/tetra-acelp-decoder

ENV WEBRADIO_ENCODER_BIN=/app/tetra-acelp-stdio
ENV WEBRADIO_FFMPEG_BIN=ffmpeg

EXPOSE 8080
ENTRYPOINT ["/app/tetra-brew"]
