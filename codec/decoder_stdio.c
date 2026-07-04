/*
 * tetra-acelp stdio streaming decoder
 * Reads 18-byte packed ACELP frames from stdin,
 * writes raw PCM s16le mono 8kHz to stdout.
 * Keeps codec state between frames.
 *
 * The file-based codec/decoder.c is block-buffered and only flushes on
 * fclose, which stalls a real-time receive path by hundreds of ms. This
 * variant flushes every frame so decoded audio streams out immediately.
 *
 * Build: gcc -Icodec/ -Ofast decoder_stdio.c codec/tetra-codec.c codec/tetra-codec-impl.c -o tetra-acelp-stdio-decoder
 */
#include "tetra-codec.h"
#include <stdio.h>
#include <signal.h>

#define BYTES_PER_FRAME 18
#define SAMPLES_PER_FRAME 240

static volatile int running = 1;
static void handle_signal(int sig) { (void)sig; running = 0; }

int main(void) {
    signal(SIGPIPE, handle_signal);
    signal(SIGINT, handle_signal);

    unsigned char inbuf[BYTES_PER_FRAME];
    short outbuf[SAMPLES_PER_FRAME];

    tetra_codec *codec = tetra_decoder_create();

    fprintf(stderr, "tetra-acelp-stdio-decoder: streaming decoder ready (18B ACELP -> 480B PCM)\n");

    while (running) {
        if (fread(inbuf, BYTES_PER_FRAME, 1, stdin) != 1)
            break;

        tetra_decode(codec, inbuf, outbuf, 0);

        if (fwrite(outbuf, 2, SAMPLES_PER_FRAME, stdout) != SAMPLES_PER_FRAME)
            break;
        fflush(stdout);
    }

    return 0;
}
