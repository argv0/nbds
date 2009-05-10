#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "runtime.h"

DECLARE_THREAD_LOCAL(rx_, uint32_t);
DECLARE_THREAD_LOCAL(ry_, uint32_t);
DECLARE_THREAD_LOCAL(rz_, uint32_t);
DECLARE_THREAD_LOCAL(rc_, uint32_t);

void rnd_init (void) {
    INIT_THREAD_LOCAL(rx_);
    INIT_THREAD_LOCAL(ry_);
    INIT_THREAD_LOCAL(rz_);
    INIT_THREAD_LOCAL(rc_);
}

// TODO: put a lock around this so that multiple threads being initialize concurrently don't read
//       the same values from /dev/urandom
void rnd_thread_init (void) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd == -1) {
        perror("Error opening /dev/urandom");
        exit(1);
    }

    char buf[16];

    int n = read(fd, buf, sizeof(buf));
    if (n != 16) {
        if (n == -1) {
            perror("Error reading from /dev/urandom");
        }
        fprintf(stderr, "Could not read enough bytes from /dev/urandom");
        exit(1);
    }

    uint32_t x, y, z, c;
    memcpy(&x, buf +  0, 4);
    memcpy(&y, buf +  4, 4);
    memcpy(&z, buf +  8, 4);
    memcpy(&c, buf + 12, 4);

    SET_THREAD_LOCAL(rx_, x);
    SET_THREAD_LOCAL(ry_, y);
    SET_THREAD_LOCAL(rz_, z);
    SET_THREAD_LOCAL(rc_, z);
}

// George Marsaglia's KISS generator
//
// Even though this returns 64 bits, this algorithm was only designed to generate 32 bits.
// The upper 32 bits is going to be highly correlated with the lower 32 bits of the next call.
uint64_t nbd_rand (void) {
    LOCALIZE_THREAD_LOCAL(rx_, unsigned);
    LOCALIZE_THREAD_LOCAL(ry_, unsigned);
    LOCALIZE_THREAD_LOCAL(rz_, unsigned);
    LOCALIZE_THREAD_LOCAL(rc_, unsigned);

    uint32_t rx = 69069 * rx_ + 12345;
    uint32_t ry = ry_;
    ry ^= (ry << 13);
    ry ^= (ry >> 17);
    ry ^= (ry <<  5);
    uint64_t t = rz_ * 698769069LL + rc_;
    uint64_t r = rx + ry + t;

    SET_THREAD_LOCAL(rx_, rx);
    SET_THREAD_LOCAL(ry_, ry);
    SET_THREAD_LOCAL(rz_, t);
    SET_THREAD_LOCAL(rc_, t >> 32);

    return r;
}
