#ifndef NSTRING_H
#define NSTRING_H

typedef struct nstring {
    uint32_t len;
    char data[];
} nstring_t;

nstring_t * ns_alloc (const void *data, uint32_t len);
int ns_cmp_raw (nstring_t *ns, const void *data, uint32_t len);

#endif//NSTRING_H 
