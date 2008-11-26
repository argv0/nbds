#ifndef NSTRING_H
#define NSTRING_H

typedef struct nstring nstring_t;

nstring_t *ns_alloc (const void *data, uint32_t len);

int         ns_cmp_raw (nstring_t *ns, const void *data, uint32_t len);
const void *ns_data    (nstring_t *ns);
uint64_t    ns_len     (nstring_t *ns);
#endif//NSTRING_H 
