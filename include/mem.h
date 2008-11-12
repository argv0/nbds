/* 
 * Written by Josh Dybnis and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
#ifndef MEM_H
#define MEM_H
void *nbd_malloc (size_t n);
void nbd_free (void *x);
void nbd_defer_free (void *x);
#endif//MEM_H
