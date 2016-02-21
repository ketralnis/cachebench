# membench

A tool for benchmarking memcached servers to compare implementations

This is designed so that

* I can compare performance of [rustcached](https://github.com/ketralnis/rustcached) vs [memcached](https://github.com/memcached/memcached)
* I can make performance changes to [rustcached](https://github.com/ketralnis/rustcached) and know if I'm making it better or worse

Note: numbers from this tool mean nothing in isolation and should
only be compared to runs of the same version of membench running
on the same machine against rustcached/memcacheds also running on
the same machine

