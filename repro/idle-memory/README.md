# Idle memory growth of `lightpanda serve`

`repro.mjs` serves synthetic pages whose memory peak rises during the run (a
generated JS bundle of distinct functions with hot loops, 500 KiB larger every
50 renders, plus some DOM building). It drives `lightpanda serve` with one CDP
connection per render (connect, new context and page, `goto`, disconnect) and,
between renders, reads the server's `/proc/<pid>/smaps_rollup` and `stat`.
Nothing attaches to the process. Without a fix, the idle footprint climbs to
the largest page served so far.

Node.js with `puppeteer-core` because that is what Lightpanda's own docs use,
and it runs on the Node.js that Ubuntu ships. Linux only (it reads `/proc`).

Setup on Ubuntu 24.04:

    sudo apt-get install -y nodejs npm && npm install

Run (one sequential client, then 8 concurrent clients):

    node repro.mjs --bin /path/to/lightpanda
    node repro.mjs --bin /path/to/lightpanda --clients 8 --renders 400 --ramp 80

It prints renders/s, CPU ms and minor faults per render, the peak PSS, the
median idle PSS and anonymous PSS for each fifth of the run, and the growth from
the first fifth to the last. `--trim-at-end` (needs `gdb` and `sudo`) also shows
how much anonymous memory `malloc_trim(0)` frees at the end. Other options are
listed at the top of `repro.mjs`.
