# Static Set-associative Hashtable.
It's optimized for heavy query situation, like sparse DNN feature searching. This solution can provide sub-billion level QPS on single machine.

![](throughput.png)

### Key Features
* extreme read performance
* low space overhead (1 byte per item + 6.25% data size)
* no online writing
* work on CPU support little-endian unaligned memory access (X86，ARM，RISC-V...)

### Other Solutions
* [low space overhead](https://github.com/PeterRK/fastCHD)
* [online writable](https://github.com/PeterRK/estuary)

---
[【Chinese】](README-CN.md) [【English】](README.md)
