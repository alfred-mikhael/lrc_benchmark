# Super Fast LRCs

## Architecture
|folder | function |
|---|---|
|benchmarks | testbed and timing functions, as well as different error conditions for testing|
|docs | technical documentation and references |
| experiments | sample benchmarked results and comparison of different codes |
|src/algebra| implementation of various finite field and matrix multiplication operations which form the backbone of the encoding / decoding|
|src/codes/lrc| shared LRC interface and implementation of various LRCs|
|src/codes/| interfaces which are shared across all LRCs, related to data handling and sharding|
|tests | unit tests and correctness checks for most functions|

Each LRC will implement the `LRC` interface found in `src/codes/lrc.h`. Of note is the `get_layout_info` function. Each code must define its own `layout_info` which dictates how many shards it wants and the size of its symbols (in bytes). 

To encode data, initialize a `Recoverable` object with the data. The `Recoverable` is responsible for managing the memory. Select a specific code and pass in its `layout_info` using the `Recoverable::set_layout` function. This will automatically allocate sufficient memory for parity blocks and compute the shard sizes. Then call `Recoverable::encode(LRC& code)` to encode the data. Read the data with `Recoverable::read_byte(size_t offset, LRC& code)` or `Recoverable::read_range(size_t start, size_t end, LRC& code)`.