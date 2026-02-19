Check original SSDB documentations for usage.

Modifications:

- Optimized hash encoding & format for Xiangqi and Chess.
- Use https://github.com/noobpwnftw/terarkdb as backend for merge operator and database size reduction.

Limitations:

- Keys in hash type is limited to algebraic move format(i.e. e2e4, a7a8q).
- Values in hash type is limited to int16_t, with 32767 reserved for delete tag.
