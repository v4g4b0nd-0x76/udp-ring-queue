# UDP Ring Queue

an abstraction of one of my challanges in work to send in memory data from one process in one server to another server using udp without data loss by optimizing syscalls to send/receive more packets with less syscalls and enqueuing data as pre allocated fixed size ring buffers  with eviction strategy to prevent memory overfloww crash.

the main technique is to know what data we shall expect to receive as bytes and how to decode them and how many of them are in one packet using a custome UDP frame


**Note:** make sure you are using linux to build this project or use containers or correct instruction as the main component of both receiver and sender is libc
