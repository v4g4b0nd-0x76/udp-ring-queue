# UDP Ring Queue

An abstraction of one of the challenges I faced at work: transferring in-memory data from one process on a server to another server using UDP without data loss.

The main goal was optimizing syscall usage to send and receive more packets with fewer syscalls and dont transform data encoding which leads to copying data into biffer buffers, while using pre-allocated fixed-size ring buffers with an eviction strategy to prevent memory overflow and process crashes.

This repository is a demo of the techniques and workflow I use in production at work. It is not the actual production code, but a simplified implementation that demonstrates the core concepts and architecture.

The main technique is defining a custom UDP frame format where we know:
- What data we should expect to receive as bytes
- How to decode the received bytes
- How many records are contained in each packet

This allows efficient serialization, batching, and processing of UDP messages while reducing unnecessary overhead.

**Note:** Make sure you are using Linux to build and run this project, or use a container with a Linux environment. The main components of both the receiver and sender rely on `libc` system calls that are not supported on all platforms.
