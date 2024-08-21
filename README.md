# KeyValor: A Key-Value Store (with valor)

Welcome to **KeyValor**, the resilient and reliable key-value store that’s here to add some valor to your data management. Whether you're building a high-performance application or just need a solid foundation for your next project, KeyValor has got your back.

## 🚀 Why KeyValor?

- **Blazing Fast Performance**: KeyValor is optimized for speed, ensuring that your data is always accessible when you need it.
- **Redis-Compatible**: Transition seamlessly with APIs that mirror Redis, making it easy to integrate into your existing stack.
- **Built with Go**: Leveraging the power of Golang, KeyValor is lightweight, concurrent, and efficient.
- **Flexibility and Power**: From simple GETs and SETs to more complex operations, KeyValor offers a rich set of commands to handle your data needs.

## TODO: 
- **Support LRU caching of keys and their values**
- **Exploring not having to load entire index into memory at all times, and instead caching parrts of it**
- **Exploring MVCC support for keys**
- **Breaking down storage into independent parts, to avoid locking entire keyspace for every operation**

## 📦 Installation

Getting started with KeyValor is as simple as:

```bash
go get github.com/tushar2708/keyvalor
```

