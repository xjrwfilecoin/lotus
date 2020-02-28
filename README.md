![Lotus](documentation/images/lotus_logo_h.png)

# Project Lotus - 莲

Lotus is an experimental implementation of the Filecoin Distributed Storage Network. For more details about Filecoin, check out the [Filecoin Spec](https://github.com/filecoin-project/specs).

## Development

All work is tracked via issues. An attempt at keeping an up-to-date view on remaining work is in the [lotus testnet github project board](https://github.com/filecoin-project/lotus/projects/1).

## Building & Documentation

For instructions on how to build lotus from source, please visit [https://docs.lotu.sh](https://docs.lotu.sh) or read the source [here](https://github.com/filecoin-project/lotus/tree/master/documentation).

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/lotus/blob/master/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/lotus/blob/master/LICENSE-APACHE)

# 星际荣威：

## 使用修改过的filecoin-ffi
```shell
 cd extern/filecoin-ffi
 git remote add xjrw https://github.com/xjrwfilecoin/filecoin-ffi
 git fetch xjrw
 git checkout -b qzopt xjrw/qzopt
 FFI_BUILD_FROM_SOURCE=1 make clean all
 cd ../..
 make 
```

## 更新filecoin-ffi库
1. 更新rust-fil-proofs代码，并且上传
2. 进入cd lotus/extern/filecoin-ffi/rust
3. 执行cargo update 更新 Cargo.lock
4. 进入cd lotus/extern/filecoin-ffi/
5. 执行
    ```shell
    rm .install-filecoin \
        ; make clean \
        ; FFI_BUILD_FROM_SOURCE=1 make
    ```  

1. 如果没有出现错误，库就已经更新了
2. 进入cd lotus，按以前的方式更新代码又大rm .install-filecoin \
    ; make clean \
    ; FFI_BUILD_FROM_SOURCE=1 make