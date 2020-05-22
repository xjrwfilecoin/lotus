![Lotus](documentation/images/lotus_logo_h.png)

# Project Lotus - 莲

Lotus is an implementation of the Filecoin Distributed Storage Network. For more details about Filecoin, check out the [Filecoin Spec](https://github.com/filecoin-project/specs).

## Development

All work is tracked via issues. An attempt at keeping an up-to-date view on remaining work is in the [lotus testnet github project board](https://github.com/filecoin-project/lotus/projects/1).

## Building & Documentation

For instructions on how to build lotus from source, please visit [https://docs.lotu.sh](https://docs.lotu.sh) or read the source [here](https://github.com/filecoin-project/lotus/tree/master/documentation).

## License

Dual-licensed under [MIT](https://github.com/filecoin-project/lotus/blob/master/LICENSE-MIT) + [Apache 2.0](https://github.com/filecoin-project/lotus/blob/master/LICENSE-APACHE)


## 修改官方代码规范
为了尽量减小修改官方代码后，后续更新合并官方代码发生冲突的可能性<br>
制定修改官方代码规则:
1. 新增函数一律增加新文件，在新文件中增加新函数
2. 修改原有文件，在每一处修改的地方，增加如下注释，方便快速查找修改过的代码： //xjrw modify
3. testnet3_official_opt分支，不再同步任何官方代码
4. 以后只在官方重大更新或旧版本不可用的情况下，才更新官方代码
5. 更新官方代码方法：
* a. 直接从testnet3_official_opt分支拉取新分支，在新分支上拉官方最新master分支，合并并解决冲突，测试
* b. 如果冲突太多，导致a方案不可用，则从官方master分支最新代码拉新分支，并通过//xjrw modify关键字查找所有我们改过的代码，在新分支上重新改一遍<br>
并把新分支作为以后的基准分支(相当于现在的testnet3_official_opt分支)

## 分支一览
* interopnet-fix 在官方互通版的基础上做了分布式存储的代码修改，没有测试 
* testnet3_official 官方原版分支，只在官方代码的基础上，修改影响流程的重大bug
* testnet3_official_opt 官方优化版，在官方版本的基础上，增加架构和流程优化等功能
* testnet3_official_opt_dbg 官方优化版testnet3_official_opt的本地测试版本，主要增加了创世节点的信息

## 变更列表：
* Add Piece 给AMD worker做？（待验证能否实现）
* Commit1放给worker做，PreCommit2，Commit1，Commit2必须在同一台机器上做，Finalizer只拉必要的数据回miner
* 扇区进入failed状态，重试时间由1分钟改为10分钟 （storage-fsm）
* http拉扇区数据失败，增加重试机制（最多重试10次，间隔1分钟）
* 增加必要的日志
* 修复官方worker封装扇区[ComputeProofFailed bug](https://github.com/filecoin-project/lotus/issues/1686)
