export LOTUS_PATH=~/.lotus

# storage miner的数据存储位置
export LOTUS_STORAGE_PATH=~/.lotusstorage

#miner api设置， 如果storage本地没有worker，可以不设置 通cat $LOTUS_STORAGE_PATH/token 和 cat $LOTUS_STORAGE_PATH/api得到
#export STORAGE_API_INFO=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0._ppPK-Pkqs62tXMBWl8sfK0fPsnXZzH-uSAdazpCdqE:/ip4/127.0.0.1/tcp/23455/http


# miner-agent和worker使用的存储目录，需要做成共享或是分布式存储
export LOTUS_AGENT_PATH=~/.agentstorage
export WORKER_PATH=~/.agentstorage
# mine-agent和worker之间的接口配置，通cat $LOTUS_AGENT_PATH/token 和 cat $LOTUS_AGENT_PATH/api得到
export AGENT_API_INFO="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.HAtdr9MTMXMi5q0g6iHgx_0Mlqgt9u-gatnTo8VG6ZY:/ip4/127.0.0.1/tcp/22345/http"
# 在miner-agent和worker上配置钱包地址,通过在miner上的 lotus wallet list查看
export MINER_ADDR="t0101"
# 参数文件存放的位置，如果条件允许（内存足够大，放到内存文件系统中可以极大提高速度）
#export FIL_PROOFS_PARAMETER_CACHE=/dev/shm/tmp/filecoin-proof-parameters
export SECTOR_SIZE=1024