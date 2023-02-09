# Dandelion_db 后期补充
Dandelion_db 底层的数据存储模型，它的名字叫做 bitcask。它本质上属于类 LSM 的模型，核心思想是利用顺序 IO 来提升写性能，只不过在实现上，比 LSM 简单多了，实现了PUT、GET、DELETE 的流程，还有对冗余数据merge的流程，有效代码大约300行左右，可以认为是对存储的一个简单的存储模型，入门理解
