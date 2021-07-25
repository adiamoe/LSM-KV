## KVStore using Log-structured Merge Tree

### 主要结构
1. SkipList 构成memTable，提供O(log n)的查找插入效率
2. memTable 到达阈值后转为 immutableTable，dump到磁盘中成为SSTable
3. SStable分层存储，Level0层键值可以有重叠，但只允许存放两个SSTable， 
   超过两个之后就将其全部合并放入更低层，除Level0层之外的其他层内SSTable键值不允许重叠。
   
### 实现细节
1. 双线程，immutable table序列化和SStable的compaction，由独立线程执行，不影响主线程在memTable和immutable Table中的操作
2. Compaction时，选择较低层键值重叠的SSTable，执行多路归并，同时对于相同键值的数据，保留时间戳较大的一项
3. 通过Bloom Filter和二分查找提高搜索效率，同时将Bloom Filter和索引部分储存在内存TableCache中，减少磁盘IO

### 执行接口
1. get get首先会在memTable中查找，如果immutable Table存在，再进入immutable Table。如果都没找到，再在TableCache中分层进行搜索，直到获取对应值。
2. put 当memTable不满时，put会直接将数据放入memTable中。如果memTable已满，immutable Table不存在，则先将memTable转为immutable Table，然后插入新建的memTable中。
   immutable Table会由后台线程执行接下来的dump和compaction。如果immutable Table存在，证明后台线程还在执行，不能执行插入，进入睡眠。后台线程结束时会将其唤醒。
3. del 由于采用LSM的形式，因此不允许原地更新，del的操作即是往数据中插入一条删除标记，因此和put流程相同

### 其他事项
1. utils.h中提供了各系统的文件操作函数，因此支持跨平台的执行
2. 运行时需要先新建data文件夹或修改路径




