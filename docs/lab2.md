## forword

这一部分主要是实现DB的一些运算子，以及完善lab1的BufferPool，因为BufferPool还没设置换页淘汰策略。

>1.实现运算符 Filter 和 Join 并验证其相应的测试是否有效。这些运算符的 Javadoc 注释包含有关它们应该如何工作的详细信息。我们已经为您提供了 Project 和 OrderBy 的实现，可以帮助您了解其他运算符的工作方式。
>
>2.实现 IntegerAggregator 和 StringAggregator。在这里，您将编写在输入元组序列中跨多个组的特定字段实际计算聚合的逻辑。使用整数除法计算平均值，因为 SimpleDB 只支持整数。 StringAgegator 只需要支持 COUNT 聚合，因为其他操作对字符串没有意义。
>
>3.实现聚合运算符。与其他运算符一样，聚合实现 OpIterator 接口，以便可以将它们放置在 SimpleDB 查询计划中。请注意，聚合运算符的输出是每次调用 next() 时整个组的聚合值，并且聚合构造函数采用聚合和分组字段。
>
>4.在BufferPool中实现tuple插入、删除、page eviction相关的方法。此时您无需担心事务。
>
>5.实现插入和删除运算符。与所有运算符一样，Insert 和 Delete 实现 OpIterator，接受一个元组流来插入或删除，并输出一个带有整数字段的单个元组，该整数字段指示插入或删除的元组数。这些操作符需要调用 BufferPool 中的适当方法来实际修改磁盘上的页面。检查插入和删除元组的测试是否正常工作。
>
>请注意，SimpleDB 不实现任何类型的一致性或完整性检查，因此可以将重复记录插入文件中，并且无法强制执行主键或外键约束。
>
>此时您应该能够通过 ant systemtest 目标中的测试，这也是本实验的目标。
>
>您还可以使用提供的 SQL 解析器对您的数据库运行 SQL 查询！参见第 2.7 节的简要教程。
>
>最后，您可能会注意到本实验中的迭代器扩展了 Operator 类，而不是实现 OpIterator 接口。因为 next/hasNext 的实现往往是重复的、烦人的、容易出错的，Operator 通用地实现了这个逻辑，只需要你实现一个更简单的 readNext。随意使用这种实现风格，或者如果您愿意，也可以只实现 OpIterator 接口。要实现 OpIterator 接口，请从迭代器类中移除 extends Operator，并在其位置放置实现 OpIterator。

## lab

### exercise1

实现filter和join运算符号。

1.filter就是sql中起到过滤作用的

```sql
select * from table where id >= 10;
```

而where后的内容就是filter的作用。

Filter继承自`Operator`，而Operator继承自OpIterator。Operator实现了hasNext()和next()方法来获取下一条Tuple，而观察其中发现都调用了fetchNext()此抽象方法，而我们的filter就需要实现这个方法从而实现获取下一条符合条件的tuple。

Filter有这些元素

```java
    /**
     * predicate
     */
    private final Predicate predicate;

    /**
     * operation Iterator
     */
    private OpIterator opIterator;
```

一个是断言/谓词，一个是过滤条件操作符的迭代器。

因此这里需要先了解一下谓词Predicate的实现

```java
    /**
     * the index of compare value in tuple1
     */
    private final int fieldNum;

    /**
     * compare operation
     */
    private final Op op;

    /**
     * tuple2's value
     */
    private final Field operand;
```

谓词有三个元素，第一个是需要比较的列在tuple中的索引；

第二个是操作符，这里支持以下操作(针对Integer而言)

![image-20220123150000455](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220123150000455.png)

第三个就是比较的数，之前弄不太明白，看了一下怎么实例化谓词之后就理解了

```java
Predicate pred = new Predicate(0, Predicate.Op.EQUALS, TestUtil.getField(0));
```

第三个参数是以一个定值传入的，那这就好理解了，就相当于SQL中的 10这个比较数

```sql
select * from table where id >= 10;
```

而谓词的比较逻辑是在filter()方法中，通过使用Field接口的compare方法来进行比较。

![image-20220123150316382](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220123150316382.png)

![image-20220123150324761](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220123150324761.png)

而这个方法有两处实例化的地方，对应上了不同类型对于不同操作符的处理逻辑。搞清楚这个调用链后，谓词就搞定了，**可抽象理解成不断获取操作符迭代器中的 Tuple进行过滤**。

再回到Filter的fetchNext，直接调用谓词的filter方法就可以获取到符合的Tuple并返回，实现如下

```java
 protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (opIterator == null) {
            throw new NoSuchElementException("Operator Iterator is empty");
        }

        while (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            if (predicate.filter(tuple)) return tuple;
        }

        return null;
 }
```

至此，Filter的处理逻辑搞定。

2.lab已经提供了OrderBy的实现，还是要看一下它的调用链以及具体实现。

~lab里的实现直接将Tuple加载到内存中，会OOM~

```java
    private OpIterator child; // Tuple数据源
    private final TupleDesc td; // 
    private final List<Tuple> childTups = new ArrayList<>(); // 临时的List存Tuple然后进行排序
    private final int orderByField; // 需要进行排序的列索引
    private final String orderByFieldName; // 需要进行排序的列名
    private Iterator<Tuple> it; // 排序后的结果迭代器
    private final boolean asc; // 是否升序
```

```java
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        // load all the tuples in a collection, and sort it
        while (child.hasNext())
            childTups.add(child.next());
        childTups.sort(new TupleComparator(orderByField, asc));
        it = childTups.iterator();
        super.open();
    }
```

在这里我们看到它是这么做的：将迭代器的数据源加载到临时list中，对list进行排序，然后返回其迭代器。其排序比较的规则则是写了个继承了Comparator接口的类进行比较逻辑。具体就是根据列索引里的值进行比较。

```java
class TupleComparator implements Comparator<Tuple> {
    final int field;
    final boolean asc;

    public TupleComparator(int field, boolean asc) {
        this.field = field;
        this.asc = asc;
    }

    public int compare(Tuple o1, Tuple o2) {
        Field t1 = (o1).getField(field);
        Field t2 = (o2).getField(field);
        if (t1.compare(Predicate.Op.EQUALS, t2))
            return 0;
        if (t1.compare(Predicate.Op.GREATER_THAN, t2))
            return asc ? 1 : -1;
        else
            return asc ? -1 : 1;
    }
    
}

```

3.Join的实现逻辑：

这里我们实现的是内连接

```sql
select a.*, b.* from a inner join b on a.id = b.id; 
```

怎么连接？这里直接使用O(MN)的算法...从a取出一条，遍历b，符合的就构造出新的tuple然后加到结果集中。

```java
protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.it1.hasNext() || this.t != null) {
            if (this.it1.hasNext() && this.t == null) {
                t = it1.next();
            }
            while (it2.hasNext()) {
                Tuple t2 = it2.next();
                if (p.filter(t, t2)) {
                    TupleDesc td1 = t.getTupleDesc();
                    TupleDesc td2 = t2.getTupleDesc();
                    TupleDesc newTd = TupleDesc.merge(td1, td2);
                    Tuple newTuple = new Tuple(newTd);
                    newTuple.setRecordId(t.getRecordId());
                    int i = 0;
                    for (; i < td1.numFields(); ++i)
                        newTuple.setField(i, t.getField(i));
                    for (int j = 0; j < td2.numFields(); ++j)
                        newTuple.setField(i + j, t2.getField(j));
                    if (!it2.hasNext()) {
                        it2.rewind();
                        t = null;
                    }
                    return newTuple;
                }
            }
            it2.rewind();
            t = null;
        }
        return null;
    }
```

这里调用了JoinPredicate的filter函数，其中的逻辑也很好理解，获取两个操作数，调用它的compare方法即可。

```java

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        Field operand1 = t1.getField(fieldNum1);
        Field operand2 = t2.getField(fieldNum2);
        return operand1.compare(op, operand2);
    }
```

4.再看看project，也就是投影的实现。

```java
    private OpIterator child; // 数据源
    private final TupleDesc td; // 投影完后得到的TupleDesc
    private final List<Integer> outFieldIds; // 选择的列的索引
```

```java
    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     *
     * @param fieldList The ids of the fields child's tupleDesc to project out
     * @param typesList the types of the fields in the final projection
     * @param child     The child operator
     */
    public Project(List<Integer> fieldList, Type[] types, OpIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childtd = child.getTupleDesc();

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childtd.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(types, fieldAr);
    }
```

可见其具体过程就是：

传入需要投影的列的索引，其类型以及数据源。然后构造出新的TupleDesc

在fetchNext中获取数据源，然后用构造好的TupleDesc构造出Tuple

```java
    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     *
     * @return The next tuple, or null if there are no more tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        if (!child.hasNext()) return null;
        Tuple t = child.next();
        Tuple newTuple = new Tuple(td);
        newTuple.setRecordId(t.getRecordId());
        for (int i = 0; i < td.numFields(); i++) {
            newTuple.setField(i, t.getField(outFieldIds.get(i)));
        }
        return newTuple;
    }

```

搞定，进入下一环节。

### exercise2

这部分需要实现的是聚合运算。

```sql
select SUM(nums) from table group by id;
```

在这里，有两个参数，一个是SUM里的，一个是group by里的。

sum里的称为aggregatorField，groupby后的称为groupByField。

而Integer类型支持的运算有SUM，MAX，MIN，AVG，COUNT。String只支持COUNT。

那么如何计算结果呢？用Map存即可，key为groupByField，value就是实际的结果。

需要注意的是，对于AVG而言，value是存实际值的List。当需要结果的时候再将List从Map中取出，遍历求和才能得到均值。

### exercise3

从HeapFile和Page级别，来进行添加或者删除Tuple。

1.删除Tuple：Tuple有唯一得RecordID，这样就可以定位到Tuple所在的page，并将其头文件的bitmap修改。

2.新增Tuple，首先需要找到空的page，然后找到空的slot，再将其添加到page中。如果未找到空的page，就需要先新创建一个page，并将其落盘。

#### HeapPage

删除Tuple：

```java
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        int tupleIndex = recordId.getTupleNumber();
        if (recordId != null && pid.equals(recordId.getPageId())) {
            if (tupleIndex < getNumTuples() && isSlotUsed(tupleIndex)) {
                tuples[tupleIndex] = null;
                markSlotUsed(tupleIndex, false);
                return;
            }
            throw new DbException("can't find tuple in the page");
        }
        throw new DbException("can't find tuple in the page");
    }
```

获取tuple的recordID，通过RecordID获取到tuple在此page中的位置。接着进行边界与非空校验，然后将tuples数组索引对应位置置为null，同时更新bitmap，设置该位置为未使用。

插入Tuple

```java
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (getNumEmptySlots() == 0) throw new DbException("Not enough space to insert tuple");
        if (!t.getTupleDesc().equals(this.td)) throw new DbException("Tuple's Description is not match for this page");

        for (int i = 0; i < tuples.length; i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i,  true);
                tuples[i] = t;
                tuples[i].setRecordId(new RecordId(pid, i));
                break;
            }
        }
    }
```

首先先检查该Page是否还有Slot可供插入tuple

找到的时候就将bitmap设置为已使用，然后更新tuples数组即可。

#### HeapFile

HeapFile的插入和删除Tuple

- 插入Tuple

```java
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() == 0) continue;

            heapPage.insertTuple(t);
            list.add(heapPage);
            return list;
        }

        // create a empty page and load with 0
        BufferedOutputStream bufferOS = new BufferedOutputStream(new FileOutputStream(this.file, true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bufferOS.write(emptyData);
        bufferOS.close();

        // load into the BufferPool
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }
```

遍历HeapFile里的所有Page，需要注意因为我们使用的是BufferPool，访问了这些page就要load到bufferPool中。对于每一个Page，查看page是否还有空的slot。有的话就插入(插入就是调用heap Page的插入Tuple方法)

- 删除Tuple

```java
   /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to delete.  This tuple should be updated to reflect that
     *            it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *                     of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
        // not necessary for lab1
    }
```

通过tuple的recordID找到PageID，通过BufferPool的map获取到page，调用Page的DeleteTuple方法删除即可。同理，需要返回访问过的page，上游在调用的时候需要将这些访问过的加载到BufferPool中。

#### BufferPool

- insert Tuple

```java
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> pageList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pageList) {
            addToBufferPool(page.getId(), page);
        }
    }

    /**
     * load page into buffer pool, if buffer pool is full, evict a page
     * @param id
     * @param page
     * @throws DbException
     */
    private void addToBufferPool(PageId id, Page page) throws DbException {
        if (!map.containsKey(id) && map.size() >= this.numPages) {
            evictPage();
        }

        map.put(id, page);
    }

```

调用HeapFile的插入Tuple方法，然后load到BufferPool中

- delete Tuple

```java
    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        dbFile.deleteTuple(tid,t);
    }

```

和插入同理。但是删除不用load到BufferPool中。

### exercise4

实现Insert Tuple和Delete Tuple操作符

- Insert Tuple

有这几个元素

```java
    private final TransactionId tid;

    private OpIterator child;

    private int tableID;

    private final TupleDesc td;

    private boolean state;
```

child是数据源，tableID是表明属于哪张表的操作。state意思是，这个操作符是否已经完成插入操作，避免多次插入。

fetchNext还是继承自OpIterator，这里调用BufferPool中的插入Tuple来完成插入操作。

最后返回影响的Tuple个数(cnt)，该结果也存在一个Tuple中来返回。

```java
    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        // return null if called more than once
        if (state) return null;

        Tuple t = new Tuple(this.td);
        int cnt = 0;
        BufferPool bufferPool = Database.getBufferPool();
        while (child.hasNext()) {
            try {
                bufferPool.insertTuple(tid, this.tableID, child.next());
            } catch (IOException e) {
                throw new DbException("fail to insert tuple");
            }
            cnt++;
        }

        t.setField(0, new IntField(cnt));
        state = true;
        return t;
    }
```

- Delete Tuple

和insert同理

```java
    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (deleted) return null;
        int cnt = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
                cnt++;
            } catch (IOException e) {
                throw new DbException("fail to delete Tuple");
            }
        }
        deleted = true;
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(cnt));
        return t;
    }
```

![image-20220125210718078](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220125210718078.png)

图片出处[插入过程](https://blog.csdn.net/weixin_45834777/article/details/120675909?spm=1001.2014.3001.5502)

### exercise5

给BufferPool实现一个LRU算法

参考Leetcode即可。

### exercise6

实现执行层的insert tuple和delete tuple

从BufferPool中调用insert/delete tuple

