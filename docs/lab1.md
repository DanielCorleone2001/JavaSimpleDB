## 任务

1.实现Tuple，TupleDesc。这两玩意是描述一行数据应该有什么类型的数据。而类型，在这儿只支持String和Integer，已经帮我们定义好了。

2.实现Catalog，这是一个全局的目录。

3.实现BufferPool构造器，并能取出page

4.实现一些api

5.实现循序扫描seqScan

## Architecture

SimpleDB有这些内容

1.fileds,tuples,tuple schemas这些构成DB逻辑数据的类

2.查询tuple的谓词(predicates)和条件(condition)类

3.提供多种方法将数据存在磁盘中（如存储格式为heapfile)，提供迭代器来访问数据

4.提供很多运算子来处理tuple (e.g., select, join, insert, delete, etc.)

5.使用bufferpool在内存中管理page，处理并发控制以及事务。

6.全局的catalog来记录可用的table和该table的格式(table schemas)

SimpleDB没有这些内容

1.没有SQL解析器或一个前端执行SQL

2.没有实现视图(view)

3.除了int和定长类型以外的string

4.查询优化器

5.指示器

## class and exercise

### The Database Class

提供了全局变量的访问方法

1.catalog

2.bufferpool

3.log file

### Fields and Tuples

Tuple就是fields的collection，也就是tuple就是由一系列fields组成的数组。和行数据的逻辑格式是一致的。

### exercise1

这里补充一下Tuple和TupleDesc的API即可。

TupleDesc是由一``TDItem数组``组成的，TDItem就是一个type类型(存实际类型，这儿只有int和定长String)和一个String类型(存列的名字)。

比较有意思的API是merge，也就是将两个TupleDesc合并成一个TupleDesc

```java
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len = td1.numFields() + td2.numFields();
        Type[] types = new Type[len];
        String[] names = new String[len];
        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.tdItem[i].fieldType;
            names[i] = td1.tdItem[i].fieldName;
        }

        for (int i = 0; i < td2.numFields(); i++) {
            types[i + td1.numFields()] = td2.tdItem[i].fieldType;
            names[i + td1.numFields()] = td2.tdItem[i].fieldName;
        }
        TupleDesc res = new TupleDesc(types, names);
        return res;
    }
```

而Tuple就是一行的逻辑结构，Tuple是必须有个RecordID的，用来标识Tuple，方便后续定位吧，可以理解为`主键`

```java
    private TupleDesc td;

    private static RecordId recordId;

    private Field[] fields;
```

td就是Tuple的描述信息，fields数组存的就是实际数据，它的长度和td的一致。

剩下就是补充一些get/set方法

### exercise2

这里需要实现catalog，也就是通过一个ID来找到对应的表。

这里使用了concurrentHashMap来存映射关系，key-value是ID-Table

table的定义如下：

```java
public  class Table {

       private DbFile dbFile;

       private String name;

       private String pkeyField;
}
```



Dbfile里 有getId()来标识唯一的文件，也就是table文件。所以map的key就是取自这儿。

DbFile就是一个接口，表征存在磁盘上的文件。

### exercise3

实现一个BufferPool，甚至不需要实现淘汰策略。。。

BufferPool用一个concurrentHashMap来维护，key-value是ID-Page

ID是page的hashCode

注意getPage()时如果没有此page，就先从DbFile中获取该page，放入到map中再返回。

### exercise4

在这里要实现Tuple存在磁盘中的访问形式，有heapfile和B+tree两种方式，这里需要实现heapfile。

heapfile是由一系列的HeapPage组成的，每个page会存一些tuple，以及一个bitmap来表征某个tuple的位置是否已经被使用。此bitmap如果某一位置为1，代表该tuple已使用。

因此需要计算处一个heappage中，能存多少tuple，能有多少长度来存bitmap。

![img](https://pic3.zhimg.com/80/v2-322369c7249f08f3c58f800c3de0a606_720w.jpg)

出处见水印。

首先计算tuple个数的方法如下

$tuples per page_ = floor((page size * 8) / (tuple size * 8 + 1))$​​

pageSize是page的字节数，在这儿就是4096bytes，也就是4KB。统一乘以8来得到比特位数。

而tuplesize是通过tupleDesc中对每种类型进行计算得出来的，加1表示每个tuple就需要一个bit的比特位用于在bitmap中表示。floor是向下取整，很好理解，我们不希望在page 中存储不完整的tuple。

得到tupleperpage后，就需要计算header的字节数

$headerBytes = ceiling(tupsPerPage/8)$

header的字节数，也就是bitmap的字节数(bitmap是一个字节数组)，除以8是指一个bitmap数组元素需要表征128个tuple的使用情况，因此我们在得到tuple的索引时，首先需要定位到对应的header索引(i / 8)，接着在具体到对应的比特位，才能判断是否已经使用

```java
    public boolean isSlotUsed(int i) {
        // some code goes here
        int index = i / 8;
        int v = header[index];
        int offset = i % 8;
        return (v >> offset & 1) == 1;
    }
```

---

介绍完整体框架，看需要补充的三个类

1.HeapPageID就是headpage在heapfile中的唯一ID。也就是说，一个headfile对应一张table，table的文件太大，一个page装不下，因此用很多个heappage来组成一个heapfile，为能识别heappage的唯一性，就通过heappageID来表征。在这里有两个元素

``` java
    private final int tableId;

    private final int pgNo;
```

用于表征哪张表(heapfile)的哪个page。

**这玩意实现了PageId接口**

2.接着到RecordID，用于表征哪张table的哪张page的哪条记录。

这玩意有两元素

```java
    private  final PageId pageId;

    private final int tupleNo;
```

通过pageID能得到table和page，通过tupleNo得到tuple的索引。

3.HeapPage就是上述说的获取tuple的个数和bitmap，不再赘述。

### exercise5

完成了heapPage，那就到heapfile了，从heapfile中取tuple。

HeapFile有两元素

```java
    private final File file;

    private final TupleDesc tupleDesc;
```

File就是对应磁盘上的文件，由很多page组成。另一个就是tupleDesc，对tuple进行描述。

挑一些比较重要的函数

1.readPage

```java
    public Page readPage(PageId pid) {
        // some code goes here
        int tableID = pid.getTableId();
        int pageNumber = pid.getPageNumber();

        RandomAccessFile accessFile = null; // use randomAccess File to read and write pages at arbitrary offsets

        try {
            accessFile = new RandomAccessFile(this.file, "r");
            if ((pageNumber + 1) * BufferPool.getPageSize() > accessFile.length()) {
                accessFile.close();
                throw new IllegalArgumentException("No match Page in HeapFile");
            }

            byte[] bytes = new byte[BufferPool.getPageSize()];
            accessFile.seek(pageNumber * BufferPool.getPageSize()); // set the start pointer of file to read
            int len = accessFile.read(bytes, 0, BufferPool.getPageSize()); // read file' content into bytes array
            if (len != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableID, pageNumber, len));
            }

            HeapPageId pageId = new HeapPageId(tableID, pageNumber);
            HeapPage targetPage = new HeapPage(pageId, bytes);
            return targetPage;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                accessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableID, pageNumber));
    }
```

readPage就是从此HeapFile中读取序号为pageNum的某个HeadPage。

这里因为要随机读写，因此使用RandomAccessFile来访问，接着定位到page开始的位置(PageNum * BufferPool.getPageSize())。将数据读取完毕后返回构造出HeapPage实例返回即可。

2.DbFileIterator，读取该DbFile中的所有Tuple。这里不能将所有的Tuple全都load到内存中，不然是会溢出的，因此先从BufferPool中读取page，获取其Iterator，如果当前Iterator读完了这个page的内容，就load下一个page进行读取即可

```java
 public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator iterator = new HeapFileIterator(tid, this);
        return iterator;
    }

    /**
     * use to iterator all the tuples of HeapFile
     * read page from BufferPool, or will occur Out of Memory !!!
     * because Physical Memory maybe can't load all tuples
     */
    public static class HeapFileIterator implements DbFileIterator {
        private final TransactionId transactionId;

        private final HeapFile heapFile;

        private Iterator<Tuple> iterator;

        private int pageNum;

        /**
         * Constructor of HeapFileIterator
         * @param transactionId
         * @param heapFile target HeapFile
         */
        public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
            this.transactionId = transactionId;
            this.heapFile = heapFile;
        }

        /**
         * load tuple iterator from page which fetch from BufferPool
         * @param pageNum target PageNum, start from 0
         * @return Iterator of All tuples of the page which fetch from BufferPool
         * @throws DbException pageNum is invalid
         * @throws TransactionAbortedException
         */
        private Iterator<Tuple> getTupleIterator(int pageNum) throws DbException, TransactionAbortedException {
            if (pageNum < 0 || pageNum >= this.heapFile.numPages()) {
                throw new DbException("pageNum {" + pageNum + "} is not valid");
            }

            HeapPageId heapPageId = new HeapPageId(this.heapFile.getId(), pageNum);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, heapPageId, Permissions.READ_ONLY);
            if (page == null) {
                throw  new DbException("can't find match page with pageNum {" + pageNum + "}");
            }
            return page.iterator();
        }

        /**
         *  Opens the iterator
         * @throws DbException  when there are problems opening/accessing the database.
         * @throws TransactionAbortedException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNum = 0;
            iterator = getTupleIterator(pageNum);
        }

        /**
         * check Has next or Not
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.iterator == null) return false; // iterator is not initial from getTupleIterator()

            if (this.pageNum >= heapFile.numPages()) return false; // pageNum is out of HeapFile len

            // at the end of iterator
            if (!this.iterator.hasNext() && pageNum == this.heapFile.numPages() - 1) return false;

            return true;
        }

        /**
         *  Gets the next tuple from the operator (typically implementing by reading
         *  from a child operator or an access method).
         * @return The next tuple in the iterator.
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(this.iterator == null) throw new NoSuchElementException("file not open, make sure open() before invoke this");

            if (!this.iterator.hasNext()) { // this page's iterator is null, fetch next page from BufferPool and read its tuple
                if (this.pageNum < this.heapFile.numPages() - 1) {
                    pageNum++;
                    this.iterator = getTupleIterator(pageNum);
                } else {
                    return null;
                }
            }
            return this.iterator.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         * @throws TransactionAbortedException
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            this.iterator = null;
        }
    }
```

### exercise6

实现seqScan，扫描DbFile里的所有tuple，应该是类似于

```sql
select * from table;
```

seqScan有这几个元素

```java
    private final TransactionId transactionId;

    private int tableID;

    private String tableAlias;

    private DbFileIterator iterator;
```

表ID，表别名，这在sql查询的时候会用到，因此不为空的时候需要重新构造TupleDesc中的name。

如下所示，要构造成 **tableAlias.name**

```java
    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableID);
        if (tableAlias == null) {
            return tupleDesc;
        }
        int numFields = tupleDesc.numFields();
        String[] fieldAr = new String[numFields]; //name
        Type[] typeAr = new Type[numFields]; // type
        for (int i = 0; i < numFields; i++) {
            fieldAr[i] = tableAlias + "." + tupleDesc.getFieldName(i);
            typeAr[i] = tupleDesc.getFieldType(i);
        }

        return new TupleDesc(typeAr,fieldAr);
    }
```

而为了遍历tule，引入DbFileIterator迭代器即可，在前面我们已经实例化了一个HeapFileIterator

### exercise7

试着将组件整合起来，跑一个简单的查询。

```java
package simpledb;


import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;

public class test {
    public static void main(String[] args) {
        // construct a 3-column table schema
        Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String names[] = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.txt
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.txt"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);

        }
    }
}

```

1.首先构造出类型和名字，这就像我们sql里的创建表

```java
Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
String names[] = new String[]{"field0", "field1", "field2"};
TupleDesc descriptor = new TupleDesc(types, names);
```

2.创建HeapFile，传入数据和数据的格式

```java
 HeapFile table1 = new HeapFile(new File("some_data_file.txt"), descriptor);
```

3.在catalog中建立table的映射

```java
Database.getCatalog().addTable(table1, "test");
```

4.原子性的创建一个事务ID，然后进行循序扫描

```java
// construct the query: we use a simple SeqScan, which spoonfeeds
// tuples via its iterator.
TransactionId tid = new TransactionId();
SeqScan f = new SeqScan(tid, table1.getId());
```

5.不断读取heapfile，遍历tuple将其打印

```java
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);

        }
```

