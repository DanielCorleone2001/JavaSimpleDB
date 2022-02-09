## lab3

这部分是要实现查询优化，简单的实现直方图模型，认为tuple是等概率分布的。

其实可以理解成柱状图，每个柱状图柱子的左右区间代表某个区间内的值，共有多少条Tuple

![lab3-hist](https://gitee.com/daniel2001/picture-bed/raw/master/lab3-hist.png)

### exercise 1

实现Int类型的直方图。

直方图的意义就是，先将数据处理成模型(直方图模型)，在需要进行查询计划时，就能更快计算出需要的成本。

需要理解如何计算选择性

```java
   /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        if (op.equals(Predicate.Op.LESS_THAN)) {
            if (v <= min) return 0.0;
            if (v >= max) return 1.0;
            final int index = getIndex(v);
            double cnt = 0;
            for (int i = 0; i < index; ++i) {
                cnt += buckets[i];
            }
            cnt += buckets[index] / width * (v - index * width - min);
            return cnt / tupleNums;
        }
        if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        }
        if (op.equals(Predicate.Op.GREATER_THAN)) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        }
        if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        }
        if (op.equals(Predicate.Op.EQUALS)) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        }
        if (op.equals(Predicate.Op.NOT_EQUALS)) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return 0.0;
    }
```

此递归函数将所有的运算符都转换到小于运算符来计算。因为可以通过一些修改从而能通过`小于`，来得到其他类似的运算:`大于`...

那么我们看一下小于运算是怎么计算的：

- 首先判断v的值是否在最值区间内
- 接着获取v对应的索引，将索引前的所有柱状图的Tuple数量全部相加

>这里可以这么理解：
>
>![image-20211015145611089](https://gitee.com/daniel2001/picture-bed/raw/master/0964d92fe9e58d5603c7d57376a84a2f.png)
>
>例如 v为13，那么就要将[1-5], [6-10]的tuple全部加起来。
>
>接着到13的。在此柱状图中，不能全部将[11-15]的tuple加起来，因为13只占了其中的一部分，因此需要通过百分比来计算。

- 通过百分比计算v所在柱状图占的比例。

这样就能算出选择性，值域为[0,1]

### exercise 2

这里的主要任务是，对给定的table，扫描所有的tuple，对每个tuple都建立一个直方图。

具体步骤：

- 扫描全表，获取每个字段的最大值和最小值，从而确定每个字段的桶的数量。进而构造出直方图。
- 再次扫描全表，填充直方图中的每个桶具有的Tuple数量。

```java
   /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableID = tableid;
        this.ioCostPerPage = ioCostPerPage;

        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.td = heapFile.getTupleDesc();

        int[] min = new int[this.td.numFields()];
        int[] max = new int[this.td.numFields()];
        for (int i = 0; i < this.td.numFields(); i++) {
            min[i] = Integer.MAX_VALUE;
            max[i] = Integer.MIN_VALUE;
        }

        this.histogram = new Object[this.td.numFields()];
        DbFileIterator it = heapFile.iterator(new TransactionId());

        try {
            it.open();
            while (it.hasNext()) {
                Tuple tuple = it.next();
                numTuples++;

                for (int i = 0; i < this.td.numFields(); i++) {
                    if (this.td.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField field = (IntField) tuple.getField(i);
                        min[i] = Math.min(min[i], field.getValue());
                        max[i] = Math.max(max[i], field.getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < this.td.numFields(); i++) {
            if (this.td.getFieldType(i).equals(Type.INT_TYPE)) {
                this.histogram[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
            } else {
                this.histogram[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        try {
            it.rewind();
            while (it.hasNext()) {
                Tuple tuple = it.next();
                for (int i = 0; i < this.td.numFields(); i++) {
                    if (this.td.getFieldType(i).equals(Type.INT_TYPE)) {
                        ((IntHistogram) histogram[i]).addValue(((IntField) tuple.getField(i)).getValue());
                    } else {
                        ((StringHistogram) histogram[i]).addValue(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        it.close();
        int pageSize = BufferPool.getPageSize();
        this.numPages = (this.numTuples * pageSize + pageSize - 1) / pageSize;
    }
```



---

TableStats中有个ConcurrentHashMap，可以通过tableName获取到TableStat

### excercise 3

[原文链接](https://blog.csdn.net/hjw199666/article/details/103639262)

>观察join plan p的开销也包括了joincost((t1 join t2) join t3)这样的表达式，为了评估这样的表达式开销，需要一些方法估计t1 join t2 的大小(ntups)。这样的 join cardinality estimation（连接选择数预估）问题比起选择率预估问题更难。在这个lab中，不需要实现多精妙的预估，只需要基于直方图的一种方法来进行 join selectivity estimation。
>
>  下列是实现时需要注意的事情：
>
>  对于equality joins，当一个属性是primary key，由表连接产生的tuples数量不能大于non-primary key属性的选择数。
>
>  对于没有primary key的equality joins，很难说连接输出的大小是多少，可以是两表被选择数的乘积（如果两表的所有tuples都有相同的值），或者也可以是0。
>
>  对于大规模scans，很难说清楚明确的数量。输出的数量应该与输入的数量是成比例的，可以预估一个固定的分数代表range scans产生的向量叉积（cross-product），比如30%。总的来说，range join的开销应该大于相同大小两表的non-primary key equality join开销。

这里需要做的是估计某条查询，需要连接时，所需的`代价`。

```sql
select * from t1 inner join t2 on t1.id = t2.id;
```

查询的代价可以这么理解：两张表t1和t2。

- t1是驱动表，每取出t1的一条记录，都要和t2所有的记录进行CPU的计算开销(比如筛选t1.id = t2.id)。

假设t1的记录数为card1，t2记录数为card2。对t1进行筛选的开销是cost1，对t2进行筛选的开销是cost2。

那么可以得到连接需要的代价公式$cost1 + card1 * cost2 + card1 * card2$

$cost1$是取出所有t1记录需要的代价；

$cost1 * cost2$是每条t1记录和t2筛选的代价；

$cost1 * card2$是每次取出t2记录需要的代价。

因此就完成了计算代价的函数

```java
    /**
     * Estimate the cost of a join.
     * <p>
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     * @param j     A LogicalJoinNode representing the join operation being
     *              performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand
     *              side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand
     *              side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     * cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }
```

----

接着到计算基数，基数其实就是连接产生的记录数。

- 当连接的两个字段都是主键时，基数就是记录数最少的字段
- 当连接的两个字段都不是主键时，基数很难估计，这里直接是乘了一个因子。
- 当某一个是主键时，基数是另一个字段的记录数。

```java

    /**
     * Estimate the join cardinality of two tables.
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp, String table1Alias, String table2Alias, String field1PureName, String field2PureName, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats, Map<String, Integer> tableAliasToId) {
        if (joinOp != Predicate.Op.EQUALS) //No Equal join, cardinality is card1 * card2 * factory(use 0.3 in this lab)
            return card1 * card2 * 3 / 10;
        else if (!t1pkey && !t2pkey)
            return Math.max(card1, card2); // if not pkey to join, cardinality just use max of them
        else if (t1pkey && t2pkey)
            return Math.min(card1, card2); // if pkey, just use the min
        else if (t1pkey)
            return card2;                  // otherwise return the other card that is not pkey
        else
            return card1;
    }

```

### exercise 4

最后是选择合适的连接顺序，使得连接查询的代价最小。

[图片出处](https://blog.csdn.net/weixin_45834777/article/details/120788433?spm=1001.2014.3001.5502)

![image-20220209210442755](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220209210442755.png)
