## B+ Tree Index

### exercise1

通过PAgeID找到对应的leafPage(递归)。

主要思路：

- 首先先获取当前page的类型，如果是叶子Page就直接返回(递归出口)
- 不然的话就从BufferPool里通过pageID获取到实际的internalPage(这样才符合LRU)
- 获取当前internalPage中的BTreeEntry(这里的entry就是有一个Field作为Key，field在本lab就是实际的数据，比如你为整数列建索引，那么这里的field就是某个具体的整数值；同时还有两个指针，都是pageID类型，也就是左右孩子Page的指针)
- 遍历entry，需要注意这里如果传入的field是空的，那么我们就默认返回leafPage的最左节点，一般是用于全表扫描的；如果field不为空，那么就判断当前entry是否大于等于field，是的话就递归进入entry的左孩子节点。
- 如果上述遍历没有找到match的page，那么就会进入右孩子。

```java

    /**
     * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
     * the left-most page possibly containing the key field f. It locks all internal
     * nodes along the path to the leaf node with READ_ONLY permission, and locks the
     * leaf node with permission perm.
     * <p>
     * If f is null, it finds the left-most leaf page -- used for the iterator
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the current page being searched
     * @param perm       - the permissions with which to lock the leaf page
     * @param f          - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm, Field f) throws DbException, TransactionAbortedException {
        // some code goes here

        // 1. get Page's Type
        int type = pid.pgcateg();
        // 2. if page is leaf page, finish recur, convert page's type to BTreeLeafPage and return
        if (type == BTreePageId.LEAF) return (BTreeLeafPage) getPage(tid, dirtypages, pid, perm);
        // 3. get this internal page with ReadOnly
        BTreeInternalPage internalPage = (BTreeInternalPage) getPage(tid, dirtypages, pid, Permissions.READ_ONLY);

        // 4. get this page's iterator
        Iterator<BTreeEntry> it = internalPage.iterator();
        BTreeEntry entry = null;
        while (it.hasNext()) {
            entry = it.next();
            if (f == null) return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, null);
            Field key = entry.getKey();
            if (key.compare(Op.GREATER_THAN_OR_EQ, f)) // if current entry's filed is bigger than target field, search from current entry's left child
                return findLeafPage(tid, dirtypages, entry.getLeftChild(), perm, f);
        }

        // if left child is not match, search from right child
        return findLeafPage(tid, dirtypages, entry.getRightChild(), perm, f);
    }
```



来看一下是如何通过B+Tree来找到对应的page的。

```java
		// greater than
		IndexPredicate ipred = new IndexPredicate(Op.GREATER_THAN, f);
		DbFileIterator it = twoLeafPageFile.indexIterator(tid, ipred);
		it.open();
```

- 首先构造出索引的比较方式，这里是大于。

```sql
select id from t where id >10
```

- 接着通过BTreeFile的迭代器indexIterator去查找
- 看一下indexIterator的实现

```java
public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

```

直接返回一个BTreeSearchIterator

- 看一下BTreeSearchInterator的实现

```java
 public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN || ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
            curp = f.findLeafPage(tid, root, ipred.getField());
        } else {
            curp = f.findLeafPage(tid, root, null);
        }
        it = curp.iterator();
    }
```

比较明显，具体做了这些事情：

1.从BufferPool中获取RootPage

2.通过findLeafPage的结果来获取LeafPage

findLeafPage是啥？就是我们上面写的函数~



所以整个获取leafPage来进行检索的过程就很明显了。

### exercise2

![img](https://gitee.com/daniel2001/picture-bed/raw/master/c471d35b48b40f914db1d296f8c350a9.png)

这一部分是要实现B+树叶子节点和内部节点的分裂。

查看B+树的插入删除过程，可以看这里[B+树动图演示](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)

#### splitLeafPage

首先是叶子Page的分裂。

叶子节点的分裂可以这么理解：

- 当叶子pageA满了之后，就会触发分裂。
- 首先创建一个新的pageB,然后将当前叶子pageA后半部分的tuple删除，插入到pageB中
- 接着检查pageA是否有右兄弟指针pageC。如果有的话，需要将pageB插入到其中。比如：为分裂前：pageA->pageC；分裂后pageA->pageB->pageC
- 接着需要更新脏页和两个page的兄弟指针
- 接着找出此page的父节点(internalPage)，然后构造出entry，插入到此父节点中。
- 最后通过field找出要插入的page，return即可。



接着以dfs的形式看一下代码如何实现。

1.找到空的page得以复制tuple。怎么找？[getEmptyPage](#getEmptyPage)可以解决。

2.得到新的页面之后，就将tuple进行复制。这就类似数组元素移动的过程。

3.复制完了之后check一下之前的page是否有右兄弟（getRightSiblingId()）

4.更新page的左右指针关系。这就是链表的操作，不讲。

5.接着将刚刚涉及到的page丢入到脏页缓存里，这里的缓存就是指一个map，具体用途我要也不太懂。。。

6.大头来了，找到父节点。找父节点是通过[getParentWithEmptySlots](#getParentWithEmptySlots)来实现的。

7.找到父节点，此时父节点保证了肯定是有空的slot来插入entry的。插入entry，同时设置entry的左右指针。

8.调用updateParentPointer，将原page和分裂出的新page的父亲指针指向上面获取到的父page。

9.最后通过field来判断是要获取原page 还是分裂后的新page

#### splitInternalPage

![分裂过程](https://gitee.com/daniel2001/picture-bed/raw/master/c471d35b48b40f914db1d296f8c350a9.png)

看图片可以看出内部节点的分裂和叶子是不一样的，内部节点分裂需要将中间节点挤到父节点里。

大致过程概述：

1.创建一个新的InternalPage B

2.将满了的internalPage A中的entry边删除边复制到pageB中。

3.注意要将中间节点挤到父亲节点中。

4.更新脏页，也就是将pageA和pageB丢到map中

5.更新pageA和pageB的左右指针的关系

6.更新pageA和pageB中所有entry的指针(有点绕，下面会讲一下)

7.获取父亲节点pageC，将中间entry插入到父亲pageC中

8.更新父亲节点的entry指针

---

接着以dfs的形式讲一下

1.新建pageB：[getEmptyPage](#getEmptyPage)

2.获取原pageA的entry迭代器，将其边复制边删除

3.当迭代器到了中间entry，将其删除，此时不可以插入到pageB中！

4.更新一下脏页，无关紧要，略

5.更新pageA和pageB的左右指针的关系，这里通过调用[updateParentPointers](#updateParentPointers)来实现

6.更新中间entry的左右孩子指针：左为pageA，右为pageB

7.通过[getParentWithEmptySlots](#getParentWithEmptySlots)获取到父亲节点，并通过**insertEntry**插入中间entry。这一步挺复杂的，这个函数不太想说，大概就是需要到entry正确的位置然后插入，然后将后面的entry往后移动。

8.通过调用[updateParentPointers](#updateParentPointers)更新父page的指针。

9.最后通过field来判断要返回pageA还是pageB

### exercise 3

**Redistributing pages**

![image-20220220150332219](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220150332219.png)

![image-20220220150339499](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220150339499.png)

触发的情况：pageA的entry数量比较少，而pageB的entry数量比较多，就可以从pageB中steal一些entry到pageA中。

而两page的entry较少的时候，就会触发merge，也就是exercise4的内容。

而重新分配的数量可以有两种思路：

- pageA和pageB的entry之和除以2，这样二者都不会太满
- pageB留下总容量的1/2，剩余全丢给A

####  steal leaf page

1.获取pageA的tuple数量和pageB的tuple数量。求和除以2

2.根据参数判断是从左page还是从右page steal tuple

3.获取迭代器，删除tuple，插入tuple 0.0

4.entry是父亲pageC中指向pageA和pageB的entry，此时它的值更新为上述迭代器中获取到的middle的值就比如上面的图中，原entry的值为8，后面变成了6

#### steal from Left internal page

1.获取半满的左边pageA的tuple数量和右边pageB(较少tuple)的tuple数量之和n，求和除以2

2.获取pageA的方向迭代器，然后将pageA的最后一个节点删除。比如图片里的将6从pageA中删除。

3.构造一个新的entry：key为pageA和pageB在父亲pageC中对应entry的key。也就是图中的8。

![image-20220220150332219](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220150332219.png)

这个新的entry的左指针为原本pageA最后一个节点的右指针：也就是6的右指针；

这个新的entry的右指针为pageB第一个节点的左指针：也就是10的右指针；

4.pageB插入这个新的entry，也就是途中10的左边多个了8

5.完成上面最重要的步骤后，下面就是将剩余的entry移动过去（注意以下步骤和图片对应不上，因为图片刚好是移动一个就达到了条件）

这里就是一个循环，pageA删tuple，pageB添加tuple

6.当pageA的数量为n/2 + 1(均值的一般多一个)时，pageA删掉这个entry X，然后将父亲pageC中对应entry的key设为此entry X的key，这里对应的就是图片里的6，只不过上面已经完成了。

7.更新pageB的孩子指针指为B，因为复制过去的tuple，他们还认为parent是A

#### steal from  right internal page

步骤和上述一样。

### exercise 4

这里要实现两个page的合并

![image-20220220154338262](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220154338262.png)

![image-20220220154343545](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220154343545.png)

#### merge leaf page

![image-20220220154338262](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220154338262.png)

1.获取右pageB的迭代器

2.pageB删除tuple，pageA插入tuple。这样就变成了pageA有1 3 4 6 8 10

3.获取pageB的右兄弟pageD。然后将pageA和pageD的指针设为兄弟节点，也就是说：

>pageA -> pageB -> pageD变成 pageA -> pageD

4.将pageB清空

5.从父pageC中删掉之前含pageA和pageB的entry，也就是图里的6

#### merge internal page

![image-20220220154343545](https://gitee.com/daniel2001/picture-bed/raw/master/image-20220220154343545.png)

1.获取左pageA的反向迭代器

2.获取右pageB的正向迭代器

3.构造一个新entry X：X的key为pageA和pageB在父亲pageC中的entry的key。也就是图中的6

4.pageA插入entry X

5.将pageB的entry插入到pageA中

6.更新pageA中的所有entry的指针，因为pageB过来的entry对应的孩子page还认为自己的父亲是B，其实是A

7.从pageC中删掉pageA和pageB对应的entry Y

## func reading

### getEmptyPage

```java
    /**
     * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
     * and creates a new page if none are available.  It wipes the page on disk and in the cache and
     * returns a clean copy locked with read-write permission
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pgcateg    - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
     * @return the new empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPageNo(TransactionId, Map)
     * @see #setEmptyPage(TransactionId, Map, int)
     */
    private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg) throws DbException, IOException, TransactionAbortedException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().discardPage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }
```



上文可知此函数的作用是获取一个空的page然后返回，方便分裂叶子节点时复制tuple。

1.首先获取一个空的pageNo，具体见[getEmptyPageNo](#getEmptyPageNo)

2.通过pageNo和page类型(如leaf，internal)创建出pageID

3.写文件，通过pageNo在文件中定位到位置然后创建page。

4.调用[getPage](#getPage)就能从文件中获取到对应的page。

### getEmptyPageNo

```java
 /**
     * Get the page number of the first empty page in this BTreeFile.
     * Creates a new page if none of the existing pages are empty.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the page number of the first empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        // get a read lock on the root pointer page and use it to locate the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if (headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            // try to find a header page with an empty slot
            while (headerPage != null && headerPage.getEmptySlot() == -1) {
                headerId = headerPage.getNextPageId();
                if (headerId != null) {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                } else {
                    headerPage = null;
                }
            }

            // if headerPage is not null, it must have an empty slot
            if (headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        // at this point if headerId is null, either there are no header pages
        // or there are no free slots
        if (headerId == null) {
            synchronized (this) {
                // create the new page
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }
```

由上文可知此步是为了得到一个空的pageNo

1.首先获取BTreeRootPtrPage，具体见[getRootPtrPage](#getRootPtrPage)

2.得到根节点后，获取根节点的pageID

3.如果根节点的pageID不为空，就尝试从此page中获取一个pageNo

往下不写了，感觉不是很重要。



### getRootPtrPage

获取根节点。

```java
/**
 * Get a read lock on the root pointer page. Create the root pointer page and root page
 * if necessary.
 *
 * @param tid        - the transaction id
 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
 * @return the root pointer page
 * @throws DbException
 * @throws IOException
 * @throws TransactionAbortedException
 */
BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
    synchronized (this) {
        if (f.length() == 0) {
            // create the root pointer page and the root page
            BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
            byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
            byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
            bw.write(emptyRootPtrData);
            bw.write(emptyLeafData);
            bw.close();
        }
    }

    // get a read lock on the root pointer page
    return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
}
```

1.获取此BTreeFile的锁

2.如果此时文件内容为空就要先构造出空的根节点和空的叶子节点内容。

3.以读锁的形式获取根节点 [getPage](#getPage)

>```java
>return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
>```

### getPage

首先查看cache的脏页是否由目标page。有的话就直接返回，否则从BufferPool中返回，并将目标页丢当脏页中。

```java
    /**
     * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
     * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
     * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
     * presumably they will soon be dirtied by this transaction.
     * <p>
     * This method is needed to ensure that page updates are not lost if the same pages are
     * accessed multiple times.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the id of the requested page
     * @param perm       - the requested permissions on the page
     * @return the requested page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm) throws DbException, TransactionAbortedException {
        if (dirtypages.containsKey(pid)) {
            return dirtypages.get(pid);
        } else {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if (perm == Permissions.READ_WRITE) {
                dirtypages.put(pid, p);
            }
            return p;
        }
    }
```

### getParentWithEmptySlots

```java
    /**
     * Method to encapsulate the process of getting a parent page ready to accept new entries.
     * This may mean creating a page to become the new root of the tree, splitting the existing
     * parent page if there are no empty slots, or simply locking and returning the existing parent page.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param parentId   - the id of the parent. May be an internal page or the RootPtr page
     * @param field      - the key of the entry which will be inserted. Needed in case the parent must be split
     *                   to accommodate the new entry
     * @return the parent page, guaranteed to have at least one empty slot
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

        BTreeInternalPage parent = null;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId.pgcateg() == BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

            // update the root pointer
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
            rootPtr.setRootId(parent.getId());

            // update the previous root to now point to this new root.
            BTreePage prevRootPage = (BTreePage) getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        } else {
            // lock the parent page
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
        }

        // split the parent if needed
        if (parent.getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;

    }
```

函数是返回某个page的父page，能接受插入new Entry。封装让父页面准备好接受新条目的过程的方法。  这可能意味着创建一个页面成为树的新根，如果没有空槽，则拆分现有的父页面，或者简单地锁定并返回现有的父页面

那这就意味着这几个问题：

>1.如果父page是根节点咋办？这种情况在只有一层internal的时候会出现：
>
>- 根节点没满的情况：很好解决，以读写的权限来返回page

- 如果父page是根节点：暂时看不懂。。。
- 不是的话，就直接以读写的形式获取即可。

接着再check父page是否能接受插入新的entry，如果不行的话，就调用[splitInternalPage](#splitInternalPage)将此page递归的分裂。



### updateParentPointers

```java
    /**
     * Update the parent pointer of every child of the given page so that it correctly points to
     * the parent
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the parent page
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointer(TransactionId, Map, BTreePageId, BTreePageId)
     */
    private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page) throws DbException, TransactionAbortedException {
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while (it.hasNext()) {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if (e != null) {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }
```

这个函数是更新page的entry中，指向的孩子的指针。比如

![分裂过程](https://gitee.com/daniel2001/picture-bed/raw/master/c471d35b48b40f914db1d296f8c350a9.png)

看下面的内部节点：例如父亲page有1和6，那么就遍历1，6：

>遍历到1：获取1的left，将其parent指向此page
>
>遍历到6：获取到6的left，也就是3，将其parent标记为此page