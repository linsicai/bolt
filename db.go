package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// The largest step that can be taken when remapping the mmap.
// mmap 大小
const maxMmapStep = 1 << 30 // 1GB

// The data file format version.
// 版本号
const version = 2

// Represents a marker value to indicate that a file is a Bolt DB.
// 魔术
const magic uint32 = 0xED0CDAED

// IgnoreNoSync specifies whether the NoSync field of a DB is ignored when
// syncing changes to a file.  This is required as some operating systems,
// such as OpenBSD, do not have a unified buffer cache (UBC) and writes
// must be synchronized using the msync(2) syscall.
// 一些特殊处理
const IgnoreNoSync = runtime.GOOS == "openbsd"

// Default values if not set in a DB instance.
// 默认参数
const (
    // 批量提交最大大小
	DefaultMaxBatchSize  int = 1000
	// 批量提交延时
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	// ？
	DefaultAllocSize         = 16 * 1024 * 1024
)

// default page size for db is set to the OS page size.
var defaultPageSize = os.Getpagesize()

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	// 强一致性
	StrictMode bool

	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	// 非同步模式
	NoSync bool

	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	// ？
	NoGrowSync bool

	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	// mmap 标记
	MmapFlags int

	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	// 批量提交大小
	MaxBatchSize int

	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	// 批量提交延时
	MaxBatchDelay time.Duration

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	// ？
	AllocSize int

	path     string   // 路径
	file     *os.File // 文件
	lockfile *os.File // windows only
	dataref  []byte   // mmap'ed readonly, write throws SEGV
	data     *[maxMapSize]byte
	datasz   int // 数据大小？
	filesz   int // current on disk file size
	meta0    *meta // 元信息
	meta1    *meta // 元信息
	pageSize int   // 页大小
	opened   bool  // 打开标记
	rwtx     *Tx   // 写事务
	txs      []*Tx // 事务列表
	freelist *freelist // 空闲列表
	stats    Stats     // 统计

	pagePool sync.Pool // 页池

	batchMu sync.Mutex // 批量锁
	batch   *batch     // 批量

    // 锁
	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.

    // 写文件
	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	// 只读标记
	readOnly bool
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// GoString returns the Go string representation of the database.
func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path:%q}", db.path)
}

// String returns the string representation of the database.
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open creates and opens a database at the given path.
// If the file does not exist then it will be created automatically.
// Passing in nil options will cause Bolt to open the database with the default options.
// 打开数据库
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	// Set default options if no options are provided.
	if options == nil {
	    // 用户默认参数
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// Set default values for later DB operations.
	// 系统默认参数
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

    // 文件标记
	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// Open data file and separate sync handler for metadata writes.
	// 打开文件
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// Lock file so that other processes using Bolt in read-write mode cannot
	// use the database  at the same time. This would cause corruption since
	// the two processes would write meta pages and free pages separately.
	// The database file is locked exclusively (only one process can grab the lock)
	// if !options.ReadOnly.
	// The database file is locked using the shared lock (more than one process may
	// hold a lock at the same time) otherwise (options.ReadOnly is set).
	// 文件锁
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// Default values for test hooks
	// 注册钩子
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
	    // 文件有问题
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages.
		// 初始化文件
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// Read the first meta page to determine the page size.
		// 读取元信息表
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
		    // 从缓存中读取页
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
				// 验证失败，取默认页大小
			} else {
			    // 制定页大小
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Initialize page pool.
	// 初始化页池
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	// mmap
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// Read in the freelist.
	// 读取空闲列表
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// Mark the database as opened and return.
	return db, nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
    // 加锁
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

    // 校验文件信息
	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	// 矫正mmap 大小
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	// 释放资源
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	// unmap
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	// mmap
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	// 元信息
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	// 校验元信息页
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap unmaps the data file from memory.
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}

	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	// 找一个合适的大小
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	// 校验是否太大了
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	// 大文件优化
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	// 适配页大小
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	// 校验是否过大
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Set the page size to the OS page size.
	// 设置页面大小
	db.pageSize = os.Getpagesize()

	// Create two meta pages on a buffer.
	// 创建两个元信息页
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
	    // 建页
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		// 初始化元信息基本信息
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// Write an empty freelist at page 3.
	// 空闲链表页
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	// 空叶子页
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// Write the buffer to our data file.
	// 写文件
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	// 强制同步
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
    // 禁止写
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

    // 禁止修改元信息
	db.metalock.Lock()
	defer db.metalock.Unlock()

    // 读取mmap lock
	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
	    // 已关闭
		return nil
	}

    // 设置关闭标记
	db.opened = false

    // 清理空闲列表
	db.freelist = nil

	// Clear ops.
	// 清理钩子
	db.ops.writeAt = nil

	// Close the mmap.
	// 关闭mmap
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	// 关闭文件句柄
	if db.file != nil {
		// No need to unlock read-only file.
		// 关闭文件锁
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// Close the file descriptor.
		// 关闭文件描述符
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin starts a new transaction.
// Multiple read-only transactions can be used concurrently but only one
// write transaction can be used at a time. Starting multiple write transactions
// will cause the calls to block and be serialized until the current write
// transaction finishes.
//
// Transactions should not be dependent on one another. Opening a read
// transaction and a write transaction in the same goroutine can cause the
// writer to deadlock because the database periodically needs to re-mmap itself
// as it grows and it cannot do that while a read transaction is open.
//
// If a long running read transaction (for example, a snapshot transaction) is
// needed, you might want to set DB.InitialMmapSize to a large enough value
// to avoid potential blocking of write transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished or
// else the database will not reclaim old pages.
// 创建事务
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
	    // 写事务
		return db.beginRWTx()
	}

	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {
	// Lock the meta pages while we initialize the transaction. We obtain
	// the meta lock before the mmap lock because that's the order that the
	// write transaction will obtain them.
	// 锁元信息
	db.metalock.Lock()

	// Obtain a read-only lock on the mmap. When the mmap is remapped it will
	// obtain a write lock so all transactions must finish before it can be
	// remapped.
	// 要读mmap
	db.mmaplock.RLock()

	// Exit if the database is not open yet.
	if !db.opened {
	    // 如果db 关闭了，报错
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	// 创建事务
	t := &Tx{}
	t.init(db)

	// Keep track of transaction until it closes.
	// 入只读事务链表
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// Unlock the meta pages.
	// 解锁元信息
	db.metalock.Unlock()

	// Update the transaction stats.
	// 更新统计
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
	    // 只读数据库
		return nil, ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	// 要写了
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	// 锁元信息
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	// 创建写事务
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// Free any pages associated with closed read-only transactions.
	var minid txid = 0xFFFFFFFFFFFFFFFF
	// 释放已关闭的只读事务
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx removes a transaction from the database.
func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	// mmap 解锁
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	// 锁元信息
	db.metalock.Lock()

	// Remove the transaction.
	// 移除事务
	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// Unlock the meta pages.
	// 解锁
	db.metalock.Unlock()

	// Merge statistics.
	// 更新统计
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Update(fn func(*Tx) error) error {
    // 开始写事务
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
	    // 出错时回滚
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually commit.
	// 需要人工提交
	t.managed = true

	// If an error is returned from the function then rollback and return error.
	// 执行
	err = fn(t)
	t.managed = false
	if err != nil {
	    // 回滚
		_ = t.Rollback()
		return err
	}

    // 提交
	return t.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
//
// Attempting to manually rollback within the function will cause a panic.
func (db *DB) View(fn func(*Tx) error) error {
    // 读事务开始
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
	    // 有问题回滚
		if t.db != nil {
			t.rollback()
		}
	}()

	// Mark as a managed tx so that the inner function cannot manually rollback.
	t.managed = true

	// If an error is returned from the function then pass it through.
	err = fn(t)
	t.managed = false
	if err != nil {
	    // 出错回滚
		_ = t.Rollback()
		return err
	}

    // 正常回滚
	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch calls fn as part of a batch. It behaves similar to Update,
// except:
//
// 1. concurrent Batch calls can be combined into a single Bolt
// transaction.
//
// 2. the function passed to Batch may be called multiple times,
// regardless of whether it returns error or not.
//
// This means that Batch function side effects must be idempotent and
// take permanent effect only after a successful return is seen in
// caller.
//
// The maximum batch size and delay can be adjusted with DB.MaxBatchSize
// and DB.MaxBatchDelay, respectively.
//
// Batch is only useful when there are multiple goroutines calling it.
func (db *DB) Batch(fn func(*Tx) error) error {
    // 错误信号量
	errCh := make(chan error, 1)

    // 批量锁
	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
	    // 新建batch
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}

    // 加入到批量列表中
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		// 强制异步触发
		go db.batch.trigger()
	}
	// 解锁
	db.batchMu.Unlock()

    // 等待错误
	err := <-errCh
	if err == trySolo {
	    // 一个头做
		err = db.Update(fn)
	}

	return err
}

// 执行体
type call struct {
    // 运行hansr
	fn  func(*Tx) error

    // 错误信号量
	err chan<- error
}

type batch struct {
	db    *DB

	timer *time.Timer

	start sync.Once

	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
    // 只运行一次
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
    // 加锁
	b.db.batchMu.Lock()
	// 停止计时器
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
	    // 与世隔绝
		b.db.batch = nil
	}
	// 解锁
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}

		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error { return fdatasync(db) }

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// This is for internal access to the raw data bytes from the C cursor, use
// carefully, or not at all.
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer retrieves a page reference from a given byte array based on the current page size.
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options represents the options that can be set when opening a database.
type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	// 超时
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int
}

// DefaultOptions represent the options used if nil options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
// 用户默认配置
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats represents statistics about the database.
// 统计
type Stats struct {
	// Freelist stats
	// 空闲页表统计
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	// 事务统计
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

    // 事务统计
	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
// 统计去除
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}

	var diff Stats

	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

// 统计合并
func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr // 数据
	PageSize int     // 页大小
}

type meta struct {
	magic    uint32 // 魔术
	version  uint32 // 版本
	pageSize uint32 // 页大小
	flags    uint32 // 标记
	root     bucket // 桶页
	freelist pgid   // 回收链表页
	pgid     pgid   // 页id
	txid     txid   // 事务id
	checksum uint64 // 校验码
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *meta) validate() error {
    // 元信息校验
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}

	return nil
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
    // 将元信息写到页中

    // 页id 校验
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	// 随机页id
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	// 计算校验码
	m.checksum = m.sum64()

    // 复制到页中
	m.copy(p.meta())
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()

	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])

	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
// 断言
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// 打印信息
func warn(v ...interface{}) {
    fmt.Fprintln(os.Stderr, v...)
}
func warnf(msg string, v ...interface{}) {
   fmt.Fprintf(os.Stderr, msg+"\n", v...)
}

// 打印栈信息
// 去除前三行
func printstack() {
	stack := strings.Join(
	    strings.Split(
	        string(debug.Stack()), "\n")[2:],
	    "\n")

	fmt.Fprintln(os.Stderr, stack)
}
