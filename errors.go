package bolt

import "errors"

// These errors can be returned when opening or calling methods on a DB.
// DB 操作错误码
var (
	// ErrDatabaseNotOpen is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	// 数据库未打开
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrDatabaseOpen is returned when opening a database that is
	// already open.
	// 数据库已打开
	ErrDatabaseOpen = errors.New("database already open")

	// ErrInvalid is returned when both meta pages on a database are invalid.
	// This typically occurs when a file is not a bolt database.
	// 异常数据库
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch is returned when the data file was created with a
	// different version of Bolt.
	// 版本错误
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum is returned when either meta page checksum does not match.
	// 校验码错误
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout is returned when a database cannot obtain an exclusive lock
	// on the data file after the timeout passed to Open().
	// 超时
	ErrTimeout = errors.New("timeout")
)

// These errors can occur when beginning or committing a Tx.
// 事务错误码
var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	// 事务不可写
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	// 事务关闭错误
	ErrTxClosed = errors.New("tx closed")

	// ErrDatabaseReadOnly is returned when a mutating transaction is started on a
	// read-only database.
	// 数据库只读
	ErrDatabaseReadOnly = errors.New("database is in read-only mode")
)

// These errors can occur when putting or deleting a value or a bucket.
// 修改桶错误码
var (
	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	// 找不到桶
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists is returned when creating a bucket that already exists.
	// 桶已存在
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	// 桶名称异常
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrKeyRequired is returned when inserting a zero-length key.
	// Key 异常
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	// Key 太大
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	// Value 太大
	ErrValueTooLarge = errors.New("value too large")

	// ErrIncompatibleValue is returned when trying create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	// 非法操作 on key
	ErrIncompatibleValue = errors.New("incompatible value")
)
