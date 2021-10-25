package mcache

import (
	"errors"
	"github.com/grandecola/mmap"
)

var (
	errEmptyQueue  = errors.New("buffer is empty")
	errReaderQueue = errors.New("unable to get reader")
)

// IsEmpty当buffer全部出列后返回True
func (c *Cache) IsEmpty() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.isEmptyNoLock()
}

// 判断出列信息和写入尾部信息是否相等
// 如果相等则认为buffer全部读取完成
func (c *Cache) isEmptyNoLock() bool {
	headSId, headOffset := c.md.getDequeueInfo()
	tailSid, tailOffset := c.md.getSequenceInfo()
	return tailSid == headSId && tailOffset == headOffset
}

// 从缓存移除Buffer offset并返回该元素
func (c *Cache) Dequeue() (uint64, uint64, []byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var sid uint64
	var offset uint64
	var err error
	fastSid, fastOffset := c.md.getDequeueInfo()
	if sid, offset, err = c.dequeueReader(&c.br); err != nil {
		c.br.b = nil
		c.md.setDequeueInfo(fastSid, fastOffset)
		return 0, 0, nil, err
	}
	r := c.br.b
	c.br.b = nil
	// 读取完成后更新buffer出列信息
	c.md.setDequeueInfo(sid, offset)
	return sid, offset, r, nil
}

// 从buffer队列中读取,并交由reader来读取buffer
// 它负责读取分布在多个buffer区中的buffer
func (c *Cache) dequeueReader(r reader) (uint64, uint64, error) {
	if c.isEmptyNoLock() {
		return 0, 0, errEmptyQueue
	}

	// 读取当前出列的信息
	sid, offset := c.md.getDequeueInfo()
	// 基于出列信息读取数据Length
	newCid, newOffset, length, err := c.readLength(sid, offset)
	if err != nil {
		return 0, 0, err
	}
	sid, offset = newCid, newOffset

	// 读取字节消息
	r.grow(int(length))

	sid, offset, err = c.readBytes(r, sid, offset, length)
	if err != nil {
		return 0, 0, err
	}

	return sid, offset, nil
}

// 读取buffer数据的length
// 注意length始终写入一个cache区,它永远不会跨多个区写入
func (c *Cache) readLength(sid uint64, offset uint64) (uint64, uint64, uint64, error) {
	if offset+cInt64Size > c.conf.cacheFileSize {
		sid, offset = sid+1, 0
	}
	aa, err := c.getReader(sid)
	if err != nil {
		return 0, 0, 0, err
	}

	length := aa.ReadUint64At(int64(offset))
	offset += cInt64Size
	return sid, offset, length, nil
}

// 获取当前cache读取句柄
func (c *Cache) getReader(sid uint64) (*mmap.File, error) {
	for _, d := range c.dg.dgs {
		if d.sequence == sid {
			return c.dg.dgs[d.number].cbp, nil
		}
	}
	return nil, errReaderQueue
}

// readBytes reads length bytes from arena aid starting at offset.
func (c *Cache) readBytes(r reader, sid uint64, offset uint64, length uint64) (uint64, uint64, error) {
	var counter uint64 = 0
	for {
		if offset+length > c.conf.cacheFileSize {
			sid, offset = sid+1, 0
		}

		aa, err := c.getReader(sid)
		if err != nil {
			return 0, 0, err
		}

		bytesRead := r.readFrom(aa, offset, int(counter))
		counter += uint64(bytesRead)
		offset += uint64(bytesRead)

		if counter == length {
			break
		}
	}

	return sid, offset, nil
}
