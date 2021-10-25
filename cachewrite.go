package mcache

import (
	"github.com/grandecola/mmap"
)

// Enqueue在cache buffer的尾部添加一个新的buffer元素
func (c *Cache) Enqueue(message []byte) (uint64, uint64, uint64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.bw.b = message

	var cid, offset, sequence uint64
	var err error
	cid, offset, sequence, err = c.enqueue(&c.bw)
	if err != nil {
		if err.Error() == switchNextCacheAreaErr.Error() {
			c.bw.b = nil
			return 0, 0, 0, err
		}
		c.bw.b = nil
		return 0, 0, 0, err
	}
	c.bw.b = nil
	c.md.setSequenceInfo(sequence, offset) //设置当前序列写入信息
	return cid, offset, sequence, err
}

// 它首先写入数据长度，然后写入数据本身，整个数据可能不适合一个cache区
// 函数在必要情况下可以跨多个cache来写入数据
func (c *Cache) enqueue(w bufferWriter) (uint64, uint64, uint64, error) {
	//从元数据文件获取元数据中尾部偏移量信息
	cid, offset := c.md.getTailInfo()
	fastCid, fastOffset := c.md.getTailInfo()

	//根据元数据文件中的尾部偏移量信息写入数据Length
	cid, offset, sequence, err := c.writeLength(cid, offset, uint64(w.len()))
	if err != nil {
		c.md.setTailInfo(fastCid, fastOffset)
		return 0, 0, 0, err
	}

	//数据Length写入Cache缓存文件后，返回写入时的offset
	//然后根据offset写入数据信息
	cid, offset, sequence, err = c.writeBytes(w, cid, offset)
	//fmt.Println("writeBytes 当前id offset length: ", cid, offset,  w.len())
	if err != nil {
		c.md.setTailInfo(fastCid, fastOffset)
		return 0, 0, 0, err
	}
	//把入列后的数据偏移量信息写入到元数据文件
	c.md.setTailInfo(cid, offset)
	return cid, offset, sequence, nil
}

// writeLength 写入length到cache区尾部
// 注意length始终写入一个cache区，它永远不会跨多个区写入
func (c *Cache) writeLength(
	cid uint64,    //元数据文件中存储的数据写入尾部 Cache id
	offset uint64, //元数据文件中存储的数据写入尾部 Cache offset
	length uint64, //写入数据的长度
) (uint64, uint64, uint64, error) {
	//如果当前元数据文件的offset+cInt64Size(8)大于Cache缓存文件Size(cacheFileSize)
	//那么久需要切换到下一组Cache缓存文件，WriteLength不可以跨越两个Cache缓存文件

	//fmt.Printf("writeLength# offset+cInt64Size: %v  cacheFileSize: %v \n", offset+cInt64Size, c.conf.cacheFileSize)
	if offset+cInt64Size > c.conf.cacheFileSize {
		if err := c.enqueueNextCacheArea(); err != nil {
			return 0, 0, 0, err
		} else {
			cid, offset = c.md.getTailInfo()
		}

	}

	//获取当前cache写入句柄
	dg, sequence, err := c.dg.getWriteCache()
	if err != nil {
		return 0, 0, 0, err
	}
	//写入数据长度到Cache文件
	dg.WriteUint64At(length, int64(offset))

	//数据length写入到文件时的offset
	//因为数据写入时会+8
	//所以这里需要加上cInt64Size
	offset += cInt64Size
	return cid, offset, sequence, nil
}

// writeBytes 从偏移量位置开始写入字节切片到cache缓存文件
func (c *Cache) writeBytes(w bufferWriter, cid uint64, offset uint64) (uint64, uint64, uint64, error) {
	length := w.len()
	counter := 0
	var sequence uint64
	var dg *mmap.File
	var err error
	for {

		if offset+uint64(length) > c.conf.cacheFileSize {
			if err := c.enqueueNextCacheArea(); err != nil {
				return 0, 0, 0, err
			} else {
				cid, offset = c.md.getTailInfo()
			}
		}

		//fmt.Printf("writeBytes# offset : %v  cacheFileSize: %v \n", offset, c.conf.cacheFileSize)

		dg, sequence, err = c.dg.getWriteCache()
		if err != nil {
			return 0, 0, 0, err
		}

		bytesWritten := w.writeTo(dg, int(offset), counter)
		counter += bytesWritten
		offset += uint64(bytesWritten)

		// 如果所有字节已经写完，那么则退出写入
		if counter == length {
			break
		}

	}

	return cid, offset, sequence, nil
}
