package mcache

import (
	"fmt"
	"github.com/grandecola/mmap"
	"os"
	"path/filepath"
	"syscall"
)

const (
	_DefaultMetadataSize = 72
	_DefaultMetaDataName = "metadata.mdp"
)

// 元数据文件结构
type metaData struct {
	mdp  *mmap.File //mmap文件映射指针
	file string     //元数据的磁盘文件
	size uint64     //元数据文件当前大小
}

// 创建一个元数据文件结构
func (m *metaData) newMetaData(dir string) (*metaData, error) {
	file := filepath.Join(dir, _DefaultMetaDataName)
	fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, _DefaultFilePerm)
	if err != nil {
		return nil, err
	}
	_, err = fd.Stat()
	if err != nil {
		return nil, err
	}

	//设置文件大小
	if err = fd.Truncate(int64(_DefaultMetadataSize)); err != nil {
		return nil, fmt.Errorf("error in extending metadata file :: %w", err)
	}

	mdp, err := mmap.NewSharedFileMmap(fd, 0, _DefaultMetadataSize, syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		return nil, fmt.Errorf("error in mmaping a file :: %w", err)
	}

	// 文件句柄可以关闭掉，但不是关闭mmap的句柄
	if err := fd.Close(); err != nil {
		return nil, fmt.Errorf("error in closing the fd :: %w", err)
	}

	m.mdp = mdp
	m.file = file
	m.size = _DefaultMetadataSize

	//如下三个位置必须配置
	m.setTailInfo(_BeginSequence, 0)
	m.setDequeueInfo(_BeginSequence, 0)
	m.setSequenceInfo(_BeginSequence, 0)

	return m, nil
}

// 设置消费偏移量信息
//
//  <--- dequeue offset ------>
//     Cache id      Offset
//  +------------+------------+
//  | byte 00-07 | byte 08-15 |
//  +------------+------------+
//

//设置读取时的序列偏移量
func (m *metaData) setDequeueInfo(cid uint64, pos uint64) {
	m.mdp.WriteUint64At(cid, 0)
	m.mdp.WriteUint64At(pos, 8)
}

//获取消费偏移量信息
func (m *metaData) getDequeueInfo() (uint64, uint64) {
	return m.mdp.ReadUint64At(0), m.mdp.ReadUint64At(8)
}

// 设置当前写入偏移量的头信息
//
//  <---- sequence info------>
//     Cache id     Offset
//  +------------+------------+
//  | byte 16-23 | byte 24-31 |
//  +------------+------------+
//

//设置写入时的序列偏移量
func (m *metaData) setSequenceInfo(sequence uint64, pos uint64) {
	m.mdp.WriteUint64At(sequence, 16)
	m.mdp.WriteUint64At(pos, 24)
}

//获取当前写入偏移量的头信息
func (m *metaData) getSequenceInfo() (sequence uint64, pos uint64) {
	return m.mdp.ReadUint64At(16), m.mdp.ReadUint64At(24)
}

// 设置当前写入偏移量的尾信息
//
//  <---------- tail --------->
//     Cache id     Offset
//  +------------+------------+
//  | byte 32-39 | byte 40-47 |
//  +------------+------------+
//
func (m *metaData) setTailInfo(cid uint64, pos uint64) {
	m.mdp.WriteUint64At(cid, 32)
	m.mdp.WriteUint64At(pos, 40)
}

// 获取当前写入偏移量的尾信息
func (m *metaData) getTailInfo() (uint64, uint64) {
	return m.mdp.ReadUint64At(32), m.mdp.ReadUint64At(40)
}

// 设置当前写入文件的大小信息 - PS：暂丢弃
//
//  <---- Current Usage ----->
//     Cache id     offset
//  +------------+------------+
//  | byte 48-55 | byte 56-63 |
//  +------------+------------+
//
func (m *metaData) setCurrentUsageInfo(cid int, pos uint64) {
	m.mdp.WriteUint64At(uint64(cid), 48)
	m.mdp.WriteUint64At(pos, 56)
}

// 获取当前写入文件的大小信息 - PS：暂丢弃
func (m *metaData) getCurrentUsageInfo() (uint64, uint64) {
	return m.mdp.ReadUint64At(48), m.mdp.ReadUint64At(56)
}

// 设置当前写入缓存数据的总大小 - PS：暂丢弃
//
//  <-Data Usage->
//       total
//  +------------+
//  | byte 64-71 |
//  +------------+
//
func (m *metaData) setTotalSize(size uint64) {
	m.mdp.WriteUint64At(size, 64)
}

// 获取当前写入缓存数据的总大小 - PS：暂丢弃
func (m *metaData) getTotalSize() uint64 {
	return m.mdp.ReadUint64At(64)
}

// 写入内存状态信息到元数据磁盘文件
func (m *metaData) Flush() error {
	return m.mdp.Flush(syscall.MS_SYNC)
}

// 关闭mmap元数据文件,关闭钱会先刷新内存中的信息到磁盘文件
func (m *metaData) close() error {
	if m.mdp != nil {
		if err := m.Flush(); err != nil {
			return err
		}
		return m.mdp.Unmap()
	}

	return nil
}

func (m *metaData) putMetaDataInfo() {
	s := m.getTotalSize()
	fmt.Println("getTotalSize: ", s)

	d, dd := m.getSequenceInfo()
	fmt.Println("getHeadInfo: ", d, dd)

	d, dd = m.getDequeueInfo()
	fmt.Println("getDequeueInfo: ", d, dd)

	d, dd = m.getCurrentUsageInfo()
	fmt.Println("getCurentInfo: ", d, dd)

	d, dd = m.getTailInfo()
	fmt.Println("getTailInfo: ", d, dd)
}

//初始化元数据文件
func initMetaData(dir string) (*metaData, error) {
	return new(metaData).newMetaData(dir)
}
