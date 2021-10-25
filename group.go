package mcache

import (
	"errors"
	"fmt"
	"github.com/grandecola/mmap"
	"os"
	"path/filepath"
	"syscall"
)

const (
	_DefaultGroupPrefixName        = "bufferArea_"
	_DefaultGroupSuffixName        = ".cdf"
	_DefaultFilePerm               = 0740
	_CURRENT                       = "CURRENT"
	_UNUSED                        = "UNUSED "
	_ONLINE                        = "ONLINE "
	_DONE                          = "DONE   "
	_BeginSequence          uint64 = 1
)

var (
	errInvalidDataGroupMMAP = errors.New("DataGroup MMAP null pointer")
)

type data struct {
	file     string     //cache数据文件名称
	number   uint64     //当前文件编号
	cbp      *mmap.File //Cache buffer文件当前mmap文件的指针
	size     uint64     //Cache buffer文件的大小
	mark     string     //当前文件使用状态： 0:_CURRENT, 1:_ONLINE, 2:_UNUSED, 3:_DONE
	fd       *os.File   //文件句柄
	sequence uint64     //写入缓存序列
}

type dataGroup struct {
	dgs          map[uint64]*data //cache数据组
	writeNumber  uint64           //Cache buffer文件当前状态. 0 Current
	ReadSequence uint64           //暂时不用
}

func (d *dataGroup) newDataGroups(ops *config, dir string) (*dataGroup, error) {
	mt := make(map[uint64]*data, ops.cacheGroup)
	for i := 1; i <= ops.cacheGroup; i++ {
		file := filepath.Join(dir, fmt.Sprintf("%s%d%s", _DefaultGroupPrefixName, i, _DefaultGroupSuffixName))
		fd, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_TRUNC, _DefaultFilePerm)
		if err != nil {
			return nil, err
		}
		_, err = fd.Stat()
		if err != nil {
			return nil, err
		}

		err = fd.Truncate(int64(ops.cacheFileSize))

		m, err := mmap.NewSharedFileMmap(fd, 0, int(ops.cacheFileSize), syscall.PROT_READ|syscall.PROT_WRITE)
		if err != nil {
			release(mt)
			return nil, fmt.Errorf("error in mmaping a file: %w", err)
		}
		var ds data
		ds.cbp = m
		ds.file = file
		ds.size = ops.cacheFileSize
		ds.fd = fd
		ds.number = uint64(i)

		if i == 1 {
			ds.mark = _CURRENT //此文件被标记为当前使用
			d.writeNumber = ds.number
			ds.sequence = _BeginSequence //文件使用序列
			d.ReadSequence = ds.sequence
		} else {
			ds.mark = _UNUSED //此文件被标记为未使用
		}
		mt[uint64(i)] = &ds
	}
	d.dgs = mt
	return d, nil
}

// 关闭并释放所有在DataGroup中的mmap资源
func (d *dataGroup) close() error {
	if err := d.flush(); err != nil {
		return fmt.Errorf("DataGroup flush Error: %v", err)
	}
	if err := d.unmap(); err != nil {
		return fmt.Errorf("DataGroup Unmap Error: %v", err)
	}
	return nil
}

// 获取当前cache写入句柄
func (d *dataGroup) getWriteCache() (*mmap.File, uint64, error) {
	dgs := d.dgs[d.writeNumber]
	if dgs == nil {
		return nil, 0, errInvalidDataGroupMMAP
	}
	return dgs.cbp, dgs.sequence, nil
}

// 获取当前cache读取句柄
func (d *dataGroup) truncateCacheAreaSpace(cid uint64) error {
	dgs := d.dgs[cid]
	_ = dgs.cbp.Flush(syscall.MS_SYNC)
	_ = dgs.cbp.Unmap()

	if err := dgs.fd.Truncate(0); err != nil {
		return errors.New(fmt.Sprintf("Truncate %v error: %v\n", 0, err))
	}

	if err := dgs.fd.Truncate(int64(dgs.size)); err != nil {
		return errors.New(fmt.Sprintf("Truncate %v error: %v\n", d.dgs[cid].size, err))
	}

	m, err := mmap.NewSharedFileMmap(dgs.fd, 0, int(d.dgs[cid].size), syscall.PROT_READ|syscall.PROT_WRITE)
	if err != nil {
		return fmt.Errorf("error in mmaping a file: %w\n", err)
	}
	dgs.cbp = m
	return nil
}

// 写入内存信息到磁盘文件
func (d *dataGroup) flush() error {
	for _, dgp := range d.dgs {
		if dgp.cbp != nil {
			if e := dgp.cbp.Flush(syscall.MS_SYNC); e != nil {
				return errors.New(fmt.Sprintf("Data Group MMAP Flush: %v", e))
			}
		}
	}
	return nil
}

// 写入内存状态信息到元数据磁盘文件
func (d *dataGroup) unmap() error {
	for _, dgp := range d.dgs {
		if e := dgp.cbp.Unmap(); e != nil {
			return errors.New(fmt.Sprintf("Data Group MMAP Unmap: %v", e))
		}
	}
	return nil
}

func initDataGroups(ops *config, dir string) (*dataGroup, error) {
	return new(dataGroup).newDataGroups(ops, dir)
}

func release(mt map[uint64]*data) {
	for _, d2 := range mt {
		if d2.cbp != nil {
			_ = d2.cbp.Flush(syscall.MS_SYNC)
			_ = d2.cbp.Unmap()
		}
	}
}
