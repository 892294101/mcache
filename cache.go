package mcache

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	cInt64Size = 8
)

type Cache struct {
	dg   *dataGroup //Cache buffer组
	md   *metaData  //元数据组
	conf *config    //Cache buffer 配置信息
	lock sync.Mutex //Buffer写入保护

	drain chan struct{}
	quit  chan struct{}
	wg    sync.WaitGroup

	bw bytesWriter //字节写入器
	br bytesReader //字节读取器
}

var (
	switchNextCacheAreaErr = fmt.Errorf("%s\n", "CACHE-0000") //Unable to switch to the next Cache area
)

//初始化Data Cache组
func (c *Cache) openBuffer(dir string) error {
	dc, err := initDataGroups(c.conf, dir)
	if err != nil {
		return err
	}
	c.dg = dc

	return nil
}

// 切换Cache数据缓存文件
func (c *Cache) enqueueNextCacheArea() error {
	cacheGroupSize := uint64(c.conf.cacheGroup)
	fastCid, _ := c.md.getTailInfo()
	nextCid := fastCid + 1
	nextSequence := c.dg.dgs[fastCid].sequence + 1
	dequeueTailCid, _ := c.md.getDequeueInfo()
	switch {
	case nextCid <= cacheGroupSize:
		if c.dg.dgs[nextCid].sequence < dequeueTailCid {
			//fmt.Printf(" <= 切换: %v ---->> ", c.dg.dgs[fastCid].sequence)
			//fmt.Println("切换写入nextCid <= cacheGroupSize c.dg.dgs[nextCid].sequence < dequeueTailCid: ", c.dg.dgs[nextCid].sequence, dequeueTailCid)
			c.dg.writeNumber = c.dg.dgs[nextCid].number
			c.dg.dgs[nextCid].mark = _CURRENT
			c.dg.dgs[nextCid].sequence = nextSequence
			c.dg.dgs[fastCid].mark = _ONLINE
			c.md.setTailInfo(c.dg.writeNumber, 0)

			if c.dg.dgs[_BeginSequence].mark == _DONE {
				if err := c.dg.truncateCacheAreaSpace(_BeginSequence); err != nil {
					return err
				}
			}

			//fmt.Printf("%v\n", nextSequence)
			/*fmt.Printf("切换写入sequence: %v , 出列序列: %v\n", c.dg.dgs[nextCid].sequence, dequeueTailCid)

			fmt.Println("切换写入后")
			for _, d := range c.dg.dgs {
				fmt.Println("切换写入", d)
			}
			fmt.Println("切换写入numberPosition: ", c.dg.writeNumber)
			fmt.Println()*/
			return nil
		}

	case nextCid > cacheGroupSize:
		if c.dg.dgs[_BeginSequence].sequence < dequeueTailCid {
			//fmt.Printf(" >  切换: %v ---->> ", c.dg.dgs[fastCid].sequence)
			//fmt.Printf("1新一轮切换 写入sequence: %v , 出列序列: %v\n ", c.dg.dgs[_BeginSequence].sequence, dequeueTailCid)

			//fmt.Println("nextCid > cacheGroupSize c.dg.dgs[_BeginSequence].sequence < dequeueTailCid: ", c.dg.dgs[_BeginSequence].sequence, dequeueTailCid)
			c.dg.writeNumber = c.dg.dgs[_BeginSequence].number
			c.dg.dgs[_BeginSequence].sequence = nextSequence
			c.dg.dgs[fastCid].mark = _ONLINE
			c.dg.dgs[_BeginSequence].mark = _CURRENT
			c.md.setTailInfo(c.dg.writeNumber, 0)

			if c.dg.dgs[_BeginSequence].mark == _DONE {
				if err := c.dg.truncateCacheAreaSpace(c.dg.writeNumber); err != nil {
					return err
				}
			}

			//fmt.Printf("%v\n", nextSequence)
			/*fmt.Printf("2新一轮切换 写入sequence: %v , 出列序列: %v\n ", c.dg.dgs[_BeginSequence].sequence, dequeueTailCid)
			fmt.Println("新一轮Cache切换后")
			for _, d := range c.dg.dgs {
				fmt.Println("新一轮切换写入", d)
			}
			fmt.Println("新一轮Cache切换后numberPosition: ", c.dg.writeNumber)
			fmt.Println()*/
			return nil
		}

	}
	return switchNextCacheAreaErr
}

//初始化Meta Data
func (c *Cache) openMetaData(dir string) error {
	md, err := initMetaData(dir)
	if err != nil {
		return err
	}
	c.md = md
	return nil
}

//Put DataGroup
func (c *Cache) PutDataGroups() {
	for _, d := range c.dg.dgs {
		fmt.Println("DataGroup: ", d)
	}
	fmt.Println("NumberPosition: ", c.dg.writeNumber, "Read Sequence: ", c.dg.ReadSequence)
	fmt.Println()
}

//创建一个缓存系统队列
func NewCache(dir string, opts ...option) (*Cache, error) {

	//初始化Cache配置
	conf := newConfig()
	for _, opt := range opts {
		if err := opt(conf); err != nil {
			return nil, err
		}
	}

	//初始化Cache
	cache := new(Cache)
	cache.conf = conf

	if err := cache.openBuffer(dir); err != nil {
		return nil, err
	}
	if err := cache.openMetaData(dir); err != nil {
		return nil, err
	}

	cache.wg.Add(1)
	go cache.periodicFlush()

	return cache, nil
}

func (c *Cache) Close(exit bool) {

	if c != nil {
		if c.dg != nil {
			_ = c.dg.close()
		} else if c.md != nil {
			_ = c.dg.close()
		}
	}
	if exit {
		os.Exit(1)
	}
}

func (c *Cache) periodicFlush() {
	defer c.wg.Done()
	if c.conf.flushPeriodTime <= 0 && c.conf.flushMultipleOps <= 0 {
		return
	}

	timer := &time.Timer{C: make(chan time.Time)}
	if c.conf.flushPeriodTime > 0 {
		timer = time.NewTimer(c.conf.flushPeriodTime)
	}

	var drainFlag bool
	for {
		if c.conf.flushPeriodTime != 0 {
			if !drainFlag && !timer.Stop() {
				<-timer.C
			}

			timer.Reset(c.conf.flushPeriodTime)
			drainFlag = false
		}

		select {
		case <-c.quit:
			return
		case <-c.drain:
			_ = c.dg.flush()
			_ = c.md.Flush()
		case <-timer.C:
			drainFlag = true
			_ = c.dg.flush()
			_ = c.md.Flush()
		}
	}
}
