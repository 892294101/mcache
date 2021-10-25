# mcache

mcache是一个基于mmap的缓存队列系统.

支持每小时T级别的数据入列和出列.



```go
func main() {
     c, err := mcache.NewCache(".", mcache.SetCacheSize(512*1024*1024*1), mcache.SetCacheGroups(9))

   if err != nil {
      fmt.Println("NewCache", err)
      c.Close(true)

   }

   t := []byte("A11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111B")
   var vg sync.WaitGroup

   var sequence0 uint64
   var offset0 uint64

   //6291456*10
   vg.Add(1)
   go func(c *mcache.Cache, seq *uint64, off *uint64) {
      defer vg.Done()
      fmt.Println("write begin: ", time.Now().Format("2006-01-02 15:04:05"))
      for i := 1; i <= 6291456*30; i++ { //188743680 180G
      RESET:
         _, o, s, err := c.Enqueue(t)
         if err != nil {
            time.Sleep(time.Second)
            fmt.Println("main: ", err)
            goto RESET
         } else {
            *seq = s
            *off = o
         }

      }

      fmt.Println("write done: ", time.Now().Format("2006-01-02 15:04:05"))
   }(c, &sequence0, &offset0)

   var ct int
   var crc int
   var sequence uint64
   var offset uint64
   vg.Add(1)
   go func(c *mcache.Cache, s *uint64, o *uint64, ct *int, cr *int) {
      defer vg.Done()
      fmt.Println("read begin: ", time.Now().Format("2006-01-02 15:04:05"))
      for {
         seq, off, _, err := c.Dequeue()
         if err == nil {
            *ct += 1
            *s = seq
            *o = off
         } else {
            time.Sleep(time.Second)
         }
      }

   }(c, &sequence, &offset, &ct, &crc)

   vg.Add(1)
   go func(c *mcache.Cache, s *uint64, o *uint64, cr *int, s1 *uint64, o1 *uint64) {
      defer vg.Done()
      for {
         t := time.NewTicker(time.Second * 1)
         select {
         case <-t.C:
            c.PutDataGroups() //188743680
            fmt.Println("Dequeue sequence: ", *s, "offset: ", *o, "total: ", *cr)
            fmt.Println("Enqueue sequence: ", *s1, "offset: ", *o1)
         }
         t.Stop()

      }
   }(c, &sequence, &offset, &ct, &sequence0, &offset0)

   vg.Wait()

}


Dequeue sequence:  4 offset:  347240128 total:  1897143
Enqueue sequence:  6 offset:  140845288
DataGroup:  &{bufferArea_5.cdf 5 0xc00005c1e0 536870912 ONLINE  0xc00009e008 5}
DataGroup:  &{bufferArea_6.cdf 6 0xc00005c210 536870912 CURRENT 0xc00000e048 6}
DataGroup:  &{bufferArea_7.cdf 7 0xc00005c240 536870912 UNUSED  0xc00000e050 0}
DataGroup:  &{bufferArea_9.cdf 9 0xc00005c2a0 536870912 UNUSED  0xc00000e060 0}
DataGroup:  &{bufferArea_4.cdf 4 0xc000094030 536870912 ONLINE  0xc00009e000 4}
DataGroup:  &{bufferArea_2.cdf 2 0xc00005c1b0 536870912 ONLINE  0xc00000e038 2}
DataGroup:  &{bufferArea_3.cdf 3 0xc000094000 536870912 ONLINE  0xc00000e040 3}
DataGroup:  &{bufferArea_8.cdf 8 0xc00005c270 536870912 UNUSED  0xc00000e058 0}
DataGroup:  &{bufferArea_1.cdf 1 0xc00005c180 536870912 ONLINE  0xc00000e030 1}
NumberPosition:  6 Read Sequence:  1

Dequeue sequence:  5 offset:  65894224 total:  2144754
Enqueue sequence:  6 offset:  190209976
DataGroup:  &{bufferArea_1.cdf 1 0xc00005c180 536870912 ONLINE  0xc00000e030 1}
DataGroup:  &{bufferArea_2.cdf 2 0xc00005c1b0 536870912 ONLINE  0xc00000e038 2}
DataGroup:  &{bufferArea_3.cdf 3 0xc000094000 536870912 ONLINE  0xc00000e040 3}
DataGroup:  &{bufferArea_8.cdf 8 0xc00005c270 536870912 UNUSED  0xc00000e058 0}
DataGroup:  &{bufferArea_4.cdf 4 0xc000094030 536870912 ONLINE  0xc00009e000 4}
DataGroup:  &{bufferArea_5.cdf 5 0xc00005c1e0 536870912 ONLINE  0xc00009e008 5}
DataGroup:  &{bufferArea_6.cdf 6 0xc00005c210 536870912 CURRENT 0xc00000e048 6}
DataGroup:  &{bufferArea_7.cdf 7 0xc00005c240 536870912 UNUSED  0xc00000e050 0}
DataGroup:  &{bufferArea_9.cdf 9 0xc00005c2a0 536870912 UNUSED  0xc00000e060 0}
NumberPosition:  6 Read Sequence:  1

Dequeue sequence:  5 offset:  306256312 total:  2377652
Enqueue sequence:  6 offset:  239258872
```

