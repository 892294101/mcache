# mcache

mcache是一个基于mmap的缓存队列系统.



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
```

