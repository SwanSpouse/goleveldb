package main

import (
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	mrand "math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SwanSpouse/goleveldb/leveldb"
	"github.com/SwanSpouse/goleveldb/leveldb/errors"
	"github.com/SwanSpouse/goleveldb/leveldb/opt"
	"github.com/SwanSpouse/goleveldb/leveldb/storage"
	"github.com/SwanSpouse/goleveldb/leveldb/table"
	"github.com/SwanSpouse/goleveldb/leveldb/util"
)

var (
	dbPath                 = path.Join(os.TempDir(), "goleveldb-testdb")
	openFilesCacheCapacity = 500
	keyLen                 = 63                                                   // 测试key的长度
	valueLen               = 256                                                  // 测试value的长度
	numKeys                = arrayInt{100000, 1332, 531, 1234, 9553, 1024, 35743} // 测试的key数量
	httpProf               = "127.0.0.1:5454"
	transactionProb        = 0.5
	enableBlockCache       = false
	enableCompression      = false
	enableBufferPool       = false

	wg         = new(sync.WaitGroup)
	done, fail uint32

	bpool *util.BufferPool
)

type arrayInt []int

func (a arrayInt) String() string {
	var str string
	for i, n := range a {
		if i > 0 {
			str += ","
		}
		str += strconv.Itoa(n)
	}
	return str
}

func (a *arrayInt) Set(str string) error {
	var na arrayInt
	for _, s := range strings.Split(str, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			n, err := strconv.Atoi(s)
			if err != nil {
				return err
			}
			na = append(na, n)
		}
	}
	*a = na
	return nil
}

// 加载配置项
func init() {
	flag.StringVar(&dbPath, "db", dbPath, "testdb path")
	flag.IntVar(&openFilesCacheCapacity, "openfilescachecap", openFilesCacheCapacity, "open files cache capacity")
	flag.IntVar(&keyLen, "keylen", keyLen, "key length")
	flag.IntVar(&valueLen, "valuelen", valueLen, "value length")
	flag.Var(&numKeys, "numkeys", "num keys")
	flag.StringVar(&httpProf, "httpprof", httpProf, "http pprof listen addr")
	flag.Float64Var(&transactionProb, "transactionprob", transactionProb, "probablity of writes using transaction")
	flag.BoolVar(&enableBufferPool, "enablebufferpool", enableBufferPool, "enable buffer pool")
	flag.BoolVar(&enableBlockCache, "enableblockcache", enableBlockCache, "enable block cache")
	flag.BoolVar(&enableCompression, "enablecompression", enableCompression, "enable block compression")
}

// 产生随机数据
func randomData(dst []byte, ns, prefix byte, i uint32, dataLen int) []byte {
	if dataLen < (2+4+4)*2+4 {
		panic("dataLen is too small")
	}
	// 将dst扩大或者截断为dataLen这么长
	if cap(dst) < dataLen {
		dst = make([]byte, dataLen)
	} else {
		dst = dst[:dataLen]
	}
	// TODO -4是啥意思
	half := (dataLen - 4) / 2
	if _, err := rand.Reader.Read(dst[2 : half-8]); err != nil {
		panic(err)
	}
	// 第一位是ns，在这段代码里面ns是key的序号；
	// 第二位是prefix
	dst[0] = ns
	dst[1] = prefix
	// TODO 这里也没看懂啊，这两行不是重复了吗？
	binary.LittleEndian.PutUint32(dst[half-8:], i)
	binary.LittleEndian.PutUint32(dst[half-8:], i)
	binary.LittleEndian.PutUint32(dst[half-4:], util.NewCRC(dst[:half-4]).Value())
	full := half * 2
	copy(dst[half:full], dst[:half])
	if full < dataLen-4 {
		if _, err := rand.Reader.Read(dst[full : dataLen-4]); err != nil {
			panic(err)
		}
	}
	binary.LittleEndian.PutUint32(dst[dataLen-4:], util.NewCRC(dst[:dataLen-4]).Value())
	return dst
}

func dataSplit(data []byte) (data0, data1 []byte) {
	n := (len(data) - 4) / 2
	return data[:n], data[n : n+n]
}

func dataNS(data []byte) byte {
	return data[0]
}

func dataPrefix(data []byte) byte {
	return data[1]
}

func dataI(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data[(len(data)-4)/2-8:])
}

// 计算校验和
func dataChecksum(data []byte) (uint32, uint32) {
	// 这个是从数据里面读出的校验和
	checksum0 := binary.LittleEndian.Uint32(data[len(data)-4:])
	// 这个是根据数据里面计算出来的校验和
	checksum1 := util.NewCRC(data[:len(data)-4]).Value()
	return checksum0, checksum1
}

func dataPrefixSlice(ns, prefix byte) *util.Range {
	return util.BytesPrefix([]byte{ns, prefix})
}

func dataNsSlice(ns byte) *util.Range {
	return util.BytesPrefix([]byte{ns})
}

type testingStorage struct {
	storage.Storage
}

func (ts *testingStorage) scanTable(fd storage.FileDesc, checksum bool) (corrupted bool) {
	r, err := ts.Open(fd)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	size, err := r.Seek(0, os.SEEK_END)
	if err != nil {
		log.Fatal(err)
	}

	o := &opt.Options{
		DisableLargeBatchTransaction: true,
		Strict:                       opt.NoStrict,
	}
	if checksum {
		o.Strict = opt.StrictBlockChecksum | opt.StrictReader
	}
	tr, err := table.NewReader(r, size, fd, nil, bpool, o)
	if err != nil {
		log.Fatal(err)
	}
	defer tr.Release()

	checkData := func(i int, t string, data []byte) bool {
		if len(data) == 0 {
			panic(fmt.Sprintf("[%v] nil data: i=%d t=%s", fd, i, t))
		}

		checksum0, checksum1 := dataChecksum(data)
		if checksum0 != checksum1 {
			atomic.StoreUint32(&fail, 1)
			atomic.StoreUint32(&done, 1)
			corrupted = true

			data0, data1 := dataSplit(data)
			data0c0, data0c1 := dataChecksum(data0)
			data1c0, data1c1 := dataChecksum(data1)
			log.Printf("FATAL: [%v] Corrupted data i=%d t=%s (%#x != %#x): %x(%v) vs %x(%v)",
				fd, i, t, checksum0, checksum1, data0, data0c0 == data0c1, data1, data1c0 == data1c1)
			return true
		}
		return false
	}

	iter := tr.NewIterator(nil, nil)
	defer iter.Release()
	for i := 0; iter.Next(); i++ {
		ukey, _, kt, kerr := parseIkey(iter.Key())
		if kerr != nil {
			atomic.StoreUint32(&fail, 1)
			atomic.StoreUint32(&done, 1)
			corrupted = true

			log.Printf("FATAL: [%v] Corrupted ikey i=%d: %v", fd, i, kerr)
			return
		}
		if checkData(i, "key", ukey) {
			return
		}
		if kt == ktVal && checkData(i, "value", iter.Value()) {
			return
		}
	}
	if err := iter.Error(); err != nil {
		if errors.IsCorrupted(err) {
			atomic.StoreUint32(&fail, 1)
			atomic.StoreUint32(&done, 1)
			corrupted = true

			log.Printf("FATAL: [%v] Corruption detected: %v", fd, err)
		} else {
			log.Fatal(err)
		}
	}

	return
}

func (ts *testingStorage) Remove(fd storage.FileDesc) error {
	if atomic.LoadUint32(&fail) == 1 {
		return nil
	}

	if fd.Type == storage.TypeTable {
		if ts.scanTable(fd, true) {
			return nil
		}
	}
	return ts.Storage.Remove(fd)
}

type latencyStats struct {
	mark          time.Time
	dur, min, max time.Duration
	num           int
}

func (s *latencyStats) start() {
	s.mark = time.Now()
}

func (s *latencyStats) record(n int) {
	if s.mark.IsZero() {
		panic("not started")
	}
	dur := time.Now().Sub(s.mark)
	dur1 := dur / time.Duration(n)
	if dur1 < s.min || s.min == 0 {
		s.min = dur1
	}
	if dur1 > s.max {
		s.max = dur1
	}
	s.dur += dur
	s.num += n
	s.mark = time.Time{}
}

func (s *latencyStats) ratePerSec() int {
	durSec := s.dur / time.Second
	if durSec > 0 {
		return s.num / int(durSec)
	}
	return s.num
}

func (s *latencyStats) avg() time.Duration {
	if s.num > 0 {
		return s.dur / time.Duration(s.num)
	}
	return 0
}

func (s *latencyStats) add(x *latencyStats) {
	if x.min < s.min || s.min == 0 {
		s.min = x.min
	}
	if x.max > s.max {
		s.max = x.max
	}
	s.dur += x.dur
	s.num += x.num
}

func main() {
	flag.Parse()

	if enableBufferPool {
		bpool = util.NewBufferPool(opt.DefaultBlockSize + 128)
	}

	log.Printf("Test DB stored at %q", dbPath)
	if httpProf != "" {
		log.Printf("HTTP pprof listening at %q", httpProf)
		/*
		debug的setBlockProfileRate方法设置go协程区块性能数据采集的速率， 单位：样本/秒。一个非零的值表示启动区块性能检测，零值表示停止区块 性能检测。采集的区块性能数据可以使用debug_writeBlockProfile方法写入文件。
		 */
		runtime.SetBlockProfileRate(1)
		go func() {
			// 这里是给HTTP Sever开了一个口子，通过用于debug/pprof路径来进行debug
			if err := http.ListenAndServe(httpProf, nil); err != nil {
				log.Fatalf("HTTPPROF: %v", err)
			}
		}()
	}

	// 设置并发数。其实不用写的话默认值也是这个。
	runtime.GOMAXPROCS(runtime.NumCPU())
	// 清空目录
	os.RemoveAll(dbPath)
	// 创建目录
	stor, err := storage.OpenFile(dbPath, false)
	if err != nil {
		log.Fatal(err)
	}
	// 创建一个测试数据库引擎
	tstor := &testingStorage{stor}
	defer tstor.Close()

	fatalf := func(err error, format string, v ...interface{}) {
		atomic.StoreUint32(&fail, 1)
		atomic.StoreUint32(&done, 1)
		log.Printf("FATAL: "+format, v...)
		if err != nil && errors.IsCorrupted(err) {
			cerr := err.(*errors.ErrCorrupted)
			if !cerr.Fd.Zero() && cerr.Fd.Type == storage.TypeTable {
				log.Print("FATAL: corruption detected, scanning...")
				if !tstor.scanTable(storage.FileDesc{Type: storage.TypeTable, Num: cerr.Fd.Num}, false) {
					log.Printf("FATAL: unable to find corrupted key/value pair in table %v", cerr.Fd)
				}
			}
		}
		runtime.Goexit()
	}

	if openFilesCacheCapacity == 0 {
		openFilesCacheCapacity = -1
	}
	// DB选项
	o := &opt.Options{
		OpenFilesCacheCapacity: openFilesCacheCapacity,
		DisableBufferPool:      !enableBufferPool,
		DisableBlockCache:      !enableBlockCache,
		ErrorIfExist:           true,
		Compression:            opt.NoCompression,
	}
	// 启用压缩
	if enableCompression {
		o.Compression = opt.DefaultCompression
	}
	// 在这里创建DB
	db, err := leveldb.Open(tstor, o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var (
		mu              = &sync.Mutex{}
		gGetStat        = &latencyStats{}
		gIterStat       = &latencyStats{}
		gWriteStat      = &latencyStats{}
		gTrasactionStat = &latencyStats{} // 这里有一个拼写错误
		startTime       = time.Now()

		writeReq    = make(chan *leveldb.Batch) // 这些都是没有缓冲区的chan
		writeAck    = make(chan error)
		writeAckAck = make(chan struct{})
	)

	// 接收writeReq中的数据并写入DB中
	go func() {
		for b := range writeReq {
			var err error
			// 产生随机数，如果小于transactionProb，就用事务进行写入
			// 所以这里测试的时候是一半正常写入，一半事务写入；
			if mrand.Float64() < transactionProb {
				log.Print("> Write using transaction")
				gTrasactionStat.start()
				var tr *leveldb.Transaction
				if tr, err = db.OpenTransaction(); err == nil {
					if err = tr.Write(b, nil); err == nil {
						if err = tr.Commit(); err == nil {
							gTrasactionStat.record(b.Len())
						}
					} else {
						tr.Discard()
					}
				}
			} else {
				gWriteStat.start()
				if err = db.Write(b, nil); err == nil {
					gWriteStat.record(b.Len())
				}
			}
			// 写入ack
			writeAck <- err
			// 写入ack的ack TODO
			<-writeAckAck
		}
	}()

	go func() {
		for {
			// 每隔3s钟输出一次当前测试的统计信息
			time.Sleep(3 * time.Second)

			log.Print("------------------------")

			log.Printf("> Elapsed=%v", time.Now().Sub(startTime))
			mu.Lock()
			log.Printf("> GetLatencyMin=%v GetLatencyMax=%v GetLatencyAvg=%v GetRatePerSec=%d", gGetStat.min, gGetStat.max, gGetStat.avg(), gGetStat.ratePerSec())
			log.Printf("> IterLatencyMin=%v IterLatencyMax=%v IterLatencyAvg=%v IterRatePerSec=%d", gIterStat.min, gIterStat.max, gIterStat.avg(), gIterStat.ratePerSec())
			log.Printf("> WriteLatencyMin=%v WriteLatencyMax=%v WriteLatencyAvg=%v WriteRatePerSec=%d", gWriteStat.min, gWriteStat.max, gWriteStat.avg(), gWriteStat.ratePerSec())
			log.Printf("> TransactionLatencyMin=%v TransactionLatencyMax=%v TransactionLatencyAvg=%v TransactionRatePerSec=%d", gTrasactionStat.min, gTrasactionStat.max, gTrasactionStat.avg(), gTrasactionStat.ratePerSec())
			mu.Unlock()

			cachedblock, _ := db.GetProperty("leveldb.cachedblock")
			openedtables, _ := db.GetProperty("leveldb.openedtables")
			alivesnaps, _ := db.GetProperty("leveldb.alivesnaps")
			aliveiters, _ := db.GetProperty("leveldb.aliveiters")
			blockpool, _ := db.GetProperty("leveldb.blockpool")
			writeDelay, _ := db.GetProperty("leveldb.writedelay")
			ioStats, _ := db.GetProperty("leveldb.iostats")
			log.Printf("> BlockCache=%s OpenedTables=%s AliveSnaps=%s AliveIter=%s BlockPool=%q WriteDelay=%q IOStats=%q", cachedblock, openedtables, alivesnaps, aliveiters, blockpool, writeDelay, ioStats)
			log.Print("------------------------")
		}
	}()

	// 这里开始写入数据
	for ns, numKey := range numKeys {
		func(ns, numKey int) {
			// 开始进入测试流程
			log.Printf("[%02d] STARTING: numKey=%d", ns, numKey)

			keys := make([][]byte, numKey)
			for i := range keys {
				// 随机产生key，prefix是1
				keys[i] = randomData(nil, byte(ns), 1, uint32(i), keyLen)
			}

			// 这个协程是负责写入数据的；
			// 这个协程是负责写入数据，生成快照，开然后进行验证的。
			wg.Add(1)
			go func() {
				// wi是写入数据的条数
				var wi uint32
				defer func() {
					log.Printf("[%02d] WRITER DONE #%d", ns, wi)
					wg.Done()
				}()

				var (
					b       = new(leveldb.Batch)
					k2, v2  []byte
					nReader int32
				)
				// 在这里判断是否要停止 done是程序的停止信号
				for atomic.LoadUint32(&done) == 0 {
					log.Printf("[%02d] WRITER #%d", ns, wi)

					// b清空
					b.Reset()
					// 在这里把所有的key value都写进去了。
					for _, k1 := range keys {
						// 向b里面写入数据
						// 随机生成key prefix是2
						k2 = randomData(k2, byte(ns), 2, wi, keyLen)
						// 随机生成key prefix是3
						v2 = randomData(v2, byte(ns), 3, wi, valueLen)
						b.Put(k2, v2) // 写入key prefix是2的
						b.Put(k1, k2) // 写入key prefix是1的
					}
					// 写入writeChan里面
					writeReq <- b
					// 在这里等ack
					if err := <-writeAck; err != nil {
						// TODO 但是这里为啥是个struct{}{}呢？
						writeAckAck <- struct{}{}
						// 这里是fatal，如果运行到这里，程序就gg了。
						fatalf(err, "[%02d] WRITER #%d db.Write: %v", ns, wi, err)
					}
					// 获取快照，每一批写入都获取依次快照
					snap, err := db.GetSnapshot()
					if err != nil {
						writeAckAck <- struct{}{}
						// 这里是fatal，如果运行到这里，程序就gg了。
						fatalf(err, "[%02d] WRITER #%d db.GetSnapshot: %v", ns, wi, err)
					}

					// 正常情况下会走到这里；
					writeAckAck <- struct{}{}

					wg.Add(1)
					// reader数量+1
					atomic.AddInt32(&nReader, 1)
					go func(snapwi uint32, snap *leveldb.Snapshot) {
						var (
							ri       int               // 读计数
							iterStat = &latencyStats{} // 遍历统计
							getStat  = &latencyStats{} // 获取值统计
						)
						// 这里是统计信息
						defer func() {
							mu.Lock()
							gGetStat.add(getStat)
							gIterStat.add(iterStat)
							mu.Unlock()

							atomic.AddInt32(&nReader, -1)
							log.Printf("[%02d] READER #%d.%d DONE Snap=%v Alive=%d IterLatency=%v GetLatency=%v", ns, snapwi, ri, snap, atomic.LoadInt32(&nReader), iterStat.avg(), getStat.avg())
							snap.Release()
							wg.Done()
						}()

						// TODO ???? 这个可能需要了解一下低层数据的存储格式；
						stopi := snapwi + 3 // 快照里面数据数量+3=stopi；也就是停止遍历的index
						// TODO 这个循环的终止条件没咋理解
						for (ri < 3 || atomic.LoadUint32(&wi) < stopi) && atomic.LoadUint32(&done) == 0 {
							var n int
							// 只遍历prefix为1的数据
							iter := snap.NewIterator(dataPrefixSlice(byte(ns), 1), nil)
							// 这里依次获取数据
							iterStat.start()
							for iter.Next() {
								k1 := iter.Key()
								k2 := iter.Value()
								iterStat.record(1)

								if dataNS(k2) != byte(ns) {
									fatalf(nil, "[%02d] READER #%d.%d K%d invalid in-key NS: want=%d got=%d", ns, snapwi, ri, n, ns, dataNS(k2))
								}

								kwritei := dataI(k2)
								if kwritei != snapwi {
									fatalf(nil, "[%02d] READER #%d.%d K%d invalid in-key iter num: %d", ns, snapwi, ri, n, kwritei)
								}

								getStat.start()
								// 从快照里面获取k2的值
								v2, err := snap.Get(k2, nil)
								if err != nil {
									fatalf(err, "[%02d] READER #%d.%d K%d snap.Get: %v\nk1: %x\n -> k2: %x", ns, snapwi, ri, n, err, k1, k2)
								}
								getStat.record(1)

								// 校验校验和
								if checksum0, checksum1 := dataChecksum(v2); checksum0 != checksum1 {
									err := &errors.ErrCorrupted{Fd: storage.FileDesc{Type: 0xff, Num: 0}, Err: fmt.Errorf("v2: %x: checksum mismatch: %v vs %v", v2, checksum0, checksum1)}
									fatalf(err, "[%02d] READER #%d.%d K%d snap.Get: %v\nk1: %x\n -> k2: %x", ns, snapwi, ri, n, err, k1, k2)
								}
								// 统计数量
								n++
								iterStat.start()
							}
							iter.Release()
							if err := iter.Error(); err != nil {
								fatalf(err, "[%02d] READER #%d.%d K%d iter.Error: %v", ns, snapwi, ri, numKey, err)
							}
							// TODO ???? 这里为啥相等呢？n是snapshot中的key value数量，numKey是当前测试批次应该写入、读取的key数量
							// TODO snapshot不是每写入两个key就生成一个吗？
							// 不是，是每一个批次的key value写入进去后生成一个快照，然后来快照里面读；不是每一个key都生成；
							if n != numKey {
								fatalf(nil, "[%02d] READER #%d.%d missing keys: want=%d got=%d", ns, snapwi, ri, numKey, n)
							}
							ri++ // 读取数据的数量
						}
					}(wi, snap)

					// 数据条数+1
					atomic.AddUint32(&wi, 1)
				}
			}()

			// TODO 这个是读协程 感觉这个协程不是用来读的，是用来删除prefix == 2 || m.rand.int()%999 = 0的这部分数据的
			// 这里的这部分数据哪里来的呢？
			// 这个协程也是写入数据的，只不过写入的是删除数据。因为levelDB只能是APPEND，所以删除也是一种APPEND
			delB := new(leveldb.Batch)
			wg.Add(1)
			go func() {
				var (
					i        int
					iterStat = &latencyStats{}
				)
				defer func() {
					log.Printf("[%02d] SCANNER DONE #%d", ns, i)
					wg.Done()
				}()

				time.Sleep(2 * time.Second)
				// 在这里判断是否要停止 done是程序的停止信号
				for atomic.LoadUint32(&done) == 0 {
					var n int
					delB.Reset()
					iter := db.NewIterator(dataNsSlice(byte(ns)), nil)
					iterStat.start()
					// 开始依次读。但是读出来的数据干嘛了呢？感觉只是删除了delB中的一部分数据，没有干别的了。
					for iter.Next() && atomic.LoadUint32(&done) == 0 {
						k := iter.Key()
						v := iter.Value()
						iterStat.record(1)

						// TODO 这个循环写的。。。ci==0的时候是key ci==1的时候是value
						// 依次校验key和value的校验和
						for ci, x := range [...][]byte{k, v} {
							checksum0, checksum1 := dataChecksum(x)
							if checksum0 != checksum1 {
								if ci == 0 {
									fatalf(nil, "[%02d] SCANNER %d.%d invalid key checksum: want %d, got %d\n%x -> %x", ns, i, n, checksum0, checksum1, k, v)
								} else {
									fatalf(nil, "[%02d] SCANNER %d.%d invalid value checksum: want %d, got %d\n%x -> %x", ns, i, n, checksum0, checksum1, k, v)
								}
							}
						}
						// 从delB中删除一部分数据
						// 哦哦哦。这里其实也是写入数据。写入删除的数据
						// 删除key prefix是2的。相当于删了一半的数据
						if dataPrefix(k) == 2 || mrand.Int()%999 == 0 { // TODO 后面这个随机数没看懂。纯粹删一半数据不是更好吗？便于统计。你这又多删除那么几个干嘛
							delB.Delete(k)
						}
						// 统计读了多少key-value对数据
						n++
						iterStat.start() // 统计信息
					}
					iter.Release()
					if err := iter.Error(); err != nil {
						fatalf(err, "[%02d] SCANNER #%d.%d iter.Error: %v", ns, i, n, err)
					}

					if n > 0 {
						log.Printf("[%02d] SCANNER #%d IterLatency=%v", ns, i, iterStat.avg())
					}

					if delB.Len() > 0 && atomic.LoadUint32(&done) == 0 {
						t := time.Now()
						writeReq <- delB
						if err := <-writeAck; err != nil {
							writeAckAck <- struct{}{}
							fatalf(err, "[%02d] SCANNER #%d db.Write: %v", ns, i, err)
						} else {
							writeAckAck <- struct{}{}
						}
						log.Printf("[%02d] SCANNER #%d Deleted=%d Time=%v", ns, i, delB.Len(), time.Now().Sub(t))
					}

					i++
				}
			}()
		}(ns, numKey)
	}

	// 这个是接收停止信号的。
	go func() {
		sig := make(chan os.Signal)
		signal.Notify(sig, os.Interrupt, os.Kill)
		log.Printf("Got signal: %v, exiting...", <-sig)
		atomic.StoreUint32(&done, 1)
	}()

	wg.Wait()
}
