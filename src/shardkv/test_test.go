package shardkv

import "6.5840/porcupine"
import "6.5840/models"
import "testing"
import "strconv"
import "time"
import "fmt"
import "sync/atomic"
import "sync"
import "math/rand"
import "io/ioutil"

const linearizabilityCheckTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// test static 2-way sharding, without shard movement.
func TestStaticShards5A(t *testing.T) {
	fmt.Printf("Test (5A): static shards ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)
	cfg.join(1)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	cfg.ShutdownGroup(1)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeClient(cfg.ctl) // only one call allowed per client
		go func(i int) {
			v := ck1.Get(ka[i])
			if v != va[i] {
				ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
			} else {
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				t.Fatal(err)
			}
			ndone += 1
		case <-time.After(time.Second * 2):
			done = true
			break
		}
	}

	if ndone != n/2 {
		t.Fatalf("expected %v completions with one shard dead; got %v\n",
			n/2, ndone)
	}

	// bring the crashed shard/group back to life.
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// do servers reject operations on shards for
// which they are not responsible?
func TestRejection5A(t *testing.T) {
	fmt.Printf("Test (5A): rejection ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)
	cfg.join(1)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	// now create a separate controller that thinks
	// there is only one group, handling all shards.
	// the k/v servers talk to the original controller,
	// so the k/v servers still think the shards are
	// divided between the two k/v groups.
	ctl1 := cfg.StartCtrlerService()
	cfg.ctljoin(0, ctl1)

	// ask clients that talk to ctl1 to fetch keys.
	// they'll send all requests to a single k/v group.
	// half the requests should be rejected due to
	// being sent to the a k/v group that doesn't think it
	// is handling the shard.
	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeClient(ctl1)
		go func(i int) {
			v := ck1.Get(ka[i])
			if v != va[i] {
				if v == "" {
					// if v is "", it probably means that a k/v group
					// returned a value for a key even though that
					// key's shard wasn't assigned to to the group.
					ch <- fmt.Sprintf("Get(%v): returned a value, but server should have rejected the request due to wrong shard", ka[i])
				} else {
					ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
				}
			} else {
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				t.Fatal(err)
			}
			ndone += 1
		case <-time.After(time.Second * 2):
			done = true
			break
		}
	}

	if ndone != n/2 {
		t.Fatalf("expected %v completions; got %v\n", n/2, ndone)
	}

	fmt.Printf("  ... Passed\n")
}

func TestJoinLeave5B(t *testing.T) {
	fmt.Printf("Test (5B): join then leave ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)
	logger.Infof("join 100")
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)

	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		logger.Infof("put: [%v]: [%v]", ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	logger.Infof("join 101")
	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		logger.Infof("append: [%v]: [%v]", ka[i], va[i])
		ck.Append(ka[i], x)
		va[i] += x
	}
	logger.Infof("leave 100")
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		logger.Infof("append: [%v]: [%v]", ka[i], va[i])
		ck.Append(ka[i], x)
		va[i] += x
	}

	// allow time for shards to transfer.
	time.Sleep(1 * time.Second)

	cfg.checklogs()
	cfg.ShutdownGroup(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshot5B(t *testing.T) {
	fmt.Printf("Test (5B): snapshots, join, and leave ...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	logger.Infof("join 100")
	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}
	logger.Infof("join 101 102, leave 100")
	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}
	logger.Infof("leave 101, join 100")
	cfg.leave(1)
	cfg.join(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	cfg.checklogs()
	logger.Infof("shutdown 100-102")
	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	logger.Infof("start 101-102")
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestMissChange5B(t *testing.T) {
	fmt.Printf("Test (5B): servers miss configuration changes...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	cfg.ShutdownServer(0, 0)
	cfg.ShutdownServer(1, 0)
	cfg.ShutdownServer(2, 0)

	cfg.join(2)
	cfg.leave(1)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(2 * time.Second)

	cfg.ShutdownServer(0, 1)
	cfg.ShutdownServer(1, 1)
	cfg.ShutdownServer(2, 1)

	cfg.join(0)
	cfg.leave(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1_5B(t *testing.T) {
	fmt.Printf("Test (5B): concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient(cfg.ctl)
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
func TestConcurrent2_5B(t *testing.T) {
	fmt.Printf("Test (5B): more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient(cfg.ctl)
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent3_5B(t *testing.T) {
	fmt.Printf("Test (5B): concurrent configuration change and restart...\n")

	cfg := make_config(t, 3, false, 300)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient(cfg.ctl)
		go ff(i, ck1)
	}

	t0 := time.Now()
	for time.Since(t0) < 12*time.Second {
		cfg.join(2)
		cfg.join(1)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.ShutdownGroup(0)
		cfg.ShutdownGroup(1)
		cfg.ShutdownGroup(2)
		cfg.StartGroup(0)
		cfg.StartGroup(1)
		cfg.StartGroup(2)

		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.leave(1)
		cfg.leave(2)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1_5B(t *testing.T) {
	fmt.Printf("Test (5B): unreliable 1...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(0)
	cfg.leave(1)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2_5B(t *testing.T) {
	fmt.Printf("Test (5B): unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient(cfg.ctl)
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3_5B(t *testing.T) {
	fmt.Printf("Test (5B): unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient(cfg.ctl)
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers are deleting
// shards for which they are no longer responsible.
func TestChallenge1Delete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")

	// "1" means force snapshot after every log entry.
	cfg := make_config(t, 3, false, 1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	cfg.join(0)

	// 30,000 bytes of total values.
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1000)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	for iters := 0; iters < 2; iters++ {
		cfg.join(1)
		cfg.leave(0)
		cfg.join(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
		cfg.leave(1)
		cfg.join(0)
		cfg.leave(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
	}

	cfg.join(1)
	cfg.join(2)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	total := 0
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			total += raft + snap
		}
	}

	// 27 keys should be stored once.
	// 3 keys should also be stored in client dup tables.
	// everything on 3 replicas.
	// plus slop.
	expected := 3 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle
// shards that are not affected by a config change
// while the config change is underway
func TestChallenge2Unaffected(t *testing.T) {
	fmt.Printf("Test: unaffected shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	// JOIN 100
	cfg.join(0)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// JOIN 101
	cfg.join(1)

	// QUERY to find shards now owned by 101
	c := cfg.ctl.ck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[1].gid
	}

	// Wait for migration to new config to complete, and for clients to
	// start using this updated config. Gets to any key k such that
	// owned[shard(k)] == true should now be served by group 101.
	<-time.After(1 * time.Second)
	for i := 0; i < n; i++ {
		if owned[i] {
			va[i] = "101"
			ck.Put(ka[i], va[i])
		}
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100
	// 101 doesn't get a chance to migrate things previously owned by 100
	cfg.leave(0)

	// Wait to make sure clients see new config
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys still complete
	for i := 0; i < n; i++ {
		shard := int(ka[i][0]) % 10
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-1")
			check(t, ck, ka[i], va[i]+"-1")
		}
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle operations on shards that
// have been received as a part of a config migration when the entire migration
// has not yet completed.
func TestChallenge2Partial(t *testing.T) {
	fmt.Printf("Test: partial migration shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.ctl)

	// JOIN 100 + 101 + 102
	cfg.joinm([]int{0, 1, 2}, cfg.ctl)

	// Give the implementation some time to reconfigure
	<-time.After(1 * time.Second)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// QUERY to find shards owned by 102
	c := cfg.ctl.ck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[2].gid
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100 + 102
	// 101 can get old shards from 102, but not from 100. 101 should start
	// serving shards that used to belong to 102 as soon as possible
	cfg.leavem([]int{0, 2})

	// Give the implementation some time to start reconfiguration
	// And to migrate 102 -> 101
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys now complete
	for i := 0; i < n; i++ {
		shard := key2shard(ka[i])
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-2")
			check(t, ck, ka[i], va[i]+"-2")
		}
	}

	fmt.Printf("  ... Passed\n")
}
