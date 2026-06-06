package pgmq

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const benchQueuePrefix = "pgmq_bench_test"

var benchDB *pgxpool.Pool

func TestMain(m *testing.M) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "postgres://postgres:password@localhost:5432/postgres"
	}

	ctx := context.Background()
	pool, err := NewPgxPool(ctx, connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	benchDB = pool
	code := m.Run()

	// cleanup all bench queues
	cleanupBenchQueues(ctx, pool)
	os.Exit(code)
}

func cleanupBenchQueues(ctx context.Context, db DB) {
	rows, err := db.Query(ctx, "SELECT queue_name FROM pgmq.meta WHERE queue_name LIKE $1", benchQueuePrefix+"_%")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		DropQueue(ctx, db, name)
	}
}

func newBenchQueue(b *testing.B) string {
	b.Helper()
	queue := fmt.Sprintf("%s_%d", benchQueuePrefix, time.Now().UnixNano())
	ctx := context.Background()
	if err := CreateUnloggedQueue(ctx, benchDB, queue); err != nil {
		b.Fatalf("create queue: %v", err)
	}
	b.Cleanup(func() {
		DropQueue(context.Background(), benchDB, queue)
	})
	return queue
}

func sampleMsg() jsontext.Value {
	return jsontext.Value(`{"id":1,"task":"process","ts":"2025-01-01T00:00:00Z"}`)
}

func sampleMsgs(n int) []jsontext.Value {
	msgs := make([]jsontext.Value, n)
	for i := range n {
		msgs[i] = jsontext.Value(fmt.Sprintf(`{"id":%d,"task":"process","ts":"2025-01-01T00:00:00Z"}`, i))
	}
	return msgs
}

func seedMessages(b *testing.B, queue string, count int) []int64 {
	b.Helper()
	ctx := context.Background()
	ids, err := SendBatch(ctx, benchDB, queue, sampleMsgs(count))
	if err != nil {
		b.Fatalf("seed send_batch: %v", err)
	}
	return ids
}

// --- Send benchmarks ---
func BenchmarkSend(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	msg := sampleMsg()

	b.ResetTimer()
	for b.Loop() {
		_, err := Send(ctx, benchDB, queue, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- SendBatch benchmarks ---
func BenchmarkSendBatch_10(b *testing.B) {
	benchSendBatch(b, 10)
}

func BenchmarkSendBatch_100(b *testing.B) {
	benchSendBatch(b, 100)
}

func BenchmarkSendBatch_1000(b *testing.B) {
	benchSendBatch(b, 1000)
}

func benchSendBatch(b *testing.B, size int) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	msgs := sampleMsgs(size)

	b.ResetTimer()
	for b.Loop() {
		_, err := SendBatch(ctx, benchDB, queue, msgs)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Read benchmarks ---

func BenchmarkRead(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	seedMessages(b, queue, 1000)

	b.ResetTimer()
	for b.Loop() {
		_, err := Read(ctx, benchDB, queue, 30)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				b.StopTimer()
				seedMessages(b, queue, 1000)
				b.StartTimer()
				continue
			}
			b.Fatal(err)
		}
	}
}

func BenchmarkReadBatch_10(b *testing.B) {
	benchReadBatch(b, 10)
}

func BenchmarkReadBatch_100(b *testing.B) {
	benchReadBatch(b, 100)
}

func benchReadBatch(b *testing.B, size int) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	seedMessages(b, queue, 1000*size)

	b.ResetTimer()
	for b.Loop() {
		msgs, err := ReadBatch(ctx, benchDB, queue, 30, int64(size))
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) == 0 {
			b.StopTimer()
			seedMessages(b, queue, 1000*size)
			b.StartTimer()
			continue
		}
	}
}

// --- Pop benchmarks ---
func BenchmarkPop(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	seedMessages(b, queue, 1000)
	b.ResetTimer()
	for b.Loop() {
		_, err := Pop(ctx, benchDB, queue)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				b.StopTimer()
				seedMessages(b, queue, 1000)
				b.StartTimer()
				continue
			}
			b.Fatal(err)
		}
	}
}

// --- Delete benchmarks ---
func BenchmarkDelete(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, 1000)
	i := 0

	b.ResetTimer()
	for b.Loop() {
		if i >= len(ids) {
			b.StopTimer()                      // 1. 暂停计时,避免把 seed 数据的耗时算进 Delete 性能里
			ids = seedMessages(b, queue, 1000) // 2. 重新塞入 1000 条新消息
			i = 0                              // 3. 索引归零
			b.StartTimer()                     // 4. 恢复计时,继续压测
		}
		_, err := Delete(ctx, benchDB, queue, ids[i])
		if err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkDeleteBatch_10(b *testing.B) {
	benchDeleteBatch(b, 10)
}

func BenchmarkDeleteBatch_100(b *testing.B) {
	benchDeleteBatch(b, 100)
}

func benchDeleteBatch(b *testing.B, size int) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, 1000*size)

	b.ResetTimer()
	for b.Loop() {
		if len(ids) < size {
			b.StopTimer()
			ids = seedMessages(b, queue, 1000*size)
			b.StartTimer()
		}
		batchIDs := ids[:size]
		ids = ids[size:]

		_, err := DeleteBatch(ctx, benchDB, queue, batchIDs)
		if err != nil {
			b.Fatal(err)
		}
	}

}

// --- Round-trip benchmark (Send + Read + Delete) ---
func BenchmarkRoundTrip_SendReadDelete(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	msg := sampleMsg()

	b.ResetTimer()
	for b.Loop() {
		msgID, err := Send(ctx, benchDB, queue, msg)
		if err != nil {
			b.Fatal(err)
		}

		readMsg, err := Read(ctx, benchDB, queue, 30)
		if err != nil {
			b.Fatal(err)
		}

		_, err = Delete(ctx, benchDB, queue, readMsg.MsgID)
		if err != nil {
			b.Fatal(err)
		}
		_ = msgID
	}
}

func BenchmarkRoundTrip_SendReadDeleteBatch(b *testing.B) {
	b.Run("Batch 1000", func(b *testing.B) {
		batchSendReadDelete(b, 1000)
	})
	b.Run("Batch 5000", func(b *testing.B) {
		batchSendReadDelete(b, 5000)
	})
	b.Run("Batch 10000", func(b *testing.B) {
		batchSendReadDelete(b, 10000)
	})
}

func batchSendReadDelete(b *testing.B, batchSize int) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	msgs := sampleMsgs(batchSize)

	b.ResetTimer()
	for b.Loop() {
		msgIDs, err := SendBatch(ctx, benchDB, queue, msgs)
		if err != nil {
			b.Fatal(err)
		}

		readMsgs, err := ReadBatch(ctx, benchDB, queue, 30, int64(batchSize))
		if err != nil {
			b.Fatal(err)
		}
		if len(readMsgs) == 0 {
			continue
		}
		readIDs := make([]int64, 0, len(readMsgs))
		for _, m := range readMsgs {
			readIDs = append(readIDs, m.MsgID)
		}

		_, err = DeleteBatch(ctx, benchDB, queue, readIDs)
		if err != nil {
			b.Fatal(err)
		}
		_ = msgIDs
	}
}

// --- Parallel benchmarks ---
func BenchmarkSend_Parallel(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	msg := sampleMsg()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := Send(ctx, benchDB, queue, msg)
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkRead_Parallel(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	seedMessages(b, queue, 1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := Read(ctx, benchDB, queue, 30)
			if err != nil {
				if errors.Is(err, ErrNoRows) {
					b.StopTimer()
					seedMessages(b, queue, 1000)
					b.StartTimer()
					continue
				}
				b.Error(err)
				return
			}
		}
	})
}
