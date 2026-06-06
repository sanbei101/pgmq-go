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
	if err := CreateQueue(ctx, benchDB, queue); err != nil {
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
	seedMessages(b, queue, b.N)

	b.ResetTimer()
	for b.Loop() {
		_, err := Read(ctx, benchDB, queue, 30)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				break
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
	// seed enough for all iterations
	seedMessages(b, queue, b.N*size)

	b.ResetTimer()
	for b.Loop() {
		msgs, err := ReadBatch(ctx, benchDB, queue, 30, int64(size))
		if err != nil {
			b.Fatal(err)
		}
		if len(msgs) == 0 {
			break
		}
	}
}

// --- Pop benchmarks ---

func BenchmarkPop(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	seedMessages(b, queue, b.N)

	b.ResetTimer()
	for b.Loop() {
		_, err := Pop(ctx, benchDB, queue)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				break
			}
			b.Fatal(err)
		}
	}
}

// --- Archive benchmarks ---

func BenchmarkArchive(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, b.N)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if i >= len(ids) {
			break
		}
		_, err := Archive(ctx, benchDB, queue, ids[i])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkArchiveBatch_10(b *testing.B) {
	benchArchiveBatch(b, 10)
}

func BenchmarkArchiveBatch_100(b *testing.B) {
	benchArchiveBatch(b, 100)
}

func benchArchiveBatch(b *testing.B, size int) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, b.N*size)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		start := i * size
		if start+size > len(ids) {
			break
		}
		_, err := ArchiveBatch(ctx, benchDB, queue, ids[start:start+size])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Delete benchmarks ---

func BenchmarkDelete(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, b.N)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if i >= len(ids) {
			break
		}
		_, err := Delete(ctx, benchDB, queue, ids[i])
		if err != nil {
			b.Fatal(err)
		}
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
	ids := seedMessages(b, queue, b.N*size)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		start := i * size
		if start+size > len(ids) {
			break
		}
		_, err := DeleteBatch(ctx, benchDB, queue, ids[start:start+size])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- SetVisibilityTimeout benchmark ---

func BenchmarkSetVisibilityTimeout(b *testing.B) {
	queue := newBenchQueue(b)
	ctx := context.Background()
	ids := seedMessages(b, queue, b.N)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if i >= len(ids) {
			break
		}
		_, err := SetVisibilityTimeout(ctx, benchDB, queue, ids[i], 60)
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
	const batchSize = 1000
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
		var readIDs []int64
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
	seedMessages(b, queue, b.N)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := Read(ctx, benchDB, queue, 30)
			if err != nil {
				if errors.Is(err, ErrNoRows) {
					return
				}
				b.Error(err)
				return
			}
		}
	})
}
