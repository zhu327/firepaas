package leader

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run leader tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestSingleLeaderAndHandover(t *testing.T) {
	pool := testPool(t)
	key := "firepaas:leader-test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	got1, err := tryAcquire(ctx1, cancel1, pool, key)
	if err != nil {
		t.Fatal(err)
	}
	if !got1 {
		t.Fatal("instance1 should acquire the free lock")
	}

	// 实例 2 抢不到。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	got2, err := tryAcquire(ctx2, cancel2, pool, key)
	cancel2()
	if err != nil {
		t.Fatal(err)
	}
	if got2 {
		t.Fatal("instance2 must not acquire while instance1 holds the lock")
	}

	// 实例 1 释放后，实例 2 可以拿到。释放是异步的（后台 goroutine）。
	cancel1()
	time.Sleep(300 * time.Millisecond)

	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	got3, err := tryAcquire(ctx3, cancel3, pool, key)
	if err != nil {
		t.Fatal(err)
	}
	if !got3 {
		t.Fatal("instance2 should acquire after instance1 releases")
	}
	cancel3()
	time.Sleep(300 * time.Millisecond)
}
