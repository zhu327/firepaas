package leader

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testConfig(t *testing.T) *pgx.ConnConfig {
	t.Helper()
	dsn := os.Getenv("FIREPAAS_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("set FIREPAAS_TEST_POSTGRES to run leader tests")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestSingleLeaderAndHandover(t *testing.T) {
	cfg := testConfig(t)
	key := "firepaas:leader-test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	got1, err := tryAcquire(ctx1, cancel1, cfg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !got1 {
		t.Fatal("instance1 should acquire the free lock")
	}

	// 实例 2 抢不到。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	got2, err := tryAcquire(ctx2, cancel2, cfg, key)
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
	got3, err := tryAcquire(ctx3, cancel3, cfg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !got3 {
		t.Fatal("instance2 should acquire after instance1 releases")
	}
	cancel3()
	time.Sleep(300 * time.Millisecond)
}
