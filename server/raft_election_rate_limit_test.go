package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNRGElectionConcurrencyLimitClusterStartup(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {max_mem_store: 2GB, max_file_store: 2GB, store_dir: '%s', limits: {max_concurrent_elections: 10}}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "TEST", 3)
	defer c.shutdown()

	c.waitOnLeader()
	c.waitOnPeerCount(3)

	s := c.leader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create 3 streams to trigger multiple elections
	streams := make([]string, 3)
	for i := 0; i < 3; i++ {
		streams[i] = fmt.Sprintf("STREAM_%d", i)
		cfg := &nats.StreamConfig{
			Name:      streams[i],
			Subjects:  []string{streams[i] + ".*"},
			Replicas:  1,
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)
	}

	// Wait for all streams to have leaders
	for _, stream := range streams {
		c.waitOnStreamLeader(globalAccountName, stream)
	}

	// Verify all streams are active
	count := 0
	for range js.StreamsInfo() {
		count++
	}
	require_Equal(t, count, 3)
}

func TestNRGElectionConcurrencyLimitConfigParsing(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 2GB,
			max_file_store: 2GB,
			store_dir: '%s',
			limits: {
				max_concurrent_elections: 5,
				election_lease: "2s",
				election_backoff_base: "100ms",
				election_backoff_max: "5s",
				election_backoff_jitter: 0.3
			}
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "CONFIGTEST", 3)
	defer c.shutdown()

	c.waitOnLeader()

	opts := c.leader().getOpts()
	require_Equal(t, opts.JetStreamLimits.MaxConcurrentElections, 5)
	require_Equal(t, opts.JetStreamLimits.ElectionLease, 2*time.Second)
	require_Equal(t, opts.JetStreamLimits.ElectionBackoffBase, 100*time.Millisecond)
	require_Equal(t, opts.JetStreamLimits.ElectionBackoffMax, 5*time.Second)
	require_Equal(t, opts.JetStreamLimits.ElectionBackoffJitter, 0.3)
}

func TestNRGElectionConcurrencyLimitMetaExempt(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 2GB,
			max_file_store: 2GB,
			store_dir: '%s',
			limits: {
				max_concurrent_elections: 1
			}
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "METAEXEMPT", 3)
	defer c.shutdown()

	// The cluster should be able to elect a meta leader even with a
	// concurrent-election cap of 1, since the meta group is exempt.
	c.waitOnLeader()
	c.waitOnPeerCount(3)
}

func TestNRGElectionConcurrencyLimitStaggeredElections(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 2GB,
			max_file_store: 2GB,
			store_dir: '%s',
			limits: {
				max_concurrent_elections: 4
			}
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "STAGGER", 3)
	defer c.shutdown()

	c.waitOnLeader()

	s := c.leader()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// Create 10 streams rapidly; with only 4 concurrent-election slots on
	// each node, most of these have to queue behind the limiter and still
	// need to land a leader eventually.
	streamNames := make([]string, 10)
	for i := 0; i < 10; i++ {
		streamNames[i] = fmt.Sprintf("STAGGER_%d", i)
		cfg := &nats.StreamConfig{
			Name:      streamNames[i],
			Subjects:  []string{streamNames[i] + ".*"},
			Replicas:  1,
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
		}
		_, err := js.AddStream(cfg)
		require_NoError(t, err)
	}

	// Wait for all streams to have leaders
	for _, stream := range streamNames {
		c.waitOnStreamLeader(globalAccountName, stream)
	}

	// Verify all streams are healthy
	count := 0
	for info := range js.StreamsInfo() {
		count++
		require_True(t, info.Cluster != nil)
	}
	require_Equal(t, count, 10)
}

func TestNRGElectionConcurrencyLimitNoLimit(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 2GB,
			max_file_store: 2GB,
			store_dir: '%s'
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "NOLIMIT", 3)
	defer c.shutdown()

	c.waitOnLeader()

	s := c.leader()
	require_Equal(t, s.getOpts().JetStreamLimits.MaxConcurrentElections, 0)
	require_True(t, s.electionLimiter.Load() == nil)
}

func TestNRGElectionConcurrencyLimitServerLimiter(t *testing.T) {
	tmpl := `
		listen: 127.0.0.1:-1
		server_name: %s
		jetstream: {
			max_mem_store: 2GB,
			max_file_store: 2GB,
			store_dir: '%s',
			limits: {
				max_concurrent_elections: 10,
				election_lease: "3s"
			}
		}

		cluster {
			name: %s
			listen: 127.0.0.1:%d
			routes = [%s]
		}

		accounts { $SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] } }
	`

	c := createJetStreamClusterWithTemplate(t, tmpl, "LIMITERTEST", 3)
	defer c.shutdown()

	c.waitOnLeader()

	s := c.leader()
	limiter := s.electionLimiter.Load()

	require_True(t, limiter != nil)
	require_Equal(t, len(limiter.expiry), 10)
	require_Equal(t, limiter.lease, 3*time.Second)

	// Exhaust every slot, then confirm the 11th caller is denied and a
	// released slot becomes immediately reusable.
	var acquired []int
	for i := 0; i < 10; i++ {
		slot, ok := limiter.Acquire()
		require_True(t, ok)
		acquired = append(acquired, slot)
	}
	if _, ok := limiter.Acquire(); ok {
		t.Fatalf("expected the 11th acquire to be denied")
	}
	limiter.Release(acquired[0])
	if _, ok := limiter.Acquire(); !ok {
		t.Fatalf("expected an acquire to succeed right after a release")
	}
}
