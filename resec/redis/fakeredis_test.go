package redis

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeRedis is a minimal RESP server for tests. It records every command
// except the client handshake (HELLO, CLIENT SETINFO, CLIENT SETNAME).
type fakeRedis struct {
	t  *testing.T
	ln net.Listener

	mu            sync.Mutex
	commands      []string
	infoResponse  string
	failAll       string // when set, every command returns this error
	failReplicaOf string // when set, REPLICAOF and SLAVEOF return this error
	conns         map[net.Conn]struct{}
	closed        bool
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return startFakeRedis(t, ln)
}

func startFakeRedis(t *testing.T, ln net.Listener) *fakeRedis {
	f := &fakeRedis{t: t, ln: ln, conns: map[net.Conn]struct{}{}}
	go f.serve()
	t.Cleanup(f.Close)
	return f
}

// restartOn starts a new fake server on the same address as a closed one.
func restartFakeRedis(t *testing.T, addr string) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return startFakeRedis(t, ln)
}

func (f *fakeRedis) Addr() string { return f.ln.Addr().String() }

func (f *fakeRedis) Close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	conns := f.conns
	f.conns = map[net.Conn]struct{}{}
	f.mu.Unlock()

	f.ln.Close()
	for c := range conns {
		c.Close()
	}
}

func (f *fakeRedis) SetInfo(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.infoResponse = s
}

func (f *fakeRedis) SetFailAll(msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAll = msg
}

func (f *fakeRedis) SetFailReplicaOf(msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failReplicaOf = msg
}

// Commands returns a copy of the recorded commands, e.g. "REPLICAOF NO ONE".
func (f *fakeRedis) Commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commands...)
}

func (f *fakeRedis) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			c.Close()
			return
		}
		f.conns[c] = struct{}{}
		f.mu.Unlock()
		go f.handle(c)
	}
}

func (f *fakeRedis) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		if _, err := c.Write([]byte(f.reply(args))); err != nil {
			return
		}
	}
}

func (f *fakeRedis) reply(args []string) string {
	name := strings.ToUpper(args[0])
	sub := ""
	if len(args) > 1 {
		sub = strings.ToUpper(args[1])
	}

	// Handshake: go-redis falls back to RESP2 on a HELLO error.
	if name == "HELLO" {
		return "-ERR unknown command 'HELLO'\r\n"
	}
	if name == "CLIENT" && (sub == "SETINFO" || sub == "SETNAME") {
		return "+OK\r\n"
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.commands = append(f.commands, strings.Join(args, " "))

	if f.failAll != "" {
		return "-" + f.failAll + "\r\n"
	}

	switch name {
	case "INFO":
		return bulk(f.infoResponse)
	case "REPLICAOF", "SLAVEOF":
		if f.failReplicaOf != "" {
			return "-" + f.failReplicaOf + "\r\n"
		}
		return "+OK\r\n"
	case "CLIENT":
		return ":0\r\n"
	}
	return "-ERR unknown command\r\n"
}

func bulk(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("unexpected line %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimRight(l, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}
