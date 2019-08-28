package cache_server

/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"

	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Memcached struct {
	client *Client
}

type StringCacheKey string

func (s StringCacheKey) String() string {
	return string(s)
}

func (s StringCacheKey) Hash() uint32 {
	return 0
}

func (s StringCacheKey) LockKey() CacheKey {
	return s
}

func (s StringCacheKey) Type() CacheKeyType {
	return CacheKeyTypeNone
}

func (s StringCacheKey) Addr() net.Addr {
	return nil
}

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func NewMemcachedBySelectors(slcSelector ServerSelector, llcSelector ServerSelector) (CacheServer, error) {
	client := &Client{slcSelector: slcSelector, llcSelector: llcSelector}
	return &Memcached{client: client}, nil
}

func (c *Memcached) addServers(selector *Selector, servers []string) error {
	var hashring *Hashring
	for _, server := range servers {
		addr, err := getAddr(server)
		if err != nil {
			return errors.WithStack(err)
		}
		hashring = selector.ring.Add(addr)
	}
	selector.ring = hashring
	return nil
}

func (c *Memcached) removeServers(selector *Selector, servers []string) error {
	var hashring *Hashring
	for _, server := range servers {
		addr, err := getAddr(server)
		if err != nil {
			return errors.WithStack(err)
		}
		ring, err := selector.ring.Remove(addr)
		if err != nil {
			return errors.WithStack(err)
		}
		hashring = ring
	}
	selector.ring = hashring
	return nil
}

func (c *Memcached) AddSecondLevelCacheServers(servers ...string) error {
	selector := c.client.slcSelector.(*Selector)
	return errors.WithStack(c.addServers(selector, servers))
}

func (c *Memcached) AddLastLevelCacheServers(servers ...string) error {
	selector := c.client.llcSelector.(*Selector)
	return errors.WithStack(c.addServers(selector, servers))
}

func (c *Memcached) RemoveSecondLevelCacheServers(servers ...string) error {
	selector := c.client.slcSelector.(*Selector)
	return errors.WithStack(c.removeServers(selector, servers))
}

func (c *Memcached) RemoveLastLevelCacheServers(servers ...string) error {
	selector := c.client.llcSelector.(*Selector)
	return errors.WithStack(c.removeServers(selector, servers))
}

func (c *Memcached) Get(key CacheKey) (*CacheGetResponse, error) {
	item, err := c.client.Get(key)
	if err == ErrMemcacheCacheMiss {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &CacheGetResponse{
		Value: item.Value,
		Flags: item.Flags,
		CasID: item.casid,
	}, nil
}

func (c *Memcached) GetMulti(keys []CacheKey) (*Iterator, error) {
	itemMap, err := c.client.GetMulti(keys)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	iter := NewIterator(keys)
	for idx, key := range keys {
		item, exists := itemMap[key.String()]
		if exists {
			iter.SetContent(idx, &CacheGetResponse{
				Value: item.Value,
				Flags: item.Flags,
				CasID: item.casid,
			})
		} else {
			iter.SetError(idx, ErrCacheMiss)
		}
	}
	return iter, nil
}

func (c *Memcached) Set(req *CacheStoreRequest) error {
	item := &Item{
		Key:        req.Key,
		Flags:      req.Key.Hash(),
		Value:      req.Value,
		casid:      req.CasID,
		Expiration: int32(req.Expiration),
	}
	if req.CasID != 0 {
		if err := c.client.CompareAndSwap(item); err != nil {
			return errors.Wrapf(err, "failed set value to %s", req.Key)
		}
		return nil
	}
	if err := c.client.Set(item); err != nil {
		return errors.Wrapf(err, "failed set value to %s", req.Key)
	}
	return nil
}

func (c *Memcached) Add(key CacheKey, value []byte, expiration time.Duration) error {
	if err := c.client.Add(&Item{
		Key:        key,
		Value:      value,
		Expiration: int32(expiration),
	}); err != nil {
		return errors.Wrapf(err, "failed add value to %s", key)
	}
	return nil
}

func (c *Memcached) Delete(key CacheKey) error {
	err := c.client.Delete(key)
	if err == ErrMemcacheCacheMiss {
		// ignore cache miss
		return nil
	}
	return errors.WithStack(err)
}

func (c *Memcached) Flush() error {
	return errors.WithStack(c.client.FlushAll())
}

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrMemcacheCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrMemcacheCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrMemcacheNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrMemcacheServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrMemcacheNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMemcacheMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrMemcacheNoServers = errors.New("memcache: no servers configured or available")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

const buffered = 8 // arbitrary buffered channel size, for readability

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrMemcacheCacheMiss, ErrMemcacheCASConflict,
		ErrMemcacheNotStored, ErrMemcacheMalformedKey:
		return true
	}
	return false
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultOk        = []byte("OK\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)

type ServerSelector interface {
	// PickServer returns the server address that a given item
	// should be shared onto.
	PickServer(CacheKey) (net.Addr, error)
	Each(func(net.Addr) error) error
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	slcSelector ServerSelector
	llcSelector ServerSelector

	lk       sync.Mutex
	freeconn map[string][]*conn
}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key CacheKey

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() error {
	return cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*conn)
	}
	freelist := c.freeconn[addr.String()]
	if len(freelist) >= c.maxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freeconn[addr.String()] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr.String()]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr.String()] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *Client) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	nc, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		if err := cn.extendDeadline(); err != nil {
			return nil, err
		}
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	if err := cn.extendDeadline(); err != nil {
		return nil, err
	}
	return cn, nil
}

func (c *Client) getAddr(key CacheKey) (net.Addr, error) {
	switch key.Type() {
	case CacheKeyTypeSLC:
		return c.slcSelector.PickServer(key)
	case CacheKeyTypeLLC:
		return c.llcSelector.PickServer(key)
	}
	return nil, errors.Errorf("cannot pick server by %s", key.String())
}

func (c *Client) onItem(item *Item, fn func(*Client, *bufio.ReadWriter, *Item) error) error {
	addr, err := c.getAddr(item.Key)
	if err != nil {
		return err
	}
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	if err = fn(c, cn.rw, item); err != nil {
		return err
	}
	return nil
}

func (c *Client) FlushAll() error {
	if err := c.slcSelector.Each(c.flushAllFromAddr); err != nil {
		return err
	}
	if err := c.llcSelector.Each(c.flushAllFromAddr); err != nil {
		return err
	}
	return nil
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key CacheKey) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddr(addr, []string{key.String()}, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. Zero means the item has
// no expiration time. ErrCacheMiss is returned if the key is not in the cache.
// The key must be at most 250 bytes in length.
func (c *Client) Touch(key CacheKey, seconds int32) (err error) {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.touchFromAddr(addr, []string{key.String()}, seconds)
	})
}

func (c *Client) withKeyAddr(key CacheKey, fn func(net.Addr) error) (err error) {
	if !legalKey(key.String()) {
		return ErrMemcacheMalformedKey
	}
	addr, err := c.getAddr(key)
	if err != nil {
		return err
	}
	return fn(addr)
}

func (c *Client) withAddrRw(addr net.Addr, fn func(*bufio.ReadWriter) error) (err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return err
	}
	defer cn.condRelease(&err)
	return fn(cn.rw)
}

func (c *Client) withKeyRw(key CacheKey, fn func(*bufio.ReadWriter) error) error {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRw(addr, fn)
	})
}

func (c *Client) getFromAddr(addr net.Addr, keys []string, cb func(*Item)) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		if err := parseGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil
	})
}

// flushAllFromAddr send the flush_all command to the given addr
func (c *Client) flushAllFromAddr(addr net.Addr) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "flush_all\r\n"); err != nil {
			return err
		}
		if err := rw.Flush(); err != nil {
			return err
		}
		line, err := rw.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOk):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

func (c *Client) touchFromAddr(addr net.Addr, keys []string, expiration int32) error {
	return c.withAddrRw(addr, func(rw *bufio.ReadWriter) error {
		for _, key := range keys {
			if _, err := fmt.Fprintf(rw, "touch %s %d\r\n", key, expiration); err != nil {
				return err
			}
			if err := rw.Flush(); err != nil {
				return err
			}
			line, err := rw.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				return ErrMemcacheCacheMiss
			default:
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		return nil
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) GetMulti(keys []CacheKey) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item, len(keys))
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key.String()] = it
	}

	keyMap := make(map[net.Addr][]string, len(keys))
	for _, key := range keys {
		k := key.String()
		if !legalKey(k) {
			return nil, ErrMemcacheMalformedKey
		}
		addr, err := c.getAddr(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], k)
	}

	addrNum := len(keyMap)
	if addrNum == 1 {
		for addr, keys := range keyMap {
			if err := c.getFromAddr(addr, keys, addItemToMap); err != nil {
				return nil, err
			}
			return m, nil
		}
	}
	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func parseGetResponse(r *bufio.Reader, cb func(*Item)) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

const (
	StateKey int = iota
	StateFlag
	StateSize
	StateCasId
)

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	headerSize := 6 // "VALUE "
	size = -1
	if len(line) <= headerSize {
		err = fmt.Errorf("memcache: unexpected line in get response: %q", line)
		return
	}
	lineWithoutHeader := line[headerSize:]
	state := StateKey
	prevIdx := 0
	for i, b := range lineWithoutHeader {
		if !(b == 0x20 || b == 0x0d) {
			continue
		}

		// byte is SPACE or CR
		bytes := lineWithoutHeader[prevIdx:i]
		s := string(bytes)
		switch state {
		case StateKey:
			it.Key = StringCacheKey(s)
			state = StateFlag
		case StateFlag:
			flag, _ := strconv.ParseUint(s, 10, 32)
			it.Flags = uint32(flag)
			state = StateSize
		case StateSize:
			size, _ = strconv.Atoi(s)
			state = StateCasId
		case StateCasId:
			casId, _ := strconv.ParseUint(s, 10, 32)
			it.casid = casId
		}
		prevIdx = i + 1
	}
	if size == -1 {
		err = fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return
}

// Set writes the given item, unconditionally.
func (c *Client) Set(item *Item) error {
	return c.onItem(item, (*Client).set)
}

func (c *Client) set(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "set", item)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(item *Item) error {
	return c.onItem(item, (*Client).add)
}

func (c *Client) add(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *Client) Replace(item *Item) error {
	return c.onItem(item, (*Client).replace)
}

func (c *Client) replace(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *Client) CompareAndSwap(item *Item) error {
	return c.onItem(item, (*Client).cas)
}

func (c *Client) cas(rw *bufio.ReadWriter, item *Item) error {
	return c.populateOne(rw, "cas", item)
}

func (c *Client) populateOne(rw *bufio.ReadWriter, verb string, item *Item) error {
	if !legalKey(item.Key.String()) {
		return ErrMemcacheMalformedKey
	}
	var err error
	if verb == "cas" {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid)
	} else {
		_, err = fmt.Fprintf(rw, "%s %s %d %d %d\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value))
	}
	if err != nil {
		return err
	}
	if _, err = rw.Write(item.Value); err != nil {
		return err
	}
	if _, err := rw.Write(crlf); err != nil {
		return err
	}
	if err := rw.Flush(); err != nil {
		return err
	}
	line, err := rw.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultStored):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrMemcacheNotStored
	case bytes.Equal(line, resultExists):
		return ErrMemcacheCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrMemcacheCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
}

func writeReadLine(rw *bufio.ReadWriter, format string, args ...interface{}) ([]byte, error) {
	_, err := fmt.Fprintf(rw, format, args...)
	if err != nil {
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		return nil, err
	}
	line, err := rw.ReadSlice('\n')
	return line, err
}

func writeExpectf(rw *bufio.ReadWriter, expect []byte, format string, args ...interface{}) error {
	line, err := writeReadLine(rw, format, args...)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrMemcacheNotStored
	case bytes.Equal(line, resultExists):
		return ErrMemcacheCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrMemcacheCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(key CacheKey) error {
	return c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
	})
}

// DeleteAll deletes all items in the cache.
func (c *Client) DeleteAll() error {
	return c.withKeyRw(StringCacheKey(""), func(rw *bufio.ReadWriter) error {
		return writeExpectf(rw, resultDeleted, "flush_all\r\n")
	})
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *Client) Increment(key CacheKey, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *Client) Decrement(key CacheKey, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *Client) incrDecr(verb string, key CacheKey, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrMemcacheCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}
