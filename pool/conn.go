package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"
)

var noDeadline = time.Time{}

// 连接池内连接定义
type Conn struct {
	usedAt  int64 // atomic
	netConn net.Conn

	br *bufio.Reader
	bw *bufio.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
}

// 新建连接池内一个连接
func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.br = bufio.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.SetUsedAt(time.Now())
	return cn
}

// 获取连接使用时间
func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

// 设置连接使用时间
func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// 重新连接，用形参连接取缔
func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.br.Reset(netConn)
	cn.bw.Reset(netConn)
}

// 使用连接写数据
func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

// 使用连接写数据
func (cn *Conn) Read(b []byte) (int, error) {
	return cn.netConn.Read(b)
}

// 获取与连接相连的对端的地址信息
func (cn *Conn) RemoteAddr() net.Addr {
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

// 从连接中读，自定义读取函数，从形参传入, 作为回调
func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(br *bufio.Reader) error) error {
	if timeout >= 0 {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}
	return fn(cn.br)
}

// 从连接中写，自定义写入函数，从形参中传入，作为回调
func (cn *Conn) WithWriter(ctx context.Context, timeout time.Duration, fn func(bw *bufio.Writer) error) error {
	if timeout >= 0 {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}

	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}

	if err := fn(cn.bw); err != nil {
		return err
	}

	return cn.bw.Flush()
}

// 关闭连接
func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

// 获取连接超时时间，
func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
