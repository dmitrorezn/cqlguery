package cqlguery

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
)

type Options []Option

func (opts Options) add(o ...Option) Options {
	return append(opts, o...)
}
func (opts Options) apply(c *gocql.ClusterConfig) {
	for _, o := range opts {
		o.apply(c)
	}
}

type Option func(config *gocql.ClusterConfig)

func (o Option) apply(c *gocql.ClusterConfig) {
	o(c)
}

func WithHosts(hosts ...string) Option {
	return func(config *gocql.ClusterConfig) {
		config.Hosts = hosts
	}
}

func WithKeyspace(Keyspace string) Option {
	return func(config *gocql.ClusterConfig) {
		config.Keyspace = Keyspace
	}
}

func WithConsistency(Consistency gocql.Consistency) Option {
	return func(config *gocql.ClusterConfig) {
		config.Consistency = Consistency
	}
}

func WithTimeout(Timeout time.Duration) Option {
	return func(config *gocql.ClusterConfig) {
		config.Timeout = Timeout
	}
}

func WithPasswordUsername(p, u string) Option {
	return func(config *gocql.ClusterConfig) {
		config.Authenticator = &gocql.PasswordAuthenticator{
			Username: p,
			Password: u,
		}
	}
}

const version = "3.11"

func DefaultOption() Options {
	return Options{
		WithTimeout(10 * time.Second),
		WithConsistency(gocql.One),
		func(config *gocql.ClusterConfig) {
			config.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
				NumRetries: 5, Min: time.Millisecond * 5, Max: time.Second * 5}
		},
		func(config *gocql.ClusterConfig) {
			config.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
				gocql.RoundRobinHostPolicy())
		},
	}
}

var defaultOption = DefaultOption()

func Connect(ctx context.Context, options ...Option) (*Session, error) {
	cluster := gocql.NewCluster()
	defaultOption.add(options...).apply(cluster)
	cluster.CQLVersion = "3.11"
	cluster.ProtoVersion = 3

	cs, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	sess := gocqlx.NewSession(cs)

	session := &Session{
		s: &sess,
	}
	go session.HandleConnect(ctx)

	return session, nil
}

type Session struct {
	options Options
	s       *gocqlx.Session
}

const reconnectAttempts = 10
const pingInterval = time.Second

func (s *Session) HandleConnect(ctx context.Context) {
	ticker := time.NewTimer(0)
	defer ticker.Stop()

	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err = s.Ping(ctx); err == nil {
			ticker.Reset(pingInterval)

			continue
		}
		for a := 0; a < reconnectAttempts && err != nil; a++ {
			err = s.Connect(ctx)
		}
		if err != nil {
			return
		}
		ticker.Reset(pingInterval)
	}

}
func (s *Session) Connect(ctx context.Context) error {
	ss, err := Connect(ctx)
	if err != nil {
		return err
	}
	s.s = ss.s

	return nil
}

const pingQuery = `SELECT uuid() FROM system.local;`

func (s *Session) Ping(ctx context.Context) error {
	var uid string
	if err := s.Select(ctx, pingQuery, &uid); err != nil {
		return err
	}
	if uid == "" {
		return errors.New("empty uuid from db")
	}

	return nil
}

type QueryOption struct {
	strict *bool
	idenpoten *bool
	con *gocql.Consistency
	rp *gocql.RetryPolicy
	value []any
	names []string
}

func (qo *QueryOption) apply(q *gocqlx.Queryx) *gocqlx.Queryx {
	if qo.strict!=nil && *qo.strict{
		q=q.Strict()
	}
	if qo.idenpoten!=nil{
		q=q.Idempotent(*qo.idenpoten)
	}
	if qo.con!=nil{
		q=q.Consistency(*qo.con)
	}
	if qo.rp!=nil{
		q=q.RetryPolicy(*qo.rp)
	}

	return q
}

type QueryOptionFunc func(qo *QueryOption)

func (o QueryOptionFunc) apply(qo *QueryOption) {
	o(qo)
}

func Strict() QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.strict=new(bool)
		*qo.strict=true
	}
}

func Idenpotent(i bool) QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.idenpoten=&i
	}
}

func Consistency(c gocql.Consistency) QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.con=&c
	}
}
func RetryPolicy(rp gocql.RetryPolicy) QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.rp=&rp
	}
}

func Bind(v ...any) QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.value=v
	}
}
func Names(names ...string) QueryOptionFunc {
	return func(qo *QueryOption) {
		qo.names=names
	}
}

type QueryOptions []QueryOptionFunc
func (opts QueryOptions) apply(qo *QueryOption) {
	for _, o := range opts {
		o.apply(qo)
	}
}

func (s *Session) Unwrap() *gocqlx.Session {
	return s.s
}

func (s *Session) Get(ctx context.Context, query string, dest any, optionFunc ...QueryOptionFunc) error {
	var opt QueryOption
	QueryOptions(optionFunc).apply(&opt)

	return opt.apply(s.s.ContextQuery(ctx, query, opt.names)).Get(dest)
}

func (s *Session) Iter(ctx context.Context, query string, optionFunc ...QueryOptionFunc) *gocqlx.Iterx {
	var opt QueryOption
	QueryOptions(optionFunc).apply(&opt)

	return opt.apply(s.s.ContextQuery(ctx, query, opt.names)).Iter()
}

func (s *Session) Select(ctx context.Context, query string, dest any, optionFunc ...QueryOptionFunc) error {
	var opt QueryOption
	QueryOptions(optionFunc).apply(&opt)

	return s.s.ContextQuery(ctx, query, opt.names).Select(dest)
}