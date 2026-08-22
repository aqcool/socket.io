package socketio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	engine "github.com/aqcool/socket.io/servers/engine/v3"
	engineconfig "github.com/aqcool/socket.io/servers/engine/v3/config"
)

type namespaceMatcherEntry struct {
	matcher NamespaceMatcher
	parent  *ParentNamespace
}

type Server struct {
	cfg Config
	engine engine.Server
	codec PacketCodec
	adapterFactory AdapterFactory

	ctx context.Context
	cancel context.CancelFunc
	done chan struct{}
	doneOnce sync.Once

	lifecycleMu sync.Mutex
	serving *http.Server
	closing bool
	closed bool

	namespaceMu sync.RWMutex
	namespaces map[string]*Namespace
	matchers []namespaceMatcherEntry

	clientMu sync.Mutex
	clients map[*client]struct{}

	hub *eventHub
}

func New(options ...Option) (*Server,error) {
	cfg:=DefaultConfig()
	for _,option:=range options { if option!=nil { if err:=option(&cfg);err!=nil{return nil,err} } }
	if cfg.PacketCodec==nil { cfg.PacketCodec=DefaultPacketCodec{} }
	if cfg.ValueCodec==nil { cfg.ValueCodec=JSONValueCodec{} }
	if cfg.Adapter==nil { cfg.Adapter=MemoryAdapterFactory{} }

	engineOpts:=engineconfig.DefaultOptions()
	engineOpts.SetPath(cfg.Path)
	engineOpts.SetAllowEIO3(true)
	eio:=engine.NewServer(engineOpts)

	ctx,cancel:=context.WithCancel(context.Background())
	s:=&Server{
		cfg:cfg,engine:eio,codec:cfg.PacketCodec,adapterFactory:cfg.Adapter,
		ctx:ctx,cancel:cancel,done:make(chan struct{}),
		namespaces:make(map[string]*Namespace),clients:make(map[*client]struct{}),
	}
	s.hub=newEventHub(nil,s.Context,nil,cfg.Logger)
	root,err:=newNamespace(s,"/")
	if err!=nil { cancel(); return nil,err }
	s.namespaces["/"]=root
	if err:=eio.On("connection",func(args ...any){
		if len(args)==0{return}
		conn,ok:=args[0].(engine.Socket); if !ok||conn==nil{return}
		c:=newClient(s,conn)
		s.clientMu.Lock(); if s.closed||s.closing { s.clientMu.Unlock(); conn.Close(true); return }; s.clients[c]=struct{}{}; s.clientMu.Unlock()
		c.start()
	});err!=nil { cancel(); return nil,err }
	return s,nil
}

func (s *Server) Config() Config {
	if s==nil{return Config{}}
	cfg:=s.cfg
	if s.cfg.Recovery!=nil { copyRecovery:=*s.cfg.Recovery; cfg.Recovery=&copyRecovery }
	return cfg
}
func (s *Server) Context() context.Context { if s==nil||s.ctx==nil{return context.Background()};return s.ctx }
func (s *Server) Handler() http.Handler { if s==nil||s.engine==nil{return http.HandlerFunc(func(w http.ResponseWriter,_ *http.Request){http.Error(w,ErrClosed.Error(),http.StatusServiceUnavailable)})};return s.engine }
func (s *Server) Serve(listener net.Listener) error {
	if s==nil||listener==nil{return fmt.Errorf("%w: listener is required",ErrInvalidArgument)}
	s.lifecycleMu.Lock(); if s.closed||s.closing{s.lifecycleMu.Unlock();return ErrClosed};if s.serving!=nil{s.lifecycleMu.Unlock();return ErrAlreadyServing};srv:=&http.Server{Handler:s.Handler()};s.serving=srv;s.lifecycleMu.Unlock()
	err:=srv.Serve(listener)
	s.lifecycleMu.Lock();if s.serving==srv{s.serving=nil};closing:=s.closing||s.closed;s.lifecycleMu.Unlock()
	if errors.Is(err,http.ErrServerClosed){return nil};if err!=nil&&!closing{_ = s.Close()};return err
}
func (s *Server) ListenAndServe(addr string) error { if strings.TrimSpace(addr)==""{return fmt.Errorf("%w: address is required",ErrInvalidArgument)};listener,err:=net.Listen("tcp",addr);if err!=nil{return err};return s.Serve(listener) }
func (s *Server) Shutdown(ctx context.Context) error { return s.shutdown(ctx,false) }
func (s *Server) Close() error { return s.shutdown(context.Background(),true) }
func (s *Server) shutdown(ctx context.Context,force bool) error {
	if s==nil{return nil};if ctx==nil{ctx=context.Background()}
	s.lifecycleMu.Lock();if s.closed{s.lifecycleMu.Unlock();return nil};if s.closing{done:=s.done;s.lifecycleMu.Unlock();select{case<-done:return nil;case<-ctx.Done():return ctx.Err()}};s.closing=true;srv:=s.serving;s.lifecycleMu.Unlock()
	s.cancel()
	var first error
	if srv!=nil { if force { first=srv.Close() } else { first=srv.Shutdown(ctx) };if errors.Is(first,http.ErrServerClosed){first=nil} }
	s.clientMu.Lock();clients:=make([]*client,0,len(s.clients));for c:=range s.clients{clients=append(clients,c)};s.clientMu.Unlock();for _,c:=range clients{c.disconnectAll()}
	s.namespaceMu.RLock();namespaces:=make([]*Namespace,0,len(s.namespaces));for _,n:=range s.namespaces{namespaces=append(namespaces,n)};s.namespaceMu.RUnlock();for _,n:=range namespaces{if err:=n.close();first==nil&&err!=nil{first=err}}
	if s.engine!=nil{s.engine.Close()}
	s.lifecycleMu.Lock();s.serving=nil;s.closed=true;s.closing=false;s.lifecycleMu.Unlock();s.doneOnce.Do(func(){close(s.done)});return first
}
func (s *Server) Done() <-chan struct{} { if s==nil{ch:=make(chan struct{});close(ch);return ch};return s.done }
func (s *Server) dropClient(c *client){if s==nil||c==nil{return};s.clientMu.Lock();delete(s.clients,c);s.clientMu.Unlock()}

func (s *Server) On(event string,fn Listener) Subscription { if s==nil{return closedSubscription{}};return s.hub.On(event,fn) }
func (s *Server) Once(event string,fn Listener) Subscription { if s==nil{return closedSubscription{}};return s.hub.Once(event,fn) }
func (s *Server) RemoveAllListeners(event string){if s!=nil{s.hub.RemoveAll(event)}}
func (s *Server) OnConnection(fn func(*Socket)) Subscription { if fn==nil{return closedSubscription{}};return s.On("connection",func(_ context.Context,args ...any)error{if len(args)>0{if socket,ok:=args[0].(*Socket);ok{fn(socket)}};return nil}) }
func (s *Server) Use(middleware ...Middleware){if s!=nil{s.Of("/").Use(middleware...)}}
func (s *Server) Emit(event string,args ...any) error { n:=s.Of("/");if n==nil{return ErrClosed};return n.Emit(event,args...) }
func (s *Server) To(rooms ...Room)*BroadcastOperator{return s.Of("/").To(rooms...)}
func (s *Server) In(rooms ...Room)*BroadcastOperator{return s.To(rooms...)}
func (s *Server) Except(rooms ...Room)*BroadcastOperator{return s.Of("/").Except(rooms...)}
func (s *Server) Volatile()*BroadcastOperator{return s.Of("/").Volatile()}
func (s *Server) Local()*BroadcastOperator{return s.Of("/").Local()}
func (s *Server) Compress(enabled bool)*BroadcastOperator{return s.Of("/").Compress(enabled)}
func (s *Server) Timeout(timeout time.Duration)*BroadcastOperator{return s.Of("/").Timeout(timeout)}

func (s *Server) Of(name string)*Namespace {
	if s==nil{return nil};name=normalizeNamespace(name)
	s.namespaceMu.RLock();n:=s.namespaces[name];s.namespaceMu.RUnlock();if n!=nil{return n}
	s.namespaceMu.Lock();defer s.namespaceMu.Unlock();if n=s.namespaces[name];n!=nil{return n};created,err:=newNamespace(s,name);if err!=nil{s.cfg.Logger.Error("socket.io: create namespace","namespace",name,"error",err);return nil};s.namespaces[name]=created;s.hub.dispatch("new_namespace",[]any{created});return created
}
func (s *Server) Namespace(name string)(*Namespace,bool){if s==nil{return nil,false};name=normalizeNamespace(name);s.namespaceMu.RLock();n,ok:=s.namespaces[name];s.namespaceMu.RUnlock();return n,ok}
func (s *Server) Namespaces()[]*Namespace{if s==nil{return nil};s.namespaceMu.RLock();out:=make([]*Namespace,0,len(s.namespaces));for _,n:=range s.namespaces{out=append(out,n)};s.namespaceMu.RUnlock();return out}
func (s *Server) OfMatch(matcher NamespaceMatcher)*ParentNamespace{if s==nil||matcher==nil{return nil};p:=&ParentNamespace{server:s,matcher:matcher,children:make(map[string]*Namespace)};s.namespaceMu.Lock();s.matchers=append(s.matchers,namespaceMatcherEntry{matcher:matcher,parent:p});s.namespaceMu.Unlock();return p}
func (s *Server) resolveNamespace(name string,auth map[string]any)(*Namespace,error){name=normalizeNamespace(name);if n,ok:=s.Namespace(name);ok{return n,nil};s.namespaceMu.RLock();entries:=append([]namespaceMatcherEntry(nil),s.matchers...);s.namespaceMu.RUnlock();for _,entry:=range entries{allowed,err:=entry.matcher(s.Context(),name,auth);if err!=nil{return nil,err};if allowed{n:=s.Of(name);if n!=nil{entry.parent.addChild(n)};return n,nil}};return nil,fmt.Errorf("invalid namespace %s",name)}
func (s *Server) DecodeValue(src any,dst any)error{if s==nil||s.cfg.ValueCodec==nil{return ErrUnsupported};return s.cfg.ValueCodec.Decode(src,dst)}
func normalizeNamespace(name string)string{name=strings.TrimSpace(name);if name==""{return "/"};if !strings.HasPrefix(name,"/"){return "/"+name};return name}
