package socketio

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Namespace struct {
	server *Server
	name string
	hub *eventHub
	adapter Adapter

	mu sync.RWMutex
	sockets map[SocketID]*Socket
	preConnect map[SocketID]*Socket
	middlewares []Middleware
	ids atomic.Uint64
	closed bool
}

func newNamespace(server *Server,name string)(*Namespace,error){
	n:=&Namespace{server:server,name:normalizeNamespace(name),sockets:make(map[SocketID]*Socket),preConnect:make(map[SocketID]*Socket)}
	n.hub=newEventHub(nil,server.Context,nil,server.cfg.Logger)
	adapter,err:=server.adapterFactory.New(n);if err!=nil{return nil,err};n.adapter=adapter
	if err:=adapter.Init(server.Context());err!=nil{return nil,err}
	return n,nil
}
func (n *Namespace) Name()string{if n==nil{return ""};return n.name}
func (n *Namespace) On(event string,listener Listener)Subscription{if n==nil{return closedSubscription{}};return n.hub.On(event,listener)}
func (n *Namespace) Once(event string,listener Listener)Subscription{if n==nil{return closedSubscription{}};return n.hub.Once(event,listener)}
func (n *Namespace) RemoveAllListeners(event string){if n!=nil{n.hub.RemoveAll(event)}}
func (n *Namespace) OnConnection(fn func(*Socket))Subscription{if fn==nil{return closedSubscription{}};return n.On("connection",func(_ context.Context,args ...any)error{if len(args)>0{if s,ok:=args[0].(*Socket);ok{fn(s)}};return nil})}
func (n *Namespace) Use(middleware ...Middleware){if n==nil{return};n.mu.Lock();for _,mw:=range middleware{if mw!=nil{n.middlewares=append(n.middlewares,mw)}};n.mu.Unlock()}

func (n *Namespace) connect(c *client,auth map[string]any)(*Socket,error){
	if n==nil||c==nil{return nil,ErrClosed}
	socket,err:=newSocket(n,c,auth);if err!=nil{return nil,err}
	n.mu.Lock();if n.closed{n.mu.Unlock();socket.close("namespace closed");return nil,ErrClosed};n.preConnect[socket.ID()]=socket;middlewares:=append([]Middleware(nil),n.middlewares...);n.mu.Unlock()

	ctx:=socket.Context();var cancel context.CancelFunc
	if timeout:=n.server.cfg.ConnectTimeout;timeout>0{ctx,cancel=context.WithTimeout(ctx,timeout);defer cancel()}
	for _,mw:=range middlewares{if err:=mw(ctx,socket);err!=nil{n.removePreConnect(socket.ID());socket.close("middleware rejection");return nil,err};if err:=ctx.Err();err!=nil{n.removePreConnect(socket.ID());socket.close("connect timeout");return nil,err}}

	n.mu.Lock();delete(n.preConnect,socket.ID());if n.closed{n.mu.Unlock();socket.close("namespace closed");return nil,ErrClosed};n.sockets[socket.ID()]=socket;n.mu.Unlock()
	if err:=n.adapter.AddAll(socket.Context(),socket.ID(),Room(socket.ID()));err!=nil{n.remove(socket);socket.close("adapter error");return nil,err}
	socket.markConnected()
	connectData:=map[string]any{"sid":socket.ID()};if socket.pid!=""{connectData["pid"]=socket.pid}
	if err:=c.writePacket(Packet{Type:PacketConnect,Namespace:n.name,Data:connectData},BroadcastFlags{});err!=nil{socket.close("transport error");return nil,err}
	n.hub.dispatch("connect",[]any{socket});n.hub.dispatch("connection",[]any{socket})
	if n.name=="/"{n.server.hub.dispatch("connect",[]any{socket});n.server.hub.dispatch("connection",[]any{socket})}
	return socket,nil
}
func (n *Namespace) removePreConnect(id SocketID){n.mu.Lock();delete(n.preConnect,id);n.mu.Unlock()}
func (n *Namespace) remove(socket *Socket){if n==nil||socket==nil{return};n.mu.Lock();delete(n.preConnect,socket.ID());delete(n.sockets,socket.ID());n.mu.Unlock();_ = n.adapter.DeleteAll(context.Background(),socket.ID())}
func (n *Namespace) Socket(id SocketID)(*Socket,bool){if n==nil{return nil,false};n.mu.RLock();s,ok:=n.sockets[id];n.mu.RUnlock();return s,ok}
func (n *Namespace) socketSnapshot()[]*Socket{n.mu.RLock();out:=make([]*Socket,0,len(n.sockets));for _,s:=range n.sockets{out=append(out,s)};n.mu.RUnlock();return out}
func (n *Namespace) close()error{if n==nil{return nil};n.mu.Lock();if n.closed{n.mu.Unlock();return nil};n.closed=true;sockets:=make([]*Socket,0,len(n.sockets)+len(n.preConnect));for _,s:=range n.sockets{sockets=append(sockets,s)};for _,s:=range n.preConnect{sockets=append(sockets,s)};clear(n.sockets);clear(n.preConnect);n.mu.Unlock();for _,s:=range sockets{s.close("server shutting down")};return n.adapter.Close()}

func (n *Namespace) nextID()uint64{return n.ids.Add(1)-1}
func (n *Namespace) Emit(event string,args ...any)error{return n.To().Emit(event,args...)}
func (n *Namespace) To(rooms ...Room)*BroadcastOperator{return newBroadcastOperator(n,BroadcastOptions{Rooms:append([]Room(nil),rooms...)})}
func (n *Namespace) In(rooms ...Room)*BroadcastOperator{return n.To(rooms...)}
func (n *Namespace) Except(rooms ...Room)*BroadcastOperator{return newBroadcastOperator(n,BroadcastOptions{Except:append([]Room(nil),rooms...)})}
func (n *Namespace) Local()*BroadcastOperator{o:=n.To();return o.Local()}
func (n *Namespace) Volatile()*BroadcastOperator{o:=n.To();return o.Volatile()}
func (n *Namespace) Compress(enabled bool)*BroadcastOperator{o:=n.To();return o.Compress(enabled)}
func (n *Namespace) Timeout(timeout time.Duration)*BroadcastOperator{o:=n.To();return o.Timeout(timeout)}
func (n *Namespace) FetchSockets(ctx context.Context)([]*RemoteSocket,error){return n.To().FetchSockets(ctx)}
func (n *Namespace) CountSockets(ctx context.Context)(uint64,error){return n.adapter.CountSockets(ctx,BroadcastOptions{})}
func (n *Namespace) ListRooms(ctx context.Context)(map[Room]uint64,error){return n.adapter.ListRooms(ctx,BroadcastOptions{})}
func (n *Namespace) SocketsJoin(ctx context.Context,rooms ...Room)error{return n.adapter.AddSockets(ctx,BroadcastOptions{},rooms...)}
func (n *Namespace) SocketsLeave(ctx context.Context,rooms ...Room)error{return n.adapter.DeleteSockets(ctx,BroadcastOptions{},rooms...)}
func (n *Namespace) DisconnectSockets(ctx context.Context,closeTransport bool)error{return n.adapter.DisconnectSockets(ctx,BroadcastOptions{},closeTransport)}
func (n *Namespace) ServerSideEmit(ctx context.Context,event string,args ...any)error{return n.adapter.ServerSideEmit(ctx,append([]any{event},args...))}
func (n *Namespace) ServerSideEmitAck(ctx context.Context,event string,args ...any)([]any,error){if err:=n.ServerSideEmit(ctx,event,args...);err!=nil{return nil,err};return nil,nil}
func (n *Namespace) EmitAcks(ctx context.Context,event string,args ...any)([][]any,error){return n.To().EmitAcks(ctx,event,args...)}
func (n *Namespace) DecodeValue(src any,dst any)error{return n.server.DecodeValue(src,dst)}

type ParentNamespace struct{server *Server;matcher NamespaceMatcher;mu sync.RWMutex;children map[string]*Namespace}
func (p *ParentNamespace)addChild(n *Namespace){if p==nil||n==nil{return};p.mu.Lock();p.children[n.Name()]=n;p.mu.Unlock()}
func (p *ParentNamespace)Children()[]*Namespace{if p==nil{return nil};p.mu.RLock();out:=make([]*Namespace,0,len(p.children));for _,n:=range p.children{out=append(out,n)};p.mu.RUnlock();return out}
func contextError(ctx context.Context)error{if ctx==nil{return nil};return ctx.Err()}
