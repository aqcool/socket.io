package socketio

import (
	"context"
	"errors"
	"time"
)

type BroadcastOperator struct {
	nsp *Namespace
	opts BroadcastOptions
}

func newBroadcastOperator(nsp *Namespace,opts BroadcastOptions)*BroadcastOperator{return &BroadcastOperator{nsp:nsp,opts:cloneBroadcastOptions(opts)}}
func cloneBroadcastOptions(opts BroadcastOptions)BroadcastOptions{out:=opts;out.Rooms=append([]Room(nil),opts.Rooms...);out.Except=append([]Room(nil),opts.Except...);if opts.Flags.Compress!=nil{value:=*opts.Flags.Compress;out.Flags.Compress=&value};if opts.Flags.Timeout!=nil{value:=*opts.Flags.Timeout;out.Flags.Timeout=&value};return out}
func (o *BroadcastOperator) clone()*BroadcastOperator{if o==nil{return nil};return newBroadcastOperator(o.nsp,o.opts)}
func appendUniqueRooms(base []Room,rooms ...Room)[]Room{seen:=make(map[Room]struct{},len(base)+len(rooms));out:=make([]Room,0,len(base)+len(rooms));for _,r:=range append(append([]Room(nil),base...),rooms...){if r==""{continue};if _,ok:=seen[r];ok{continue};seen[r]=struct{}{};out=append(out,r)};return out}
func (o *BroadcastOperator) To(rooms ...Room)*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Rooms=appendUniqueRooms(c.opts.Rooms,rooms...)};return c}
func (o *BroadcastOperator) In(rooms ...Room)*BroadcastOperator{return o.To(rooms...)}
func (o *BroadcastOperator) Except(rooms ...Room)*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Except=appendUniqueRooms(c.opts.Except,rooms...)};return c}
func (o *BroadcastOperator) Local()*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Flags.Local=true};return c}
func (o *BroadcastOperator) Volatile()*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Flags.Volatile=true};return c}
func (o *BroadcastOperator) Compress(enabled bool)*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Flags.Compress=&enabled};return c}
func (o *BroadcastOperator) Timeout(timeout time.Duration)*BroadcastOperator{c:=o.clone();if c!=nil{c.opts.Flags.Timeout=&timeout};return c}
func (o *BroadcastOperator) Emit(event string,args ...any)error{
	if o==nil||o.nsp==nil{return ErrClosed};if reservedEvent(event){return errors.New("socket.io: reserved event name: "+event)}
	if len(args)>0{if ack,ok:=args[len(args)-1].(Ack);ok{responses,err:=o.EmitAcks(context.Background(),event,args[:len(args)-1]...);if err!=nil{ack(nil,err);return err};flat:=make([]any,0,len(responses));for _,values:=range responses{if len(values)==1{flat=append(flat,values[0])}else{flat=append(flat,values)}};ack(flat,nil);return nil}}
	return o.nsp.adapter.Broadcast(context.Background(),Packet{Type:PacketEvent,Namespace:o.nsp.Name(),Data:append([]any{event},args...)},o.opts)
}
func (o *BroadcastOperator) EmitAcks(ctx context.Context,event string,args ...any)([][]any,error){
	if o==nil||o.nsp==nil{return nil,ErrClosed};if ctx==nil{ctx=context.Background()};if deadline,ok:=ctx.Deadline();ok{d:=time.Until(deadline);if d<=0{return nil,context.DeadlineExceeded};if o.opts.Flags.Timeout==nil||d<*o.opts.Flags.Timeout{o=o.Timeout(d)}}
	packet:=Packet{Type:PacketEvent,Namespace:o.nsp.Name(),Data:append([]any{event},args...)}
	var muResponses = make(chan struct{},1);muResponses<-struct{}{};responses:=make([][]any,0);var firstErr error
	err:=o.nsp.adapter.BroadcastWithAck(ctx,packet,o.opts,nil,func(values []any,err error){<-muResponses;if err!=nil&&firstErr==nil{firstErr=err};if err==nil{responses=append(responses,append([]any(nil),values...))};muResponses<-struct{}{}});if err!=nil{return nil,err};<-muResponses;muResponses<-struct{}{};return responses,firstErr
}
func (o *BroadcastOperator) FetchSockets(ctx context.Context)([]*RemoteSocket,error){if o==nil||o.nsp==nil{return nil,ErrClosed};details,err:=o.nsp.adapter.FetchSockets(ctx,o.opts);if err!=nil{return nil,err};out:=make([]*RemoteSocket,0,len(details));for _,d:=range details{out=append(out,newRemoteSocket(o.nsp,d))};return out,nil}
func (o *BroadcastOperator) CountSockets(ctx context.Context)(uint64,error){if o==nil||o.nsp==nil{return 0,ErrClosed};return o.nsp.adapter.CountSockets(ctx,o.opts)}
func (o *BroadcastOperator) ListRooms(ctx context.Context)(map[Room]uint64,error){if o==nil||o.nsp==nil{return nil,ErrClosed};return o.nsp.adapter.ListRooms(ctx,o.opts)}
func (o *BroadcastOperator) SocketsJoin(ctx context.Context,rooms ...Room)error{if o==nil||o.nsp==nil{return ErrClosed};return o.nsp.adapter.AddSockets(ctx,o.opts,rooms...)}
func (o *BroadcastOperator) SocketsLeave(ctx context.Context,rooms ...Room)error{if o==nil||o.nsp==nil{return ErrClosed};return o.nsp.adapter.DeleteSockets(ctx,o.opts,rooms...)}
func (o *BroadcastOperator) DisconnectSockets(ctx context.Context,closeTransport bool)error{if o==nil||o.nsp==nil{return ErrClosed};return o.nsp.adapter.DisconnectSockets(ctx,o.opts,closeTransport)}
func (o *BroadcastOperator) DecodeValue(src any,dst any)error{if o==nil||o.nsp==nil{return ErrUnsupported};return o.nsp.DecodeValue(src,dst)}

type RemoteSocket struct{nsp *Namespace;details SocketDetails;operator *BroadcastOperator}
func newRemoteSocket(nsp *Namespace,details SocketDetails)*RemoteSocket{return &RemoteSocket{nsp:nsp,details:details,operator:newBroadcastOperator(nsp,BroadcastOptions{Rooms:[]Room{Room(details.ID)},Flags:BroadcastFlags{ExpectSingleResponse:true}})}}
func (s *RemoteSocket) ID()SocketID{if s==nil{return ""};return s.details.ID}
func (s *RemoteSocket) Handshake()Handshake{if s==nil{return Handshake{}};return s.details.Handshake}
func (s *RemoteSocket) Rooms()[]Room{if s==nil{return nil};return append([]Room(nil),s.details.Rooms...)}
func (s *RemoteSocket) Data()any{if s==nil{return nil};return s.details.Data}
func (s *RemoteSocket) Join(ctx context.Context,rooms ...Room)error{if s==nil{return ErrClosed};return s.operator.SocketsJoin(ctx,rooms...)}
func (s *RemoteSocket) Leave(ctx context.Context,rooms ...Room)error{if s==nil{return ErrClosed};return s.operator.SocketsLeave(ctx,rooms...)}
func (s *RemoteSocket) Disconnect(ctx context.Context,closeTransport bool)error{if s==nil{return ErrClosed};return s.operator.DisconnectSockets(ctx,closeTransport)}
func (s *RemoteSocket) Emit(ctx context.Context,event string,args ...any)error{if err:=contextError(ctx);err!=nil{return err};if s==nil{return ErrClosed};return s.operator.Emit(event,args...)}
func (s *RemoteSocket) EmitAck(ctx context.Context,event string,args ...any)([]any,error){if s==nil{return nil,ErrClosed};responses,err:=s.operator.EmitAcks(ctx,event,args...);if err!=nil{return nil,err};if len(responses)==0{return nil,nil};return responses[0],nil}
func (s *RemoteSocket) DecodeValue(src any,dst any)error{if s==nil||s.nsp==nil{return ErrUnsupported};return s.nsp.DecodeValue(src,dst)}
