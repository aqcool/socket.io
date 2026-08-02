package auth

import (
	"sync"

	socket "github.com/aqcool/socket.io/servers/socket/v3"
)

type UserIDResolver func(any) (string, bool)

// DisconnectUser disconnects every local or remote Socket whose data resolves
// to userID. The callback receives the number of matched namespace sockets.
func DisconnectUser(
	server *socket.Server,
	userID string,
	resolve UserIDResolver,
	closeConnection bool,
) func(func(int, error)) {
	return func(done func(int, error)) {
		if done == nil {
			done = func(int, error) {}
		}
		if server == nil || resolve == nil {
			done(0, ErrInvalidOptions)
			return
		}
		namespaces := server.Namespaces()
		var wait sync.WaitGroup
		var mu sync.Mutex
		count := 0
		var firstError error
		for _, nsp := range namespaces {
			wait.Add(1)
			nsp.FetchSockets()(func(sockets []*socket.RemoteSocket, err error) {
				defer wait.Done()
				if err != nil {
					mu.Lock()
					if firstError == nil {
						firstError = err
					}
					mu.Unlock()
					return
				}
				matched := 0
				for _, remote := range sockets {
					if currentUserID, ok := resolve(remote.Data()); ok && currentUserID == userID {
						remote.Disconnect(closeConnection)
						matched++
					}
				}
				mu.Lock()
				count += matched
				mu.Unlock()
			})
		}
		go func() {
			wait.Wait()
			mu.Lock()
			defer mu.Unlock()
			done(count, firstError)
		}()
	}
}

// MapUserID reads a user identifier from map-shaped Socket data.
func MapUserID(field string) UserIDResolver {
	return func(data any) (string, bool) {
		values, ok := data.(map[string]any)
		if !ok {
			return "", false
		}
		userID, ok := values[field].(string)
		return userID, ok && userID != ""
	}
}
