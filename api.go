package mapper

import (
	"fmt"

	"encoding/json"
	"net/http"
)

type (
	apiHandler func(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
)

// eventually, we want to embed and serve a nice polymer UI for the mapper
// this just helps show that the mapper is hosted on the endpoint in the app
func init() {
	server.HandleFunc("/", index)
}

func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "mapper %s", r.URL.Path)
}

// REST handler mapping code inspired by https://github.com/dougblack/sleepy

// GetSupported is the interface that provides the Get
// method a resource must support to receive HTTP GETs.
type GetSupported interface {
	Get(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

// PostSupported is the interface that provides the Post
// method a resource must support to receive HTTP POSTs.
type PostSupported interface {
	Post(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

// PutSupported is the interface that provides the Put
// method a resource must support to receive HTTP PUTs.
type PutSupported interface {
	Put(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

// DeleteSupported is the interface that provides the Delete
// method a resource must support to receive HTTP DELETEs.
type DeleteSupported interface {
	Delete(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

// HeadSupported is the interface that provides the Head
// method a resource must support to receive HTTP HEADs.
type HeadSupported interface {
	Head(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

// PatchSupported is the interface that provides the Patch
// method a resource must support to receive HTTP PATCHs.
type PatchSupported interface {
	Patch(http.ResponseWriter, *http.Request, string) (int, interface{}, error)
}

func (m *mapper) requestHandler(path string, resource interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		id := r.URL.Path[len(path):]
		var handler apiHandler

		switch r.Method {
		case "GET":
			if resource, ok := resource.(GetSupported); ok {
				handler = resource.Get
			}
		case "POST":
			if resource, ok := resource.(PostSupported); ok {
				handler = resource.Post
			}
		case "PUT":
			if resource, ok := resource.(PutSupported); ok {
				handler = resource.Put
			}
		case "DELETE":
			if resource, ok := resource.(DeleteSupported); ok {
				handler = resource.Delete
			}
		case "HEAD":
			if resource, ok := resource.(HeadSupported); ok {
				handler = resource.Head
			}
		case "PATCH":
			if resource, ok := resource.(PatchSupported); ok {
				handler = resource.Patch
			}
		}

		if handler == nil {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		code, data, err := handler(w, r, id)
		if err != nil {
			w.WriteHeader(code)
			w.Write([]byte(err.Error()))
			return
		}

		content, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.Header().Add("Content-type", "application/json")
		w.WriteHeader(code)
		w.Write(content)
	}
}

// AddResource adds a new resource to an API. The API will route
// requests that match one of the given paths to the matching HTTP
// method on the resource.
func (m *mapper) addResource(path string, resource interface{}) {
	m.HandleFunc(path, m.requestHandler(path, resource))
}
