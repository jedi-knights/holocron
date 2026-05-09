// Package sdk is the public Go client for the holocron broker. Through
// stage 2 it talks to an in-process broker; stage 3 introduces a real
// network transport behind the same Producer/Consumer surface.
//
// SDK code may import only github.com/jedi-knights/holocron/proto. It must
// never reach into broker internals.
package sdk
