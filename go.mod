module github.com/madmike/go-pipeline

go 1.25.5

require (
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/madmike/go-ai-providers v0.0.2
	github.com/madmike/go-infra v0.0.1
	github.com/madmike/go-storage v0.0.2
	pgregory.net/rapid v1.1.0
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.35.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/madmike/go-infra => ../infra

replace github.com/madmike/go-ai-providers => ../providers

replace github.com/madmike/go-storage => ../storage
