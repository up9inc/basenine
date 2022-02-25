module main

go 1.16

require (
	github.com/fsnotify/fsnotify v1.5.1
	github.com/ohler55/ojg v1.12.13
	github.com/stretchr/testify v1.7.0
	github.com/up9inc/basenine/server/lib v0.0.0
)

replace github.com/up9inc/basenine/server/lib => ./lib
