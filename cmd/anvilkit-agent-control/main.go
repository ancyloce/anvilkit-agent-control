// anvilkit-agent-control is the Control service: sole writer of
// anvilkit_control, authority for intake, tracked commands, attempts,
// launch inventory and accepted results (architecture.md service catalog).
package main

import (
	"go.uber.org/fx"

	"github.com/ancyloce/anvilkit-agent-control/internal/bootstrap"
)

func main() {
	fx.New(bootstrap.Module()).Run()
}
