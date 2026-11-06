package redis

import "github.com/aireuropa/resec/resec/state"

const (
	StartCommand       = CommandName("start")
	StopCommand        = CommandName("stop")
	RunAsSlaveCommand  = CommandName("run_as_slave")
	RunAsMasterCommand = CommandName("run_as_master")
)

type CommandName string

type Command struct {
	name        CommandName
	consulState state.Consul
}

func (c *Command) Name() CommandName {
	return c.name
}

func (c *Command) String() string {
	return string(c.name)
}

// ConsulState returns the Consul state sent with the command
func (c *Command) ConsulState() state.Consul {
	return c.consulState
}

func NewCommand(cmd CommandName, consulState state.Consul) Command {
	return Command{
		name:        cmd,
		consulState: consulState,
	}
}
