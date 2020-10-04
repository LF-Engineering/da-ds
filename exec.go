package dads

import (
	"bytes"
	"os/exec"
)

// ExecCommand - execute command given by array of strings with eventual environment map
func ExecCommand(ctx *Ctx, cmdAndArgs []string) (s string, err error) {
	command := cmdAndArgs[0]
	arguments := cmdAndArgs[1:]
	if ctx.Debug > 1 {
		Printf("executing command %+v\n", cmdAndArgs)
	}
	cmd := exec.Command(command, arguments...)
	var stdOut bytes.Buffer
	cmd.Stdout = &stdOut
	err = cmd.Start()
	if err != nil {
		return
	}
	err = cmd.Wait()
	if err != nil {
		return
	}
	s = stdOut.String()
	if ctx.Debug > 1 {
		Printf("executed command %+v -> %s\n", cmdAndArgs, s)
	}
	return
}
