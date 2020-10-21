package dads

import (
	"bytes"
	"os"
	"os/exec"
)

// ExecCommand - execute command given by array of strings with eventual environment map
func ExecCommand(ctx *Ctx, cmdAndArgs []string, cwd string, env map[string]string) (sout, serr string, err error) {
	command := cmdAndArgs[0]
	arguments := cmdAndArgs[1:]
	if ctx.Debug > 1 {
		Printf("executing command %s:%v:%+v\n", cwd, env, cmdAndArgs)
	}
	cmd := exec.Command(command, arguments...)
	if len(env) > 0 {
		newEnv := os.Environ()
		for key, value := range env {
			newEnv = append(newEnv, key+"="+value)
		}
		cmd.Env = newEnv
	}
	var (
		stdOut bytes.Buffer
		stdErr bytes.Buffer
	)
	cmd.Stdout = &stdOut
	cmd.Stderr = &stdErr
	if cwd != "" {
		cmd.Dir = cwd
	}
	err = cmd.Start()
	if err != nil {
		return
	}
	err = cmd.Wait()
	sout = stdOut.String()
	serr = stdErr.String()
	if ctx.Debug > 1 {
		Printf("executed command %s:%v:%+v -> (%v,%s,%s)\n", cwd, env, cmdAndArgs, err, sout, serr)
	}
	return
}
